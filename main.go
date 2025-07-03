package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/godror/godror"
)

type Task struct {
	SolID     string
	Procedure string
}

type Progress struct {
	total        int
	current      int
	overallStart time.Time
}

type ProcLog struct {
	SolID         string
	Procedure     string
	StartTime     time.Time
	EndTime       time.Time
	ExecutionTime time.Duration
	Status        string
	ErrorDetails  string
}

type ProcSummary struct {
	Procedure string
	StartTime time.Time
	EndTime   time.Time
	Status    string
}

var mode string

func worker(ctx context.Context, id int, db *sql.DB, tasks <-chan Task, runCfg *ExtractionConfig, templates map[string][]ColumnConfig, logCh chan<- ProcLog, summaryMu *sync.Mutex, procSummary map[string]ProcSummary, progress *Progress, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Worker %d started", id)
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d exiting", id)
			return
		case task, ok := <-tasks:
			if !ok {
				log.Printf("Worker %d: task channel closed", id)
				return
			}
			start := time.Now()
			var err error
			if mode == "E" {
				log.Printf("📥 Extracting %s for SOL %s", task.Procedure, task.SolID)
				err = extractData(ctx, db, task.Procedure, task.SolID, runCfg, templates)
			} else if mode == "I" {
				log.Printf("🔁 Inserting: %s.%s for SOL %s", runCfg.PackageName, task.Procedure, task.SolID)
				err = callProcedure(ctx, db, runCfg.PackageName, task.Procedure, task.SolID)
			}
			end := time.Now()
			plog := ProcLog{
				SolID:         task.SolID,
				Procedure:     task.Procedure,
				StartTime:     start,
				EndTime:       end,
				ExecutionTime: end.Sub(start),
			}
			if err != nil {
				plog.Status = "FAIL"
				plog.ErrorDetails = err.Error()
			} else {
				plog.Status = "SUCCESS"
			}
			logCh <- plog
			summaryMu.Lock()
			s, exists := procSummary[task.Procedure]
			if !exists {
				s = ProcSummary{Procedure: task.Procedure, StartTime: start, EndTime: end, Status: plog.Status}
			} else {
				if start.Before(s.StartTime) {
					s.StartTime = start
				}
				if end.After(s.EndTime) {
					s.EndTime = end
				}
				if s.Status != "FAIL" && plog.Status == "FAIL" {
					s.Status = "FAIL"
				}
			}
			procSummary[task.Procedure] = s
			summaryMu.Unlock()
			progress.Increment()
		}
	}
}

func main() {
	// Parse flags and load config
	appCfgFile := flag.String("appCfg", "", "Path to the main application configuration file")
	runCfgFile := flag.String("runCfg", "", "Path to the extraction configuration file")
	flag.StringVar(&mode, "mode", "", "Mode of operation: E - Extract, I - Insert")
	flag.Parse()
	if mode != "E" && mode != "I" {
		log.Fatal("Invalid mode. Valid values are 'E' for Extract and 'I' for Insert.")
	}
	if *appCfgFile == "" || *runCfgFile == "" {
		log.Fatal("Both appCfg and runCfg must be specified")
	}
	if _, err := os.Stat(*appCfgFile); os.IsNotExist(err) {
		log.Fatalf("Application configuration file does not exist: %s", *appCfgFile)
	}
	if _, err := os.Stat(*runCfgFile); os.IsNotExist(err) {
		log.Fatalf("Extraction configuration file does not exist: %s", *runCfgFile)
	}
	appCfg, err := loadMainConfig(*appCfgFile)
	if err != nil {
		log.Fatalf("Failed to load main config: %v", err)
	}
	runCfg, err := loadExtractionConfig(*runCfgFile)
	if err != nil {
		log.Fatalf("Failed to load extraction config: %v", err)
	}

	// Load templates
	templates := make(map[string][]ColumnConfig)
	for _, proc := range runCfg.Procedures {
		tmplPath := filepath.Join(runCfg.TemplatePath, fmt.Sprintf("%s.csv", proc))
		cols, err := readColumnsFromCSV(tmplPath)
		if err != nil {
			log.Fatalf("Failed to read template for %s: %v", proc, err)
		}
		templates[proc] = cols
	}

	connString := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`,
		appCfg.DBUser, appCfg.DBPassword, appCfg.DBHost, appCfg.DBPort, appCfg.DBSid)
	db, err := sql.Open("godror", connString)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(appCfg.Concurrency)
	db.SetMaxIdleConns(appCfg.Concurrency)
	db.SetConnMaxLifetime(30 * time.Minute)

	sols, err := readSols(appCfg.SolFilePath)
	if err != nil {
		log.Fatalf("Failed to read SOL IDs: %v", err)
	}

	// Create a flat list of tasks
	tasksList := []Task{}
	for _, sol := range sols {
		for _, proc := range runCfg.Procedures {
			tasksList = append(tasksList, Task{SolID: sol, Procedure: proc})
		}
	}
	totalTasks := len(tasksList)
	log.Printf("Found %d SOLs and %d procedures, creating %d tasks.", len(sols), len(runCfg.Procedures), totalTasks)

	procLogCh := make(chan ProcLog, 1000)
	var summaryMu sync.Mutex
	procSummary := make(map[string]ProcSummary)

	var LogFile, LogFileSummary string
	if mode == "I" {
		LogFile = runCfg.PackageName + "_insert.csv"
		LogFileSummary = runCfg.PackageName + "_insert_summary.csv"
	} else if mode == "E" {
		LogFile = runCfg.PackageName + "_extract.csv"
		LogFileSummary = runCfg.PackageName + "_extract_summary.csv"
	}
	go writeLog(filepath.Join(appCfg.LogFilePath, LogFile), procLogCh)

	minWorkers := 2
	maxWorkers := appCfg.Concurrency
	workerCount := minWorkers
	workerCancels := make([]context.CancelFunc, 0, maxWorkers)

	tasks := make(chan Task, totalTasks)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	overallStart := time.Now()
	progress := &Progress{total: totalTasks, overallStart: overallStart}

	// Start initial workers
	for i := 0; i < workerCount; i++ {
		wctx, wcancel := context.WithCancel(ctx)
		wg.Add(1)
		go worker(wctx, i+1, db, tasks, &runCfg, templates, procLogCh, &summaryMu, procSummary, progress, &wg)
		workerCancels = append(workerCancels, wcancel)
	}

	// Manager goroutine for dynamic scaling
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				qlen := len(tasks)
				if qlen > 20 && workerCount < maxWorkers {
					wctx, wcancel := context.WithCancel(ctx)
					wg.Add(1)
					go worker(wctx, workerCount+1, db, tasks, &runCfg, templates, procLogCh, &summaryMu, procSummary, progress, &wg)
					workerCancels = append(workerCancels, wcancel)
					workerCount++
					log.Printf("[Manager] Increased workers to %d", workerCount)
				} else if qlen < 5 && workerCount > minWorkers {
					workerCancels[workerCount-1]()
					workerCancels = workerCancels[:workerCount-1]
					workerCount--
					log.Printf("[Manager] Decreased workers to %d", workerCount)
				}
			}
		}
	}()

	// Feed tasks
	for _, task := range tasksList {
		tasks <- task
	}
	close(tasks)

	wg.Wait()
	close(procLogCh)
	writeSummary(filepath.Join(appCfg.LogFilePath, LogFileSummary), procSummary)
	if mode == "E" {
		mergeFiles(&runCfg)
	}
	log.Printf("🎯 All done! Processed %d tasks in %s", totalTasks, time.Since(overallStart).Round(time.Second))
}
