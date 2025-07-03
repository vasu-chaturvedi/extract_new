// ...existing code from /home/vasuc/workbench/go/extract/ExtractData.go...

func extractData(ctx context.Context, db *sql.DB, procName, solID string, cfg *ExtractionConfig, templates map[string][]ColumnConfig) error {
	cols, ok := templates[procName]
	if !ok {
		return fmt.Errorf("missing template for procedure %s", procName)
	}
	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.Name
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE SOL_ID = :1", strings.Join(colNames, ", "), procName)
	start := time.Now()
	rows, err := db.QueryContext(ctx, query, solID)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()
	log.Printf("🧑‍💻 Query executed for %s (SOL %s) in %s", procName, solID, time.Since(start).Round(time.Millisecond))

	splitCols, doSplit := cfg.SplitRules[procName]
	if doSplit && len(splitCols) > 0 {
		// --- SPLIT LOGIC ---
		// Read all rows, group by splitCols
		rowGroups := make(map[string][][]string) // key: group value, value: rows
		for rows.Next() {
			values := make([]sql.NullString, len(cols))
			scanArgs := make([]interface{}, len(cols))
			for i := range values {
				scanArgs[i] = &values[i]
			}
			if err := rows.Scan(scanArgs...); err != nil {
				return err
			}
			strValues := make([]string, len(values))
			for i, v := range values {
				if v.Valid {
					strValues[i] = v.String
				} else {
					strValues[i] = ""
				}
			}
			// Build group key from splitCols
			var keyParts []string
			for _, splitCol := range splitCols {
				for i, col := range cols {
					if col.Name == splitCol {
						keyParts = append(keyParts, strValues[i])
					}
				}
			}
			groupKey := strings.Join(keyParts, "_")
			rowGroups[groupKey] = append(rowGroups[groupKey], strValues)
		}
		// Write each group to a separate file
		for groupKey, groupRows := range rowGroups {
			fileName := fmt.Sprintf("%s_%s_%s.spool", procName, solID, groupKey)
			spoolPath := filepath.Join(cfg.SpoolOutputPath, fileName)
			f, err := os.Create(spoolPath)
			if err != nil {
				return err
			}
			buf := bufio.NewWriter(f)
			csvWriter := csv.NewWriter(buf)
			csvWriter.Write(colNames)
			for _, row := range groupRows {
				csvWriter.Write(row)
			}
			csvWriter.Flush()
			buf.Flush()
			f.Close()
			log.Printf("✂️ Wrote split file: %s (%d rows)", fileName, len(groupRows))
		}
		return nil
	}
	// --- FULL FILE LOGIC (default) ---
	spoolPath := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s_%s.spool", procName, solID))
	f, err := os.Create(spoolPath)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := bufio.NewWriter(f)
	csvWriter := csv.NewWriter(buf)
	csvWriter.Write(colNames)
	for rows.Next() {
		values := make([]sql.NullString, len(cols))
		scanArgs := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		strValues := make([]string, len(values))
		for i, v := range values {
			if v.Valid {
				strValues[i] = v.String
			} else {
				strValues[i] = ""
			}
		}
		csvWriter.Write(strValues)
	}
	csvWriter.Flush()
	buf.Flush()
	log.Printf("📝 Wrote full file: %s", spoolPath)
	return nil
}
// ...existing code...
