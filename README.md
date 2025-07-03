# Dynamic Worker Pool ETL Utility

This Go utility performs high-performance data extraction and insertion using a dynamic worker pool. It supports both full-file and split-file extraction, with flexible configuration for concurrency and file splitting.

## Features
- **Two Modes:**
  - **Extract (E):** Extracts data from the database and writes to files (full or split by columns).
  - **Insert (I):** Calls stored procedures for each SOL/procedure pair.
- **Dynamic Worker Scaling:**
  - The number of worker goroutines automatically scales up or down based on the task queue length.
- **Configurable Concurrency:**
  - Set the maximum number of concurrent workers via config.
- **Flexible File Output:**
  - Write all data to a single file or split output into multiple files based on one or more columns, per procedure.
- **Robust Logging and Progress Tracking:**
  - Logs all operations, errors, and progress.

## Configuration

### Main Config (`main_config.json`)
Contains database connection and global settings:
```json
{
  "db_user": "<db_user>",
  "db_password": "<db_password>",
  "db_host": "<db_host>",
  "db_port": 1521,
  "db_sid": "<db_sid>",
  "concurrency": 8,
  "log_path": "./logs",
  "sol_list_path": "./sol_list.txt"
}
```

### Extraction/Insertion Config (`job_config.json` or similar)
Specifies extraction/insertion details and split rules:
```json
{
  "package_name": "<package_name>",
  "procedures": ["PROC1", "PROC2", "PROC3"],
  "spool_output_path": "./output",
  "run_insertion_parallel": true,
  "run_extraction_parallel": true,
  "template_path": "./config/templates",
  "format": "csv",
  "delimiter": ",",
  "split_rules": {
    "PROC1": ["BRANCH_ID"],
    "PROC2": ["REGION", "DATE"]
  }
}
```
- Procedures not listed in `split_rules` will be written as a single file.
- For procedures in `split_rules`, specify the column(s) to split by as an array of strings.

## Usage

### Build
```sh
make build
```

### Run (Extract Mode)
```sh
./extract_new -appCfg=./main_config.json -runCfg=./job_config.json -mode="E"
```

### Run (Insert Mode)
```sh
./extract_new -appCfg=./main_config.json -runCfg=./job_config.json -mode="I"
```

## How Dynamic Worker Scaling Works
- The utility starts with a minimum number of workers.
- A manager goroutine monitors the task queue:
  - If the queue grows, more workers are added (up to the configured maximum).
  - If the queue shrinks, workers are removed (down to the minimum).
- This ensures efficient resource usage and high throughput.

## File Splitting Logic
- For procedures listed in `split_rules`, output files are split by the specified columns.
- Each unique combination of split column values results in a separate file.
- Example output file name: `PROC1_SOL123_BRANCH42.spool` (for split by `BRANCH_ID`).
- Procedures not in `split_rules` are written as a single file per SOL/procedure.

## Logging and Output
- Logs are written to the path specified in `log_path`.
- Extraction/insertion logs and summaries are written as CSV files.
- Output data files are written to `spool_output_path`.

## Requirements
- Go 1.18+
- Oracle database (with godror driver)

## License
MIT
