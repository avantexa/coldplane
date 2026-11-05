package query

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/avantexa/coldplane/internal/archive"

	_ "github.com/marcboeker/go-duckdb"
)

// QueryOptions defines parameters for running a query against a local backup.
type QueryOptions struct {
	BackupID string // Required: the backup to query
	Table    string // Optional: default table to query
	SQL      string // Optional: SQL query text
	Output   string // Output format: "table" (default), "json", or "csv"
}

// printQueryResults prints the query output in table, csv, or json format.
func printQueryResults(results []map[string]interface{}, format string) error {
	if len(results) == 0 {
		fmt.Println("No results found.")
		return nil
	}

	// Determine columns (use the first row); sort for consistent ordering across runs
	cols := make([]string, 0, len(results[0]))
	for k := range results[0] {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	out := os.Stdout

	switch strings.ToLower(format) {
	case "json":
		enc := json.NewEncoder(out)
		for _, row := range results {
			if err := enc.Encode(row); err != nil {
				return fmt.Errorf("json encode error: %w", err)
			}
		}
		return nil

	case "csv":
		w := csv.NewWriter(out)
		if err := w.Write(cols); err != nil {
			return fmt.Errorf("csv header error: %w", err)
		}
		for _, row := range results {
			record := make([]string, len(cols))
			for i, col := range cols {
				record[i] = fmt.Sprintf("%v", row[col])
			}
			if err := w.Write(record); err != nil {
				return fmt.Errorf("csv write error: %w", err)
			}
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return fmt.Errorf("csv flush error: %w", err)
		}
		return nil

	default: // "table"
		widths := make([]int, len(cols))
		for i, col := range cols {
			widths[i] = len(col)
		}

		// Precompute all rows as strings
		var allRows [][]string
		for _, row := range results {
			r := make([]string, len(cols))
			for i, col := range cols {
				val := row[col]
				var s string
				switch v := val.(type) {
				case nil:
					s = ""
				case float64:
					if v == math.Trunc(v) && !math.IsInf(v, 0) && !math.IsNaN(v) {
						s = strconv.FormatInt(int64(v), 10)
					} else {
						s = strconv.FormatFloat(v, 'f', -1, 64)
					}
				case time.Time:
					s = v.Format("2006-01-02 15:04:05")
				default:
					s = fmt.Sprintf("%v", v)
				}
				r[i] = s
				if len(s) > widths[i] {
					widths[i] = len(s)
				}
			}
			allRows = append(allRows, r)
		}

		// Print header
		for i, col := range cols {
			fmt.Fprintf(out, "%-*s ", widths[i], col)
		}
		fmt.Fprintln(out)
		for i := range cols {
			fmt.Fprintf(out, "%s ", strings.Repeat("-", widths[i]))
		}
		fmt.Fprintln(out)

		// Print rows
		for _, row := range allRows {
			for i, val := range row {
				fmt.Fprintf(out, "%-*s ", widths[i], val)
			}
			fmt.Fprintln(out)
		}
		return nil
	}
}

// RunLocal executes a SQL query against local Parquet files using DuckDB.
func RunLocal(ctx context.Context, backupNameOrPath string, opts QueryOptions) error {
	backupDir := archive.ResolveLocalBackupDir(backupNameOrPath)

	manifest, err := archive.ReadLocalManifest(backupDir)
	if err != nil {
		return fmt.Errorf("local backup %q not found. Run 'coldplane backup list' to see available backups", backupNameOrPath)
	}

	// Collect Parquet file paths from the manifest, optionally filtered by table
	parquetFiles, err := collectLocalParquetFiles(backupDir, manifest, opts.Table)
	if err != nil {
		return err
	}
	if len(parquetFiles) == 0 {
		if opts.Table != "" {
			return fmt.Errorf("no parquet files found for table %q in backup %s. Check the table name with 'coldplane backup list'", opts.Table, backupNameOrPath)
		}
		return fmt.Errorf("no parquet files found in backup %s", backupNameOrPath)
	}

	// Open DuckDB in-memory
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	// Register the parquet files as a view
	quotedPaths := make([]string, len(parquetFiles))
	for i, p := range parquetFiles {
		quotedPaths[i] = fmt.Sprintf("'%s'", p)
	}
	viewSQL := fmt.Sprintf("CREATE VIEW restore_input AS SELECT * FROM read_parquet([%s])", strings.Join(quotedPaths, ", "))
	if _, err := db.ExecContext(ctx, viewSQL); err != nil {
		return fmt.Errorf("create view: %w", err)
	}

	// Build the query
	sqlText := strings.TrimSpace(opts.SQL)
	if sqlText == "" {
		return fmt.Errorf("--sql is required")
	}

	// Execute query
	rows, err := db.QueryContext(ctx, sqlText)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	// Read results into maps
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns: %w", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration: %w", err)
	}

	return printQueryResults(results, opts.Output)
}

// collectLocalParquetFiles walks the backup directory and collects .parquet file paths.
// If tableName is specified, only files under that table's subdirectory are included.
func collectLocalParquetFiles(backupDir string, manifest map[string]interface{}, tableName string) ([]string, error) {
	var files []string

	tables, ok := manifest["tables"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("manifest missing tables array")
	}

	for _, t := range tables {
		tMap, ok := t.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := tMap["name"].(string)
		if tableName != "" && name != tableName {
			continue
		}

		// Try to get files from the manifest first
		if fileList, ok := tMap["files"].([]interface{}); ok {
			for _, f := range fileList {
				if fMap, ok := f.(map[string]interface{}); ok {
					if key, ok := fMap["key"].(string); ok {
						fullPath := filepath.Join(backupDir, filepath.FromSlash(key))
						if _, err := os.Stat(fullPath); err == nil {
							files = append(files, fullPath)
						}
					}
				}
			}
		}
	}

	// Fallback: if no files found via manifest, walk the directory
	if len(files) == 0 {
		searchDir := backupDir
		if tableName != "" {
			searchDir = filepath.Join(backupDir, tableName)
		}
		_ = filepath.Walk(searchDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() && strings.HasSuffix(path, ".parquet") {
				files = append(files, path)
			}
			return nil
		})
	}

	return files, nil
}
