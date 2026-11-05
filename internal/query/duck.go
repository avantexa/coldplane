package query

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
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

// RunLocal executes a SQL query against a backup's Parquet files using DuckDB.
func RunLocal(ctx context.Context, backupNameOrPath string, opts QueryOptions) error {
	backupDir := archive.ResolveLocalBackupDir(backupNameOrPath)

	manifest, err := archive.ReadLocalManifest(backupDir)
	if err != nil {
		return fmt.Errorf("local backup %q not found. Run 'coldplane backup list' to see available backups", backupNameOrPath)
	}

	parquetFiles, err := collectLocalParquetFiles(backupDir, manifest, opts.Table)
	if err != nil {
		return err
	}
	if len(parquetFiles) == 0 {
		return noFilesError(backupNameOrPath, opts.Table)
	}

	sqlText := strings.TrimSpace(opts.SQL)
	if sqlText == "" {
		return fmt.Errorf("--sql is required")
	}

	db, err := openParquetView(ctx, parquetFiles)
	if err != nil {
		return err
	}
	defer db.Close()

	results, err := queryRows(ctx, db, sqlText)
	if err != nil {
		return err
	}
	return printQueryResults(results, opts.Output)
}

func noFilesError(backup, table string) error {
	if table != "" {
		return fmt.Errorf("no parquet files found for table %q in backup %s. Check the table name with 'coldplane backup list'", table, backup)
	}
	return fmt.Errorf("no parquet files found in backup %s", backup)
}

// openParquetView opens an in-memory DuckDB with the files exposed as the view restore_input.
func openParquetView(ctx context.Context, files []string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	quoted := make([]string, len(files))
	for i, p := range files {
		quoted[i] = fmt.Sprintf("'%s'", p)
	}
	viewSQL := fmt.Sprintf("CREATE VIEW restore_input AS SELECT * FROM read_parquet([%s])", strings.Join(quoted, ", "))
	if _, err := db.ExecContext(ctx, viewSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("create view: %w", err)
	}
	return db, nil
}

// queryRows runs sqlText and returns every row as a column-name map.
func queryRows(ctx context.Context, db *sql.DB, sqlText string) ([]map[string]interface{}, error) {
	rows, err := db.QueryContext(ctx, sqlText)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		row := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration: %w", err)
	}
	return results, nil
}

// collectLocalParquetFiles returns the backup's Parquet files, optionally
// limited to one table: the manifest's files when they exist on disk, else
// whatever .parquet files a directory walk finds.
func collectLocalParquetFiles(backupDir string, manifest map[string]interface{}, tableName string) ([]string, error) {
	tables, ok := manifest["tables"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("manifest missing tables array")
	}
	files := manifestParquetFiles(backupDir, tables, tableName)
	if len(files) == 0 {
		searchDir := backupDir
		if tableName != "" {
			searchDir = filepath.Join(backupDir, tableName)
		}
		files = walkParquetFiles(searchDir)
	}
	return files, nil
}

// manifestParquetFiles returns the manifest file keys that exist on disk.
func manifestParquetFiles(backupDir string, tables []interface{}, tableName string) []string {
	var files []string
	for _, t := range tables {
		tMap, ok := t.(map[string]interface{})
		if !ok {
			continue
		}
		if name, _ := tMap["name"].(string); tableName != "" && name != tableName {
			continue
		}
		fileList, _ := tMap["files"].([]interface{})
		for _, f := range fileList {
			fMap, _ := f.(map[string]interface{})
			key, ok := fMap["key"].(string)
			if !ok {
				continue
			}
			full := filepath.Join(backupDir, filepath.FromSlash(key))
			if _, err := os.Stat(full); err == nil {
				files = append(files, full)
			}
		}
	}
	return files
}

// walkParquetFiles returns every .parquet file under dir.
func walkParquetFiles(dir string) []string {
	var files []string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(path, ".parquet") {
			files = append(files, path)
		}
		return nil
	})
	return files
}

// printQueryResults prints the query output to stdout in table, csv, or json format.
func printQueryResults(results []map[string]interface{}, format string) error {
	return writeQueryResults(os.Stdout, results, format)
}

// writeQueryResults renders results to w in table, csv, or json format.
func writeQueryResults(w io.Writer, results []map[string]interface{}, format string) error {
	if len(results) == 0 {
		fmt.Fprintln(w, "No results found.")
		return nil
	}
	cols := sortedColumns(results[0])
	switch strings.ToLower(format) {
	case "json":
		return writeJSON(w, results)
	case "csv":
		return writeCSV(w, cols, results)
	default:
		return writeTable(w, cols, results)
	}
}

// sortedColumns returns the row's column names in a stable order.
func sortedColumns(row map[string]interface{}) []string {
	cols := make([]string, 0, len(row))
	for k := range row {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}

func writeJSON(w io.Writer, results []map[string]interface{}) error {
	enc := json.NewEncoder(w)
	for _, row := range results {
		if err := enc.Encode(row); err != nil {
			return fmt.Errorf("json encode error: %w", err)
		}
	}
	return nil
}

func writeCSV(w io.Writer, cols []string, results []map[string]interface{}) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(cols); err != nil {
		return fmt.Errorf("csv header error: %w", err)
	}
	for _, row := range results {
		record := make([]string, len(cols))
		for i, col := range cols {
			record[i] = fmt.Sprintf("%v", row[col])
		}
		if err := cw.Write(record); err != nil {
			return fmt.Errorf("csv write error: %w", err)
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return fmt.Errorf("csv flush error: %w", err)
	}
	return nil
}

func writeTable(w io.Writer, cols []string, results []map[string]interface{}) error {
	widths := make([]int, len(cols))
	for i, col := range cols {
		widths[i] = len(col)
	}
	rows := make([][]string, 0, len(results))
	for _, row := range results {
		r := make([]string, len(cols))
		for i, col := range cols {
			r[i] = formatCell(row[col])
			if len(r[i]) > widths[i] {
				widths[i] = len(r[i])
			}
		}
		rows = append(rows, r)
	}
	writePadded(w, cols, widths)
	seps := make([]string, len(cols))
	for i := range cols {
		seps[i] = strings.Repeat("-", widths[i])
	}
	writePadded(w, seps, widths)
	for _, r := range rows {
		writePadded(w, r, widths)
	}
	return nil
}

func writePadded(w io.Writer, cells []string, widths []int) {
	for i, c := range cells {
		fmt.Fprintf(w, "%-*s ", widths[i], c)
	}
	fmt.Fprintln(w)
}

// formatCell renders one value for the table output: whole floats without a
// decimal point, times as YYYY-MM-DD HH:MM:SS, nil as empty.
func formatCell(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return ""
	case float64:
		if x == math.Trunc(x) && !math.IsInf(x, 0) && !math.IsNaN(x) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case time.Time:
		return x.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", x)
	}
}
