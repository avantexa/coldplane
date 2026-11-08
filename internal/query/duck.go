package query

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/avantexa/coldplane/internal/archive"
	"github.com/avantexa/coldplane/internal/dbutil"
	"github.com/avantexa/coldplane/internal/duck"

	_ "github.com/marcboeker/go-duckdb"
)

// QueryOptions defines parameters for running a query against a local backup.
type QueryOptions struct {
	SQL    string // SQL query text; each archived table is a view under its own name
	Output string // Output format: "table" (default), "json", or "csv"
}

// Run executes a SQL query against a backup's Parquet files using DuckDB.
func Run(ctx context.Context, backupNameOrPath string, opts QueryOptions) error {
	sqlText := strings.TrimSpace(opts.SQL)
	if sqlText == "" {
		return fmt.Errorf("--sql is required")
	}

	backupDir := archive.ResolveBackupDir(backupNameOrPath)
	manifest, err := archive.ReadManifest(backupDir)
	if err != nil {
		return fmt.Errorf("backup %q: %w. Run 'coldplane backup list' to see available backups", backupNameOrPath, err)
	}

	db, err := openBackupViews(ctx, backupDir, manifest.Tables)
	if err != nil {
		return fmt.Errorf("backup %s: %w", backupNameOrPath, err)
	}
	defer db.Close()

	results, err := queryRows(ctx, db, sqlText)
	if err != nil {
		return err
	}
	return printQueryResults(results, opts.Output)
}

// openBackupViews opens an in-memory DuckDB with one view per archived table,
// named after the table, over that table's Parquet files. Tables that
// archived no rows have no files and get no view.
func openBackupViews(ctx context.Context, backupDir string, tables []archive.TableEntry) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	views := 0
	for _, t := range tables {
		tableDir := filepath.Join(backupDir, t.Name)
		if !hasParquet(tableDir) {
			continue
		}
		viewSQL := fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM %s", dbutil.QuotePG(t.Name), duck.ParquetSQL(tableDir))
		if _, err := db.ExecContext(ctx, viewSQL); err != nil {
			db.Close()
			return nil, fmt.Errorf("create view %s: %w", t.Name, err)
		}
		views++
	}
	if views == 0 {
		db.Close()
		return nil, fmt.Errorf("no parquet files found")
	}
	return db, nil
}

// hasParquet reports whether any .parquet file exists under dir.
func hasParquet(dir string) bool {
	found := errors.New("found")
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			return found
		}
		return nil
	})
	return err == found
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
		if x == math.Trunc(x) && !math.IsInf(x, 0) { // NaN never equals its truncation
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case time.Time:
		return x.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", x)
	}
}
