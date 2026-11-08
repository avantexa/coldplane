package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/avantexa/coldplane/internal/archive"
)

// writeParquet writes the rows of a DuckDB SELECT as one Parquet file.
func writeParquet(t *testing.T, path, selectSQL string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	copySQL := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT parquet)", selectSQL, strings.ReplaceAll(path, "'", "''"))
	if _, err := db.Exec(copySQL); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// twoTableBackup lays out a backup the way backup create does: orders and
// users with different schemas, in year=/month= directories, plus a table
// that archived no rows and so has no files.
func twoTableBackup(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeParquet(t, filepath.Join(dir, "orders", "year=2025", "month=01", "orders-chunk-0-part-0.parquet"),
		"SELECT * FROM (VALUES (1, 10, 9.5), (2, 10, 20.0), (3, 11, 7.25)) t(id, user_id, amount)")
	writeParquet(t, filepath.Join(dir, "orders", "year=2025", "month=02", "orders-chunk-1-part-0.parquet"),
		"SELECT * FROM (VALUES (4, 11, 1.0)) t(id, user_id, amount)")
	writeParquet(t, filepath.Join(dir, "users", "year=unknown", "month=unknown", "users-chunk-0-part-0.parquet"),
		"SELECT * FROM (VALUES (10, 'ana'), (11, 'bo')) t(id, name)")
	m := archive.Manifest{Name: "bk", Tables: []archive.TableEntry{{Name: "orders"}, {Name: "users"}, {Name: "empty"}}}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.json"), data, 0644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func queryBackup(t *testing.T, dir, sqlText string) []map[string]interface{} {
	t.Helper()
	m, err := archive.ReadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	db, err := openBackupViews(context.Background(), dir, m.Tables)
	if err != nil {
		t.Fatalf("open views: %v", err)
	}
	defer db.Close()
	rows, err := queryRows(context.Background(), db, sqlText)
	if err != nil {
		t.Fatalf("%s: %v", sqlText, err)
	}
	return rows
}

func TestBackupViews_OneViewPerTable(t *testing.T) {
	dir := twoTableBackup(t)
	rows := queryBackup(t, dir, "SELECT (SELECT count(*) FROM orders) AS orders, (SELECT count(*) FROM users) AS users")
	if got := fmt.Sprint(rows[0]["orders"], " ", rows[0]["users"]); got != "4 2" {
		t.Errorf("orders users = %s, want 4 2", got)
	}
}

func TestBackupViews_TablesJoin(t *testing.T) {
	dir := twoTableBackup(t)
	rows := queryBackup(t, dir, "SELECT u.name, sum(o.amount)::VARCHAR AS total FROM orders o JOIN users u ON u.id = o.user_id GROUP BY u.name ORDER BY u.name")
	var got []string
	for _, r := range rows {
		got = append(got, fmt.Sprint(r["name"], "=", r["total"]))
	}
	if strings.Join(got, ",") != "ana=29.50,bo=8.25" {
		t.Errorf("join = %v", got)
	}
}

func TestBackupViews_SourceColumnsOnly(t *testing.T) {
	dir := twoTableBackup(t)
	rows := queryBackup(t, dir, "SELECT string_agg(column_name, ',' ORDER BY column_name) AS cols FROM (DESCRIBE orders)")
	if got := rows[0]["cols"]; got != "amount,id,user_id" {
		t.Errorf("orders columns = %v, want the source columns without year/month", got)
	}
}

func TestBackupViews_NoFilesAtAll(t *testing.T) {
	dir := t.TempDir()
	_, err := openBackupViews(context.Background(), dir, []archive.TableEntry{{Name: "empty"}})
	if err == nil || !strings.Contains(err.Error(), "no parquet files") {
		t.Errorf("want a no parquet files error, got %v", err)
	}
}

func TestRun_QueriesTablesByName(t *testing.T) {
	dir := twoTableBackup(t)
	out := captureStdout(t, func() {
		if err := Run(context.Background(), dir, QueryOptions{SQL: "SELECT name FROM users ORDER BY id", Output: "csv"}); err != nil {
			t.Fatalf("Run: %v", err)
		}
	})
	if out != "name\nana\nbo\n" {
		t.Errorf("output = %q", out)
	}
}
