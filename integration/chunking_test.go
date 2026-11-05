//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/avantexa/coldplane/internal/archive"

	_ "github.com/marcboeker/go-duckdb"
)

// seedChunkingTables creates two tables that force the non-time chunking
// strategies: inttest_ids has an integer primary key and no timestamps,
// inttest_plain has neither a key nor a timestamp.
func seedChunkingTables(driver, dsn string) error {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	stmts := []string{
		`DROP TABLE IF EXISTS inttest_ids`,
		`DROP TABLE IF EXISTS inttest_plain`,
		`CREATE TABLE inttest_ids (id INT PRIMARY KEY, label VARCHAR(20))`,
		`CREATE TABLE inttest_plain (label VARCHAR(20), qty INT)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	for i := 1; i <= 25; i++ {
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO inttest_ids VALUES (%d, 'row%d')`, i*3, i)); err != nil {
			return err
		}
	}
	for i := 1; i <= 7; i++ {
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO inttest_plain VALUES ('p%d', %d)`, i, i)); err != nil {
			return err
		}
	}
	return nil
}

// parquetRowCount counts the rows of every Parquet file under a table's directory.
func parquetRowCount(t *testing.T, backupDir, table string) int64 {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	glob := filepath.Join(backupDir, table, "**", "*.parquet")
	var n int64
	if err := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", glob)).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func testChunkingStrategies(t *testing.T, run func(ctx context.Context, name string, opts archive.ArchiveOptions) error, dbType string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backupDir := t.TempDir()
	opts := archive.DefaultOptions()
	opts.LocalOutputDir = backupDir
	opts.ChunkSize = 10 // 25 ids spread over 73 values -> several PK chunks
	opts.Workers = 2

	if err := run(ctx, "chunking", opts); err != nil {
		t.Fatalf("archive: %v", err)
	}

	if got := parquetRowCount(t, filepath.Join(backupDir, "chunking"), "inttest_ids"); got != 25 {
		t.Errorf("inttest_ids: %d rows archived, want 25", got)
	}
	if got := parquetRowCount(t, filepath.Join(backupDir, "chunking"), "inttest_plain"); got != 7 {
		t.Errorf("inttest_plain: %d rows archived, want 7", got)
	}

	manifest, err := archive.ReadLocalManifest(filepath.Join(backupDir, "chunking"))
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if manifest["db_type"] != dbType {
		t.Errorf("db_type = %v", manifest["db_type"])
	}
	for _, tb := range manifest["tables"].([]interface{}) {
		m := tb.(map[string]interface{})
		// ids 3..75 in PK chunks of 10 -> 8 ranges, each with rows -> 8 files.
		// OFFSET/LIMIT would produce only 3, so this pins the strategy.
		if m["name"] == "inttest_ids" {
			if n := len(m["files"].([]interface{})); n != 8 {
				t.Errorf("inttest_ids: %d files, want 8 (PK chunking)", n)
			}
		}
		if m["partition_column"] != nil {
			t.Errorf("%v: expected no partition column, got %v", m["name"], m["partition_column"])
		}
		if m["size_bytes"].(float64) <= 0 {
			t.Errorf("%v: size_bytes should be positive", m["name"])
		}
	}
}

func TestLocalChunkingStrategies_Postgres(t *testing.T) {
	skipUnlessLocalTest(t)
	if err := verifyPostgres(); err != nil {
		t.Fatalf("PostgreSQL not reachable: %v", err)
	}
	if err := seedChunkingTables("postgres", pgDSN); err != nil {
		t.Fatalf("seed: %v", err)
	}
	testChunkingStrategies(t, func(ctx context.Context, name string, opts archive.ArchiveOptions) error {
		return archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_ids", "inttest_plain"}, name, opts)
	}, "postgres")
}

func TestLocalChunkingStrategies_MySQL(t *testing.T) {
	skipUnlessLocalTest(t)
	if err := verifyMySQL(); err != nil {
		t.Fatalf("MySQL not reachable: %v", err)
	}
	if err := seedChunkingTables("mysql", mysqlGoDSN); err != nil {
		t.Fatalf("seed: %v", err)
	}
	testChunkingStrategies(t, func(ctx context.Context, name string, opts archive.ArchiveOptions) error {
		return archive.ArchiveMySQL(ctx, mysqlGoDSN, []string{"inttest_ids", "inttest_plain"}, name, opts)
	}, "mysql")
}
