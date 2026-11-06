//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/avantexa/coldplane/internal/archive"

	_ "github.com/marcboeker/go-duckdb"
)

// seedChunkingTables creates three tables that force each chunking strategy:
// inttest_ids has an integer primary key and no timestamps, inttest_plain has
// neither a key nor a timestamp, inttest_events has a timestamp but no key.
func seedChunkingTables(driver, dsn string) error {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	stmts := []string{
		`DROP TABLE IF EXISTS inttest_ids`,
		`DROP TABLE IF EXISTS inttest_plain`,
		`DROP TABLE IF EXISTS inttest_events`,
		`DROP TABLE IF EXISTS inttest_empty`,
		`DROP TABLE IF EXISTS inttest_strkey`,
		`CREATE TABLE inttest_ids (id INT PRIMARY KEY, label VARCHAR(20))`,
		`CREATE TABLE inttest_plain (label VARCHAR(20), qty INT)`,
		`CREATE TABLE inttest_events (happened_at TIMESTAMP, note VARCHAR(20))`,
		`CREATE TABLE inttest_empty (id INT PRIMARY KEY, happened_at TIMESTAMP)`,
		`CREATE TABLE inttest_strkey (code VARCHAR(10) PRIMARY KEY, qty INT)`,
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
	for i := 1; i <= 5; i++ {
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO inttest_strkey VALUES ('k%d', %d)`, i, i)); err != nil {
			return err
		}
	}
	for i := 0; i < 12; i++ {
		ts := time.Date(2023, 1, 15, 12, 0, 0, 0, time.UTC).AddDate(0, i, 0).Format("2006-01-02 15:04:05")
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO inttest_events VALUES ('%s', 'e%d')`, ts, i)); err != nil {
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

func testChunkingStrategies(t *testing.T, run func(ctx context.Context, name string, opts archive.ArchiveOptions, tables ...string) error, dbType string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backupDir := t.TempDir()
	opts := archive.DefaultOptions()
	opts.OutputDir = backupDir
	opts.ChunkSize = 10 // 25 ids spread over 73 values -> several PK chunks
	opts.Workers = 2

	if err := run(ctx, "chunking", opts, "inttest_ids", "inttest_plain", "inttest_events", "inttest_strkey"); err != nil {
		t.Fatalf("archive: %v", err)
	}

	if got := parquetRowCount(t, filepath.Join(backupDir, "chunking"), "inttest_ids"); got != 25 {
		t.Errorf("inttest_ids: %d rows archived, want 25", got)
	}
	if got := parquetRowCount(t, filepath.Join(backupDir, "chunking"), "inttest_plain"); got != 7 {
		t.Errorf("inttest_plain: %d rows archived, want 7", got)
	}
	if got := parquetRowCount(t, filepath.Join(backupDir, "chunking"), "inttest_events"); got != 12 {
		t.Errorf("inttest_events: %d rows archived, want 12", got)
	}
	// A non-numeric primary key must not be used for range chunking.
	if got := parquetRowCount(t, filepath.Join(backupDir, "chunking"), "inttest_strkey"); got != 5 {
		t.Errorf("inttest_strkey: %d rows archived, want 5", got)
	}

	// An empty table and a WHERE that matches nothing both archive cleanly with no files.
	whereOpts := opts
	whereOpts.Where = "id > 1000"
	if err := run(ctx, "nothing", whereOpts, "inttest_empty", "inttest_ids"); err != nil {
		t.Fatalf("archive with no matching rows: %v", err)
	}
	nothing, err := archive.ReadManifest(filepath.Join(backupDir, "nothing"))
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	for _, tb := range nothing.Tables {
		if len(tb.Files) != 0 {
			t.Errorf("%s: expected no files, got %d", tb.Name, len(tb.Files))
		}
	}
	if nothing.SizeBytes != 0 {
		t.Errorf("empty backup size = %d", nothing.SizeBytes)
	}

	manifest, err := archive.ReadManifest(filepath.Join(backupDir, "chunking"))
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if manifest.DBType != dbType {
		t.Errorf("db_type = %q", manifest.DBType)
	}
	for _, m := range manifest.Tables {
		// ids 3..75 in PK chunks of 10 -> 8 ranges, each with rows -> 8 files.
		// OFFSET/LIMIT would produce only 3, so this pins the strategy.
		if m.Name == "inttest_ids" && len(m.Files) != 8 {
			t.Errorf("inttest_ids: %d files, want 8 (PK chunking)", len(m.Files))
		}
		switch m.Name {
		case "inttest_events":
			if m.PartitionColumn == nil || *m.PartitionColumn != "happened_at" {
				t.Errorf("inttest_events: partition_column = %v, want happened_at", m.PartitionColumn)
			}
			// 12 monthly rows over 334 days in 30-day windows -> about a dozen
			// chunk numbers (a window can hold two rows or none). OFFSET/LIMIT
			// with ChunkSize=10 would use only chunks 0 and 1.
			chunks := map[string]bool{}
			for _, f := range m.Files {
				chunks[regexp.MustCompile(`-chunk-(\d+)-`).FindStringSubmatch(f.Key)[1]] = true
			}
			if len(chunks) < 10 {
				t.Errorf("inttest_events: %d distinct chunks, want at least 10 (time chunking)", len(chunks))
			}
		default:
			if m.PartitionColumn != nil {
				t.Errorf("%s: expected no partition column, got %q", m.Name, *m.PartitionColumn)
			}
		}
		if m.SizeBytes <= 0 {
			t.Errorf("%s: size_bytes should be positive", m.Name)
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
	testChunkingStrategies(t, func(ctx context.Context, name string, opts archive.ArchiveOptions, tables ...string) error {
		return archive.ArchivePostgres(ctx, pgDSN, tables, name, opts)
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
	testChunkingStrategies(t, func(ctx context.Context, name string, opts archive.ArchiveOptions, tables ...string) error {
		return archive.ArchiveMySQL(ctx, mysqlGoDSN, tables, name, opts)
	}, "mysql")
}

// parquetTimestampProbe reads a timestamp column's DuckDB type and minimum.
func parquetTimestampProbe(t *testing.T, backupDir, table, column string) (string, string) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	glob := filepath.Join(backupDir, table, "**", "*.parquet")
	var typ, min string
	q := fmt.Sprintf("SELECT typeof(min(%s)), CAST(min(%s) AS VARCHAR) FROM read_parquet('%s')", column, column, glob)
	if err := db.QueryRow(q).Scan(&typ, &min); err != nil {
		t.Fatalf("probe %s.%s: %v", table, column, err)
	}
	return typ, min
}

func assertTimestampColumn(t *testing.T, backupDir string) {
	t.Helper()
	typ, min := parquetTimestampProbe(t, backupDir, "inttest_events", "happened_at")
	if !strings.HasPrefix(typ, "TIMESTAMP") {
		t.Errorf("happened_at stored as %s, want a TIMESTAMP type", typ)
	}
	if !strings.HasPrefix(min, "2023-01-15 12:00:00") {
		t.Errorf("min(happened_at) = %s, want 2023-01-15 12:00:00", min)
	}
}

// Timestamps must become Parquet TIMESTAMP columns on both engines, and for
// MySQL whether or not the DSN asks the driver to parse times.
func TestLocalTimestampTyping(t *testing.T) {
	skipUnlessLocalTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	t.Run("postgres", func(t *testing.T) {
		if err := seedChunkingTables("postgres", pgDSN); err != nil {
			t.Fatal(err)
		}
		opts := archive.DefaultOptions()
		opts.OutputDir = t.TempDir()
		if err := archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_events"}, "ts", opts); err != nil {
			t.Fatal(err)
		}
		assertTimestampColumn(t, filepath.Join(opts.OutputDir, "ts"))
	})

	for name, dsn := range map[string]string{
		"mysql parseTime":    mysqlGoDSN,
		"mysql no parseTime": strings.Replace(mysqlGoDSN, "?parseTime=true", "", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if err := seedChunkingTables("mysql", mysqlGoDSN); err != nil {
				t.Fatal(err)
			}
			opts := archive.DefaultOptions()
			opts.OutputDir = t.TempDir()
			if err := archive.ArchiveMySQL(ctx, dsn, []string{"inttest_events"}, "ts", opts); err != nil {
				t.Fatal(err)
			}
			assertTimestampColumn(t, filepath.Join(opts.OutputDir, "ts"))
		})
	}
}

// A NULL in the first row must not decide a column's Parquet type.
func TestLocalNullFirstRowTyping(t *testing.T) {
	skipUnlessLocalTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	seed := func(driver, dsn string) {
		db, err := sql.Open(driver, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		for _, q := range []string{
			`DROP TABLE IF EXISTS inttest_nulls`,
			`CREATE TABLE inttest_nulls (id INT PRIMARY KEY, n BIGINT NULL, f FLOAT NULL, s VARCHAR(10) NULL, ts TIMESTAMP NULL)`,
			`INSERT INTO inttest_nulls VALUES (1, NULL, NULL, NULL, NULL)`,
			`INSERT INTO inttest_nulls VALUES (2, 42, 1.5, 'x', '2024-01-01 00:00:00')`,
		} {
			if _, err := db.Exec(q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
	check := func(t *testing.T, backupDir string) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		glob := filepath.Join(backupDir, "inttest_nulls", "**", "*.parquet")
		var tn, tf, ts, tts string
		var n int64
		q := fmt.Sprintf("SELECT typeof(max(n)), typeof(max(f)), typeof(max(s)), typeof(max(ts)), max(n) FROM read_parquet('%s')", glob)
		if err := db.QueryRow(q).Scan(&tn, &tf, &ts, &tts, &n); err != nil {
			t.Fatalf("probe: %v", err)
		}
		// MySQL FLOAT is 4 bytes and stays FLOAT; Postgres FLOAT is double precision.
		if tn != "BIGINT" || (tf != "DOUBLE" && tf != "FLOAT") || ts != "VARCHAR" || !strings.HasPrefix(tts, "TIMESTAMP") || n != 42 {
			t.Errorf("types n=%s f=%s s=%s ts=%s max(n)=%d; want BIGINT FLOAT|DOUBLE VARCHAR TIMESTAMP* 42", tn, tf, ts, tts, n)
		}
	}

	t.Run("postgres", func(t *testing.T) {
		seed("postgres", pgDSN)
		opts := archive.DefaultOptions()
		opts.OutputDir = t.TempDir()
		if err := archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_nulls"}, "nulls", opts); err != nil {
			t.Fatal(err)
		}
		check(t, filepath.Join(opts.OutputDir, "nulls"))
	})
	t.Run("mysql", func(t *testing.T) {
		seed("mysql", mysqlGoDSN)
		opts := archive.DefaultOptions()
		opts.OutputDir = t.TempDir()
		if err := archive.ArchiveMySQL(ctx, mysqlGoDSN, []string{"inttest_nulls"}, "nulls", opts); err != nil {
			t.Fatal(err)
		}
		check(t, filepath.Join(opts.OutputDir, "nulls"))
	})
}

// parquetTypes returns DuckDB's type for each named column of an archived table.
func parquetTypes(t *testing.T, backupDir, table string, columns ...string) map[string]string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	glob := filepath.Join(backupDir, table, "**", "*.parquet")
	rows, err := db.Query(fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s', hive_partitioning=false)", glob))
	if err != nil {
		t.Fatalf("describe %s: %v", table, err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var name, typ, a, b, c, d sql.NullString
		if err := rows.Scan(&name, &typ, &a, &b, &c, &d); err != nil {
			t.Fatal(err)
		}
		got[name.String] = typ.String
	}
	return got
}

// The archive must keep the source's types: decimals, booleans, dates, blobs,
// and on Postgres uuids and arrays, instead of flattening them to strings.
func TestLocalTypeFidelity(t *testing.T) {
	skipUnlessLocalTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	t.Run("postgres", func(t *testing.T) {
		if err := seedPostgresAllTypes(); err != nil {
			t.Fatal(err)
		}
		opts := archive.DefaultOptions()
		opts.OutputDir = t.TempDir()
		if err := archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_alltypes"}, "types", opts); err != nil {
			t.Fatal(err)
		}
		got := parquetTypes(t, filepath.Join(opts.OutputDir, "types"), "inttest_alltypes")
		want := map[string]string{"col_numeric": "DECIMAL(12,4)", "col_boolean": "BOOLEAN", "col_uuid": "UUID", "col_date": "DATE", "col_bytea": "BLOB", "col_int_array": "INTEGER[]", "col_bigint": "BIGINT", "col_timestamp": "TIMESTAMP"}
		for col, typ := range want {
			if got[col] != typ {
				t.Errorf("%s: archived as %q, want %q", col, got[col], typ)
			}
		}
	})
	t.Run("mysql", func(t *testing.T) {
		if err := seedMySQLAllTypes(); err != nil {
			t.Fatal(err)
		}
		opts := archive.DefaultOptions()
		opts.OutputDir = t.TempDir()
		if err := archive.ArchiveMySQL(ctx, mysqlGoDSN, []string{"inttest_alltypes"}, "types", opts); err != nil {
			t.Fatal(err)
		}
		got := parquetTypes(t, filepath.Join(opts.OutputDir, "types"), "inttest_alltypes")
		want := map[string]string{"col_decimal": "DECIMAL(12,4)", "col_boolean": "BOOLEAN", "col_date": "DATE", "col_blob": "BLOB", "col_bigint": "BIGINT", "col_datetime": "TIMESTAMP", "col_tinyint": "TINYINT"}
		for col, typ := range want {
			if got[col] != typ {
				t.Errorf("%s: archived as %q, want %q", col, got[col], typ)
			}
		}
	})
}
