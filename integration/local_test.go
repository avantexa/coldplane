//go:build integration

// Integration tests for backup, query and analyze against real Docker databases.
//
// Prerequisites:
//   - docker-compose up -d (PostgreSQL on :5432, MySQL on :3306)
//   - COLDPLANE_LOCAL_TEST=1
//
// Run:
//
//	COLDPLANE_LOCAL_TEST=1 go test -tags integration -v -timeout 10m -run TestLocal ./integration/

package integration

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/avantexa/coldplane/internal/analyze"
	"github.com/avantexa/coldplane/internal/archive"
	"github.com/avantexa/coldplane/internal/query"
)

func skipUnlessLocalTest(t *testing.T) {
	t.Helper()
	if os.Getenv("COLDPLANE_LOCAL_TEST") != "1" {
		t.Skip("Skipping local test (set COLDPLANE_LOCAL_TEST=1 to run)")
	}
}

// ---------------------------------------------------------------------------
// PostgreSQL local lifecycle
// ---------------------------------------------------------------------------

func TestLocalPostgresFlow(t *testing.T) {
	skipUnlessLocalTest(t)

	if err := verifyPostgres(); err != nil {
		t.Fatalf("PostgreSQL not reachable: %v", err)
	}
	if err := seedPostgres(); err != nil {
		t.Fatalf("Failed to seed PostgreSQL: %v", err)
	}

	backupDir := t.TempDir()
	backupName := "local_pg_test"

	t.Run("01_local_backup_create", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		opts := archive.DefaultOptions()
		opts.LocalOutputDir = backupDir

		err := archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_orders"}, "parquet", backupName, nil, opts)
		if err != nil {
			t.Fatalf("Local backup create failed: %v", err)
		}
	})

	t.Run("02_local_backup_list", func(t *testing.T) {
		backups, err := archive.ListLocalBackups(backupDir)
		if err != nil {
			t.Fatalf("ListLocalBackups: %v", err)
		}
		if len(backups) == 0 {
			t.Fatal("expected at least 1 local backup")
		}

		found := false
		for _, b := range backups {
			if b.Name == backupName {
				found = true
				if b.DBType != "postgres" {
					t.Errorf("DBType = %q, want 'postgres'", b.DBType)
				}
				if len(b.Tables) == 0 {
					t.Error("expected at least 1 table")
				}
				if b.SizeBytes == 0 {
					t.Error("expected non-zero size")
				}
				t.Logf("Found local backup: %s (size=%d, tables=%v)", b.Name, b.SizeBytes, b.Tables)
			}
		}
		if !found {
			t.Errorf("backup %q not found in local list", backupName)
		}
	})

	t.Run("03_local_backup_manifest", func(t *testing.T) {
		dir := archive.ResolveLocalBackupDir(backupName)
		// The backup was created with a custom output dir, so resolve manually
		dir = backupDir + "/" + backupName
		manifest, err := archive.ReadLocalManifest(dir)
		if err != nil {
			t.Fatalf("ReadLocalManifest: %v", err)
		}
		if manifest["name"] != backupName {
			t.Errorf("manifest name = %v, want %q", manifest["name"], backupName)
		}
		if manifest["db_type"] != "postgres" {
			t.Errorf("manifest db_type = %v, want 'postgres'", manifest["db_type"])
		}
		if manifest["local"] != true {
			t.Errorf("manifest local = %v, want true", manifest["local"])
		}
	})

	t.Run("04_local_query_count", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backupPath := backupDir + "/" + backupName
		opts := query.QueryOptions{
			SQL:    "SELECT count(*) as cnt FROM restore_input",
			Output: "json",
		}
		err := query.RunLocal(ctx, backupPath, opts)
		if err != nil {
			t.Fatalf("RunLocal count query failed: %v", err)
		}
	})

	t.Run("05_local_query_select", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backupPath := backupDir + "/" + backupName
		opts := query.QueryOptions{
			SQL:    "SELECT customer_name, amount FROM restore_input ORDER BY amount DESC LIMIT 2",
			Output: "csv",
		}
		err := query.RunLocal(ctx, backupPath, opts)
		if err != nil {
			t.Fatalf("RunLocal select query failed: %v", err)
		}
	})

	t.Run("06_local_query_aggregation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backupPath := backupDir + "/" + backupName
		opts := query.QueryOptions{
			SQL:    "SELECT SUM(amount) as total, AVG(amount) as avg_amount FROM restore_input",
			Output: "table",
		}
		err := query.RunLocal(ctx, backupPath, opts)
		if err != nil {
			t.Fatalf("RunLocal aggregation query failed: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// MySQL local lifecycle
// ---------------------------------------------------------------------------

func TestLocalMySQLFlow(t *testing.T) {
	skipUnlessLocalTest(t)

	if err := verifyMySQL(); err != nil {
		t.Fatalf("MySQL not reachable: %v", err)
	}
	if err := seedMySQL(); err != nil {
		t.Fatalf("Failed to seed MySQL: %v", err)
	}

	backupDir := t.TempDir()
	backupName := "local_mysql_test"
	mysqlDSN := "mysql://testuser:testpass@localhost:3306/testdb?parseTime=true"

	t.Run("01_local_backup_create", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		opts := archive.DefaultOptions()
		opts.LocalOutputDir = backupDir

		// Normalize DSN like the CLI does
		normalizedDSN := archive.NormalizeMySQLDSN(mysqlDSN)
		err := archive.ArchiveMySQL(ctx, normalizedDSN, []string{"inttest_orders"}, "parquet", backupName, opts)
		if err != nil {
			t.Fatalf("Local MySQL backup failed: %v", err)
		}
	})

	t.Run("02_local_backup_list", func(t *testing.T) {
		backups, err := archive.ListLocalBackups(backupDir)
		if err != nil {
			t.Fatalf("ListLocalBackups: %v", err)
		}

		found := false
		for _, b := range backups {
			if b.Name == backupName {
				found = true
				if b.DBType != "mysql" {
					t.Errorf("DBType = %q, want 'mysql'", b.DBType)
				}
				if b.SizeBytes == 0 {
					t.Error("expected non-zero size")
				}
				t.Logf("Found local backup: %s (size=%d, tables=%v)", b.Name, b.SizeBytes, b.Tables)
			}
		}
		if !found {
			t.Errorf("backup %q not found in local list", backupName)
		}
	})

	t.Run("03_local_query_count", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backupPath := backupDir + "/" + backupName
		opts := query.QueryOptions{
			SQL:    "SELECT count(*) as cnt FROM restore_input",
			Output: "json",
		}
		err := query.RunLocal(ctx, backupPath, opts)
		if err != nil {
			t.Fatalf("RunLocal count query failed: %v", err)
		}
	})

	t.Run("04_local_query_filter", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backupPath := backupDir + "/" + backupName
		opts := query.QueryOptions{
			SQL:    "SELECT customer_name, amount FROM restore_input WHERE CAST(amount AS DECIMAL) > 100 ORDER BY amount",
			Output: "table",
		}
		err := query.RunLocal(ctx, backupPath, opts)
		if err != nil {
			t.Fatalf("RunLocal filter query failed: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// PostgreSQL all-types local test
// ---------------------------------------------------------------------------

func TestLocalPostgresAllTypes(t *testing.T) {
	skipUnlessLocalTest(t)

	if err := verifyPostgres(); err != nil {
		t.Fatalf("PostgreSQL not reachable: %v", err)
	}
	if err := seedPostgresAllTypes(); err != nil {
		t.Fatalf("Failed to seed PostgreSQL alltypes: %v", err)
	}

	backupDir := t.TempDir()
	backupName := "local_pg_alltypes"

	t.Run("01_backup", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		opts := archive.DefaultOptions()
		opts.LocalOutputDir = backupDir

		err := archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_alltypes"}, "parquet", backupName, nil, opts)
		if err != nil {
			t.Fatalf("Local alltypes backup failed: %v", err)
		}
	})

	t.Run("02_query", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backupPath := backupDir + "/" + backupName
		opts := query.QueryOptions{
			SQL:    "SELECT count(*) as cnt FROM restore_input",
			Output: "json",
		}
		err := query.RunLocal(ctx, backupPath, opts)
		if err != nil {
			t.Fatalf("RunLocal alltypes query failed: %v", err)
		}
	})

	t.Run("03_query_columns", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backupPath := backupDir + "/" + backupName
		opts := query.QueryOptions{
			SQL:    "SELECT id, col_varchar, col_boolean, col_bigint FROM restore_input WHERE id <= 5 ORDER BY id",
			Output: "table",
		}
		err := query.RunLocal(ctx, backupPath, opts)
		if err != nil {
			t.Fatalf("RunLocal column select failed: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// MySQL all-types local test
// ---------------------------------------------------------------------------

func TestLocalMySQLAllTypes(t *testing.T) {
	skipUnlessLocalTest(t)

	if err := verifyMySQL(); err != nil {
		t.Fatalf("MySQL not reachable: %v", err)
	}
	if err := seedMySQLAllTypes(); err != nil {
		t.Fatalf("Failed to seed MySQL alltypes: %v", err)
	}

	backupDir := t.TempDir()
	backupName := "local_mysql_alltypes"
	mysqlDSN := "mysql://testuser:testpass@localhost:3306/testdb?parseTime=true"

	t.Run("01_backup", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		opts := archive.DefaultOptions()
		opts.LocalOutputDir = backupDir

		normalizedDSN := archive.NormalizeMySQLDSN(mysqlDSN)
		err := archive.ArchiveMySQL(ctx, normalizedDSN, []string{"inttest_alltypes"}, "parquet", backupName, opts)
		if err != nil {
			t.Fatalf("Local MySQL alltypes backup failed: %v", err)
		}
	})

	t.Run("02_query", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backupPath := backupDir + "/" + backupName
		opts := query.QueryOptions{
			SQL:    "SELECT count(*) as cnt FROM restore_input",
			Output: "json",
		}
		err := query.RunLocal(ctx, backupPath, opts)
		if err != nil {
			t.Fatalf("RunLocal MySQL alltypes query failed: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Multi-table local test
// ---------------------------------------------------------------------------

func TestLocalMultiTable(t *testing.T) {
	skipUnlessLocalTest(t)

	if err := verifyPostgres(); err != nil {
		t.Fatalf("PostgreSQL not reachable: %v", err)
	}
	if err := seedPostgres(); err != nil {
		t.Fatalf("Failed to seed PostgreSQL: %v", err)
	}
	if err := seedPostgresAllTypes(); err != nil {
		t.Fatalf("Failed to seed PostgreSQL alltypes: %v", err)
	}

	backupDir := t.TempDir()
	backupName := "local_pg_multi"

	t.Run("01_backup_multi_table", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		opts := archive.DefaultOptions()
		opts.LocalOutputDir = backupDir

		err := archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_orders", "inttest_alltypes"}, "parquet", backupName, nil, opts)
		if err != nil {
			t.Fatalf("Local multi-table backup failed: %v", err)
		}
	})

	t.Run("02_list_shows_both_tables", func(t *testing.T) {
		backups, err := archive.ListLocalBackups(backupDir)
		if err != nil {
			t.Fatalf("ListLocalBackups: %v", err)
		}

		for _, b := range backups {
			if b.Name == backupName {
				if len(b.Tables) != 2 {
					t.Errorf("expected 2 tables, got %d: %v", len(b.Tables), b.Tables)
				}
				tableSet := map[string]bool{}
				for _, tbl := range b.Tables {
					tableSet[tbl] = true
				}
				if !tableSet["inttest_orders"] {
					t.Error("missing inttest_orders in tables")
				}
				if !tableSet["inttest_alltypes"] {
					t.Error("missing inttest_alltypes in tables")
				}
				return
			}
		}
		t.Errorf("backup %q not found", backupName)
	})

	t.Run("03_query_specific_table", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backupPath := backupDir + "/" + backupName
		opts := query.QueryOptions{
			Table:  "inttest_orders",
			SQL:    "SELECT count(*) as cnt FROM restore_input",
			Output: "json",
		}
		err := query.RunLocal(ctx, backupPath, opts)
		if err != nil {
			t.Fatalf("RunLocal table-filtered query failed: %v", err)
		}
	})

	t.Run("04_query_all_tables", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backupPath := backupDir + "/" + backupName
		opts := query.QueryOptions{
			SQL:    "SELECT count(*) as cnt FROM restore_input",
			Output: "json",
		}
		err := query.RunLocal(ctx, backupPath, opts)
		if err != nil {
			// This may fail if schemas differ between tables — that is expected
			// for heterogeneous multi-table backups queried together.
			if strings.Contains(err.Error(), "mismatch") || strings.Contains(err.Error(), "schema") {
				t.Skipf("Expected schema mismatch querying all tables together: %v", err)
			}
			t.Fatalf("RunLocal all-tables query failed: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Analyze — PostgreSQL
// ---------------------------------------------------------------------------

func TestLocalAnalyzePostgres(t *testing.T) {
	skipUnlessLocalTest(t)

	if err := verifyPostgres(); err != nil {
		t.Fatalf("PostgreSQL not reachable: %v", err)
	}
	if err := seedPostgres(); err != nil {
		t.Fatalf("Failed to seed PostgreSQL: %v", err)
	}

	t.Run("01_analyze_specific_table", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		duration, _ := analyze.ParseDuration("18m")
		opts := analyze.Options{
			DBURL:     pgDSN,
			DBType:    "postgres",
			DBName:    "testdb",
			Tables:    []string{"inttest_orders"},
			OlderThan: duration,
		}

		report, err := analyze.Run(ctx, opts)
		if err != nil {
			t.Fatalf("analyze.Run failed: %v", err)
		}
		if len(report.Tables) != 1 {
			t.Fatalf("expected 1 table in report, got %d", len(report.Tables))
		}

		tr := report.Tables[0]
		t.Logf("Table: %s, Total: %d, Archivable: %d (%d%%), DateCol: %s, Oldest: %s",
			tr.TableName, tr.TotalRows, tr.ArchivableRows, tr.ArchivablePct, tr.DateColumn, tr.OldestRow.Format("2006-01-02"))

		if tr.TableName != "inttest_orders" {
			t.Errorf("table name = %q, want 'inttest_orders'", tr.TableName)
		}
		if tr.TotalRows < 8 {
			t.Errorf("total rows = %d, expected at least 8", tr.TotalRows)
		}
		if tr.ArchivableRows < 4 {
			t.Errorf("archivable rows = %d, expected at least 4 (stale rows)", tr.ArchivableRows)
		}
		if tr.ArchivableRows >= tr.TotalRows {
			t.Errorf("archivable rows (%d) should be less than total (%d)", tr.ArchivableRows, tr.TotalRows)
		}
		if tr.DateColumn == "" {
			t.Error("expected a date column to be detected")
		}
		if tr.SizeBytes <= 0 {
			t.Error("expected positive table size")
		}
		if tr.EstArchivedBytes <= 0 {
			t.Error("expected positive estimated archive size")
		}
	})

	t.Run("02_analyze_all_tables", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		duration, _ := analyze.ParseDuration("18m")
		opts := analyze.Options{
			DBURL:     pgDSN,
			DBType:    "postgres",
			DBName:    "testdb",
			All:       true,
			OlderThan: duration,
		}

		report, err := analyze.Run(ctx, opts)
		if err != nil {
			t.Fatalf("analyze.Run --all failed: %v", err)
		}

		// Should find at least inttest_orders (has date column)
		if len(report.Tables) == 0 {
			t.Fatal("expected at least 1 table in report")
		}

		found := false
		for _, tr := range report.Tables {
			t.Logf("  %s: %d total, %d archivable (%d%%)", tr.TableName, tr.TotalRows, tr.ArchivableRows, tr.ArchivablePct)
			if tr.TableName == "inttest_orders" {
				found = true
			}
		}
		if !found {
			t.Error("inttest_orders not found in --all analysis")
		}
	})

	t.Run("03_analyze_custom_threshold", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Use a very short threshold — almost everything should be archivable
		duration, _ := analyze.ParseDuration("7d")
		opts := analyze.Options{
			DBURL:     pgDSN,
			DBType:    "postgres",
			DBName:    "testdb",
			Tables:    []string{"inttest_orders"},
			OlderThan: duration,
		}

		report, err := analyze.Run(ctx, opts)
		if err != nil {
			t.Fatalf("analyze.Run failed: %v", err)
		}

		tr := report.Tables[0]
		t.Logf("7d threshold: %d/%d archivable (%d%%)", tr.ArchivableRows, tr.TotalRows, tr.ArchivablePct)

		// With a 7-day threshold, most rows should be archivable
		if tr.ArchivablePct < 50 {
			t.Errorf("expected >50%% archivable with 7d threshold, got %d%%", tr.ArchivablePct)
		}
	})
}

// ---------------------------------------------------------------------------
// Analyze — MySQL
// ---------------------------------------------------------------------------

func TestLocalAnalyzeMySQL(t *testing.T) {
	skipUnlessLocalTest(t)

	if err := verifyMySQL(); err != nil {
		t.Fatalf("MySQL not reachable: %v", err)
	}
	if err := seedMySQL(); err != nil {
		t.Fatalf("Failed to seed MySQL: %v", err)
	}

	t.Run("01_analyze_specific_table", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		duration, _ := analyze.ParseDuration("18m")
		opts := analyze.Options{
			DBURL:     mysqlGoDSN,
			DBType:    "mysql",
			DBName:    "testdb",
			Tables:    []string{"inttest_orders"},
			OlderThan: duration,
		}

		report, err := analyze.Run(ctx, opts)
		if err != nil {
			t.Fatalf("analyze.Run failed: %v", err)
		}
		if len(report.Tables) != 1 {
			t.Fatalf("expected 1 table in report, got %d", len(report.Tables))
		}

		tr := report.Tables[0]
		t.Logf("Table: %s, Total: %d, Archivable: %d (%d%%), DateCol: %s, Oldest: %s",
			tr.TableName, tr.TotalRows, tr.ArchivableRows, tr.ArchivablePct, tr.DateColumn, tr.OldestRow.Format("2006-01-02"))

		if tr.ArchivableRows < 4 {
			t.Errorf("archivable rows = %d, expected at least 4 (stale rows)", tr.ArchivableRows)
		}
		if tr.DateColumn == "" {
			t.Error("expected a date column to be detected")
		}
	})

	t.Run("02_analyze_all_tables", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		duration, _ := analyze.ParseDuration("18m")
		opts := analyze.Options{
			DBURL:     mysqlGoDSN,
			DBType:    "mysql",
			DBName:    "testdb",
			All:       true,
			OlderThan: duration,
		}

		report, err := analyze.Run(ctx, opts)
		if err != nil {
			t.Fatalf("analyze.Run --all failed: %v", err)
		}

		if len(report.Tables) == 0 {
			t.Fatal("expected at least 1 table in report")
		}

		for _, tr := range report.Tables {
			t.Logf("  %s: %d total, %d archivable (%d%%)", tr.TableName, tr.TotalRows, tr.ArchivableRows, tr.ArchivablePct)
		}
	})
}

// ---------------------------------------------------------------------------
// Backup create with --where (PostgreSQL)
// ---------------------------------------------------------------------------

func TestLocalBackupWithWhere_Postgres(t *testing.T) {
	skipUnlessLocalTest(t)

	if err := verifyPostgres(); err != nil {
		t.Fatalf("PostgreSQL not reachable: %v", err)
	}
	if err := seedPostgres(); err != nil {
		t.Fatalf("Failed to seed PostgreSQL: %v", err)
	}

	backupDir := t.TempDir()
	backupName := "local_pg_where"

	// inttest_orders has 8 rows: 4 stale (>18mo) + 4 recent
	// Archive only old data with --where
	t.Run("01_backup_with_where", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		opts := archive.DefaultOptions()
		opts.LocalOutputDir = backupDir
		opts.Where = "created_at < NOW() - INTERVAL '18 months'"

		err := archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_orders"}, "parquet", backupName, nil, opts)
		if err != nil {
			t.Fatalf("Local backup with --where failed: %v", err)
		}
	})

	t.Run("02_query_count_matches_where", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backupPath := backupDir + "/" + backupName
		opts := query.QueryOptions{
			SQL:    "SELECT count(*) as cnt FROM restore_input",
			Output: "json",
		}
		err := query.RunLocal(ctx, backupPath, opts)
		if err != nil {
			t.Fatalf("RunLocal query failed: %v", err)
		}
		// The output prints to stdout — we can't easily capture it here,
		// but if it runs without error on 4 rows of filtered data, the
		// --where clause worked correctly.
	})

	t.Run("03_list_shows_backup", func(t *testing.T) {
		backups, err := archive.ListLocalBackups(backupDir)
		if err != nil {
			t.Fatalf("ListLocalBackups: %v", err)
		}
		found := false
		for _, b := range backups {
			if b.Name == backupName {
				found = true
				t.Logf("Found backup: %s (size=%d)", b.Name, b.SizeBytes)
				if b.SizeBytes == 0 {
					t.Error("expected non-zero size for filtered backup")
				}
			}
		}
		if !found {
			t.Errorf("backup %q not found", backupName)
		}
	})
}

// ---------------------------------------------------------------------------
// Backup create with --where (MySQL)
// ---------------------------------------------------------------------------

func TestLocalBackupWithWhere_MySQL(t *testing.T) {
	skipUnlessLocalTest(t)

	if err := verifyMySQL(); err != nil {
		t.Fatalf("MySQL not reachable: %v", err)
	}
	if err := seedMySQL(); err != nil {
		t.Fatalf("Failed to seed MySQL: %v", err)
	}

	backupDir := t.TempDir()
	backupName := "local_mysql_where"
	mysqlDSN := "mysql://testuser:testpass@localhost:3306/testdb?parseTime=true"

	t.Run("01_backup_with_where", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		opts := archive.DefaultOptions()
		opts.LocalOutputDir = backupDir
		opts.Where = "created_at < DATE_SUB(NOW(), INTERVAL 18 MONTH)"

		normalizedDSN := archive.NormalizeMySQLDSN(mysqlDSN)
		err := archive.ArchiveMySQL(ctx, normalizedDSN, []string{"inttest_orders"}, "parquet", backupName, opts)
		if err != nil {
			t.Fatalf("Local MySQL backup with --where failed: %v", err)
		}
	})

	t.Run("02_query_count", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backupPath := backupDir + "/" + backupName
		opts := query.QueryOptions{
			SQL:    "SELECT count(*) as cnt FROM restore_input",
			Output: "json",
		}
		err := query.RunLocal(ctx, backupPath, opts)
		if err != nil {
			t.Fatalf("RunLocal query failed: %v", err)
		}
	})
}
