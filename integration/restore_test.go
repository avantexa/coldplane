//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/avantexa/coldplane/internal/archive"
)

func openDB(t *testing.T, driver, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func dropTable(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
		t.Fatal(err)
	}
}

func rowCountAndSum(t *testing.T, db *sql.DB, table string) (int64, float64) {
	t.Helper()
	var n int64
	var sum float64
	if err := db.QueryRow(fmt.Sprintf("SELECT count(*), coalesce(sum(amount), 0) FROM %s", table)).Scan(&n, &sum); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n, sum
}

// TestLocalRestore covers the four directions between the two engines on
// inttest_orders, the --table filter, --as, and the missing-table refusal.
func TestLocalRestore(t *testing.T) {
	skipUnlessLocalTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := seedPostgres(); err != nil {
		t.Fatal(err)
	}
	if err := seedMySQL(); err != nil {
		t.Fatal(err)
	}
	pg := openDB(t, "postgres", pgDSN)
	my := openDB(t, "mysql", mysqlGoDSN)
	engines := map[string]struct {
		db      *sql.DB
		dsn     string
		archive func(ctx context.Context, name string, opts archive.ArchiveOptions) error
	}{
		"postgres": {pg, pgDSN, func(ctx context.Context, name string, opts archive.ArchiveOptions) error {
			return archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_orders"}, name, opts)
		}},
		"mysql": {my, mysqlGoDSN, func(ctx context.Context, name string, opts archive.ArchiveOptions) error {
			return archive.ArchiveMySQL(ctx, mysqlGoDSN, []string{"inttest_orders"}, name, opts)
		}},
	}

	for srcName, src := range engines {
		opts := archive.DefaultOptions()
		opts.OutputDir = t.TempDir()
		opts.Where = "amount > 100"
		if err := src.archive(ctx, "r", opts); err != nil {
			t.Fatalf("%s archive: %v", srcName, err)
		}
		dir := opts.OutputDir + "/r"
		wantRows, wantSum := rowCountAndSum(t, src.db, "inttest_orders WHERE amount > 100")

		for dstName, dst := range engines {
			t.Run(srcName+"->"+dstName, func(t *testing.T) {
				target := "inttest_restored_" + srcName
				dropTable(t, dst.db, target)

				// Missing target without --create is refused and explains itself.
				_, err := archive.Restore(ctx, dir, dst.dsn, archive.RestoreOptions{Tables: []string{"inttest_orders"}, As: target})
				if err == nil || !strings.Contains(err.Error(), "--create") || !strings.Contains(err.Error(), "customer_name") {
					t.Fatalf("missing table should be refused with the columns: %v", err)
				}

				results, err := archive.Restore(ctx, dir, dst.dsn, archive.RestoreOptions{Tables: []string{"inttest_orders"}, As: target, Create: true})
				if err != nil {
					t.Fatalf("restore: %v", err)
				}
				if len(results) != 1 || results[0].Target != target || results[0].Rows != wantRows {
					t.Fatalf("results: %+v (want %d rows into %s)", results, wantRows, target)
				}
				gotRows, gotSum := rowCountAndSum(t, dst.db, target)
				if gotRows != wantRows || gotSum != wantSum {
					t.Errorf("restored %d rows summing %.2f, want %d summing %.2f", gotRows, gotSum, wantRows, wantSum)
				}

				// A second restore appends into the existing table.
				if _, err := archive.Restore(ctx, dir, dst.dsn, archive.RestoreOptions{Tables: []string{"inttest_orders"}, As: target}); err != nil {
					t.Fatal(err)
				}
				if n, _ := rowCountAndSum(t, dst.db, target); n != 2*wantRows {
					t.Errorf("append: %d rows, want %d", n, 2*wantRows)
				}
				dropTable(t, dst.db, target)
			})
		}
	}

	t.Run("unknown table", func(t *testing.T) {
		opts := archive.DefaultOptions()
		opts.OutputDir = t.TempDir()
		if err := archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_orders"}, "u", opts); err != nil {
			t.Fatal(err)
		}
		if _, err := archive.Restore(ctx, opts.OutputDir+"/u", pgDSN, archive.RestoreOptions{Tables: []string{"nope"}}); err == nil {
			t.Error("unknown table must be refused")
		}
	})
}

// TestLocalRestoreAllTypes restores the all-types table into the same engine
// with --create, which exercises DuckDB's type mapping on the way back.
func TestLocalRestoreAllTypes(t *testing.T) {
	skipUnlessLocalTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cases := []struct {
		name, driver, dsn string
		seed              func() error
		archive           func(ctx context.Context, name string, opts archive.ArchiveOptions) error
	}{
		{"postgres", "postgres", pgDSN, seedPostgresAllTypes, func(ctx context.Context, name string, opts archive.ArchiveOptions) error {
			return archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_alltypes"}, name, opts)
		}},
		{"mysql", "mysql", mysqlGoDSN, seedMySQLAllTypes, func(ctx context.Context, name string, opts archive.ArchiveOptions) error {
			return archive.ArchiveMySQL(ctx, mysqlGoDSN, []string{"inttest_alltypes"}, name, opts)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.seed(); err != nil {
				t.Fatal(err)
			}
			db := openDB(t, c.driver, c.dsn)
			dropTable(t, db, "inttest_alltypes_restored")
			opts := archive.DefaultOptions()
			opts.OutputDir = t.TempDir()
			if err := c.archive(ctx, "a", opts); err != nil {
				t.Fatal(err)
			}
			results, err := archive.Restore(ctx, opts.OutputDir+"/a", c.dsn, archive.RestoreOptions{As: "inttest_alltypes_restored", Create: true})
			if err != nil {
				t.Fatalf("restore: %v", err)
			}
			var want, got int64
			db.QueryRow("SELECT count(*) FROM inttest_alltypes").Scan(&want)
			db.QueryRow("SELECT count(*) FROM inttest_alltypes_restored").Scan(&got)
			if got != want || results[0].Rows != want {
				t.Errorf("restored %d rows, want %d (%+v)", got, want, results)
			}
			dropTable(t, db, "inttest_alltypes_restored")
		})
	}
}
