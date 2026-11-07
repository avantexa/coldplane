//go:build integration

package integration

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/avantexa/coldplane/internal/archive"
)

// purgeScenario archives the rows with amount > 300, verifies, then purges
// them from the source, checking every refusal along the way.
func purgeScenario(t *testing.T, driver, dsn string, run func(ctx context.Context, name string, opts archive.ArchiveOptions) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	count := func(where string) int64 {
		var n int64
		q := "SELECT count(*) FROM inttest_orders"
		if where != "" {
			q += " WHERE " + where
		}
		if err := db.QueryRow(q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	total, cold := count(""), count("amount > 300")
	if cold == 0 || cold == total {
		t.Fatalf("seed should have some rows above 300 and some below: %d of %d", cold, total)
	}

	opts := archive.DefaultOptions()
	opts.OutputDir = t.TempDir()
	opts.Where = "amount > 300"
	if err := run(ctx, "p", opts); err != nil {
		t.Fatal(err)
	}
	dir := opts.OutputDir + "/p"
	popts := archive.PurgeOptions{BatchSize: 2, Execute: true}

	// Unverified: refused, nothing deleted.
	if _, err := archive.Purge(ctx, dir, dsn, popts); err == nil || !strings.Contains(err.Error(), "not verified") {
		t.Fatalf("unverified backup must be refused: %v", err)
	}
	if count("") != total {
		t.Fatal("refusal must not delete anything")
	}
	if _, err := archive.Verify(ctx, dir, dsn, archive.VerifyOptions{Checksum: true}); err != nil {
		t.Fatal(err)
	}

	// Wrong database: refused.
	other := strings.Replace(dsn, "/testdb", "/otherdb", 1)
	if _, err := archive.Purge(ctx, dir, other, popts); err == nil || !strings.Contains(err.Error(), "otherdb") {
		t.Fatalf("another database must be refused: %v", err)
	}

	// Dry run: reports the archived rows, deletes nothing.
	plan, err := archive.Purge(ctx, dir, dsn, archive.PurgeOptions{BatchSize: 2})
	if err != nil || len(plan) != 1 || plan[0].Rows != cold {
		t.Fatalf("dry run: %v %+v (want %d rows)", err, plan, cold)
	}
	if count("") != total {
		t.Fatal("dry run must not delete anything")
	}

	// Source changed since verification: refused.
	if _, err := db.Exec(`INSERT INTO inttest_orders (customer_name, amount, created_at) VALUES ('Late', 999.00, '2020-01-01 00:00:00')`); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.Purge(ctx, dir, dsn, popts); err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("a changed source must be refused: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM inttest_orders WHERE customer_name = 'Late'`); err != nil {
		t.Fatal(err)
	}

	// Execute: exactly the archived rows go, the rest stay, and the manifest records it.
	done, err := archive.Purge(ctx, dir, dsn, popts)
	if err != nil || len(done) != 1 || done[0].Rows != cold {
		t.Fatalf("purge: %v %+v (want %d rows)", err, done, cold)
	}
	if got := count(""); got != total-cold {
		t.Errorf("%d rows remain, want %d", got, total-cold)
	}
	if count("amount > 300") != 0 {
		t.Error("archived rows should be gone")
	}
	m, _ := archive.ReadManifest(dir)
	if m.PurgedAt == "" || m.Tables[0].Purged == nil || m.Tables[0].Purged.Rows != cold {
		t.Errorf("manifest should record the purge: purged_at=%q %+v", m.PurgedAt, m.Tables[0].Purged)
	}
}

func TestLocalPurge_Postgres(t *testing.T) {
	skipUnlessLocalTest(t)
	if err := seedPostgres(); err != nil {
		t.Fatal(err)
	}
	purgeScenario(t, "postgres", pgDSN, func(ctx context.Context, name string, opts archive.ArchiveOptions) error {
		return archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_orders"}, name, opts)
	})
}

func TestLocalPurge_MySQL(t *testing.T) {
	skipUnlessLocalTest(t)
	if err := seedMySQL(); err != nil {
		t.Fatal(err)
	}
	purgeScenario(t, "mysql", mysqlGoDSN, func(ctx context.Context, name string, opts archive.ArchiveOptions) error {
		return archive.ArchiveMySQL(ctx, mysqlGoDSN, []string{"inttest_orders"}, name, opts)
	})
}
