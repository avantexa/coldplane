//go:build integration

package integration

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/avantexa/coldplane/internal/archive"
)

// verifyScenario archives inttest_orders, verifies it, then breaks the
// source and the archive in turn and expects verification to notice.
func verifyScenario(t *testing.T, driver, dsn string, run func(ctx context.Context, name string, opts archive.ArchiveOptions) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	opts := archive.DefaultOptions()
	opts.OutputDir = t.TempDir()
	opts.Where = "amount > 0"
	if err := run(ctx, "v", opts); err != nil {
		t.Fatalf("archive: %v", err)
	}
	dir := filepath.Join(opts.OutputDir, "v")

	m, err := archive.Verify(ctx, dir, dsn, archive.VerifyOptions{Checksum: true})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	v := m.Tables[0].Verified
	if m.VerifiedAt == "" || v == nil || v.Status != "pass" || v.SourceRows != 8 || v.ArchiveRows != 8 || v.SourceChecksum != v.ArchiveChecksum {
		t.Fatalf("fresh archive should verify: verified_at=%q table=%+v", m.VerifiedAt, v)
	}
	if again, _ := archive.ReadManifest(dir); again.VerifiedAt == "" || again.Tables[0].Verified == nil {
		t.Error("verification must be recorded in index.json")
	}
	if infos, _ := archive.ListBackups(opts.OutputDir); len(infos) != 1 || infos[0].Verified != "yes" {
		t.Errorf("list should show the backup as verified: %+v", infos)
	}

	// A row added to the source after the backup: counts differ.
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO inttest_orders (customer_name, amount, created_at) VALUES ('Late', 5.00, '2020-01-01 00:00:00')`); err != nil {
		t.Fatal(err)
	}
	m, err = archive.Verify(ctx, dir, dsn, archive.VerifyOptions{Checksum: true})
	if err != nil {
		t.Fatal(err)
	}
	if m.VerifiedAt != "" || m.Tables[0].Verified.Status != "count_mismatch" || m.Tables[0].Verified.SourceRows != 9 {
		t.Errorf("extra source row should fail: verified_at=%q %+v", m.VerifiedAt, m.Tables[0].Verified)
	}
	if infos, _ := archive.ListBackups(opts.OutputDir); infos[0].Verified != "partial" {
		t.Errorf("a failed verification shows as partial, got %q", infos[0].Verified)
	}
	if _, err := db.Exec(`DELETE FROM inttest_orders WHERE customer_name = 'Late'`); err != nil {
		t.Fatal(err)
	}

	// A changed value in the source with the same row count: checksums differ.
	if _, err := db.Exec(`UPDATE inttest_orders SET amount = amount + 1 WHERE customer_name = 'Zara'`); err != nil {
		t.Fatal(err)
	}
	m, _ = archive.Verify(ctx, dir, dsn, archive.VerifyOptions{Checksum: true})
	if m.Tables[0].Verified.Status != "checksum_mismatch" {
		t.Errorf("changed value should fail the checksum: %+v", m.Tables[0].Verified)
	}
	m, _ = archive.Verify(ctx, dir, dsn, archive.VerifyOptions{Checksum: false})
	if m.Tables[0].Verified.Status != "pass" {
		t.Errorf("without checksums equal counts pass: %+v", m.Tables[0].Verified)
	}
	if _, err := db.Exec(`UPDATE inttest_orders SET amount = amount - 1 WHERE customer_name = 'Zara'`); err != nil {
		t.Fatal(err)
	}

	// A Parquet file missing from the archive: archive rows fall short.
	first := filepath.Join(dir, filepath.FromSlash(m.Tables[0].Files[0].Key))
	if err := os.Remove(first); err != nil {
		t.Fatal(err)
	}
	m, _ = archive.Verify(ctx, dir, dsn, archive.VerifyOptions{Checksum: true})
	if m.VerifiedAt != "" || m.Tables[0].Verified.Status != "count_mismatch" || m.Tables[0].Verified.ArchiveRows >= 8 {
		t.Errorf("missing parquet file should fail: %+v", m.Tables[0].Verified)
	}
}

func TestLocalVerify_Postgres(t *testing.T) {
	skipUnlessLocalTest(t)
	if err := seedPostgres(); err != nil {
		t.Fatal(err)
	}
	verifyScenario(t, "postgres", pgDSN, func(ctx context.Context, name string, opts archive.ArchiveOptions) error {
		return archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_orders"}, name, opts)
	})
}

func TestLocalVerify_MySQL(t *testing.T) {
	skipUnlessLocalTest(t)
	if err := seedMySQL(); err != nil {
		t.Fatal(err)
	}
	verifyScenario(t, "mysql", mysqlGoDSN, func(ctx context.Context, name string, opts archive.ArchiveOptions) error {
		return archive.ArchiveMySQL(ctx, mysqlGoDSN, []string{"inttest_orders"}, name, opts)
	})
}
