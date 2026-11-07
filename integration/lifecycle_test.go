//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/avantexa/coldplane/internal/archive"
)

// TestLocalOlderThanSinceLast runs the monthly-job pattern: archive rows older
// than 30 months, verify, then archive rows older than 18 months with
// --since-last. The second run must take only the rows the first one left,
// and the two together must equal one plain run over 18 months.
func TestLocalOlderThanSinceLast(t *testing.T) {
	skipUnlessLocalTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	engines := []struct {
		name    string
		seed    func() error
		archive func(ctx context.Context, name string, opts archive.ArchiveOptions) error
		dsn     string
	}{
		{"postgres", seedPostgres, func(ctx context.Context, name string, opts archive.ArchiveOptions) error {
			return archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_orders"}, name, opts)
		}, pgDSN},
		{"mysql", seedMySQL, func(ctx context.Context, name string, opts archive.ArchiveOptions) error {
			return archive.ArchiveMySQL(ctx, mysqlGoDSN, []string{"inttest_orders"}, name, opts)
		}, mysqlGoDSN},
	}
	month := 30 * 24 * time.Hour
	for _, e := range engines {
		t.Run(e.name, func(t *testing.T) {
			if err := e.seed(); err != nil {
				t.Fatal(err)
			}
			base := t.TempDir()
			now := time.Now().UTC()

			first := archive.DefaultOptions()
			first.OutputDir = base
			first.OlderThan, first.Cutoff = "30m", now.Add(-30*month)
			if err := e.archive(ctx, "run1", first); err != nil {
				t.Fatal(err)
			}
			m1, err := archive.Verify(ctx, base+"/run1", e.dsn, archive.VerifyOptions{Checksum: true})
			if err != nil || m1.VerifiedAt == "" {
				t.Fatalf("run1 should verify: %v %+v", err, m1.Tables[0].Verified)
			}
			if m1.Cutoff == "" || m1.OlderThan != "30m" || m1.Tables[0].Where == "" {
				t.Errorf("run1 manifest should record the cutoff and filter: %+v", m1)
			}
			rows1 := m1.Tables[0].RowCount // Zara (48 months) and Yuri (36)
			if rows1 != 2 {
				t.Errorf("run1 archived %d rows, want 2", rows1)
			}

			second := archive.DefaultOptions()
			second.OutputDir = base
			second.OlderThan, second.Cutoff, second.SinceLast = "18m", now.Add(-18*month), true
			if err := e.archive(ctx, "run2", second); err != nil {
				t.Fatal(err)
			}
			m2, err := archive.Verify(ctx, base+"/run2", e.dsn, archive.VerifyOptions{Checksum: true})
			if err != nil || m2.VerifiedAt == "" {
				t.Fatalf("run2 should verify: %v %+v", err, m2.Tables[0].Verified)
			}
			if m2.Since != m1.Cutoff {
				t.Errorf("run2 should start at run1's cutoff: since=%q cutoff1=%q", m2.Since, m1.Cutoff)
			}
			rows2 := m2.Tables[0].RowCount // Xena (24) and Wade (20)
			if rows2 != 2 {
				t.Errorf("run2 archived %d rows, want 2", rows2)
			}

			full := archive.DefaultOptions()
			full.OutputDir = t.TempDir()
			full.OlderThan, full.Cutoff = "18m", now.Add(-18*month)
			if err := e.archive(ctx, "full", full); err != nil {
				t.Fatal(err)
			}
			mf, _ := archive.ReadManifest(full.OutputDir + "/full")
			if mf.Tables[0].RowCount != rows1+rows2 {
				t.Errorf("the two incremental runs (%d+%d) should equal one full run (%d)", rows1, rows2, mf.Tables[0].RowCount)
			}
		})
	}
}

func TestLocalOlderThanNeedsTimeColumn(t *testing.T) {
	skipUnlessLocalTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := seedChunkingTables("postgres", pgDSN); err != nil {
		t.Fatal(err)
	}
	opts := archive.DefaultOptions()
	opts.OutputDir = t.TempDir()
	opts.OlderThan, opts.Cutoff = "1y", time.Now().UTC().AddDate(-1, 0, 0)
	err := archive.ArchivePostgres(ctx, pgDSN, []string{"inttest_plain"}, "x", opts)
	if err == nil {
		t.Fatal("a table without a date column cannot be archived by age")
	}
}
