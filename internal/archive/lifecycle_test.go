package archive

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWhereFor(t *testing.T) {
	m := Manifest{Where: "global"}
	if got := m.whereFor(TableEntry{}); got != "global" {
		t.Errorf("falls back to the backup filter, got %q", got)
	}
	if got := m.whereFor(TableEntry{Where: "created_at < '2024'"}); got != "created_at < '2024'" {
		t.Errorf("a table filter wins, got %q", got)
	}
}

func TestCutoffWhere(t *testing.T) {
	cutoff := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
	since := time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC)
	if got := cutoffWhere("mysql", chunkKey{column: "created_at"}, time.Time{}, cutoff); got != "`created_at` < '2025-05-01 00:00:00.000000'" {
		t.Errorf("mysql: %s", got)
	}
	if got := cutoffWhere("mysql", chunkKey{column: "created_at"}, since, cutoff); got != "`created_at` >= '2024-11-01 00:00:00.000000' AND `created_at` < '2025-05-01 00:00:00.000000'" {
		t.Errorf("mysql since: %s", got)
	}
	if got := cutoffWhere("postgres", chunkKey{column: "ts", withZone: true}, time.Time{}, cutoff); got != `"ts" < '2025-05-01 00:00:00.000000+00:00'::timestamptz` {
		t.Errorf("pg tz: %s", got)
	}
	if got := cutoffWhere("postgres", chunkKey{column: "ts"}, since, cutoff); got != `"ts" >= '2024-11-01 00:00:00.000000'::timestamp AND "ts" < '2025-05-01 00:00:00.000000'::timestamp` {
		t.Errorf("pg naive since: %s", got)
	}
}

func TestLastCutoff(t *testing.T) {
	base := t.TempDir()
	write := func(name, db, created, cutoff, verified string, tables ...string) {
		m := Manifest{Name: name, DBName: db, CreatedAt: created, Cutoff: cutoff, VerifiedAt: verified}
		for _, tb := range tables {
			m.Tables = append(m.Tables, TableEntry{Name: tb})
		}
		if err := writeManifest(filepath.Join(base, name), m); err != nil {
			t.Fatal(err)
		}
	}
	write("old", "shop", "2025-01-01T00:00:00Z", "2024-01-01T00:00:00Z", "2025-01-01T01:00:00Z", "orders")
	write("newer", "shop", "2025-02-01T00:00:00Z", "2024-02-01T00:00:00Z", "2025-02-01T01:00:00Z", "orders", "events")
	write("unverified", "shop", "2025-03-01T00:00:00Z", "2024-03-01T00:00:00Z", "", "orders")
	write("otherdb", "crm", "2025-04-01T00:00:00Z", "2024-04-01T00:00:00Z", "2025-04-01T01:00:00Z", "orders")
	write("nocutoff", "shop", "2025-05-01T00:00:00Z", "", "2025-05-01T01:00:00Z", "orders")

	got, ok := lastCutoff(base, "shop", "orders")
	if !ok || !got.Equal(time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("newest verified cutoff for shop/orders should be from 'newer', got %v %v", got, ok)
	}
	if _, ok := lastCutoff(base, "shop", "nothing"); ok {
		t.Error("a table never archived has no last cutoff")
	}
	if _, ok := lastCutoff(t.TempDir(), "shop", "orders"); ok {
		t.Error("an empty base dir has no last cutoff")
	}
}

func TestSelectPrunable(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	infos := []BackupInfo{
		{Name: "ancient", CreatedAt: "2023-06-01T00:00:00Z", Verified: "yes"},
		{Name: "old-unverified", CreatedAt: "2023-07-01T00:00:00Z", Verified: "no"},
		{Name: "recent", CreatedAt: "2025-12-01T00:00:00Z", Verified: "yes"},
		{Name: "garbage-date", CreatedAt: "not a date", Verified: "yes"},
	}
	got := selectPrunable(infos, now.Add(-2*365*24*time.Hour), false)
	if len(got) != 1 || got[0].Name != "ancient" {
		t.Errorf("only old verified backups prune by default, got %v", names(got))
	}
	got = selectPrunable(infos, now.Add(-2*365*24*time.Hour), true)
	if len(got) != 2 {
		t.Errorf("with unverified included, got %v", names(got))
	}
}

func names(infos []BackupInfo) []string {
	var out []string
	for _, i := range infos {
		out = append(out, i.Name)
	}
	return out
}

func TestPruneDeletesSelected(t *testing.T) {
	base := t.TempDir()
	for _, n := range []string{"a", "b"} {
		if err := writeManifest(filepath.Join(base, n), Manifest{Name: n, CreatedAt: "2020-01-01T00:00:00Z", VerifiedAt: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := Prune(base, time.Now(), PruneOptions{Execute: false})
	if err != nil || len(removed) != 2 {
		t.Fatalf("dry run: %v %v", removed, err)
	}
	if _, err := os.Stat(filepath.Join(base, "a")); err != nil {
		t.Error("dry run must not delete")
	}
	removed, err = Prune(base, time.Now(), PruneOptions{Execute: true})
	if err != nil || len(removed) != 2 {
		t.Fatalf("execute: %v %v", removed, err)
	}
	if _, err := os.Stat(filepath.Join(base, "a")); !os.IsNotExist(err) {
		t.Error("executed prune should remove the backup directory")
	}
}

func TestCommonSince(t *testing.T) {
	if got := commonSince([]TableEntry{{Since: "a"}, {Since: "a"}}); got != "a" {
		t.Errorf("shared since: %q", got)
	}
	if got := commonSince([]TableEntry{{Since: "a"}, {Since: "b"}}); got != "" {
		t.Errorf("differing since: %q", got)
	}
	if got := commonSince(nil); got != "" {
		t.Errorf("no tables: %q", got)
	}
}
