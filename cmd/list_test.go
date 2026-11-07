package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/avantexa/coldplane/internal/archive"
)

func TestRenderLocalBackups(t *testing.T) {
	var buf bytes.Buffer
	renderBackups(&buf, nil)
	if buf.String() != "No backups found.\n" {
		t.Errorf("empty: %q", buf.String())
	}

	buf.Reset()
	renderBackups(&buf, []archive.BackupInfo{{
		Name: "orders_x", DBName: "shop", DBType: "postgres", Tables: []string{"orders", "items"},
		CreatedAt: "2025-01-01T00:00:00Z", SizeBytes: 3 << 20, Path: "/b/orders_x",
	}})
	out := buf.String()
	for _, want := range []string{"orders_x", "shop", "postgres", "orders, items", "2025-01-01T00:00:00Z", "3.00 MB", "/b/orders_x", "NAME"} {
		if !strings.Contains(out, want) {
			t.Errorf("table output missing %q:\n%s", want, out)
		}
	}
}

func TestRenderVerification(t *testing.T) {
	var buf bytes.Buffer
	m := archive.Manifest{Tables: []archive.TableEntry{
		{Name: "orders", Verified: &archive.TableVerification{SourceRows: 8, ArchiveRows: 8, SourceChecksum: "x", ArchiveChecksum: "x", Status: "pass"}},
		{Name: "events", Verified: &archive.TableVerification{SourceRows: 9, ArchiveRows: 8, SourceChecksum: "x", ArchiveChecksum: "y", Status: "count_mismatch"}},
		{Name: "counts", Verified: &archive.TableVerification{SourceRows: 3, ArchiveRows: 3, Status: "pass"}},
		{Name: "broken", Verified: &archive.TableVerification{Status: "error", Error: "source: boom"}},
		{Name: "untouched"},
	}}
	renderVerification(&buf, m)
	out := buf.String()
	for _, want := range []string{"orders", "match", "events", "differs", "count_mismatch", "counts", "skipped", "error: source: boom", "untouched", "not verified"} {
		if !strings.Contains(out, want) {
			t.Errorf("verification table missing %q:\n%s", want, out)
		}
	}
}

func TestRenderPurge(t *testing.T) {
	var buf bytes.Buffer
	results := []archive.PurgeResult{{Table: "orders", Rows: 1200}, {Table: "events", Rows: 34}}
	renderPurge(&buf, results, false)
	if out := buf.String(); !strings.Contains(out, "ROWS TO DELETE") || !strings.Contains(out, "1234 rows would be deleted") || !strings.Contains(out, "--yes") {
		t.Errorf("dry run output:\n%s", out)
	}
	buf.Reset()
	renderPurge(&buf, results, true)
	if out := buf.String(); !strings.Contains(out, "ROWS DELETED") || !strings.Contains(out, "Deleted 1234 rows") {
		t.Errorf("executed output:\n%s", out)
	}
}

func TestRenderRestore(t *testing.T) {
	var buf bytes.Buffer
	renderRestore(&buf, nil)
	if buf.Len() != 0 {
		t.Error("nothing restored prints nothing")
	}
	renderRestore(&buf, []archive.RestoreResult{{Table: "orders", Target: "orders_2024", Rows: 12}, {Table: "events", Target: "events", Rows: 30}})
	if out := buf.String(); !strings.Contains(out, "orders_2024") || !strings.Contains(out, "Restored 42 rows") {
		t.Errorf("restore output:\n%s", out)
	}
}

func TestRenderPrune(t *testing.T) {
	var buf bytes.Buffer
	renderPrune(&buf, nil, false)
	if buf.String() != "No backups to prune.\n" {
		t.Errorf("empty: %q", buf.String())
	}
	buf.Reset()
	renderPrune(&buf, []archive.BackupInfo{{Name: "old", CreatedAt: "2023-01-01T00:00:00Z", Verified: "yes"}}, false)
	if out := buf.String(); !strings.Contains(out, "old") || !strings.Contains(out, "1 backups would be removed") {
		t.Errorf("dry run: %s", out)
	}
	buf.Reset()
	renderPrune(&buf, []archive.BackupInfo{{Name: "old"}}, true)
	if !strings.Contains(buf.String(), "Removed 1 backups") {
		t.Errorf("executed: %s", buf.String())
	}
}

func TestBackupCreate_FlagCombinations(t *testing.T) {
	run := func(args ...string) error {
		dbURL, tables, whereFlag, olderThanFlag, sinceLast = "", "", "", "", false
		rootCmd.SetArgs(append([]string{"backup", "create", "--db-url", "postgres://x", "--tables", "t"}, args...))
		return rootCmd.Execute()
	}
	if err := run("--where", "x > 0", "--older-than", "1y"); err == nil || !strings.Contains(err.Error(), "not both") {
		t.Errorf("where and older-than together: %v", err)
	}
	if err := run("--since-last"); err == nil || !strings.Contains(err.Error(), "needs --older-than") {
		t.Errorf("since-last alone: %v", err)
	}
}
