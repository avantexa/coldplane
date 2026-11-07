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
