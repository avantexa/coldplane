package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/avantexa/coldplane/internal/archive"
)

func TestRenderLocalBackups(t *testing.T) {
	var buf bytes.Buffer
	renderLocalBackups(&buf, nil)
	if buf.String() != "No backups found.\n" {
		t.Errorf("empty: %q", buf.String())
	}

	buf.Reset()
	renderLocalBackups(&buf, []archive.LocalBackupInfo{{
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
