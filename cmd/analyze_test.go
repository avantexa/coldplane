package cmd

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/avantexa/coldplane/internal/analyze"
)

func sampleReport() *analyze.Report {
	return &analyze.Report{
		OlderThan:  "18m",
		CutoffDate: time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC),
		Tables: []analyze.TableReport{
			{TableName: "orders", TotalRows: 1000, SizeBytes: 4 << 20, DateColumn: "created_at", OldestRow: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), ArchivableRows: 250, ArchivablePct: 25, EstArchivedBytes: 1 << 18},
			{TableName: "events", TotalRows: 500, SizeBytes: 1 << 20, DateColumn: "created_at", OldestRow: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), ArchivableRows: 100, ArchivablePct: 20, EstArchivedBytes: 1 << 16},
			{TableName: "logs", TotalRows: 300, SizeBytes: 1 << 20, DateColumn: "logged_at", OldestRow: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), ArchivableRows: 30, ArchivablePct: 10, EstArchivedBytes: 1 << 10},
			{TableName: "fresh", TotalRows: 10, SizeBytes: 1 << 10, DateColumn: "created_at", OldestRow: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)},
		},
	}
}

func TestArchiveSuggestions(t *testing.T) {
	got := archiveSuggestions(sampleReport(), "postgres://u:p@h/db")
	if len(got) != 2 {
		t.Fatalf("want one suggestion per date column with archivable rows, got %d:\n%s", len(got), strings.Join(got, "\n"))
	}
	if !strings.Contains(got[0], `--tables "orders,events"`) || !strings.Contains(got[0], `--where "created_at < '2024-05-01'"`) {
		t.Errorf("first suggestion should group orders and events on created_at:\n%s", got[0])
	}
	if !strings.Contains(got[1], `--tables "logs"`) || !strings.Contains(got[1], "logged_at < '2024-05-01'") {
		t.Errorf("second suggestion should be logs on logged_at:\n%s", got[1])
	}
	for _, s := range got {
		if strings.Contains(s, "fresh") {
			t.Error("a table with nothing to archive must not be suggested")
		}
		if !strings.Contains(s, `--db-url "postgres://u:p@h/db"`) {
			t.Error("suggestion should carry the original db url")
		}
	}
	if got := archiveSuggestions(&analyze.Report{}, "x"); len(got) != 0 {
		t.Errorf("empty report: %v", got)
	}
}

func TestRenderAnalyzeReport(t *testing.T) {
	var buf bytes.Buffer
	renderAnalyzeReport(&buf, sampleReport())
	out := buf.String()
	for _, want := range []string{"orders", "logs", "created_at", "2020-01-01", "250 (25%)", "380 rows (20%) across 4 tables are older than 18m.", "Estimated archive size"} {
		if !strings.Contains(out, want) {
			t.Errorf("report missing %q:\n%s", want, out)
		}
	}
}
