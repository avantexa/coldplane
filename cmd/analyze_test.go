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

func TestArchiveSuggestion_OlderThan(t *testing.T) {
	got := archiveSuggestion(sampleReport(), "postgres://u:p@h/db", "")
	for _, want := range []string{`--db-url "postgres://u:p@h/db"`, `--tables "orders,events,logs"`, "--older-than 18m"} {
		if !strings.Contains(got, want) {
			t.Errorf("suggestion missing %s:\n%s", want, got)
		}
	}
	// backup create picks each table's first date column itself, the one analyze detected.
	if strings.Contains(got, "--where") {
		t.Errorf("auto-detected columns need no --where:\n%s", got)
	}
	if strings.Contains(got, "fresh") {
		t.Error("a table with nothing to archive must not be suggested")
	}
}

func TestArchiveSuggestion_DateColumnOverride(t *testing.T) {
	report := sampleReport()
	for i := range report.Tables {
		report.Tables[i].DateColumn = "updated_at"
	}
	// --older-than cannot name a column, so an override keeps the cutoff as a --where on it.
	got := archiveSuggestion(report, "postgres://u:p@h/db", "updated_at")
	if !strings.Contains(got, `--where "updated_at < '2024-05-01'"`) || strings.Contains(got, "--older-than") {
		t.Errorf("override should suggest a --where on that column:\n%s", got)
	}
	if !strings.Contains(got, `--tables "orders,events,logs"`) {
		t.Errorf("override suggestion should list the archivable tables:\n%s", got)
	}
}

func TestArchiveSuggestion_NothingToArchive(t *testing.T) {
	if got := archiveSuggestion(&analyze.Report{OlderThan: "1y"}, "x", ""); got != "" {
		t.Errorf("empty report: %q", got)
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
