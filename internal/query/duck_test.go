package query

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

// captureStdout redirects os.Stdout for the duration of fn and returns what was written.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func TestPrintQueryResults_Empty(t *testing.T) {
	out := captureStdout(t, func() {
		if err := printQueryResults(nil, "table"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	if !strings.Contains(out, "No results found") {
		t.Errorf("expected 'No results found' message, got: %q", out)
	}
}

func TestPrintQueryResults_CSVColumnOrder(t *testing.T) {
	// Deliberately construct rows with keys that would appear in reverse alphabetical
	// order if map iteration were random. The output must always be alphabetical.
	rows := []map[string]interface{}{
		{"zebra": "z1", "apple": "a1", "mango": "m1"},
		{"zebra": "z2", "apple": "a2", "mango": "m2"},
	}

	out := captureStdout(t, func() {
		if err := printQueryResults(rows, "csv"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	r := csv.NewReader(strings.NewReader(out))
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("failed to parse CSV output: %v", err)
	}
	if len(records) != 3 { // header + 2 data rows
		t.Fatalf("expected 3 CSV rows, got %d", len(records))
	}

	header := records[0]
	if len(header) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(header))
	}
	// Columns must be sorted alphabetically
	if header[0] != "apple" || header[1] != "mango" || header[2] != "zebra" {
		t.Errorf("CSV header columns not sorted: %v", header)
	}

	// Values must align with sorted column order
	if records[1][0] != "a1" || records[1][1] != "m1" || records[1][2] != "z1" {
		t.Errorf("CSV row 1 values misaligned: %v", records[1])
	}
}

func TestPrintQueryResults_JSONFormat(t *testing.T) {
	rows := []map[string]interface{}{
		{"id": 1.0, "name": "alice"},
		{"id": 2.0, "name": "bob"},
	}

	out := captureStdout(t, func() {
		if err := printQueryResults(rows, "json"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Output is newline-delimited JSON; each line should be a valid JSON object
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 JSON lines, got %d", len(lines))
	}

	var first map[string]interface{}
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("failed to parse first JSON line: %v", err)
	}
	if first["name"] != "alice" {
		t.Errorf("expected name=alice in first row, got: %v", first["name"])
	}
}

func TestPrintQueryResults_TableFormat(t *testing.T) {
	rows := []map[string]interface{}{
		{"user_id": "usr_001", "total": 42.0},
	}

	out := captureStdout(t, func() {
		if err := printQueryResults(rows, "table"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Header and separator must appear before the data row
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 3 {
		t.Fatalf("expected at least 3 lines (header, separator, data), got %d:\n%s", len(lines), out)
	}

	header := lines[0]
	if !strings.Contains(header, "total") || !strings.Contains(header, "user_id") {
		t.Errorf("header missing expected column names: %q", header)
	}

	// Columns must appear in alphabetical order: total before user_id
	if strings.Index(header, "total") > strings.Index(header, "user_id") {
		t.Errorf("columns not in alphabetical order in header: %q", header)
	}

	separator := lines[1]
	if !strings.Contains(separator, "---") {
		t.Errorf("expected separator line with dashes, got: %q", separator)
	}

	dataRow := lines[2]
	if !strings.Contains(dataRow, "usr_001") {
		t.Errorf("data row missing expected value: %q", dataRow)
	}
}

func TestPrintQueryResults_TableFormatNilValue(t *testing.T) {
	rows := []map[string]interface{}{
		{"col_a": nil, "col_b": "present"},
	}

	out := captureStdout(t, func() {
		if err := printQueryResults(rows, "table"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Nil values should render as empty string, not "nil" or "<nil>"
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 3 {
		t.Fatalf("expected at least 3 lines, got %d", len(lines))
	}
	dataRow := lines[2]
	if strings.Contains(dataRow, "<nil>") || strings.Contains(dataRow, "nil") {
		t.Errorf("nil value should render as empty string, got: %q", dataRow)
	}
	if !strings.Contains(dataRow, "present") {
		t.Errorf("data row missing non-nil value: %q", dataRow)
	}
}
