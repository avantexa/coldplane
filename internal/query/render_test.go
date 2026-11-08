package query

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"
)

func TestFormatCell(t *testing.T) {
	ts := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	cases := map[string]struct {
		in   interface{}
		want string
	}{
		"nil":         {nil, ""},
		"whole float": {float64(42), "42"},
		"fraction":    {1.5, "1.5"},
		"time":        {ts, "2025-06-15 10:30:00"},
		"string":      {"x", "x"},
		"int":         {int64(7), "7"},
		"bool":        {true, "true"},
	}
	for name, c := range cases {
		if got := formatCell(c.in); got != c.want {
			t.Errorf("%s: got %q, want %q", name, got, c.want)
		}
	}
}

func TestWriteQueryResults_UnknownFormatIsTable(t *testing.T) {
	var buf bytes.Buffer
	rows := []map[string]interface{}{{"b": 1, "a": "x"}}
	if err := writeQueryResults(&buf, rows, "whatever"); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "a b") || !strings.HasPrefix(lines[1], "- -") || !strings.HasPrefix(lines[2], "x 1") {
		t.Errorf("unexpected table output:\n%s", buf.String())
	}
}

func TestWriteQueryResults_Empty(t *testing.T) {
	var buf bytes.Buffer
	if err := writeQueryResults(&buf, nil, "csv"); err != nil || buf.String() != "No results found.\n" {
		t.Errorf("got %q, %v", buf.String(), err)
	}
}

func TestFormatCell_SpecialFloats(t *testing.T) {
	cases := map[string]struct {
		in   float64
		want string
	}{
		"inf":      {math.Inf(1), "+Inf"},
		"neg inf":  {math.Inf(-1), "-Inf"},
		"nan":      {math.NaN(), "NaN"},
		"two dp":   {1.25, "1.25"},
		"negative": {-2.5, "-2.5"},
	}
	for name, c := range cases {
		if got := formatCell(c.in); got != c.want {
			t.Errorf("%s: got %q, want %q", name, got, c.want)
		}
	}
}
