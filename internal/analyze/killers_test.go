package analyze

import (
	"testing"
	"time"
)

func TestNewTableReport_SingleRow(t *testing.T) {
	r := newTableReport("t", "c", 1, 400, time.Time{}, time.Time{}, 1)
	if r.ArchivablePct != 100 || r.EstArchivedBytes != 100 {
		t.Errorf("one archivable row of one: pct=%d est=%d", r.ArchivablePct, r.EstArchivedBytes)
	}
}

func TestParseDuration_ErrorsReturnZero(t *testing.T) {
	for _, in := range []string{"", "abc", "0d", "  "} {
		d, err := ParseDuration(in)
		if err == nil || d != 0 {
			t.Errorf("ParseDuration(%q) = %v, %v; want 0 and an error", in, d, err)
		}
	}
}

func TestFormatDuration_SingleUnits(t *testing.T) {
	day := 24 * time.Hour
	cases := map[time.Duration]string{day: "1d", 30 * day: "1m", 365 * day: "1y", 0: "0d", 31 * day: "1m1d"}
	for d, want := range cases {
		if got := formatDuration(d); got != want {
			t.Errorf("formatDuration(%v) = %q, want %q", d, got, want)
		}
	}
}

func TestParseDuration_OverflowingNumber(t *testing.T) {
	if d, err := ParseDuration("99999999999999999999d"); err == nil || d != 0 {
		t.Errorf("got %v, %v", d, err)
	}
}
