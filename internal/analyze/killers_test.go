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

func TestParseDuration_OverflowingNumber(t *testing.T) {
	if d, err := ParseDuration("99999999999999999999d"); err == nil || d != 0 {
		t.Errorf("got %v, %v", d, err)
	}
}
