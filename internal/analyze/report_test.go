package analyze

import (
	"errors"
	"testing"
	"time"
)

type fakeRows struct {
	vals []string
	i    int
	err  error
}

func (f *fakeRows) Next() bool { f.i++; return f.i <= len(f.vals) }
func (f *fakeRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = f.vals[f.i-1]
	return nil
}
func (f *fakeRows) Err() error { return f.err }

func TestScanStrings(t *testing.T) {
	got, err := scanStrings(&fakeRows{vals: []string{"a", "b"}})
	if err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("got %v, %v", got, err)
	}
	boom := errors.New("boom")
	if _, err := scanStrings(&fakeRows{err: boom}); !errors.Is(err, boom) {
		t.Errorf("iteration error should propagate, got %v", err)
	}
}

func TestResolveDateColumn(t *testing.T) {
	detectCalls := 0
	detect := func() (string, error) { detectCalls++; return "created_at", nil }
	if col, err := resolveDateColumn("ts", detect); col != "ts" || err != nil || detectCalls != 0 {
		t.Errorf("override should win without detecting: %q %v %d", col, err, detectCalls)
	}
	if col, err := resolveDateColumn("", detect); col != "created_at" || err != nil {
		t.Errorf("detected: %q %v", col, err)
	}
	if _, err := resolveDateColumn("", func() (string, error) { return "", nil }); err == nil {
		t.Error("no column anywhere should be an error")
	}
	boom := errors.New("boom")
	if _, err := resolveDateColumn("", func() (string, error) { return "", boom }); !errors.Is(err, boom) {
		t.Errorf("detect error should propagate, got %v", err)
	}
}

func TestNewTableReport(t *testing.T) {
	oldest := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	newest := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	r := newTableReport("orders", "created_at", 1000, 4_000_000, oldest, newest, 250)
	if r.TableName != "orders" || r.DateColumn != "created_at" || r.TotalRows != 1000 || r.SizeBytes != 4_000_000 {
		t.Errorf("fields: %+v", r)
	}
	if r.ArchivablePct != 25 {
		t.Errorf("pct = %d, want 25", r.ArchivablePct)
	}
	// 25% of 4 MB at the 4:1 compression estimate = 250 KB
	if r.EstArchivedBytes != 250_000 {
		t.Errorf("est = %d, want 250000", r.EstArchivedBytes)
	}
	if !r.OldestRow.Equal(oldest) || !r.NewestRow.Equal(newest) {
		t.Errorf("dates: %+v", r)
	}
	if z := newTableReport("t", "c", 0, 10, oldest, newest, 0); z.ArchivablePct != 0 || z.EstArchivedBytes != 0 {
		t.Errorf("zero rows must not divide: %+v", z)
	}
}
