package archive

import (
	"testing"
	"time"
)

func TestTimeValue(t *testing.T) {
	now := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	if got := timeValue(now); !got.Equal(now) {
		t.Errorf("time.Time passthrough: %v", got)
	}
	if got := timeValue("2025-06-15 10:30:00"); !got.Equal(now) {
		t.Errorf("mysql string: %v", got)
	}
	if got := timeValue([]byte("2025-06-15T10:30:00Z")); !got.Equal(now) {
		t.Errorf("rfc3339 bytes: %v", got)
	}
	for _, v := range []interface{}{nil, 42, "hello", []byte("x"), 3.5} {
		if got := timeValue(v); !got.IsZero() {
			t.Errorf("%#v should not be a time, got %v", v, got)
		}
	}
}

func TestFirstAndLastTimeColumn(t *testing.T) {
	cols := []string{"id", "created_at", "name", "updated_at"}
	row := []interface{}{int64(1), time.Now(), "x", "2025-01-01"}
	if got := firstTimeColumn(cols, row); got != "created_at" {
		t.Errorf("first: got %q", got)
	}
	if got := lastTimeColumn(cols, row); got != "updated_at" {
		t.Errorf("last: got %q", got)
	}
	if got := firstTimeColumn(cols, []interface{}{int64(1), "a", "b", nil}); got != "" {
		t.Errorf("no time column: got %q", got)
	}
	if got := firstTimeColumn(cols, nil); got != "" {
		t.Errorf("no row: got %q", got)
	}
}
