package archive

import (
	"testing"
	"time"
)

func TestPlanIDChunks_Edges(t *testing.T) {
	if got := planIDChunks(1, 10, 1); len(got) != 10 || got[9] != (idRange{10, 11}) {
		t.Errorf("chunk size 1: %v", got)
	}
	if got := planIDChunks(1, 10, 0); got != nil {
		t.Errorf("chunk size 0 must plan nothing, got %v", got)
	}
}

func TestTimeWindow_Boundary(t *testing.T) {
	day := 24 * time.Hour
	if w := timeWindow(119*30*day, 30*day, 120); w != 30*day {
		t.Errorf("119 windows fit: got %s", w)
	}
	total := 120*30*day + time.Hour
	if w := timeWindow(total, 30*day, 120); w != total/120 {
		t.Errorf("121 windows must widen: got %s, want %s", w, total/120)
	}
}

func TestPlanTimeChunks_ZeroWindowUsesDefault(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := planTimeChunks(t0, t0.Add(70*24*time.Hour), 0, time.Second); len(got) != 3 {
		t.Errorf("zero window should fall back to 30 days: %d chunks", len(got))
	}
}

func TestPlanOffsetChunks_Edges(t *testing.T) {
	if got := planOffsetChunks(10, 5); len(got) != 2 {
		t.Errorf("exact multiple: %v", got)
	}
	if got := planOffsetChunks(1, 1); len(got) != 1 {
		t.Errorf("single row: %v", got)
	}
	if got := planOffsetChunks(5, 0); got != nil {
		t.Errorf("chunk size 0 must plan nothing, got %v", got)
	}
}

func TestTimeColumns_RowLongerThanColumns(t *testing.T) {
	cols := []string{"id"}
	row := []interface{}{int64(1), time.Now()}
	if got := firstTimeColumn(cols, row); got != "" {
		t.Errorf("first: %q", got)
	}
}
