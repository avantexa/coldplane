package archive

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPlanIDChunks(t *testing.T) {
	tests := []struct {
		name           string
		min, max, size int64
		want           []idRange
	}{
		{"exact multiple", 1, 10, 5, []idRange{{1, 6}, {6, 11}}},
		{"remainder", 1, 10, 4, []idRange{{1, 5}, {5, 9}, {9, 11}}},
		{"single row", 7, 7, 100, []idRange{{7, 8}}},
		{"empty range", 10, 1, 5, nil},
		{"negative ids", -5, 4, 5, []idRange{{-5, 0}, {0, 5}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := planIDChunks(tt.min, tt.max, tt.size)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("chunk %d: got %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestTimeWindow(t *testing.T) {
	day := 24 * time.Hour
	if w := timeWindow(100*day, 30*day, 120); w != 30*day {
		t.Errorf("small range should keep default window, got %s", w)
	}
	// 10 years at a 30-day window would be ~122 chunks; cap at 120 by widening.
	total := 3650 * day
	if w := timeWindow(total, 30*day, 120); w != total/120 {
		t.Errorf("large range: got %s, want %s", w, total/120)
	}
}

func TestPlanTimeChunks(t *testing.T) {
	day := 24 * time.Hour
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	got := planTimeChunks(t0, t0.Add(70*day), 30*day, time.Second)
	want := []timeRange{
		{t0, t0.Add(30 * day)},
		{t0.Add(30 * day), t0.Add(60 * day)},
		{t0.Add(60 * day), t0.Add(70*day + time.Second)}, // last chunk includes rows at max
	}
	if len(got) != len(want) {
		t.Fatalf("got %d chunks %v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if !got[i].Start.Equal(want[i].Start) || !got[i].End.Equal(want[i].End) {
			t.Errorf("chunk %d: got %v, want %v", i, got[i], want[i])
		}
	}

	// A window landing exactly on max still nudges past it.
	got = planTimeChunks(t0, t0.Add(30*day), 30*day, time.Microsecond)
	if len(got) != 1 || !got[0].End.Equal(t0.Add(30*day+time.Microsecond)) {
		t.Errorf("exact window: got %v", got)
	}

	// Pinned current behavior: min == max yields no chunks. See TestPlanTimeChunks_SameInstant once fixed.
	if got := planTimeChunks(t0, t0, 30*day, time.Second); len(got) != 0 {
		t.Errorf("min==max: got %v, want none", got)
	}
}

func TestPlanOffsetChunks(t *testing.T) {
	got := planOffsetChunks(10, 4)
	want := []offsetRange{{0, 4}, {4, 4}, {8, 4}}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("chunk %d: got %v, want %v", i, got[i], want[i])
		}
	}
	if got := planOffsetChunks(0, 4); len(got) != 0 {
		t.Errorf("zero rows: got %v, want none", got)
	}
}

func TestRunChunks(t *testing.T) {
	t.Run("runs every chunk once within the worker limit", func(t *testing.T) {
		const n, workers = 20, 3
		var mu sync.Mutex
		seen := map[int]int{}
		var active, peak int32
		err := runChunks(n, workers, func(i int) error {
			cur := atomic.AddInt32(&active, 1)
			for {
				p := atomic.LoadInt32(&peak)
				if cur <= p || atomic.CompareAndSwapInt32(&peak, p, cur) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			mu.Lock()
			seen[i]++
			mu.Unlock()
			atomic.AddInt32(&active, -1)
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(seen) != n {
			t.Fatalf("ran %d distinct chunks, want %d", len(seen), n)
		}
		for i, c := range seen {
			if c != 1 {
				t.Errorf("chunk %d ran %d times", i, c)
			}
		}
		if peak > workers {
			t.Errorf("peak concurrency %d exceeded %d workers", peak, workers)
		}
	})

	t.Run("returns a chunk error and still finishes the rest", func(t *testing.T) {
		boom := errors.New("boom")
		var ran int32
		err := runChunks(5, 2, func(i int) error {
			atomic.AddInt32(&ran, 1)
			if i == 2 {
				return boom
			}
			return nil
		})
		if !errors.Is(err, boom) {
			t.Fatalf("got %v, want wrapped boom", err)
		}
		if ran != 5 {
			t.Errorf("ran %d chunks, want 5", ran)
		}
	})

	t.Run("zero chunks and zero workers are fine", func(t *testing.T) {
		if err := runChunks(0, 4, func(int) error { t.Fatal("should not run"); return nil }); err != nil {
			t.Fatal(err)
		}
		if err := runChunks(3, 0, func(int) error { return nil }); err != nil {
			t.Fatal(err)
		}
	})
}
