package archive

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	defaultTimeWindow = 30 * 24 * time.Hour
	maxTimeChunks     = 120
)

// idRange is a half-open [Start, End) range of primary-key values.
type idRange struct{ Start, End int64 }

// planIDChunks splits [minID, maxID] into consecutive ranges of chunkSize ids.
func planIDChunks(minID, maxID, chunkSize int64) []idRange {
	if minID > maxID || chunkSize <= 0 {
		return nil
	}
	var plan []idRange
	for start := minID; start <= maxID; start += chunkSize {
		end := start + chunkSize
		if end > maxID+1 {
			end = maxID + 1
		}
		plan = append(plan, idRange{start, end})
	}
	return plan
}

// timeRange is a half-open [Start, End) window of timestamps.
type timeRange struct{ Start, End time.Time }

// timeWindow widens defaultWindow so that total fits in at most maxChunks windows.
func timeWindow(total, defaultWindow time.Duration, maxChunks int) time.Duration {
	if int(total/defaultWindow)+1 > maxChunks {
		return time.Duration(total.Nanoseconds() / int64(maxChunks))
	}
	return defaultWindow
}

// planTimeChunks splits [min, max] into windows. The last window ends at
// max+nudge so rows stamped exactly at max are included.
func planTimeChunks(min, max time.Time, window, nudge time.Duration) []timeRange {
	if window <= 0 {
		window = defaultTimeWindow
	}
	var plan []timeRange
	for t0 := min; !t0.After(max); {
		t1 := t0.Add(window)
		if !t1.Before(max) {
			t1 = max.Add(nudge)
		}
		plan = append(plan, timeRange{t0, t1})
		t0 = t1
	}
	return plan
}

// offsetRange is one LIMIT/OFFSET page.
type offsetRange struct{ Offset, Limit int }

// planOffsetChunks pages totalRows rows chunkSize at a time.
func planOffsetChunks(totalRows, chunkSize int) []offsetRange {
	if totalRows <= 0 || chunkSize <= 0 {
		return nil
	}
	var plan []offsetRange
	for off := 0; off < totalRows; off += chunkSize {
		plan = append(plan, offsetRange{off, chunkSize})
	}
	return plan
}

// runChunks runs fn(i) for every i in [0, n) with at most workers goroutines
// at once. Every chunk runs even after a failure; the first error is returned.
func runChunks(n, workers int, fn func(i int) error) error {
	if workers < 1 {
		workers = 1
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)
	var once sync.Once
	var first error
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(i); err != nil {
				once.Do(func() { first = fmt.Errorf("[worker %d] %w", i, err) })
			}
		}(i)
	}
	wg.Wait()
	return first
}

// chunkKind is the strategy used to split a table into chunks.
type chunkKind int

const (
	chunkByOffset chunkKind = iota // LIMIT/OFFSET pages: no usable key or timestamp
	chunkByID                      // ranges of an integer primary key
	chunkByTime                    // windows of a timestamp column
)

// chunkPlan is a table's planned chunk queries: n chunks, each a query with
// its bound arguments.
type chunkPlan struct {
	n     int
	query func(i int) (string, []interface{})
}

// chunkRunner executes one chunk query against an engine and returns its rows
// plus a release function that closes them.
type chunkRunner func(ctx context.Context, query string, args []interface{}) (rowSource, func(), error)

// archiveTable runs every chunk of plan through run, with up to workers in
// parallel, writes the rows as partitioned Parquet into store and returns the
// manifest entries of the stored files.
func archiveTable(ctx context.Context, run chunkRunner, store FileStore, table, partitionColumn string, plan chunkPlan, workers int, progress *ProgressTracker) ([]FileEntry, error) {
	var mu sync.Mutex
	var entries []FileEntry
	err := runChunks(plan.n, workers, func(i int) error {
		q, args := plan.query(i)
		src, release, err := run(ctx, q, args)
		if err != nil {
			return err
		}
		defer release()
		got, err := writeChunk(ctx, src, table, i, partitionColumn, store, progress)
		if err != nil {
			return err
		}
		mu.Lock()
		entries = append(entries, got...)
		mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, err
	}
	fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n", table, progress.DoneRows, progress.DoneChunks)
	return entries, nil
}
