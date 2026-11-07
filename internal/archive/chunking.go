package archive

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/avantexa/coldplane/internal/duck"
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

var chunkKindNames = map[chunkKind]string{chunkByOffset: "offset", chunkByID: "id", chunkByTime: "time"}

// chunkKey is the column a table is chunked on and how. withZone marks a
// Postgres timestamptz column, whose range bounds must carry an offset.
type chunkKey struct {
	kind     chunkKind
	column   string
	withZone bool
}

func (k chunkKind) String() string { return chunkKindNames[k] }

// chunkPlan is a table's planned chunk queries: n chunks, each a native SQL
// SELECT for the source engine that DuckDB passes through unchanged.
type chunkPlan struct {
	n     int
	query func(i int) string
}

const unknownPartition = "year=unknown/month=unknown"

// archiveTable runs every chunk of plan as a DuckDB COPY into Parquet under
// <backupDir>/<table>, with up to workers chunks in flight, and returns the
// manifest entries of the written files.
func archiveTable(ctx context.Context, eng *duck.Engine, kind duck.Kind, alias, backupDir, table, partitionColumn string, plan chunkPlan, workers int, progress *ProgressTracker) ([]FileEntry, error) {
	// COPY does not create missing directories, partitioned or not.
	tableDir := filepath.Join(backupDir, table)
	mk := tableDir
	if partitionColumn == "" {
		mk = filepath.Join(tableDir, filepath.FromSlash(unknownPartition))
	}
	if err := os.MkdirAll(mk, 0755); err != nil {
		return nil, fmt.Errorf("create table dir: %w", err)
	}
	var mu sync.Mutex
	var entries []FileEntry
	err := runChunks(plan.n, workers, func(i int) error {
		res, err := eng.DB().ExecContext(ctx, duck.CopyChunkSQL(kind, alias, plan.query(i), partitionColumn, tableDir, table, i))
		if err != nil {
			return fmt.Errorf("copy chunk to parquet: %w", err)
		}
		rows, _ := res.RowsAffected()
		files, err := chunkFiles(backupDir, table, i)
		if err != nil {
			return err
		}
		if rows == 0 { // an empty unpartitioned chunk still writes a header-only file
			for _, f := range files {
				os.Remove(filepath.Join(backupDir, filepath.FromSlash(f.Key)))
			}
			files = nil
		}
		mu.Lock()
		entries = append(entries, files...)
		mu.Unlock()
		progress.AddRows(int(rows))
		progress.ChunkDone()
		return nil
	})
	if err != nil {
		return nil, err
	}
	fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n", table, progress.DoneRows, progress.DoneChunks)
	return entries, nil
}

// chunkFiles lists the Parquet files one chunk wrote, keyed relative to backupDir.
func chunkFiles(backupDir, table string, chunkNum int) ([]FileEntry, error) {
	pattern := filepath.Join(backupDir, table, "year=*", "month=*", fmt.Sprintf("%s-chunk-%d-part-*.parquet", table, chunkNum))
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("list chunk files: %w", err)
	}
	sort.Strings(paths)
	var files []FileEntry
	for _, p := range paths {
		st, err := os.Stat(p)
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", p, err)
		}
		rel, _ := filepath.Rel(backupDir, p)
		files = append(files, FileEntry{Key: filepath.ToSlash(rel), SizeBytes: st.Size()})
	}
	return files, nil
}
