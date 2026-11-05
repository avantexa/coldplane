package archive

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeRows is an in-memory rowSource.
type fakeRows struct {
	cols   []string
	tsCols map[int]bool
	rows   [][]interface{}
	i      int
	failAt int   // 1-based row whose Values() fails; 0 = never
	err    error // returned by Err() after iteration
}

func (f *fakeRows) Columns() ([]string, map[int]bool) { return f.cols, f.tsCols }
func (f *fakeRows) Next() bool                        { f.i++; return f.i <= len(f.rows) }
func (f *fakeRows) Values() ([]interface{}, error) {
	if f.i == f.failAt {
		return nil, errors.New("driver exploded")
	}
	return f.rows[f.i-1], nil
}
func (f *fakeRows) Err() error { return f.err }

func eventRows(n int) *fakeRows {
	f := &fakeRows{cols: []string{"id", "ts"}}
	base := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		f.rows = append(f.rows, []interface{}{int64(i), base.AddDate(0, i, 0)})
	}
	return f
}

func TestWriteChunk(t *testing.T) {
	ctx := context.Background()

	t.Run("writes one file per partition and reports progress", func(t *testing.T) {
		store := &LocalFileStore{BaseDir: t.TempDir()}
		progress := &ProgressTracker{Table: "events"}
		entries, err := writeChunk(ctx, eventRows(3), "events", 7, "ts", store, progress)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 3 {
			t.Fatalf("3 rows in 3 months should give 3 files, got %d", len(entries))
		}
		for _, e := range entries {
			key := e["key"].(string)
			if !strings.HasPrefix(key, "events/year=2024/month=") || !strings.HasSuffix(key, "events-chunk-7-part-0.parquet") {
				t.Errorf("unexpected key %s", key)
			}
		}
		if progress.DoneRows != 3 || progress.DoneChunks != 1 {
			t.Errorf("progress rows=%d chunks=%d", progress.DoneRows, progress.DoneChunks)
		}
	})

	t.Run("no rows means no files but the chunk still counts", func(t *testing.T) {
		progress := &ProgressTracker{Table: "t"}
		entries, err := writeChunk(ctx, eventRows(0), "t", 0, "ts", &LocalFileStore{BaseDir: t.TempDir()}, progress)
		if err != nil || entries != nil {
			t.Errorf("got %v, %v", entries, err)
		}
		if progress.DoneChunks != 1 {
			t.Errorf("DoneChunks = %d", progress.DoneChunks)
		}
	})

	t.Run("a failing row aborts the chunk", func(t *testing.T) {
		src := eventRows(3)
		src.failAt = 2
		_, err := writeChunk(ctx, src, "t", 0, "ts", &LocalFileStore{BaseDir: t.TempDir()}, &ProgressTracker{})
		if err == nil || !strings.Contains(err.Error(), "failed to read row") {
			t.Errorf("got %v", err)
		}
	})

	t.Run("a failing first row aborts without a writer", func(t *testing.T) {
		src := eventRows(1)
		src.failAt = 1
		if _, err := writeChunk(ctx, src, "t", 0, "ts", &LocalFileStore{BaseDir: t.TempDir()}, &ProgressTracker{}); err == nil {
			t.Error("expected error")
		}
	})

	t.Run("an iteration error aborts the chunk", func(t *testing.T) {
		src := eventRows(2)
		src.err = errors.New("connection lost")
		_, err := writeChunk(ctx, src, "t", 0, "ts", &LocalFileStore{BaseDir: t.TempDir()}, &ProgressTracker{})
		if err == nil || !strings.Contains(err.Error(), "row iteration error") {
			t.Errorf("got %v", err)
		}
	})

	t.Run("unknown partition column lands in the unknown partition", func(t *testing.T) {
		entries, err := writeChunk(ctx, eventRows(2), "t", 1, "nope", &LocalFileStore{BaseDir: t.TempDir()}, &ProgressTracker{})
		if err != nil || len(entries) != 1 || !strings.Contains(entries[0]["key"].(string), unknownPartition) {
			t.Errorf("got %v, %v", entries, err)
		}
	})
}

func TestPartitionedWriter_DiscardNilIsSafe(t *testing.T) {
	var w *partitionedWriter
	w.discard() // must not panic
}
