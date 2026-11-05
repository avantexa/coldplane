package archive

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

const unknownPartition = "year=unknown/month=unknown"

// partitionKey renders the year=YYYY/month=MM directory for a timestamp
// value, or the unknown partition when the value is missing or not a time.
func partitionKey(v interface{}) string {
	t := timeValue(v)
	if t.IsZero() {
		return unknownPartition
	}
	return fmt.Sprintf("year=%s/month=%s", t.Format("2006"), t.Format("01"))
}

// valueAt returns values[i], or nil when i is out of range (no such column).
func valueAt(values []interface{}, i int) interface{} {
	if i < 0 || i >= len(values) {
		return nil
	}
	return values[i]
}

// indexOf returns the position of name in columns, or -1.
func indexOf(columns []string, name string) int {
	for i, c := range columns {
		if c == name {
			return i
		}
	}
	return -1
}

// rowToMap converts a scanned row into the JSON object the Parquet writer
// consumes. nil values are omitted so they become NULLs, []byte become
// strings, columns flagged in timestampCols become UTC epoch milliseconds,
// and everything else goes through toParquetSafeValue.
func rowToMap(columns []string, values []interface{}, timestampCols map[int]bool) map[string]interface{} {
	m := make(map[string]interface{}, len(columns))
	for i, name := range columns {
		val := valueAt(values, i)
		if val == nil {
			continue
		}
		if timestampCols[i] {
			if ms, ok := epochMillis(val); ok {
				m[name] = ms
			} else if _, isText := val.(string); !isText {
				if _, isBytes := val.([]byte); !isBytes {
					m[name] = val
				}
			}
			continue
		}
		if b, ok := val.([]byte); ok {
			m[name] = string(b)
			continue
		}
		m[name] = toParquetSafeValue(val)
	}
	return m
}

// epochMillis converts a time or time string to UTC epoch milliseconds.
func epochMillis(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case time.Time:
		return v.UTC().UnixMilli(), true
	case string, []byte:
		t := timeValue(v)
		if t.IsZero() {
			return 0, false
		}
		return t.UTC().UnixMilli(), true
	}
	return 0, false
}

// partFile is one partition's temp Parquet file while a chunk is being written.
type partFile struct {
	file *os.File
	path string
	pw   *writer.JSONWriter
	rows int
}

// partitionedWriter writes rows of one chunk into one temp Parquet file per
// partition key, opening files on demand.
type partitionedWriter struct {
	schema string
	parts  map[string]*partFile
}

func newPartitionedWriter(schema string) *partitionedWriter {
	return &partitionedWriter{schema: schema, parts: map[string]*partFile{}}
}

// write appends one row to the partition's file.
func (w *partitionedWriter) write(partition string, row map[string]interface{}) error {
	p, ok := w.parts[partition]
	if !ok {
		var err error
		if p, err = w.open(); err != nil {
			return err
		}
		w.parts[partition] = p
	}
	rowJSON, _ := json.Marshal(row)
	if err := p.pw.Write(string(rowJSON)); err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}
	p.rows++
	return nil
}

func (w *partitionedWriter) open() (*partFile, error) {
	tmp, err := os.CreateTemp("", "chunk-*.parquet")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	pw, err := writer.NewJSONWriter(w.schema, writerfile.NewWriterFile(tmp), 4)
	if err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	pw.CompressionType = parquet.CompressionCodec_ZSTD
	return &partFile{file: tmp, path: tmp.Name(), pw: pw}, nil
}

// finish closes every partition file and returns partition -> temp path for
// the non-empty ones. Empty files are deleted. On error all files are removed.
func (w *partitionedWriter) finish() (map[string]string, error) {
	out := map[string]string{}
	for partition, p := range w.parts {
		if p.rows == 0 {
			p.file.Close()
			os.Remove(p.path)
			continue
		}
		if err := p.pw.WriteStop(); err != nil {
			w.discard()
			return nil, fmt.Errorf("failed to finalize Parquet: %w", err)
		}
		p.file.Sync()
		p.file.Close()
		out[partition] = p.path
	}
	return out, nil
}

// discard closes and removes every temp file.
func (w *partitionedWriter) discard() {
	for _, p := range w.parts {
		p.file.Close()
		os.Remove(p.path)
	}
}

// storeParts moves finished partition files into the store under
// <table>/<partition>/<table>-chunk-<n>-part-0.parquet, up to four at a
// time, and returns the manifest entries for the stored files.
func storeParts(ctx context.Context, store FileStore, table string, chunkNum int, parts map[string]string, rowCount int) ([]map[string]interface{}, error) {
	type item struct{ key, path string }
	var items []item
	for partition, filePath := range parts {
		name := fmt.Sprintf("%s-chunk-%d-part-0.parquet", table, chunkNum)
		items = append(items, item{path.Join(table, partition, name), filePath})
	}

	entries := make([]map[string]interface{}, len(items))
	err := runChunks(len(items), 4, func(i int) error {
		size, err := store.StoreFile(ctx, items[i].key, items[i].path)
		if err != nil {
			os.Remove(items[i].path)
			return fmt.Errorf("store failed for %s: %w", items[i].key, err)
		}
		entries[i] = map[string]interface{}{"key": items[i].key, "size_bytes": size}
		fmt.Printf("[%s] [worker %d] Stored chunk (%d rows, %.2f MB)\n", table, chunkNum, rowCount, float64(size)/1024/1024)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// storeChunk finalizes a chunk's writer and stores its files. A nil writer
// means the chunk had no rows.
func storeChunk(ctx context.Context, store FileStore, w *partitionedWriter, table string, chunkNum, rowCount int) ([]map[string]interface{}, error) {
	if w == nil {
		return nil, nil
	}
	parts, err := w.finish()
	if err != nil {
		return nil, err
	}
	return storeParts(ctx, store, table, chunkNum, parts, rowCount)
}
