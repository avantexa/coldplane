package archive

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPartitionKey(t *testing.T) {
	ts := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	cases := map[string]struct {
		in   interface{}
		want string
	}{
		"time":       {ts, "year=2025/month=06"},
		"string":     {"2025-06-15 10:30:00", "year=2025/month=06"},
		"bytes":      {[]byte("2024-12-31"), "year=2024/month=12"},
		"nil":        {nil, "year=unknown/month=unknown"},
		"not a time": {"hello", "year=unknown/month=unknown"},
		"number":     {int64(7), "year=unknown/month=unknown"},
		"zero time":  {time.Time{}, "year=unknown/month=unknown"},
	}
	for name, c := range cases {
		if got := partitionKey(c.in); got != c.want {
			t.Errorf("%s: got %q, want %q", name, got, c.want)
		}
	}
}

func TestValueAt(t *testing.T) {
	vals := []interface{}{"a", "b"}
	if got := valueAt(vals, 1); got != "b" {
		t.Errorf("got %v", got)
	}
	if got := valueAt(vals, -1); got != nil {
		t.Errorf("missing column should be nil, got %v", got)
	}
	if got := valueAt(vals, 5); got != nil {
		t.Errorf("out of range should be nil, got %v", got)
	}
}

func TestRowToMap(t *testing.T) {
	ts := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	columns := []string{"id", "blob", "nothing", "created_at", "updated_at", "bad_ts", "weird_ts"}
	values := []interface{}{int64(1), []byte("raw"), nil, ts, "2025-06-15 10:30:00", "not a date", 12345}
	timestampCols := map[int]bool{3: true, 4: true, 5: true, 6: true}

	m := rowToMap(columns, values, timestampCols)

	if m["id"] != int64(1) {
		t.Errorf("id: %#v", m["id"])
	}
	if m["blob"] != "raw" {
		t.Errorf("[]byte should become string: %#v", m["blob"])
	}
	if _, ok := m["nothing"]; ok {
		t.Error("nil value must be omitted")
	}
	if m["created_at"] != ts.UnixMilli() {
		t.Errorf("time.Time timestamp column should be epoch millis: %#v", m["created_at"])
	}
	if m["updated_at"] != ts.UnixMilli() {
		t.Errorf("string timestamp column should be parsed to epoch millis: %#v", m["updated_at"])
	}
	if _, ok := m["bad_ts"]; ok {
		t.Error("unparseable timestamp string must be omitted")
	}
	if m["weird_ts"] != 12345 {
		t.Errorf("non-time value in timestamp column passes through: %#v", m["weird_ts"])
	}

	// Without timestamp columns (MySQL path) a time.Time goes through the generic sanitizer.
	m = rowToMap([]string{"ts"}, []interface{}{ts}, nil)
	if _, ok := m["ts"]; !ok {
		t.Error("time without timestamp hint should still be written")
	}
}

func TestPartitionedWriter_WriteFinishStore(t *testing.T) {
	columns := []string{"id", "ts"}
	schema := inferParquetSchema(columns, []interface{}{int64(1), "2025-01-01 00:00:00"})
	w := newPartitionedWriter(schema)

	for i, p := range []string{"year=2025/month=01", "year=2025/month=01", "year=2025/month=02"} {
		row := map[string]interface{}{"id": int64(i), "ts": "2025-01-01 00:00:00"}
		if err := w.write(p, row); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	parts, err := w.finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("got %d parts, want 2: %v", len(parts), parts)
	}
	for p, path := range parts {
		st, err := os.Stat(path)
		if err != nil || st.Size() == 0 {
			t.Errorf("partition %s: temp file missing or empty (%v)", p, err)
		}
	}

	dest := t.TempDir()
	store := &LocalFileStore{BaseDir: dest}
	entries, err := storeParts(context.Background(), store, "orders", 3, parts, 3)
	if err != nil {
		t.Fatalf("storeParts: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	for _, e := range entries {
		key := e.Key
		if _, err := os.Stat(filepath.Join(dest, filepath.FromSlash(key))); err != nil {
			t.Errorf("stored file missing for key %s: %v", key, err)
		}
		if e.SizeBytes <= 0 {
			t.Errorf("size for %s should be positive", key)
		}
		want1 := "orders/year=2025/month=01/orders-chunk-3-part-0.parquet"
		want2 := "orders/year=2025/month=02/orders-chunk-3-part-0.parquet"
		if key != want1 && key != want2 {
			t.Errorf("unexpected key %s", key)
		}
	}
	for _, path := range parts {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("temp file %s should be gone after store", path)
		}
	}
}

func TestPartitionedWriter_DiscardRemovesTempFiles(t *testing.T) {
	schema := inferParquetSchema([]string{"id"}, []interface{}{int64(1)})
	w := newPartitionedWriter(schema)
	if err := w.write("year=2025/month=01", map[string]interface{}{"id": int64(1)}); err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, p := range w.parts {
		paths = append(paths, p.path)
	}
	w.discard()
	for _, p := range paths {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s should be removed", p)
		}
	}
}

func TestStoreChunk_NoRows(t *testing.T) {
	entries, err := storeChunk(context.Background(), &LocalFileStore{BaseDir: t.TempDir()}, nil, "t", 0, 0)
	if err != nil || entries != nil {
		t.Errorf("no rows: got %v, %v", entries, err)
	}
}
