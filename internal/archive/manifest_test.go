package archive

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSumSizes(t *testing.T) {
	files := []map[string]interface{}{
		{"key": "a", "size_bytes": int64(10)},
		{"key": "b", "size_bytes": 5},
		{"key": "c", "size_bytes": float64(2.9)},
		{"key": "d"},
		{"key": "e", "size_bytes": "oops"},
	}
	if got := sumSizes(files); got != 17 {
		t.Errorf("sumSizes = %d, want 17", got)
	}
	if got := sumSizes(nil); got != 0 {
		t.Errorf("sumSizes(nil) = %d, want 0", got)
	}
}

func TestTableManifest(t *testing.T) {
	files := []map[string]interface{}{{"key": "t/x.parquet", "size_bytes": int64(7)}}
	m := tableManifest("orders", "created_at", files)
	if m["name"] != "orders" || m["partition_column"] != "created_at" || m["size_bytes"] != int64(7) {
		t.Errorf("unexpected manifest entry: %v", m)
	}
	if got := m["files"].([]map[string]interface{}); len(got) != 1 {
		t.Errorf("files not carried over: %v", m["files"])
	}
	m = tableManifest("t", "", nil)
	if m["partition_column"] != nil {
		t.Errorf("empty partition column should be nil, got %#v", m["partition_column"])
	}
	if files, ok := m["files"].([]map[string]interface{}); !ok || files == nil || len(files) != 0 {
		t.Errorf("no files must be an empty list, not nil: %#v", m["files"])
	}
}

func TestWriteBackupManifest(t *testing.T) {
	dir := t.TempDir()
	tables := []map[string]interface{}{
		tableManifest("a", "ts", []map[string]interface{}{{"key": "a/1.parquet", "size_bytes": int64(100)}}),
		tableManifest("b", "", []map[string]interface{}{{"key": "b/1.parquet", "size_bytes": int64(50)}}),
	}
	total, err := writeBackupManifest(dir, "bk", "postgres", "mydb", tables)
	if err != nil {
		t.Fatal(err)
	}
	if total != 150 {
		t.Errorf("total = %d, want 150", total)
	}
	m, err := ReadLocalManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m["name"] != "bk" || m["db_type"] != "postgres" || m["db_name"] != "mydb" || m["local"] != true {
		t.Errorf("manifest fields: %v", m)
	}
	if m["size_bytes"].(float64) != 150 {
		t.Errorf("size_bytes = %v", m["size_bytes"])
	}
	if _, ok := m["created_at"].(string); !ok {
		t.Errorf("created_at missing: %v", m["created_at"])
	}
	if got := m["tables"].([]interface{}); len(got) != 2 {
		t.Errorf("tables = %v", m["tables"])
	}
}

func TestWriteBackupManifest_UnwritableDir(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	total, err := writeBackupManifest(filepath.Join(file, "bk"), "bk", "mysql", "db", nil)
	if err == nil || total != 0 {
		t.Errorf("writing under a file should fail: %d, %v", total, err)
	}
}
