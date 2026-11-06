package archive

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSumSizes(t *testing.T) {
	if got := sumSizes([]FileEntry{{"a", 10}, {"b", 5}, {"c", 2}}); got != 17 {
		t.Errorf("sumSizes = %d, want 17", got)
	}
	if got := sumSizes(nil); got != 0 {
		t.Errorf("sumSizes(nil) = %d, want 0", got)
	}
}

func TestNewTableEntry(t *testing.T) {
	e := newTableEntry("orders", "created_at", []FileEntry{{"t/x.parquet", 7}})
	if e.Name != "orders" || e.PartitionColumn == nil || *e.PartitionColumn != "created_at" || e.SizeBytes != 7 || len(e.Files) != 1 {
		t.Errorf("unexpected entry: %+v", e)
	}
	e = newTableEntry("t", "", nil)
	if e.PartitionColumn != nil {
		t.Errorf("empty partition column should be nil, got %q", *e.PartitionColumn)
	}
	if e.Files == nil || len(e.Files) != 0 {
		t.Errorf("no files must be an empty list, not nil: %#v", e.Files)
	}
}

func TestWriteBackupManifest_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	tables := []TableEntry{
		newTableEntry("a", "ts", []FileEntry{{"a/1.parquet", 100}}),
		newTableEntry("b", "", []FileEntry{{"b/1.parquet", 50}}),
	}
	total, err := writeBackupManifest(dir, "bk", "postgres", "mydb", "id > 0", tables)
	if err != nil {
		t.Fatal(err)
	}
	if total != 150 {
		t.Errorf("total = %d, want 150", total)
	}
	m, err := ReadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != "bk" || m.DBType != "postgres" || m.DBName != "mydb" || !m.Local || m.SizeBytes != 150 || m.CreatedAt == "" || m.Where != "id > 0" || m.Engine != "duckdb" {
		t.Errorf("manifest fields: %+v", m)
	}
	if len(m.Tables) != 2 || m.Tables[0].Files[0].Key != "a/1.parquet" || m.Tables[1].PartitionColumn != nil {
		t.Errorf("tables: %+v", m.Tables)
	}
	if got := m.TableNames(); len(got) != 2 || got[1] != "b" {
		t.Errorf("TableNames = %v", got)
	}

	// The on-disk shape stays as readers expect: null partition, [] files.
	data, _ := os.ReadFile(filepath.Join(dir, "index.json"))
	for _, want := range []string{`"partition_column": null`, `"local": true`, `"files": [`} {
		if !containsBytes(data, want) {
			t.Errorf("index.json missing %s:\n%s", want, data)
		}
	}
	if _, err := writeBackupManifest(dir, "empty", "mysql", "db", "", nil); err != nil {
		t.Fatal(err)
	}
	if data, _ = os.ReadFile(filepath.Join(dir, "index.json")); !containsBytes(data, `"tables": []`) {
		t.Errorf("no tables must serialize as an empty list:\n%s", data)
	}
}

func TestWriteBackupManifest_UnwritableDir(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	total, err := writeBackupManifest(filepath.Join(file, "bk"), "bk", "mysql", "db", "", nil)
	if err == nil || total != 0 {
		t.Errorf("writing under a file should fail: %d, %v", total, err)
	}
}

func TestReadManifest_Errors(t *testing.T) {
	if _, err := ReadManifest(t.TempDir()); err == nil {
		t.Error("missing index.json should fail")
	}
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "index.json"), []byte("{not json"), 0644)
	if _, err := ReadManifest(dir); err == nil {
		t.Error("corrupt index.json should fail")
	}
}

func containsBytes(data []byte, s string) bool {
	return len(data) > 0 && len(s) > 0 && string(data) != "" && (func() bool {
		for i := 0; i+len(s) <= len(data); i++ {
			if string(data[i:i+len(s)]) == s {
				return true
			}
		}
		return false
	})()
}

func TestPrepareBackupDir(t *testing.T) {
	opts := ArchiveOptions{OutputDir: t.TempDir()}
	dir, err := prepareBackupDir(opts, "fresh")
	if err != nil || dir != filepath.Join(opts.OutputDir, "fresh") {
		t.Fatalf("fresh: %q %v", dir, err)
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareBackupDir(opts, "fresh"); err == nil {
		t.Error("an existing backup directory must be refused")
	}
	discardBackupDir(dir)
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Error("discardBackupDir should remove the directory")
	}
}
