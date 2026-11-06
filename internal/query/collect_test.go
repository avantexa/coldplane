package query

import (
	"github.com/avantexa/coldplane/internal/archive"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func touch(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestCollectLocalParquetFiles_ManifestWinsOverDisk(t *testing.T) {
	dir := t.TempDir()
	listed := filepath.Join(dir, "orders", "year=2025", "orders-chunk-0-part-0.parquet")
	touch(t, listed)
	touch(t, filepath.Join(dir, "orders", "year=2025", "stray.parquet"))
	manifest := archive.Manifest{Tables: []archive.TableEntry{{
		Name: "orders",
		Files: []archive.FileEntry{
			{Key: "orders/year=2025/orders-chunk-0-part-0.parquet"},
			{Key: "orders/year=2025/missing.parquet"},
		},
	}}}
	files := collectLocalParquetFiles(dir, manifest, "")
	if len(files) != 1 || files[0] != listed {
		t.Errorf("manifest files that exist should win over the directory walk, got %v", files)
	}
	files = collectLocalParquetFiles(dir, manifest, "orders")
	if len(files) != 1 || files[0] != listed {
		t.Errorf("filtered by table: %v", files)
	}
}

func TestCollectLocalParquetFiles_WalkIsScopedToTable(t *testing.T) {
	dir := t.TempDir()
	touch(t, filepath.Join(dir, "events", "year=2025", "a.parquet"))
	touch(t, filepath.Join(dir, "events", "year=2025", "notes.txt"))
	touch(t, filepath.Join(dir, "other", "year=2025", "b.parquet"))
	manifest := archive.Manifest{Tables: []archive.TableEntry{{Name: "events"}, {Name: "other"}}}
	files := collectLocalParquetFiles(dir, manifest, "events")
	if len(files) != 1 || !strings.HasSuffix(files[0], "a.parquet") {
		t.Errorf("walk for one table should only find its parquet files, got %v", files)
	}
	all := collectLocalParquetFiles(dir, manifest, "")
	if len(all) != 2 {
		t.Errorf("walk for all tables: %v", all)
	}
}
