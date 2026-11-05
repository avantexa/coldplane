package archive

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMoveByCopy(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	dst := filepath.Join(dir, "sub", "dst.bin")
	if err := os.WriteFile(src, []byte("hello parquet"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		t.Fatal(err)
	}
	n, err := moveByCopy(src, dst)
	if err != nil || n != 13 {
		t.Fatalf("moveByCopy: n=%d err=%v", n, err)
	}
	if got, _ := os.ReadFile(dst); string(got) != "hello parquet" {
		t.Errorf("dest content %q", got)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Error("source should be removed after copy")
	}
	if _, err := moveByCopy(filepath.Join(dir, "missing"), dst); err == nil {
		t.Error("missing source should fail")
	}
}

func TestBackupInfoFromManifest(t *testing.T) {
	m := map[string]interface{}{
		"name": "", "db_type": "mysql", "db_name": "shop", "created_at": "2025-01-01T00:00:00Z",
		"size_bytes": float64(2048),
		"tables":     []interface{}{map[string]interface{}{"name": "orders"}, map[string]interface{}{"name": "items"}, "junk"},
	}
	info := backupInfoFromManifest(m, "/backups/orders_20250101")
	if info.Name != "orders_20250101" {
		t.Errorf("name should fall back to the directory, got %q", info.Name)
	}
	if info.DBType != "mysql" || info.DBName != "shop" || info.SizeBytes != 2048 || info.Path != "/backups/orders_20250101" {
		t.Errorf("fields: %+v", info)
	}
	if len(info.Tables) != 2 || info.Tables[1] != "items" {
		t.Errorf("tables: %v", info.Tables)
	}
}
