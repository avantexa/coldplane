package archive

import (
	"testing"
)

func TestBackupInfoFromManifest(t *testing.T) {
	m := Manifest{
		DBType: "mysql", DBName: "shop", CreatedAt: "2025-01-01T00:00:00Z", SizeBytes: 2048,
		Tables: []TableEntry{{Name: "orders"}, {Name: "items"}},
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
