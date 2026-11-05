package archive

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultLocalBackupDir(t *testing.T) {
	dir := DefaultLocalBackupDir()
	if dir == "" {
		t.Fatal("DefaultLocalBackupDir returned empty string")
	}
	if !filepath.IsAbs(dir) {
		// On systems where UserHomeDir works, it should be absolute
		home, err := os.UserHomeDir()
		if err == nil {
			_ = home
			t.Errorf("expected absolute path, got %q", dir)
		}
	}
	if filepath.Base(dir) != "backups" {
		t.Errorf("expected path to end with 'backups', got %q", dir)
	}
}

func TestWriteLocalManifest(t *testing.T) {
	dir := t.TempDir()
	backupDir := filepath.Join(dir, "my_backup")

	manifest := map[string]interface{}{
		"name":       "my_backup",
		"db_type":    "postgres",
		"created_at": "2025-01-01T00:00:00Z",
		"tables":     []interface{}{},
		"size_bytes": float64(1024),
		"local":      true,
	}

	err := WriteLocalManifest(backupDir, manifest)
	if err != nil {
		t.Fatalf("WriteLocalManifest failed: %v", err)
	}

	// Verify the file was created
	data, err := os.ReadFile(filepath.Join(backupDir, "index.json"))
	if err != nil {
		t.Fatalf("index.json not created: %v", err)
	}

	// Verify the content is valid JSON with expected fields
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("index.json is not valid JSON: %v", err)
	}
	if parsed["name"] != "my_backup" {
		t.Errorf("name = %v, want 'my_backup'", parsed["name"])
	}
	if parsed["db_type"] != "postgres" {
		t.Errorf("db_type = %v, want 'postgres'", parsed["db_type"])
	}
	if parsed["local"] != true {
		t.Errorf("local = %v, want true", parsed["local"])
	}
}

func TestWriteLocalManifest_CreatesDir(t *testing.T) {
	dir := t.TempDir()
	backupDir := filepath.Join(dir, "deeply", "nested", "backup")

	manifest := map[string]interface{}{"name": "test"}
	err := WriteLocalManifest(backupDir, manifest)
	if err != nil {
		t.Fatalf("WriteLocalManifest failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(backupDir, "index.json")); os.IsNotExist(err) {
		t.Error("index.json was not created in nested directory")
	}
}

func TestListLocalBackups_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	backups, err := ListLocalBackups(dir)
	if err != nil {
		t.Fatalf("ListLocalBackups failed: %v", err)
	}
	if len(backups) != 0 {
		t.Errorf("expected 0 backups, got %d", len(backups))
	}
}

func TestListLocalBackups_NonExistentDir(t *testing.T) {
	backups, err := ListLocalBackups("/nonexistent/path/12345")
	if err != nil {
		t.Fatalf("ListLocalBackups should not error on missing dir: %v", err)
	}
	if backups != nil {
		t.Errorf("expected nil backups, got %d", len(backups))
	}
}

func TestListLocalBackups_WithBackups(t *testing.T) {
	dir := t.TempDir()

	// Create two backup directories with manifests
	for _, name := range []string{"backup_a", "backup_b"} {
		manifest := map[string]interface{}{
			"name":       name,
			"db_type":    "postgres",
			"db_name":    "testdb",
			"created_at": "2025-01-01T00:00:00Z",
			"size_bytes": float64(2048),
			"tables": []interface{}{
				map[string]interface{}{"name": "orders"},
				map[string]interface{}{"name": "users"},
			},
		}
		if err := WriteLocalManifest(filepath.Join(dir, name), manifest); err != nil {
			t.Fatal(err)
		}
	}

	// Create a non-backup directory (no index.json)
	os.MkdirAll(filepath.Join(dir, "not_a_backup"), 0755)

	backups, err := ListLocalBackups(dir)
	if err != nil {
		t.Fatalf("ListLocalBackups failed: %v", err)
	}
	if len(backups) != 2 {
		t.Fatalf("expected 2 backups, got %d", len(backups))
	}

	// Check fields
	found := map[string]bool{}
	for _, b := range backups {
		found[b.Name] = true
		if b.DBType != "postgres" {
			t.Errorf("backup %s: DBType = %q, want 'postgres'", b.Name, b.DBType)
		}
		if b.DBName != "testdb" {
			t.Errorf("backup %s: DBName = %q, want 'testdb'", b.Name, b.DBName)
		}
		if b.SizeBytes != 2048 {
			t.Errorf("backup %s: SizeBytes = %d, want 2048", b.Name, b.SizeBytes)
		}
		if len(b.Tables) != 2 {
			t.Errorf("backup %s: expected 2 tables, got %d", b.Name, len(b.Tables))
		}
	}
	if !found["backup_a"] || !found["backup_b"] {
		t.Errorf("missing expected backups: %v", found)
	}
}

func TestResolveLocalBackupDir_AbsolutePath(t *testing.T) {
	got := ResolveLocalBackupDir("/tmp/my_backup")
	if got != "/tmp/my_backup" {
		t.Errorf("expected /tmp/my_backup, got %q", got)
	}
}

func TestResolveLocalBackupDir_RelativeName(t *testing.T) {
	got := ResolveLocalBackupDir("my_backup")
	expected := filepath.Join(DefaultLocalBackupDir(), "my_backup")
	if got != expected {
		t.Errorf("got %q, want %q", got, expected)
	}
}

func TestReadLocalManifest(t *testing.T) {
	dir := t.TempDir()
	manifest := map[string]interface{}{
		"name":    "test_backup",
		"db_type": "mysql",
	}
	if err := WriteLocalManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}

	got, err := ReadLocalManifest(dir)
	if err != nil {
		t.Fatalf("ReadLocalManifest failed: %v", err)
	}
	if got["name"] != "test_backup" {
		t.Errorf("name = %v, want 'test_backup'", got["name"])
	}
	if got["db_type"] != "mysql" {
		t.Errorf("db_type = %v, want 'mysql'", got["db_type"])
	}
}

func TestReadLocalManifest_NotFound(t *testing.T) {
	_, err := ReadLocalManifest("/nonexistent/path")
	if err == nil {
		t.Error("expected error for missing manifest")
	}
}

func TestDeleteLocalBackup(t *testing.T) {
	dir := t.TempDir()
	backupName := "to_delete"

	manifest := map[string]interface{}{"name": backupName, "db_type": "postgres"}
	if err := WriteLocalManifest(filepath.Join(dir, backupName), manifest); err != nil {
		t.Fatal(err)
	}

	// Delete using absolute path
	err := DeleteLocalBackup(filepath.Join(dir, backupName))
	if err != nil {
		t.Fatalf("DeleteLocalBackup failed: %v", err)
	}

	// Verify directory is gone
	if _, err := os.Stat(filepath.Join(dir, backupName)); !os.IsNotExist(err) {
		t.Error("backup directory should have been removed")
	}
}

func TestDeleteLocalBackup_NotFound(t *testing.T) {
	err := DeleteLocalBackup("/nonexistent/path/fake_backup")
	if err == nil {
		t.Error("expected error for missing backup")
	}
}

func TestGetString(t *testing.T) {
	m := map[string]interface{}{
		"name":  "value",
		"count": float64(42),
	}
	if getString(m, "name") != "value" {
		t.Error("expected 'value'")
	}
	if getString(m, "count") != "" {
		t.Error("expected empty string for non-string value")
	}
	if getString(m, "missing") != "" {
		t.Error("expected empty string for missing key")
	}
}
