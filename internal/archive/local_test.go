package archive

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultBackupDir(t *testing.T) {
	dir := defaultBackupDir()
	if dir == "" {
		t.Fatal("defaultBackupDir returned empty string")
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

func TestListBackups_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	backups, err := ListBackups(dir)
	if err != nil {
		t.Fatalf("ListBackups failed: %v", err)
	}
	if len(backups) != 0 {
		t.Errorf("expected 0 backups, got %d", len(backups))
	}
}

func TestListBackups_NonExistentDir(t *testing.T) {
	backups, err := ListBackups("/nonexistent/path/12345")
	if err != nil {
		t.Fatalf("ListBackups should not error on missing dir: %v", err)
	}
	if backups != nil {
		t.Errorf("expected nil backups, got %d", len(backups))
	}
}

func TestListBackups_WithBackups(t *testing.T) {
	dir := t.TempDir()

	// Create two backup directories with manifests
	for _, name := range []string{"backup_a", "backup_b"} {
		manifest := Manifest{
			Name: name, DBType: "postgres", DBName: "testdb", CreatedAt: "2025-01-01T00:00:00Z", SizeBytes: 2048,
			Tables: []TableEntry{{Name: "orders"}, {Name: "users"}},
		}
		if err := writeManifest(filepath.Join(dir, name), manifest); err != nil {
			t.Fatal(err)
		}
	}

	// Create a non-backup directory (no index.json)
	os.MkdirAll(filepath.Join(dir, "not_a_backup"), 0755)

	backups, err := ListBackups(dir)
	if err != nil {
		t.Fatalf("ListBackups failed: %v", err)
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

func TestResolveBackupDir_AbsolutePath(t *testing.T) {
	got := ResolveBackupDir("/tmp/my_backup")
	if got != "/tmp/my_backup" {
		t.Errorf("expected /tmp/my_backup, got %q", got)
	}
}

func TestResolveBackupDir_RelativeName(t *testing.T) {
	got := ResolveBackupDir("my_backup")
	expected := filepath.Join(defaultBackupDir(), "my_backup")
	if got != expected {
		t.Errorf("got %q, want %q", got, expected)
	}
}

func TestDeleteBackup(t *testing.T) {
	dir := t.TempDir()
	backupName := "to_delete"

	if err := writeManifest(filepath.Join(dir, backupName), Manifest{Name: backupName, DBType: "postgres"}); err != nil {
		t.Fatal(err)
	}

	// Delete using absolute path
	err := DeleteBackup(filepath.Join(dir, backupName))
	if err != nil {
		t.Fatalf("DeleteBackup failed: %v", err)
	}

	// Verify directory is gone
	if _, err := os.Stat(filepath.Join(dir, backupName)); !os.IsNotExist(err) {
		t.Error("backup directory should have been removed")
	}
}

func TestDeleteLocalBackup_NotFound(t *testing.T) {
	err := DeleteBackup("/nonexistent/path/fake_backup")
	if err == nil {
		t.Error("expected error for missing backup")
	}
}
