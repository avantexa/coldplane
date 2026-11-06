package archive

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// defaultBackupDir returns the default directory for local backups.
func defaultBackupDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".coldplane/backups"
	}
	return filepath.Join(home, ".coldplane", "backups")
}

// BackupInfo holds metadata about a local backup, read from its index.json.
type BackupInfo struct {
	Name      string
	DBType    string
	DBName    string
	Tables    []string
	CreatedAt string
	SizeBytes int64
	Path      string
}

// ListBackups scans the local backup directory and returns info for each backup.
func ListBackups(baseDir string) ([]BackupInfo, error) {
	if baseDir == "" {
		baseDir = defaultBackupDir()
	}

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read backup dir: %w", err)
	}

	var backups []BackupInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(baseDir, entry.Name())
		manifest, err := ReadManifest(dir)
		if err != nil {
			continue // not a backup, or an unreadable one
		}
		backups = append(backups, backupInfoFromManifest(manifest, dir))
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].CreatedAt > backups[j].CreatedAt // newest first
	})
	return backups, nil
}

// backupInfoFromManifest summarizes a parsed index.json; the directory name
// stands in when the manifest has no name.
func backupInfoFromManifest(m Manifest, dir string) BackupInfo {
	info := BackupInfo{
		Name:      m.Name,
		Path:      dir,
		DBType:    m.DBType,
		DBName:    m.DBName,
		CreatedAt: m.CreatedAt,
		SizeBytes: m.SizeBytes,
		Tables:    m.TableNames(),
	}
	if info.Name == "" {
		info.Name = filepath.Base(dir)
	}
	return info
}

// ResolveBackupDir resolves a backup name or path to the backup directory.
func ResolveBackupDir(nameOrPath string) string {
	// If it looks like an absolute path, use it directly
	if filepath.IsAbs(nameOrPath) {
		return nameOrPath
	}
	return filepath.Join(defaultBackupDir(), nameOrPath)
}

// DeleteBackup removes a local backup directory by name or path.
func DeleteBackup(nameOrPath string) error {
	dir := ResolveBackupDir(nameOrPath)

	// Verify it's actually a backup (has index.json) before deleting
	if _, err := os.Stat(filepath.Join(dir, "index.json")); os.IsNotExist(err) {
		return fmt.Errorf("backup %q not found", nameOrPath)
	}

	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("delete local backup: %w", err)
	}
	fmt.Printf("Deleted backup: %s\n", dir)
	return nil
}
