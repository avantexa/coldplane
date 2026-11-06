package archive

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// defaultLocalBackupDir returns the default directory for local backups.
func defaultLocalBackupDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".coldplane/backups"
	}
	return filepath.Join(home, ".coldplane", "backups")
}

// writeLocalManifest writes an index.json manifest to a local backup directory.
func writeLocalManifest(backupDir string, manifest map[string]interface{}) error {
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("create backup dir: %w", err)
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return os.WriteFile(filepath.Join(backupDir, "index.json"), data, 0644)
}

// LocalBackupInfo holds metadata about a local backup, read from its index.json.
type LocalBackupInfo struct {
	Name      string
	DBType    string
	DBName    string
	Tables    []string
	CreatedAt string
	SizeBytes int64
	Path      string
}

// ListLocalBackups scans the local backup directory and returns info for each backup.
func ListLocalBackups(baseDir string) ([]LocalBackupInfo, error) {
	if baseDir == "" {
		baseDir = defaultLocalBackupDir()
	}

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read backup dir: %w", err)
	}

	var backups []LocalBackupInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		manifestPath := filepath.Join(baseDir, entry.Name(), "index.json")
		data, err := os.ReadFile(manifestPath)
		if err != nil {
			continue // skip dirs without index.json
		}

		var manifest map[string]interface{}
		if err := json.Unmarshal(data, &manifest); err != nil {
			continue
		}
		info := backupInfoFromManifest(manifest, filepath.Join(baseDir, entry.Name()))
		backups = append(backups, info)
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].CreatedAt > backups[j].CreatedAt // newest first
	})
	return backups, nil
}

// backupInfoFromManifest summarizes a parsed index.json; the directory name
// stands in when the manifest has no name.
func backupInfoFromManifest(manifest map[string]interface{}, dir string) LocalBackupInfo {
	info := LocalBackupInfo{
		Name:      getString(manifest, "name"),
		Path:      dir,
		DBType:    getString(manifest, "db_type"),
		DBName:    getString(manifest, "db_name"),
		CreatedAt: getString(manifest, "created_at"),
	}
	if info.Name == "" {
		info.Name = filepath.Base(dir)
	}
	if sz, ok := manifest["size_bytes"].(float64); ok {
		info.SizeBytes = int64(sz)
	}
	if tables, ok := manifest["tables"].([]interface{}); ok {
		for _, t := range tables {
			if tMap, ok := t.(map[string]interface{}); ok {
				if name, ok := tMap["name"].(string); ok {
					info.Tables = append(info.Tables, name)
				}
			}
		}
	}
	return info
}

// ReadLocalManifest reads and parses the index.json from a local backup directory.
func ReadLocalManifest(backupDir string) (map[string]interface{}, error) {
	data, err := os.ReadFile(filepath.Join(backupDir, "index.json"))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var manifest map[string]interface{}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	return manifest, nil
}

// ResolveLocalBackupDir resolves a backup name or path to the backup directory.
func ResolveLocalBackupDir(nameOrPath string) string {
	// If it looks like an absolute path, use it directly
	if filepath.IsAbs(nameOrPath) {
		return nameOrPath
	}
	return filepath.Join(defaultLocalBackupDir(), nameOrPath)
}

// DeleteLocalBackup removes a local backup directory by name or path.
func DeleteLocalBackup(nameOrPath string) error {
	dir := ResolveLocalBackupDir(nameOrPath)

	// Verify it's actually a backup (has index.json) before deleting
	if _, err := os.Stat(filepath.Join(dir, "index.json")); os.IsNotExist(err) {
		return fmt.Errorf("local backup %q not found", nameOrPath)
	}

	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("delete local backup: %w", err)
	}
	fmt.Printf("Deleted local backup: %s\n", dir)
	return nil
}

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}
