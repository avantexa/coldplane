package archive

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"
)

// FileEntry is one Parquet file of a backup, keyed relative to the backup directory.
type FileEntry struct {
	Key       string `json:"key"`
	SizeBytes int64  `json:"size_bytes"`
}

// TableEntry describes one archived table.
type TableEntry struct {
	Name            string      `json:"name"`
	PartitionColumn *string     `json:"partition_column"`
	Files           []FileEntry `json:"files"`
	SizeBytes       int64       `json:"size_bytes"`
}

// Manifest is the content of a backup's index.json.
type Manifest struct {
	Name      string       `json:"name"`
	DBType    string       `json:"db_type"`
	DBName    string       `json:"db_name"`
	CreatedAt string       `json:"created_at"`
	Tables    []TableEntry `json:"tables"`
	SizeBytes int64        `json:"size_bytes"`
	Local     bool         `json:"local"`
}

// TableNames lists the archived tables in manifest order.
func (m Manifest) TableNames() []string {
	names := make([]string, 0, len(m.Tables))
	for _, t := range m.Tables {
		names = append(names, t.Name)
	}
	return names
}

// backupDirFor returns the directory a named backup is written to.
func backupDirFor(opts ArchiveOptions, name string) string {
	baseDir := opts.LocalOutputDir
	if baseDir == "" {
		baseDir = defaultLocalBackupDir()
	}
	return path.Join(baseDir, name)
}

// sumSizes adds up the sizes of file entries.
func sumSizes(files []FileEntry) int64 {
	var total int64
	for _, f := range files {
		total += f.SizeBytes
	}
	return total
}

// newTableEntry builds the manifest entry for one archived table. An empty
// partition column is recorded as null and no files as an empty list.
func newTableEntry(table, partitionColumn string, files []FileEntry) TableEntry {
	var partCol *string
	if partitionColumn != "" {
		partCol = &partitionColumn
	}
	if files == nil {
		files = []FileEntry{}
	}
	return TableEntry{Name: table, PartitionColumn: partCol, Files: files, SizeBytes: sumSizes(files)}
}

// writeBackupManifest writes index.json for a finished backup and returns the
// total size of its files.
func writeBackupManifest(backupDir, name, dbType, dbName string, tables []TableEntry) (int64, error) {
	if tables == nil {
		tables = []TableEntry{}
	}
	m := Manifest{
		Name:      name,
		DBType:    dbType,
		DBName:    dbName,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Tables:    tables,
		Local:     true,
	}
	for _, t := range tables {
		m.SizeBytes += t.SizeBytes
	}
	if err := writeManifest(backupDir, m); err != nil {
		return 0, err
	}
	return m.SizeBytes, nil
}

// writeManifest serializes m as index.json in backupDir, creating the directory.
func writeManifest(backupDir string, m Manifest) error {
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("create backup dir: %w", err)
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return os.WriteFile(filepath.Join(backupDir, "index.json"), data, 0644)
}

// ReadLocalManifest reads and parses the index.json of a backup directory.
func ReadLocalManifest(backupDir string) (Manifest, error) {
	data, err := os.ReadFile(filepath.Join(backupDir, "index.json"))
	if err != nil {
		return Manifest{}, fmt.Errorf("read manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("parse manifest: %w", err)
	}
	return m, nil
}

// prepareBackupDir returns the directory for a new backup and refuses to
// reuse one that already exists, so a rerun never merges into an old backup.
func prepareBackupDir(opts ArchiveOptions, name string) (string, error) {
	dir := backupDirFor(opts, name)
	if _, err := os.Stat(dir); err == nil {
		return "", fmt.Errorf("backup %q already exists at %s; choose another --name or delete it first", name, dir)
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("check backup dir: %w", err)
	}
	return dir, nil
}

// discardBackupDir removes a backup directory a failed run left behind, so
// no half-written backup without index.json lingers. Best effort.
func discardBackupDir(dir string) {
	if err := os.RemoveAll(dir); err == nil {
		fmt.Printf("Removed incomplete backup at %s\n", dir)
	}
}
