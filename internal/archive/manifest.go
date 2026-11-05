package archive

import (
	"path"
	"time"
)

// backupDirFor returns the directory a named backup is written to.
func backupDirFor(opts ArchiveOptions, name string) string {
	baseDir := opts.LocalOutputDir
	if baseDir == "" {
		baseDir = DefaultLocalBackupDir()
	}
	return path.Join(baseDir, name)
}

// sumSizes adds up the size_bytes of manifest file entries, tolerating the
// numeric types a JSON round trip can produce.
func sumSizes(files []map[string]interface{}) int64 {
	var total int64
	for _, f := range files {
		switch v := f["size_bytes"].(type) {
		case int64:
			total += v
		case int:
			total += int64(v)
		case float64:
			total += int64(v)
		}
	}
	return total
}

// tableManifest builds the manifest entry for one archived table.
func tableManifest(table, partitionColumn string, files []map[string]interface{}) map[string]interface{} {
	var partCol interface{}
	if partitionColumn != "" {
		partCol = partitionColumn
	}
	return map[string]interface{}{
		"name":             table,
		"partition_column": partCol,
		"files":            files,
		"size_bytes":       sumSizes(files),
	}
}

// writeBackupManifest writes index.json for a finished backup and returns the
// total size of its files.
func writeBackupManifest(backupDir, name, dbType, dbName string, tables []map[string]interface{}) (int64, error) {
	var total int64
	for _, t := range tables {
		if files, ok := t["files"].([]map[string]interface{}); ok {
			total += sumSizes(files)
		}
	}
	manifest := map[string]interface{}{
		"name":       name,
		"db_type":    dbType,
		"db_name":    dbName,
		"created_at": time.Now().UTC().Format(time.RFC3339),
		"tables":     tables,
		"size_bytes": total,
		"local":      true,
	}
	if err := WriteLocalManifest(backupDir, manifest); err != nil {
		return 0, err
	}
	return total, nil
}
