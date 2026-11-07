package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/avantexa/coldplane/internal/dbutil"
	"github.com/avantexa/coldplane/internal/duck"
)

// FileEntry is one Parquet file of a backup, keyed relative to the backup directory.
type FileEntry struct {
	Key       string `json:"key"`
	SizeBytes int64  `json:"size_bytes"`
}

// TableEntry describes one archived table.
type TableEntry struct {
	Name            string             `json:"name"`
	PartitionColumn *string            `json:"partition_column"`
	Files           []FileEntry        `json:"files"`
	SizeBytes       int64              `json:"size_bytes"`
	RowCount        int64              `json:"row_count"`
	Columns         []string           `json:"columns,omitempty"`      // source column order
	ChunkKind       string             `json:"chunk_kind,omitempty"`   // id, time or offset
	ChunkColumn     string             `json:"chunk_column,omitempty"` // key or timestamp the chunks ranged over
	Verified        *TableVerification `json:"verified,omitempty"`
	Purged          *TablePurge        `json:"purged,omitempty"`
}

// Manifest is the content of a backup's index.json.
type Manifest struct {
	Name       string       `json:"name"`
	DBType     string       `json:"db_type"`
	DBName     string       `json:"db_name"`
	CreatedAt  string       `json:"created_at"`
	Tables     []TableEntry `json:"tables"`
	SizeBytes  int64        `json:"size_bytes"`
	Local      bool         `json:"local"`
	Where      string       `json:"where,omitempty"`       // row filter the backup was taken with
	Engine     string       `json:"engine,omitempty"`      // extraction engine, "duckdb"
	VerifiedAt string       `json:"verified_at,omitempty"` // set when every table passed verification
	PurgedAt   string       `json:"purged_at,omitempty"`   // set when the archived rows were removed from the source
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
	baseDir := opts.OutputDir
	if baseDir == "" {
		baseDir = defaultBackupDir()
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
func writeBackupManifest(backupDir, name, dbType, dbName, where string, tables []TableEntry) (int64, error) {
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
		Where:     where,
		Engine:    "duckdb",
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

// ReadManifest reads and parses the index.json of a backup directory.
func ReadManifest(backupDir string) (Manifest, error) {
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

// srcAlias is the name the source database is attached under in DuckDB.
const srcAlias = "src"

// sourceColumns returns the table's column names in source order, as DuckDB sees them.
func sourceColumns(ctx context.Context, eng *duck.Engine, kind duck.Kind, table string) ([]string, error) {
	rows, err := eng.DB().QueryContext(ctx, duck.DescribeSQL(kind, srcAlias, table))
	if err != nil {
		return nil, fmt.Errorf("describe table %s: %w", table, err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var name, typ, null, key, def, extra sql.NullString
		if err := rows.Scan(&name, &typ, &null, &key, &def, &extra); err != nil {
			return nil, fmt.Errorf("describe table %s: %w", table, err)
		}
		columns = append(columns, name.String)
	}
	return columns, rows.Err()
}

// tableEntry builds a table's manifest entry with its chunking provenance.
func tableEntry(table, partitionColumn string, files []FileEntry, columns []string, kind chunkKind, chunkColumn string, rows int64) TableEntry {
	e := newTableEntry(table, partitionColumn, files)
	e.Columns = columns
	e.ChunkKind = kind.String()
	e.ChunkColumn = chunkColumn
	e.RowCount = rows
	return e
}

// tableArchiver archives one table of the attached source into backupDir.
type tableArchiver func(ctx context.Context, eng *duck.Engine, kind duck.Kind, table, backupDir string) (TableEntry, error)

// runBackup is the engine-independent backup: prepare the directory, attach
// the source to DuckDB read-only, archive each table, write index.json. A
// failure removes the directory so no half-written backup lingers.
func runBackup(ctx context.Context, dbType, dsn string, tables []string, name string, opts ArchiveOptions, archiveOne tableArchiver) (err error) {
	tag := "[archive-" + dbType + "]"
	fmt.Printf("%s Starting archive process\n", tag)
	backupDir, err := prepareBackupDir(opts, name)
	if err != nil {
		return fmt.Errorf("%s %w", tag, err)
	}
	defer func() {
		if err != nil {
			discardBackupDir(backupDir)
		}
	}()

	eng, err := duck.Open(ctx)
	if err != nil {
		return fmt.Errorf("%s %w", tag, err)
	}
	defer eng.Close()
	kind, err := eng.Attach(ctx, dsn, srcAlias, true)
	if err != nil {
		return fmt.Errorf("%s %w", tag, err)
	}

	var entries []TableEntry
	for _, table := range tables {
		entry, err := archiveOne(ctx, eng, kind, table, backupDir)
		if err != nil {
			return fmt.Errorf("%s failed to archive table %s: %w", tag, table, err)
		}
		entries = append(entries, entry)
	}

	total, err := writeBackupManifest(backupDir, name, dbType, dbutil.DBNameFromDSN(dsn), opts.Where, entries)
	if err != nil {
		return fmt.Errorf("%s failed to write local manifest: %w", tag, err)
	}
	fmt.Printf("%s Backup complete: %s (%d bytes)\n", tag, backupDir, total)
	return nil
}
