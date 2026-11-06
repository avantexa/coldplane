package archive

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/avantexa/coldplane/internal/dbutil"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// ArchiveMySQL archives one or more tables into local Parquet files under a single backup name.
func ArchiveMySQL(ctx context.Context, dsn string, tables []string, name string, opts ArchiveOptions) (err error) {
	fmt.Println("[archive-mysql] Starting archive process")
	backupDir, err := prepareBackupDir(opts, name)
	if err != nil {
		return fmt.Errorf("[archive-mysql] %w", err)
	}
	defer func() {
		if err != nil {
			discardBackupDir(backupDir)
		}
	}()
	store := &LocalFileStore{BaseDir: backupDir}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("[archive-mysql] failed to connect to MySQL: %w", err)
	}
	defer db.Close()

	var manifestTables []TableEntry
	for _, table := range tables {
		files, partCol, err := archiveMySQLTable(ctx, db, store, table, opts.Where, opts)
		if err != nil {
			return fmt.Errorf("[archive-mysql] failed to archive table %s: %w", table, err)
		}
		manifestTables = append(manifestTables, newTableEntry(table, partCol, files))
	}

	total, err := writeBackupManifest(backupDir, name, "mysql", dbutil.DBNameFromDSN(dsn), manifestTables)
	if err != nil {
		return fmt.Errorf("[archive-mysql] failed to write local manifest: %w", err)
	}
	fmt.Printf("[archive-mysql] Backup complete: %s (%d bytes)\n", backupDir, total)
	return nil
}

// archiveMySQLTable archives one table and returns its manifest entries and
// the partition column it used.
func archiveMySQLTable(ctx context.Context, db *sql.DB, store FileStore, table, where string, opts ArchiveOptions) ([]FileEntry, string, error) {
	progress := &ProgressTracker{Table: table}

	kind, column, err := mysqlChunkColumn(db, table)
	if err != nil {
		return nil, "", fmt.Errorf("[archive-mysql] failed to detect chunk column: %w", err)
	}
	partitionColumn := mysqlPartitionColumn(db, table)
	if partitionColumn == "" && kind == chunkByOffset {
		partitionColumn = detectMySQLDatetimeColumn(db, table)
	}

	plan, err := mysqlChunkPlan(db, table, where, kind, column, opts.ChunkSize, progress)
	if err != nil {
		return nil, partitionColumn, err
	}
	entries, err := archiveTable(ctx, mysqlRunner(db), store, table, partitionColumn, plan, opts.Workers, progress)
	return entries, partitionColumn, err
}

// mysqlRunner executes chunk queries on db.
func mysqlRunner(db *sql.DB) chunkRunner {
	return func(ctx context.Context, query string, args []interface{}) (rowSource, func(), error) {
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, nil, fmt.Errorf("query error: %w", err)
		}
		src, err := newSQLRowSource(rows)
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
		if debug {
			fmt.Printf("[DEBUG] Detected columns: %v\n", src.columns)
		}
		return src, func() { rows.Close() }, nil
	}
}

// mysqlChunkPlan plans the chunks for the detected strategy.
func mysqlChunkPlan(db *sql.DB, table, where string, kind chunkKind, column string, chunkSize int, progress *ProgressTracker) (chunkPlan, error) {
	switch kind {
	case chunkByID:
		return mysqlIDChunks(db, table, where, column, chunkSize, progress)
	case chunkByTime:
		return mysqlTimeChunks(db, table, where, column, progress)
	default:
		return mysqlOffsetChunks(db, table, where, chunkSize, progress)
	}
}

// mysqlPartitionColumn infers the partition column from the first
// datetime-looking value of the table's first row.
func mysqlPartitionColumn(db *sql.DB, table string) string {
	columns, firstRow := mysqlSampleRow(db, table)
	if col := firstTimeColumn(columns, firstRow); col != "" {
		fmt.Printf("[INFO] Inferred partition column '%s' from first row.\n", col)
		return col
	}
	fmt.Printf("[WARNING] No valid datetime column found in first row. Partitioning will fallback to 'unknown'.\n")
	return ""
}

// mysqlSampleRow returns the table's column names and the values of its first
// row, or a nil row when the table is empty or cannot be read.
func mysqlSampleRow(db *sql.DB, table string) ([]string, []interface{}) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", dbutil.QuoteMySQL(table)))
	if err != nil {
		return nil, nil
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil || !rows.Next() {
		return columns, nil
	}
	vals := make([]interface{}, len(columns))
	ptrs := make([]interface{}, len(columns))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return columns, nil
	}
	return columns, vals
}

// detectMySQLDatetimeColumn picks a DATETIME/TIMESTAMP column by schema for
// partitioning when the first row offered no hint.
func detectMySQLDatetimeColumn(db *sql.DB, table string) string {
	col, _ := mysqlFirstDatetimeColumn(db, table)
	if col == "" {
		fmt.Printf("[WARN] No DATETIME column detected. Partitioning may default to 'unknown'.\n")
		return ""
	}
	fmt.Printf("[WARN] Detected DATETIME column '%s' for partitioning.\n", col)
	return col
}

// mysqlReportNoRows explains why a chunk plan came out empty.
func mysqlReportNoRows(db *sql.DB, table, kind string) {
	var total int
	_ = db.QueryRow(mysqlCountQuery(table, "")).Scan(&total)
	if total == 0 {
		fmt.Printf("[archive-mysql] Table %s is empty (0 rows)\n", table)
	} else {
		fmt.Printf("[archive-mysql] Table %s has %d rows but none match the %s\n", table, total, kind)
	}
}

// mysqlIDChunks plans primary-key range chunks.
func mysqlIDChunks(db *sql.DB, table, where, idColumn string, chunkSize int, progress *ProgressTracker) (chunkPlan, error) {
	var minID, maxID sql.NullInt64
	if err := db.QueryRow(mysqlMinMaxQuery(table, where, idColumn)).Scan(&minID, &maxID); err != nil {
		return chunkPlan{}, fmt.Errorf("[archive-mysql] failed to get min/max id: %w", err)
	}
	if !minID.Valid || !maxID.Valid || minID.Int64 > maxID.Int64 {
		mysqlReportNoRows(db, table, "ID range")
		return chunkPlan{}, nil
	}
	ranges := planIDChunks(minID.Int64, maxID.Int64, int64(chunkSize))
	progress.TotalRows = maxID.Int64 - minID.Int64 + 1
	progress.TotalChunks = len(ranges)
	fmt.Printf("[archive-mysql] Using %q for chunking (min: %d, max: %d)\n", idColumn, minID.Int64, maxID.Int64)
	sel := mysqlRangeQuery(table, where, idColumn)
	return chunkPlan{n: len(ranges), query: func(i int) (string, []interface{}) {
		return sel, []interface{}{ranges[i].Start, ranges[i].End}
	}}, nil
}

// mysqlTimeChunks plans time-window chunks. MySQL DATETIME has second
// precision by default, so the last window is nudged by one second.
func mysqlTimeChunks(db *sql.DB, table, where, timeColumn string, progress *ProgressTracker) (chunkPlan, error) {
	var minStr, maxStr sql.NullString
	if err := db.QueryRow(mysqlMinMaxQuery(table, where, timeColumn)).Scan(&minStr, &maxStr); err != nil {
		return chunkPlan{}, fmt.Errorf("[archive-mysql] failed to get min/max timestamp: %w", err)
	}
	if !minStr.Valid || !maxStr.Valid {
		mysqlReportNoRows(db, table, "time range")
		return chunkPlan{}, nil
	}
	minTime, errMin := dbutil.ParseTime(minStr.String)
	maxTime, errMax := dbutil.ParseTime(maxStr.String)
	if errMin != nil || errMax != nil {
		return chunkPlan{}, fmt.Errorf("[archive-mysql] failed to parse min/max timestamp: min: %v max: %v", errMin, errMax)
	}
	if minTime.After(maxTime) {
		mysqlReportNoRows(db, table, "time range")
		return chunkPlan{}, nil
	}
	window := timeWindow(maxTime.Sub(minTime), defaultTimeWindow, maxTimeChunks)
	ranges := planTimeChunks(minTime, maxTime, window, time.Second)
	progress.TotalChunks = len(ranges)
	fmt.Printf("[archive-mysql] Using %q for time chunking (min: %s, max: %s) with window %s (%d chunks expected)\n",
		timeColumn, minTime.Format(time.RFC3339), maxTime.Format(time.RFC3339), window, len(ranges))
	sel := mysqlRangeQuery(table, where, timeColumn)
	return chunkPlan{n: len(ranges), query: func(i int) (string, []interface{}) {
		return sel, []interface{}{ranges[i].Start.Format(mysqlTimeLayout), ranges[i].End.Format(mysqlTimeLayout)}
	}}, nil
}

// mysqlOffsetChunks plans LIMIT/OFFSET pages.
func mysqlOffsetChunks(db *sql.DB, table, where string, chunkSize int, progress *ProgressTracker) (chunkPlan, error) {
	fmt.Println("[archive-mysql] Fallback: using OFFSET/LIMIT chunking (no PK or timestamp found)")
	var totalRows int
	if err := db.QueryRow(mysqlCountQuery(table, where)).Scan(&totalRows); err != nil {
		return chunkPlan{}, fmt.Errorf("[archive-mysql] failed to count rows: %w", err)
	}
	fmt.Printf("[archive-mysql] Row count: %d\n", totalRows)
	pages := planOffsetChunks(totalRows, chunkSize)
	progress.TotalRows = int64(totalRows)
	progress.TotalChunks = len(pages)
	sel := mysqlOffsetQuery(table, where)
	return chunkPlan{n: len(pages), query: func(i int) (string, []interface{}) {
		return sel, []interface{}{pages[i].Limit, pages[i].Offset}
	}}, nil
}

// mysqlChunkColumn picks the chunking strategy: an integer primary key
// first, then the first DATETIME/TIMESTAMP column, else OFFSET/LIMIT.
func mysqlChunkColumn(db *sql.DB, table string) (chunkKind, string, error) {
	if col := mysqlNumericPrimaryKey(db, table); col != "" {
		return chunkByID, col, nil
	}
	col, err := mysqlFirstDatetimeColumn(db, table)
	if err != nil {
		return chunkByOffset, "", err
	}
	if col != "" {
		return chunkByTime, col, nil
	}
	return chunkByOffset, "", nil
}

// mysqlNumericPrimaryKey returns the first primary-key column of the table in
// the current schema when it has an integer type, else "".
func mysqlNumericPrimaryKey(db *sql.DB, table string) string {
	const q = `SELECT c.COLUMN_NAME, c.DATA_TYPE
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
		JOIN INFORMATION_SCHEMA.COLUMNS c
		  ON c.TABLE_SCHEMA = k.TABLE_SCHEMA AND c.TABLE_NAME = k.TABLE_NAME AND c.COLUMN_NAME = k.COLUMN_NAME
		WHERE k.TABLE_SCHEMA = DATABASE() AND k.TABLE_NAME = ? AND k.CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY k.ORDINAL_POSITION LIMIT 1`
	var col, dataType string
	if err := db.QueryRow(q, table).Scan(&col, &dataType); err != nil || !isMySQLIntegerType(dataType) {
		return ""
	}
	return col
}

func isMySQLIntegerType(dataType string) bool {
	switch strings.ToLower(dataType) {
	case "int", "bigint", "mediumint", "smallint", "tinyint":
		return true
	}
	return false
}

// mysqlFirstDatetimeColumn returns the first DATETIME/TIMESTAMP/DATE column of the table, or "".
func mysqlFirstDatetimeColumn(db *sql.DB, table string) (string, error) {
	rows, err := db.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", dbutil.QuoteMySQL(table)))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var field, typ, null, key, def, extra sql.NullString
		if err := rows.Scan(&field, &typ, &null, &key, &def, &extra); err != nil {
			continue
		}
		ft := strings.ToLower(typ.String)
		if field.Valid && (strings.Contains(ft, "datetime") || strings.Contains(ft, "timestamp") || ft == "date") {
			return field.String, nil
		}
	}
	return "", rows.Err()
}
