package archive

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ArchiveMySQL archives one or more tables into local Parquet files under a single backup name.
func ArchiveMySQL(ctx context.Context, dsn string, tables []string, name string, opts ArchiveOptions) error {
	fmt.Println("[archive-mysql] Starting archive process")
	backupDir := backupDirFor(opts, name)
	store := &LocalFileStore{BaseDir: backupDir}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("[archive-mysql] failed to connect to MySQL: %w", err)
	}
	defer db.Close()

	var manifestTables []map[string]interface{}
	for _, table := range tables {
		files, partCol, err := archiveTable(ctx, db, store, table, opts.Where, opts)
		if err != nil {
			return fmt.Errorf("[archive-mysql] failed to archive table %s: %w", table, err)
		}
		manifestTables = append(manifestTables, tableManifest(table, partCol, files))
	}

	total, err := writeBackupManifest(backupDir, name, "mysql", extractDBNameFromMySQLDSN(dsn), manifestTables)
	if err != nil {
		return fmt.Errorf("[archive-mysql] failed to write local manifest: %w", err)
	}
	fmt.Printf("[archive-mysql] Backup complete: %s (%d bytes)\n", backupDir, total)
	return nil
}

// archiveTable handles chunking, processing, and storing for a single table.
func archiveTable(ctx context.Context, db *sql.DB, store FileStore, table, where string, opts ArchiveOptions) ([]map[string]interface{}, string, error) {
	progress := &ProgressTracker{Table: table}
	partitionColumnUsed := mysqlPartitionColumn(db, table)

	n, query, err := mysqlChunkPlan(db, table, where, opts.ChunkSize, progress, &partitionColumnUsed)
	if err != nil {
		return nil, partitionColumnUsed, err
	}

	var entriesMu sync.Mutex
	var entries []map[string]interface{}
	err = runChunks(n, opts.Workers, func(i int) error {
		got, err := processMySQLChunkGeneric(ctx, db, query(i), table, i, partitionColumnUsed, store, progress)
		if err != nil {
			return err
		}
		entriesMu.Lock()
		entries = append(entries, got...)
		entriesMu.Unlock()
		return nil
	})
	if err != nil {
		return nil, partitionColumnUsed, err
	}
	fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n", table, progress.DoneRows, progress.DoneChunks)
	return entries, partitionColumnUsed, nil
}

// mysqlChunkPlan detects the chunking column and plans the chunks for it,
// returning the chunk count and the per-chunk query builder. In the
// OFFSET/LIMIT fallback it also tries the schema for a partition column.
func mysqlChunkPlan(db *sql.DB, table, where string, chunkSize int, progress *ProgressTracker, partitionColumn *string) (int, func(int) string, error) {
	hasID, idColumn, hasTime, timeColumn, err := detectChunkingColumn(db, table)
	if err != nil {
		return 0, nil, fmt.Errorf("[archive-mysql] failed to detect chunk column: %w", err)
	}
	switch {
	case hasID:
		return mysqlIDChunks(db, table, where, idColumn, chunkSize, progress)
	case hasTime:
		return mysqlTimeChunks(db, table, where, timeColumn, progress)
	default:
		if *partitionColumn == "" {
			*partitionColumn = detectMySQLDatetimeColumn(db, table)
		}
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
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", quoteMySQLIdentifier(table)))
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

// mysqlIDChunks plans primary-key range chunks and returns the per-chunk query.
func mysqlIDChunks(db *sql.DB, table, where, idColumn string, chunkSize int, progress *ProgressTracker) (int, func(int) string, error) {
	var minID, maxID sql.NullInt64
	if err := db.QueryRow(mysqlMinMaxQuery(table, where, idColumn)).Scan(&minID, &maxID); err != nil {
		return 0, nil, fmt.Errorf("[archive-mysql] failed to get min/max id: %w", err)
	}
	if !minID.Valid || !maxID.Valid || minID.Int64 > maxID.Int64 {
		mysqlReportNoRows(db, table, "ID range")
		return 0, nil, nil
	}
	plan := planIDChunks(minID.Int64, maxID.Int64, int64(chunkSize))
	progress.TotalRows = maxID.Int64 - minID.Int64 + 1
	progress.TotalChunks = len(plan)
	fmt.Printf("[archive-mysql] Using %q for chunking (min: %d, max: %d)\n", idColumn, minID.Int64, maxID.Int64)
	return len(plan), func(i int) string {
		return mysqlRangeQuery(table, where, idColumn, strconv.FormatInt(plan[i].Start, 10), strconv.FormatInt(plan[i].End, 10))
	}, nil
}

// mysqlTimeChunks plans time-window chunks and returns the per-chunk query.
func mysqlTimeChunks(db *sql.DB, table, where, timeColumn string, progress *ProgressTracker) (int, func(int) string, error) {
	var minStr, maxStr sql.NullString
	if err := db.QueryRow(mysqlMinMaxQuery(table, where, timeColumn)).Scan(&minStr, &maxStr); err != nil {
		return 0, nil, fmt.Errorf("[archive-mysql] failed to get min/max timestamp: %w", err)
	}
	if !minStr.Valid || !maxStr.Valid {
		mysqlReportNoRows(db, table, "time range")
		return 0, nil, nil
	}
	minTime, errMin := parseAnyTime(minStr.String)
	maxTime, errMax := parseAnyTime(maxStr.String)
	if errMin != nil || errMax != nil {
		return 0, nil, fmt.Errorf("[archive-mysql] failed to parse min/max timestamp: min: %v max: %v", errMin, errMax)
	}
	if minTime.After(maxTime) {
		mysqlReportNoRows(db, table, "time range")
		return 0, nil, nil
	}
	window := timeWindow(maxTime.Sub(minTime), defaultTimeWindow, maxTimeChunks)
	plan := planTimeChunks(minTime, maxTime, window, time.Second)
	progress.TotalChunks = len(plan)
	fmt.Printf("[archive-mysql] Using %q for time chunking (min: %s, max: %s) with window %s (%d chunks expected)\n",
		timeColumn, minTime.Format(time.RFC3339), maxTime.Format(time.RFC3339), window, len(plan))
	return len(plan), func(i int) string {
		r := plan[i]
		return mysqlRangeQuery(table, where, timeColumn, "'"+r.Start.Format(mysqlTimeLayout)+"'", "'"+r.End.Format(mysqlTimeLayout)+"'")
	}, nil
}

// mysqlOffsetChunks plans LIMIT/OFFSET pages and returns the per-chunk query.
func mysqlOffsetChunks(db *sql.DB, table, where string, chunkSize int, progress *ProgressTracker) (int, func(int) string, error) {
	fmt.Println("[archive-mysql] Fallback: using OFFSET/LIMIT chunking (no PK or timestamp found)")
	var totalRows int
	if err := db.QueryRow(mysqlCountQuery(table, where)).Scan(&totalRows); err != nil {
		return 0, nil, fmt.Errorf("[archive-mysql] failed to count rows: %w", err)
	}
	fmt.Printf("[archive-mysql] Row count: %d\n", totalRows)
	plan := planOffsetChunks(totalRows, chunkSize)
	progress.TotalRows = int64(totalRows)
	progress.TotalChunks = len(plan)
	return len(plan), func(i int) string {
		return mysqlOffsetQuery(table, where, plan[i].Offset, plan[i].Limit)
	}, nil
}

// processMySQLChunkGeneric runs one chunk query and archives its rows.
func processMySQLChunkGeneric(ctx context.Context, db *sql.DB, query, table string, chunkNum int, partitionColumn string, store FileStore, progress *ProgressTracker) ([]map[string]interface{}, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()
	src, err := newSQLRowSource(rows)
	if err != nil {
		return nil, err
	}
	if debug {
		fmt.Printf("[DEBUG] Detected columns: %v\n", src.columns)
	}
	return writeChunk(ctx, src, table, chunkNum, partitionColumn, store, progress)
}

// detectChunkingColumn picks the best chunking column: a numeric primary
// key first, then the first DATETIME/TIMESTAMP column, else none (OFFSET/LIMIT).
func detectChunkingColumn(db *sql.DB, table string) (hasID bool, idColumn string, hasTime bool, timeColumn string, err error) {
	if col := mysqlNumericPrimaryKey(db, table); col != "" {
		return true, col, false, "", nil
	}
	col, err := mysqlFirstDatetimeColumn(db, table)
	if err != nil {
		return false, "", false, "", err
	}
	if col != "" {
		return false, "", true, col, nil
	}
	return false, "", false, "", nil
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

// mysqlFirstDatetimeColumn returns the first DATETIME/TIMESTAMP column of the table, or "".
func mysqlFirstDatetimeColumn(db *sql.DB, table string) (string, error) {
	rows, err := db.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", quoteMySQLIdentifier(table)))
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
		if field.Valid && (strings.Contains(ft, "datetime") || strings.Contains(ft, "timestamp")) {
			return field.String, nil
		}
	}
	return "", rows.Err()
}

// Helper to extract db name from MySQL DSN
func extractDBNameFromMySQLDSN(dsn string) string {
	// Find last '/' and strip any '?'
	slash := strings.LastIndex(dsn, "/")
	if slash == -1 || slash == len(dsn)-1 {
		return ""
	}
	rest := dsn[slash+1:]
	q := strings.Index(rest, "?")
	if q >= 0 {
		rest = rest[:q]
	}
	return rest
}
