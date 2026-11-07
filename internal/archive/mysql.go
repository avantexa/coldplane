package archive

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/avantexa/coldplane/internal/dbutil"
	"github.com/avantexa/coldplane/internal/duck"

	_ "github.com/go-sql-driver/mysql"
)

// ArchiveMySQL archives one or more tables into local Parquet files under a single backup name.
func ArchiveMySQL(ctx context.Context, dsn string, tables []string, name string, opts ArchiveOptions) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("[archive-mysql] failed to connect to MySQL: %w", err)
	}
	defer db.Close()
	return runBackup(ctx, "mysql", dsn, tables, name, opts, func(ctx context.Context, eng *duck.Engine, kind duck.Kind, table, backupDir string) (TableEntry, error) {
		return archiveMySQLTable(ctx, db, eng, kind, table, backupDir, opts)
	})
}

// archiveMySQLTable archives one table and returns its manifest entry.
func archiveMySQLTable(ctx context.Context, db *sql.DB, eng *duck.Engine, kind duck.Kind, table, backupDir string, opts ArchiveOptions) (TableEntry, error) {
	progress := &ProgressTracker{Table: table}

	columns, err := sourceColumns(ctx, eng, kind, table)
	if err != nil {
		return TableEntry{}, err
	}
	chunkKind, column, err := mysqlChunkColumn(db, table)
	if err != nil {
		return TableEntry{}, fmt.Errorf("failed to detect chunk column: %w", err)
	}
	// The time column we chunk on is also the natural partition column.
	partitionColumn := column
	if chunkKind != chunkByTime {
		partitionColumn = mysqlPartitionColumn(db, table)
	}
	if partitionColumn == "" && chunkKind == chunkByOffset {
		partitionColumn = detectMySQLDatetimeColumn(db, table)
	}

	plan, err := mysqlChunkPlan(db, table, opts.Where, chunkKind, column, opts.ChunkSize, progress)
	if err != nil {
		return TableEntry{}, err
	}
	files, err := archiveTable(ctx, eng, kind, srcAlias, backupDir, table, partitionColumn, plan, opts.Workers, progress)
	if err != nil {
		return TableEntry{}, err
	}
	return tableEntry(table, partitionColumn, files, columns, chunkKind, column, progress.DoneRows), nil
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
	return chunkPlan{n: len(ranges), query: func(i int) string {
		return mysqlRangeQuery(table, where, idColumn, strconv.FormatInt(ranges[i].Start, 10), strconv.FormatInt(ranges[i].End, 10))
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
	return chunkPlan{n: len(ranges), query: func(i int) string {
		return mysqlRangeQuery(table, where, timeColumn, sqlString(ranges[i].Start.Format(mysqlTimeLayout)), sqlString(ranges[i].End.Format(mysqlTimeLayout)))
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
	return chunkPlan{n: len(pages), query: func(i int) string {
		return mysqlOffsetQuery(table, where, pages[i].Limit, pages[i].Offset)
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
