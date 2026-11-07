// internal/archive/postgres.go
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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// archivePostgresTable archives one table and returns its manifest entry.
func archivePostgresTable(ctx context.Context, pool *pgxpool.Pool, eng *duck.Engine, kind duck.Kind, table, backupDir string, opts ArchiveOptions) (TableEntry, error) {
	progress := &ProgressTracker{Table: table}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return TableEntry{}, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	columns, err := sourceColumns(ctx, eng, kind, table)
	if err != nil {
		return TableEntry{}, err
	}
	key, err := pgChunkColumn(ctx, conn.Conn(), table)
	if err != nil {
		return TableEntry{}, fmt.Errorf("failed to detect chunk column: %w", err)
	}
	// The time column we chunk on is also the natural partition column.
	partitionColumn := key.column
	if key.kind != chunkByTime {
		partitionColumn = pgPartitionColumn(ctx, conn, table)
	}
	plan, err := pgChunkPlan(ctx, conn, table, opts.Where, key, opts.ChunkSize, progress)
	if err != nil {
		return TableEntry{}, err
	}
	files, err := archiveTable(ctx, eng, kind, srcAlias, backupDir, table, partitionColumn, plan, opts.Workers, progress)
	if err != nil {
		return TableEntry{}, err
	}
	return tableEntry(table, partitionColumn, files, columns, key.kind, key.column, progress.DoneRows), nil
}

// pgChunkPlan plans the chunks for the detected strategy.
func pgChunkPlan(ctx context.Context, conn *pgxpool.Conn, table, where string, key chunkKey, chunkSize int, progress *ProgressTracker) (chunkPlan, error) {
	switch key.kind {
	case chunkByTime:
		return pgTimeChunks(ctx, conn, table, where, key, progress)
	case chunkByID:
		return pgIDChunks(ctx, conn, table, where, key.column, chunkSize, progress)
	default:
		return pgOffsetChunks(ctx, conn, table, where, chunkSize, progress)
	}
}

// pgPartitionColumn infers the partition column from the first
// timestamp-looking value of the table's first row, like MySQL.
func pgPartitionColumn(ctx context.Context, conn *pgxpool.Conn, table string) string {
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 1", dbutil.QuotePG(table)))
	if err != nil {
		fmt.Printf("[WARNING] Table %s: no valid partition column found or inferred.\n", table)
		return ""
	}
	defer rows.Close()
	fieldDescs := rows.FieldDescriptions()
	columns := make([]string, len(fieldDescs))
	for i, fd := range fieldDescs {
		columns[i] = fd.Name
	}
	found := ""
	if rows.Next() {
		if vals, err := rows.Values(); err == nil {
			found = firstTimeColumn(columns, vals)
		}
	}
	if found == "" {
		fmt.Printf("[WARNING] Table %s: no valid partition column found or inferred.\n", table)
	}
	return found
}

// pgReportNoRows explains why a chunk plan came out empty.
func pgReportNoRows(ctx context.Context, conn *pgxpool.Conn, table, kind string) {
	var total int
	_ = conn.QueryRow(ctx, pgCountQuery(table, "")).Scan(&total)
	if total == 0 {
		fmt.Printf("[archive-postgres] Table %s is empty (0 rows)\n", table)
	} else {
		fmt.Printf("[archive-postgres] Table %s has %d rows but none match the %s\n", table, total, kind)
	}
}

// pgTimeChunks plans time-window chunks. Postgres TIMESTAMP has microsecond
// precision, so the last window is nudged by one microsecond.
func pgTimeChunks(ctx context.Context, conn *pgxpool.Conn, table, where string, key chunkKey, progress *ProgressTracker) (chunkPlan, error) {
	timeColumn := key.column
	var minT, maxT sql.NullTime
	if err := conn.QueryRow(ctx, pgMinMaxQuery(table, where, timeColumn)).Scan(&minT, &maxT); err != nil {
		return chunkPlan{}, fmt.Errorf("failed to get min/max timestamp: %w", err)
	}
	var ranges []timeRange
	if minT.Valid && maxT.Valid {
		window := timeWindow(maxT.Time.Sub(minT.Time), defaultTimeWindow, maxTimeChunks)
		ranges = planTimeChunks(minT.Time, maxT.Time, window, time.Microsecond)
		fmt.Printf("[archive-postgres] Using %q for time chunking: min=%s max=%s window=%s (%d chunks)\n",
			timeColumn, minT.Time.Format(time.RFC3339), maxT.Time.Format(time.RFC3339), window, len(ranges))
	} else {
		pgReportNoRows(ctx, conn, table, "time range")
	}
	// Rows with a NULL timestamp fall outside every window: one more chunk collects them.
	progress.TotalChunks = len(ranges) + 1
	return chunkPlan{n: len(ranges) + 1, query: func(i int) string {
		if i == len(ranges) {
			return pgNullQuery(table, where, timeColumn)
		}
		return pgRangeQuery(table, where, timeColumn, pgTimeLiteral(ranges[i].Start, key.withZone), pgTimeLiteral(ranges[i].End, key.withZone))
	}}, nil
}

// pgIDChunks plans primary-key range chunks.
func pgIDChunks(ctx context.Context, conn *pgxpool.Conn, table, where, idColumn string, chunkSize int, progress *ProgressTracker) (chunkPlan, error) {
	var minID, maxID sql.NullInt64
	if err := conn.QueryRow(ctx, pgMinMaxQuery(table, where, idColumn)).Scan(&minID, &maxID); err != nil {
		return chunkPlan{}, fmt.Errorf("failed to get min/max id: %w", err)
	}
	if !minID.Valid || !maxID.Valid || minID.Int64 > maxID.Int64 {
		pgReportNoRows(ctx, conn, table, "ID range")
		return chunkPlan{}, nil
	}
	ranges := planIDChunks(minID.Int64, maxID.Int64, int64(chunkSize))
	progress.TotalRows = maxID.Int64 - minID.Int64 + 1
	progress.TotalChunks = len(ranges)
	fmt.Printf("[archive-postgres] Using PK %q for chunking: min=%d max=%d, chunks=%d\n", idColumn, minID.Int64, maxID.Int64, len(ranges))
	return chunkPlan{n: len(ranges), query: func(i int) string {
		return pgRangeQuery(table, where, idColumn, strconv.FormatInt(ranges[i].Start, 10), strconv.FormatInt(ranges[i].End, 10))
	}}, nil
}

// pgOffsetChunks plans LIMIT/OFFSET pages.
func pgOffsetChunks(ctx context.Context, conn *pgxpool.Conn, table, where string, chunkSize int, progress *ProgressTracker) (chunkPlan, error) {
	fmt.Println("[archive-postgres] Fallback: OFFSET/LIMIT chunking")
	var totalRows int
	if err := conn.QueryRow(ctx, pgCountQuery(table, where)).Scan(&totalRows); err != nil {
		return chunkPlan{}, fmt.Errorf("failed to count rows: %w", err)
	}
	pages := planOffsetChunks(totalRows, chunkSize)
	progress.TotalRows = int64(totalRows)
	progress.TotalChunks = len(pages)
	fmt.Printf("[archive-postgres] Row count: %d; Chunks: %d\n", totalRows, len(pages))
	return chunkPlan{n: len(pages), query: func(i int) string {
		return pgOffsetQuery(table, where, pages[i].Limit, pages[i].Offset)
	}}, nil
}

// ArchivePostgres archives one or more tables into local Parquet files under a single backup name.
func ArchivePostgres(ctx context.Context, dbURL string, tables []string, name string, opts ArchiveOptions) error {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("[archive-postgres] failed to connect to DB pool: %w", err)
	}
	defer pool.Close()
	return runBackup(ctx, "postgres", dbURL, tables, name, opts, func(ctx context.Context, eng *duck.Engine, kind duck.Kind, table, backupDir string) (TableEntry, error) {
		return archivePostgresTable(ctx, pool, eng, kind, table, backupDir, opts)
	})
}

// pgChunkColumn picks the chunking strategy: the first date/timestamp
// column first, then an integer primary key, else OFFSET/LIMIT.
func pgChunkColumn(ctx context.Context, conn *pgx.Conn, table string) (chunkKey, error) {
	col, dataType, err := pgFirstTimestampColumn(ctx, conn, table)
	if err != nil {
		return chunkKey{}, err
	}
	if col != "" {
		return chunkKey{kind: chunkByTime, column: col, withZone: strings.Contains(dataType, "with time zone")}, nil
	}
	if col := pgNumericPrimaryKey(ctx, conn, table); col != "" {
		return chunkKey{kind: chunkByID, column: col}, nil
	}
	return chunkKey{kind: chunkByOffset}, nil
}

// pgFirstTimestampColumn returns the table's first date or timestamp column
// and its data type, or "" when there is none.
func pgFirstTimestampColumn(ctx context.Context, conn *pgx.Conn, table string) (string, string, error) {
	rows, err := conn.Query(ctx,
		`SELECT column_name, data_type FROM information_schema.columns
		 WHERE table_schema = current_schema() AND table_name = $1
		 ORDER BY ordinal_position`, table)
	if err != nil {
		return "", "", err
	}
	defer rows.Close()
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			continue
		}
		if lt := strings.ToLower(typ); strings.Contains(lt, "timestamp") || strings.Contains(lt, "date") {
			return name, lt, nil
		}
	}
	return "", "", nil
}

// pgNumericPrimaryKey returns the table's first integer primary-key column, or "".
func pgNumericPrimaryKey(ctx context.Context, conn *pgx.Conn, table string) string {
	rows, err := conn.Query(ctx, `
		SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary`, dbutil.QuotePG(table))
	if err != nil {
		return ""
	}
	defer rows.Close()
	for rows.Next() {
		var col, dataType string
		if err := rows.Scan(&col, &dataType); err == nil && strings.Contains(dataType, "int") { // int2/int4/int8
			return col
		}
	}
	return ""
}
