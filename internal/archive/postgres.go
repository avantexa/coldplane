// internal/archive/postgres.go
package archive

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/avantexa/coldplane/internal/dbutil"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// archivePostgresTable handles chunking, processing, and storing for a single table.
func archivePostgresTable(ctx context.Context, pool *pgxpool.Pool, store FileStore, table, where string, opts ArchiveOptions) ([]map[string]interface{}, string, error) {
	progress := &ProgressTracker{Table: table}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	partitionColumnUsed := pgPartitionColumn(ctx, conn, table)

	n, query, err := pgChunkPlan(ctx, conn, table, where, opts.ChunkSize, progress)
	if err != nil {
		return nil, partitionColumnUsed, err
	}

	var entriesMu sync.Mutex
	var entries []map[string]interface{}
	err = runChunks(n, opts.Workers, func(i int) error {
		dbConn, err := pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("failed to acquire DB connection: %w", err)
		}
		defer dbConn.Release()
		q, args := query(i)
		got, err := processPostgresChunkGeneric(ctx, dbConn.Conn(), q, args, table, i, partitionColumnUsed, store, progress)
		if err != nil {
			return err
		}
		entriesMu.Lock()
		entries = append(entries, got...)
		entriesMu.Unlock()
		return nil
	})
	if err != nil {
		return entries, partitionColumnUsed, err
	}
	fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n", table, progress.DoneRows, progress.DoneChunks)
	return entries, partitionColumnUsed, nil
}

// pgChunkPlan detects the chunking column and plans the chunks for it,
// returning the chunk count and the per-chunk query builder.
func pgChunkPlan(ctx context.Context, conn *pgxpool.Conn, table, where string, chunkSize int, progress *ProgressTracker) (int, func(int) (string, []interface{}), error) {
	hasTime, timeColumn, hasID, idColumn, err := detectPostgresChunkingColumn(ctx, conn.Conn(), table)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to detect chunk column: %w", err)
	}
	switch {
	case hasTime:
		return pgTimeChunks(ctx, conn, table, where, timeColumn, progress)
	case hasID:
		return pgIDChunks(ctx, conn, table, where, idColumn, chunkSize, progress)
	default:
		return pgOffsetChunks(ctx, conn, table, where, chunkSize, progress)
	}
}

// pgPartitionColumn infers the partition column from the last
// timestamp-looking value of the table's first row.
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
			found = lastTimeColumn(columns, vals)
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

// pgTimeChunks plans time-window chunks and returns the per-chunk query and arguments.
func pgTimeChunks(ctx context.Context, conn *pgxpool.Conn, table, where, timeColumn string, progress *ProgressTracker) (int, func(int) (string, []interface{}), error) {
	var minT, maxT sql.NullTime
	if err := conn.QueryRow(ctx, pgMinMaxQuery(table, where, timeColumn)).Scan(&minT, &maxT); err != nil {
		return 0, nil, fmt.Errorf("failed to get min/max timestamp: %w", err)
	}
	if !minT.Valid || !maxT.Valid || minT.Time.After(maxT.Time) {
		pgReportNoRows(ctx, conn, table, "time range")
		return 0, nil, nil
	}
	window := timeWindow(maxT.Time.Sub(minT.Time), defaultTimeWindow, maxTimeChunks)
	// Postgres TIMESTAMP precision is microseconds, so +1µs is the minimal nudge.
	plan := planTimeChunks(minT.Time, maxT.Time, window, time.Microsecond)
	progress.TotalChunks = len(plan)
	fmt.Printf("[archive-postgres] Using %q for time chunking: min=%s max=%s window=%s (%d chunks)\n",
		timeColumn, minT.Time.Format(time.RFC3339), maxT.Time.Format(time.RFC3339), window, len(plan))
	sel := pgRangeQuery(table, where, timeColumn)
	return len(plan), func(i int) (string, []interface{}) {
		return sel, []interface{}{plan[i].Start, plan[i].End}
	}, nil
}

// pgIDChunks plans primary-key range chunks and returns the per-chunk query and arguments.
func pgIDChunks(ctx context.Context, conn *pgxpool.Conn, table, where, idColumn string, chunkSize int, progress *ProgressTracker) (int, func(int) (string, []interface{}), error) {
	var minID, maxID sql.NullInt64
	if err := conn.QueryRow(ctx, pgMinMaxQuery(table, where, idColumn)).Scan(&minID, &maxID); err != nil {
		return 0, nil, fmt.Errorf("failed to get min/max id: %w", err)
	}
	if !minID.Valid || !maxID.Valid || minID.Int64 > maxID.Int64 {
		pgReportNoRows(ctx, conn, table, "ID range")
		return 0, nil, nil
	}
	plan := planIDChunks(minID.Int64, maxID.Int64, int64(chunkSize))
	progress.TotalRows = maxID.Int64 - minID.Int64 + 1
	progress.TotalChunks = len(plan)
	fmt.Printf("[archive-postgres] Using PK %q for chunking: min=%d max=%d, chunks=%d\n", idColumn, minID.Int64, maxID.Int64, len(plan))
	sel := pgRangeQuery(table, where, idColumn)
	return len(plan), func(i int) (string, []interface{}) {
		return sel, []interface{}{plan[i].Start, plan[i].End}
	}, nil
}

// pgOffsetChunks plans LIMIT/OFFSET pages and returns the per-chunk query and arguments.
func pgOffsetChunks(ctx context.Context, conn *pgxpool.Conn, table, where string, chunkSize int, progress *ProgressTracker) (int, func(int) (string, []interface{}), error) {
	fmt.Println("[archive-postgres] Fallback: OFFSET/LIMIT chunking")
	var totalRows int
	if err := conn.QueryRow(ctx, pgCountQuery(table, where)).Scan(&totalRows); err != nil {
		return 0, nil, fmt.Errorf("failed to count rows: %w", err)
	}
	plan := planOffsetChunks(totalRows, chunkSize)
	progress.TotalRows = int64(totalRows)
	progress.TotalChunks = len(plan)
	fmt.Printf("[archive-postgres] Row count: %d; Chunks: %d\n", totalRows, len(plan))
	sel := pgOffsetQuery(table, where)
	return len(plan), func(i int) (string, []interface{}) {
		return sel, []interface{}{plan[i].Limit, plan[i].Offset}
	}, nil
}

// ArchivePostgres archives one or more tables into local Parquet files under a single backup name.
func ArchivePostgres(ctx context.Context, dbURL string, tables []string, name string, opts ArchiveOptions) error {
	fmt.Println("[archive-postgres] Starting archive process")
	backupDir := backupDirFor(opts, name)
	store := &LocalFileStore{BaseDir: backupDir}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("[archive-postgres] failed to connect to DB pool: %w", err)
	}
	defer pool.Close()

	var manifestTables []map[string]interface{}
	for _, table := range tables {
		files, partCol, err := archivePostgresTable(ctx, pool, store, table, opts.Where, opts)
		if err != nil {
			return fmt.Errorf("[archive-postgres] failed to archive table %s: %w", table, err)
		}
		manifestTables = append(manifestTables, tableManifest(table, partCol, files))
	}

	total, err := writeBackupManifest(backupDir, name, "postgres", dbutil.DBNameFromDSN(dbURL), manifestTables)
	if err != nil {
		return fmt.Errorf("[archive-postgres] failed to write local manifest: %w", err)
	}
	fmt.Printf("[archive-postgres] Backup complete: %s (%d bytes)\n", backupDir, total)
	return nil
}

// processPostgresChunkGeneric runs one chunk query and archives its rows.
func processPostgresChunkGeneric(ctx context.Context, conn *pgx.Conn, query string, args []interface{}, table string, chunkNum int, partitionColumn string, store FileStore, progress *ProgressTracker) ([]map[string]interface{}, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()
	return writeChunk(ctx, &pgxRowSource{rows: rows}, table, chunkNum, partitionColumn, store, progress)
}

// pgColumns returns the column names and the positions of date/timestamp
// columns (OIDs: timestamp 1114, timestamptz 1184, date 1082).
func pgColumns(fields []pgconn.FieldDescription) ([]string, map[int]bool) {
	columns := make([]string, len(fields))
	timestampCols := map[int]bool{}
	for i, f := range fields {
		columns[i] = f.Name
		if f.DataTypeOID == 1114 || f.DataTypeOID == 1184 || f.DataTypeOID == 1082 {
			timestampCols[i] = true
		}
	}
	return columns, timestampCols
}

// detectPostgresChunkingColumn picks the best chunking column: the first
// date/timestamp column, then an integer primary key, else none (OFFSET/LIMIT).
func detectPostgresChunkingColumn(ctx context.Context, conn *pgx.Conn, table string) (hasTime bool, timeColumn string, hasID bool, idColumn string, err error) {
	col, err := pgFirstTimestampColumn(ctx, conn, table)
	if err != nil {
		return false, "", false, "", err
	}
	if col != "" {
		return true, col, false, "", nil
	}
	if col := pgNumericPrimaryKey(ctx, conn, table); col != "" {
		return false, "", true, col, nil
	}
	return false, "", false, "", nil
}

// pgFirstTimestampColumn returns the table's first date or timestamp column, or "".
func pgFirstTimestampColumn(ctx context.Context, conn *pgx.Conn, table string) (string, error) {
	rows, err := conn.Query(ctx,
		`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1`, table)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			continue
		}
		if lt := strings.ToLower(typ); strings.Contains(lt, "timestamp") || strings.Contains(lt, "date") {
			return name, nil
		}
	}
	return "", nil
}

// pgNumericPrimaryKey returns the table's first integer primary-key column, or "".
func pgNumericPrimaryKey(ctx context.Context, conn *pgx.Conn, table string) string {
	rows, err := conn.Query(ctx, `
		SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary`, table)
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
