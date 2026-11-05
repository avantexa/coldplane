// internal/archive/postgres.go
package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// archivePostgresTable handles chunking, processing, and storing for a single table.
func archivePostgresTable(ctx context.Context, pool *pgxpool.Pool, store FileStore, table, where, partitionColumn string, opts ArchiveOptions) ([]map[string]interface{}, string, error) {
	progress := &ProgressTracker{Table: table}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	partitionColumnUsed := pgPartitionColumn(ctx, conn, table, partitionColumn)

	hasTime, timeColumn, hasID, idColumn, err := DetectPostgresChunkingColumn(ctx, conn.Conn(), table)
	if err != nil {
		return nil, "", fmt.Errorf("failed to detect chunk column: %w", err)
	}

	var n int
	var query func(i int) (string, []interface{})
	switch {
	case hasTime:
		n, query, err = pgTimeChunks(ctx, conn, table, where, timeColumn, progress)
	case hasID:
		n, query, err = pgIDChunks(ctx, conn, table, where, idColumn, opts.ChunkSize, progress)
	default:
		n, query, err = pgOffsetChunks(ctx, conn, table, where, opts.ChunkSize, progress)
	}
	if err != nil {
		return nil, partitionColumnUsed, err
	}

	var uploadedKeysMu sync.Mutex
	var uploadedKeys []map[string]interface{}
	err = runChunks(n, opts.Workers, func(i int) error {
		dbConn, err := pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("failed to acquire DB connection: %w", err)
		}
		defer dbConn.Release()
		q, args := query(i)
		rowCount, err := processPostgresChunkGeneric(ctx, dbConn.Conn(), q, args, table, i, partitionColumnUsed, &uploadedKeysMu, &uploadedKeys, store)
		if err != nil {
			return err
		}
		progress.AddRows(rowCount)
		progress.ChunkDone()
		return nil
	})
	if err != nil {
		return uploadedKeys, partitionColumnUsed, err
	}
	fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n", table, progress.DoneRows, progress.DoneChunks)
	return uploadedKeys, partitionColumnUsed, nil
}

// pgPartitionColumn validates the requested partition column against the
// table, or infers one from the last timestamp-looking value of the first row.
func pgPartitionColumn(ctx context.Context, conn *pgxpool.Conn, table, requested string) string {
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 1", quotePGIdentifier(table)))
	if err != nil {
		if requested == "" {
			fmt.Printf("[WARNING] Table %s: no valid partition column found or inferred.\n", table)
		}
		return requested
	}
	defer rows.Close()
	fieldDescs := rows.FieldDescriptions()
	columns := make([]string, len(fieldDescs))
	for i, fd := range fieldDescs {
		columns[i] = fd.Name
	}
	if requested != "" && containsString(columns, requested) {
		return requested
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
func ArchivePostgres(ctx context.Context, dbURL string, tables []string, name string, partitionColumns map[string]string, opts ArchiveOptions) error {
	fmt.Println("[archive-postgres] Starting archive process")

	baseDir := opts.LocalOutputDir
	if baseDir == "" {
		baseDir = DefaultLocalBackupDir()
	}
	backupDir := path.Join(baseDir, name)
	store := &LocalFileStore{BaseDir: backupDir}

	dbName := ExtractDBName(dbURL)

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("[archive-postgres] failed to connect to DB pool: %w", err)
	}
	defer pool.Close()

	var manifestTables []map[string]interface{}

	for _, table := range tables {
		partCol := ""
		if partitionColumns != nil {
			if v, ok := partitionColumns[table]; ok {
				partCol = v
			}
		}

		files, actualPartCol, err := archivePostgresTable(ctx, pool, store, table, opts.Where, partCol, opts)
		if err != nil {
			return fmt.Errorf("[archive-postgres] failed to archive table %s: %w", table, err)
		}

		partColValue := interface{}(nil)
		if actualPartCol != "" {
			partColValue = actualPartCol
		}

		manifestTables = append(manifestTables, map[string]interface{}{
			"name":             table,
			"partition_column": partColValue,
			"files":            files,
		})
	}

	// Compute total size
	var totalSize int64
	for _, t := range manifestTables {
		if files, ok := t["files"].([]map[string]interface{}); ok {
			for _, f := range files {
				switch v := f["size_bytes"].(type) {
				case int64:
					totalSize += v
				case int:
					totalSize += int64(v)
				case float64:
					totalSize += int64(v)
				}
			}
		}
	}

	// Write local manifest
	manifest := map[string]interface{}{
		"name":       name,
		"db_type":    "postgres",
		"db_name":    dbName,
		"created_at": time.Now().UTC().Format(time.RFC3339),
		"tables":     manifestTables,
		"size_bytes": totalSize,
		"local":      true,
	}

	if err := WriteLocalManifest(backupDir, manifest); err != nil {
		return fmt.Errorf("[archive-postgres] failed to write local manifest: %w", err)
	}

	fmt.Printf("[archive-postgres] Backup complete: %s (%d bytes)\n", backupDir, totalSize)
	return nil
}

func processPostgresChunkGeneric(
	ctx context.Context, conn *pgx.Conn, query string, args []interface{},
	table string, chunkNum int, partitionColumn string,
	uploadedKeysMu *sync.Mutex, uploadedKeys *[]map[string]interface{},
	store FileStore,
) (int, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("[worker %d] query error: %w", chunkNum, err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	if fields == nil {
		return 0, fmt.Errorf("[worker %d] FieldDescriptions returned nil", chunkNum)
	}
	columns := make([]string, len(fields))
	timestampCols := make(map[int]bool)
	for i, f := range fields {
		columns[i] = string(f.Name)
		if f.DataTypeOID == 1114 || f.DataTypeOID == 1184 || f.DataTypeOID == 1082 {
			timestampCols[i] = true
		}
	}

	type PartitionWriter struct {
		File     *os.File
		FilePath string
		PW       *writer.JSONWriter
		Schema   string
		Rows     int
	}

	partitionWriters := map[string]*PartitionWriter{}
	parquetFilePaths := map[string]string{}

	var sampleRow []interface{}
	var schema string
	var initialized bool
	rowCount := 0

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return rowCount, fmt.Errorf("[worker %d] failed to get row values: %w", chunkNum, err)
		}
		if !initialized {
			sampleRow = make([]interface{}, len(values))
			copy(sampleRow, values)
			schema = inferParquetSchema(columns, sampleRow)
			initialized = true
		}

		// Partition key (year/month from partitionColumn)
		year, month := "unknown", "unknown"
		for i, name := range columns {
			if name == partitionColumn {
				switch v := values[i].(type) {
				case time.Time:
					year, month = v.Format("2006"), v.Format("01")
				case string:
					if t, err := parseAnyTime(v); err == nil {
						year, month = t.Format("2006"), t.Format("01")
					}
				case []byte:
					if t, err := parseAnyTime(string(v)); err == nil {
						year, month = t.Format("2006"), t.Format("01")
					}
				}
			}
		}
		partitionKey := fmt.Sprintf("year=%s/month=%s", year, month)

		// Open writer if not exists
		pw, ok := partitionWriters[partitionKey]
		if !ok {
			tmpFile, err := os.CreateTemp("", "pgchunk-*.parquet")
			if err != nil {
				return 0, fmt.Errorf("[worker %d] failed to create temp file: %w", chunkNum, err)
			}
			jsonWriter, err := writer.NewJSONWriter(schema, writerfile.NewWriterFile(tmpFile), 4)
			if err != nil {
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				return 0, fmt.Errorf("[worker %d] failed to create Parquet writer: %w", chunkNum, err)
			}
			jsonWriter.CompressionType = parquet.CompressionCodec_ZSTD
			pw = &PartitionWriter{
				File:     tmpFile,
				FilePath: tmpFile.Name(),
				PW:       jsonWriter,
				Schema:   schema,
			}
			partitionWriters[partitionKey] = pw
		}

		// Build row map — skip nil values so the parquet writer treats them
		// as missing (NULL) rather than trying to assert nil as a typed value.
		rowMap := make(map[string]interface{})
		for i, name := range columns {
			val := values[i]
			if val == nil {
				continue
			}
			if timestampCols[i] {
				switch t := val.(type) {
				case time.Time:
					rowMap[name] = t.UTC().UnixMilli()
				case string:
					if parsed, err := parseAnyTime(t); err == nil {
						rowMap[name] = parsed.UTC().UnixMilli()
					}
				case []byte:
					if parsed, err := parseAnyTime(string(t)); err == nil {
						rowMap[name] = parsed.UTC().UnixMilli()
					}
				default:
					rowMap[name] = val
				}
			} else if b, ok := val.([]byte); ok {
				rowMap[name] = string(b)
			} else {
				rowMap[name] = toParquetSafeValue(val)
			}
		}
		rowJSON, _ := json.Marshal(rowMap)
		if err := pw.PW.Write(string(rowJSON)); err != nil {
			return 0, fmt.Errorf("[worker %d] failed to write row: %w", chunkNum, err)
		}
		pw.Rows++
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("[worker %d] row iteration error: %w", chunkNum, err)
	}

	// Final flush
	for partitionKey, pw := range partitionWriters {
		if pw.Rows == 0 {
			pw.File.Close()
			os.Remove(pw.FilePath)
			continue
		}
		if err := pw.PW.WriteStop(); err != nil {
			return rowCount, fmt.Errorf("[worker %d] failed to finalize Parquet: %w", chunkNum, err)
		}
		pw.File.Sync()
		pw.File.Close()
		fileKey := path.Join(table, partitionKey,
			fmt.Sprintf("%s-chunk-%d-part-0.parquet", table, chunkNum))
		parquetFilePaths[fileKey] = pw.FilePath
	}

	// Store Parquet partition files in parallel (up to 4 concurrent stores)
	var uploadWg sync.WaitGroup
	uploadSem := make(chan struct{}, 4)
	var uploadErr error
	var uploadErrOnce sync.Once

	for key, filePath := range parquetFilePaths {
		uploadWg.Add(1)
		uploadSem <- struct{}{}
		go func(key, filePath string) {
			defer uploadWg.Done()
			defer func() { <-uploadSem }()

			fileSize, err := store.StoreFile(ctx, key, filePath)
			if err != nil {
				uploadErrOnce.Do(func() {
					uploadErr = fmt.Errorf("[worker %d] store failed for %s: %w", chunkNum, key, err)
				})
				os.Remove(filePath)
				return
			}

			uploadedKeysMu.Lock()
			*uploadedKeys = append(*uploadedKeys, map[string]interface{}{
				"key":        key,
				"size_bytes": fileSize,
			})
			fmt.Printf("[%s] [worker %d] Stored chunk (%d rows, %.2f MB)\n",
				table, chunkNum, rowCount, float64(fileSize)/1024/1024)
			uploadedKeysMu.Unlock()
		}(key, filePath)
	}
	uploadWg.Wait()
	if uploadErr != nil {
		return rowCount, uploadErr
	}
	if debug {
		fmt.Printf("[worker %d] Done: %d rows (Parquet files written and uploaded)\n", chunkNum, rowCount)
	}
	return rowCount, nil
}

// Add this helper function:
func parseAnyTime(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02",
		time.RFC3339,
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("could not parse time: %q", s)
}

func DetectPostgresChunkingColumn(ctx context.Context, conn *pgx.Conn, table string) (hasTime bool, timeColumn string, hasID bool, idColumn string, err error) {
	// 1. Try to find a TIMESTAMP/DATETIME column
	colRows, err := conn.Query(ctx,
		`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1`, table)
	if err != nil {
		return false, "", false, "", err
	}
	defer colRows.Close()
	for colRows.Next() {
		var name, typ string
		if err := colRows.Scan(&name, &typ); err == nil {
			if strings.Contains(strings.ToLower(typ), "timestamp") || strings.Contains(strings.ToLower(typ), "date") {
				return true, name, false, "", nil
			}
		}
	}
	// 2. Fallback: Try numeric PK
	query := `
		SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary
	`
	rows, err := conn.Query(ctx, query, table)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var colName, dataType string
			if err := rows.Scan(&colName, &dataType); err == nil {
				if strings.Contains(dataType, "int") { // int2/int4/int8
					return false, "", true, colName, nil
				}
			}
		}
	}
	// 3. No timestamp or numeric PK found
	return false, "", false, "", nil
}
