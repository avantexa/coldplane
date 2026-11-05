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
	// Progress tracker for this table
	progress := &ProgressTracker{Table: table}

	// Validate or infer partition column
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	partitionColumnUsed := partitionColumn
	colQuery := fmt.Sprintf("SELECT * FROM %s LIMIT 1", quotePGIdentifier(table))
	rows, err := conn.Query(ctx, colQuery)
	if err == nil {
		fieldDescs := rows.FieldDescriptions()
		tableColumns := make([]string, len(fieldDescs))
		for i, fd := range fieldDescs {
			tableColumns[i] = fd.Name
		}
		valid := false
		if partitionColumnUsed != "" {
			for _, col := range tableColumns {
				if col == partitionColumnUsed {
					valid = true
					break
				}
			}
		}
		if !valid {
			// Infer from first row
			if rows.Next() {
				vals := make([]interface{}, len(tableColumns))
				valPtrs := make([]interface{}, len(tableColumns))
				for i := range tableColumns {
					valPtrs[i] = &vals[i]
				}
				if err := rows.Scan(valPtrs...); err == nil {
					for i, v := range vals {
						switch val := v.(type) {
						case time.Time:
							partitionColumnUsed = tableColumns[i]
						case string:
							if _, err := time.Parse(time.RFC3339, val); err == nil {
								partitionColumnUsed = tableColumns[i]
							}
						case []byte:
							if _, err := time.Parse(time.RFC3339, string(val)); err == nil {
								partitionColumnUsed = tableColumns[i]
							}
						}
					}
				}
			}
		}
	}
	if partitionColumnUsed == "" {
		fmt.Printf("[WARNING] Table %s: no valid partition column found or inferred.\n", table)
	}
	rows.Close()

	// Use one connection to detect chunking columns
	conn2, err := pool.Acquire(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn2.Release()

	hasTime, timeColumn, hasID, idColumn, err := DetectPostgresChunkingColumn(ctx, conn2.Conn(), table)
	if err != nil {
		return nil, "", fmt.Errorf("failed to detect chunk column: %w", err)
	}

	chunkSize := opts.ChunkSize
	workerCount := opts.Workers
	var wg sync.WaitGroup
	sem := make(chan struct{}, workerCount)
	errCh := make(chan error, workerCount*2)

	// Collect all uploaded parquet keys for manifest
	var uploadedKeysMu sync.Mutex
	var uploadedKeys []map[string]interface{}

	// 1. TIME CHUNKING
	if hasTime {
		var minTime, maxTime sql.NullTime
		timeRangeQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", quotePGIdentifier(timeColumn), quotePGIdentifier(timeColumn), quotePGIdentifier(table))
		if where != "" {
			timeRangeQuery += " WHERE " + where
		}
		if err := conn2.QueryRow(ctx, timeRangeQuery).Scan(&minTime, &maxTime); err != nil {
			return nil, partitionColumnUsed, fmt.Errorf("failed to get min/max timestamp: %w", err)
		}
		if !minTime.Valid || !maxTime.Valid || minTime.Time.After(maxTime.Time) {
			var totalCount int
			_ = conn2.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", quotePGIdentifier(table))).Scan(&totalCount)
			if totalCount == 0 {
				fmt.Printf("[archive-postgres] Table %s is empty (0 rows)\n", table)
			} else {
				fmt.Printf("[archive-postgres] Table %s has %d rows but none match the time range\n", table, totalCount)
			}
			return uploadedKeys, partitionColumnUsed, nil
		}
		tMin := minTime.Time
		tMax := maxTime.Time

		totalDuration := tMax.Sub(tMin)
		defaultWindow := 30 * 24 * time.Hour // 1 month
		window := defaultWindow
		maxChunks := 120

		estChunks := int(totalDuration/window) + 1
		if estChunks > maxChunks {
			window = time.Duration(totalDuration.Nanoseconds() / int64(maxChunks))
			fmt.Printf("[archive-postgres] Large date range; window set to %s\n", window)
			estChunks = int(totalDuration / window)
		}
		progress.TotalChunks = estChunks

		fmt.Printf("[archive-postgres] Using %q for time chunking: min=%s max=%s window=%s (%d chunks)\n", timeColumn, tMin.Format(time.RFC3339), tMax.Format(time.RFC3339), window, estChunks)

		chunkNum := 0
		t0 := tMin
		for t0.Before(tMax) {
			t1 := t0.Add(window)
			// If this is the last chunk, nudge the upper bound so rows with ts == tMax are included.
			// Postgres TIMESTAMP precision is microseconds, so +1µs is safe and minimal.
			if t1.After(tMax) || t1.Equal(tMax) {
				t1 = tMax.Add(time.Microsecond)
			}
			t0Copy, t1Copy, chunkNumCopy := t0, t1, chunkNum
			wg.Add(1)
			sem <- struct{}{}
			go func(chunkNum int, t0, t1 time.Time) {
				defer wg.Done()
				defer func() { <-sem }()
				dbConn, err := pool.Acquire(ctx)
				if err != nil {
					errCh <- fmt.Errorf("[worker %d] failed to acquire DB connection: %w", chunkNum, err)
					return
				}
				defer dbConn.Release()
				q := fmt.Sprintf("SELECT * FROM %s WHERE %s >= $1 AND %s < $2", quotePGIdentifier(table), quotePGIdentifier(timeColumn), quotePGIdentifier(timeColumn))
				if where != "" {
					q = fmt.Sprintf("SELECT * FROM %s WHERE (%s) AND (%s >= $1 AND %s < $2)", quotePGIdentifier(table), where, quotePGIdentifier(timeColumn), quotePGIdentifier(timeColumn))
				}
				rowCount, err := processPostgresChunkGeneric(ctx, dbConn.Conn(), q, []interface{}{t0, t1}, table, chunkNum, partitionColumnUsed, &uploadedKeysMu, &uploadedKeys, store)
				if err != nil {
					errCh <- fmt.Errorf("[worker %d] %w", chunkNum, err)
				} else {
					progress.AddRows(rowCount)
					progress.ChunkDone()
				}
			}(chunkNumCopy, t0Copy, t1Copy)
			t0 = t1
			chunkNum++
		}
		wg.Wait()
		close(errCh)

		var firstErr error
		for err := range errCh {
			if err != nil {
				fmt.Println("[ERROR]", err)
				if firstErr == nil {
					firstErr = err
				}
			}
		}
		if firstErr != nil {
			return uploadedKeys, partitionColumnUsed, firstErr
		}
		fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n",
			table, progress.DoneRows, progress.DoneChunks)
		return uploadedKeys, partitionColumnUsed, nil
	}

	// 2. PK (ID) CHUNKING
	if hasID {
		var minID, maxID int64
		idRangeQuery := fmt.Sprintf(`SELECT MIN(%s), MAX(%s) FROM %s`, quotePGIdentifier(idColumn), quotePGIdentifier(idColumn), quotePGIdentifier(table))
		if where != "" {
			idRangeQuery += " WHERE " + where
		}
		if err := conn2.QueryRow(ctx, idRangeQuery).Scan(&minID, &maxID); err != nil {
			return nil, partitionColumnUsed, fmt.Errorf("failed to get min/max id: %w", err)
		}
		if minID > maxID {
			var totalCount int
			_ = conn2.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", quotePGIdentifier(table))).Scan(&totalCount)
			if totalCount == 0 {
				fmt.Printf("[archive-postgres] Table %s is empty (0 rows)\n", table)
			} else {
				fmt.Printf("[archive-postgres] Table %s has %d rows but none match the ID range\n", table, totalCount)
			}
			return uploadedKeys, partitionColumnUsed, nil
		}

		cs := int64(chunkSize)
		numChunks := ((maxID - minID + 1) + cs - 1) / cs
		progress.TotalChunks = int(numChunks)
		fmt.Printf("[archive-postgres] Using PK %q for chunking: min=%d max=%d, chunks=%d\n", idColumn, minID, maxID, numChunks)
		for i := int64(0); i < numChunks; i++ {
			startID := minID + i*cs
			endID := startID + cs
			if endID > maxID+1 {
				endID = maxID + 1
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(chunkNum int64, startID, endID int64) {
				defer wg.Done()
				defer func() { <-sem }()
				dbConn, err := pool.Acquire(ctx)
				if err != nil {
					errCh <- fmt.Errorf("[worker %d] failed to acquire DB connection: %w", chunkNum, err)
					return
				}
				defer dbConn.Release()
				q := fmt.Sprintf("SELECT * FROM %s WHERE %s >= $1 AND %s < $2", quotePGIdentifier(table), quotePGIdentifier(idColumn), quotePGIdentifier(idColumn))
				if where != "" {
					q = fmt.Sprintf("SELECT * FROM %s WHERE (%s) AND (%s >= $1 AND %s < $2)", quotePGIdentifier(table), where, quotePGIdentifier(idColumn), quotePGIdentifier(idColumn))
				}
				rowCount, err := processPostgresChunkGeneric(ctx, dbConn.Conn(), q, []interface{}{startID, endID}, table, int(chunkNum), partitionColumnUsed, &uploadedKeysMu, &uploadedKeys, store)
				if err != nil {
					errCh <- fmt.Errorf("[worker %d] %w", chunkNum, err)
				} else {
					progress.AddRows(rowCount)
					progress.ChunkDone()
				}
			}(i, startID, endID)
		}
		wg.Wait()
		close(errCh)

		var firstErr error
		for err := range errCh {
			if err != nil {
				fmt.Println("[ERROR]", err)
				if firstErr == nil {
					firstErr = err
				}
			}
		}
		if firstErr != nil {
			return uploadedKeys, partitionColumnUsed, firstErr
		}
		fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n",
			table, progress.DoneRows, progress.DoneChunks)
		return uploadedKeys, partitionColumnUsed, nil
	}

	// 3. OFFSET/LIMIT (fallback)
	fmt.Println("[archive-postgres] Fallback: OFFSET/LIMIT chunking")
	var totalRows int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", quotePGIdentifier(table))
	if where != "" {
		countQuery += " WHERE " + where
	}
	if err := conn2.QueryRow(ctx, countQuery).Scan(&totalRows); err != nil {
		return nil, partitionColumnUsed, fmt.Errorf("failed to count rows: %w", err)
	}
	numChunks := (totalRows + int(chunkSize) - 1) / int(chunkSize)
	progress.TotalChunks = numChunks
	progress.TotalRows = int64(totalRows)
	fmt.Printf("[archive-postgres] Row count: %d; Chunks: %d\n", totalRows, numChunks)

	for i := 0; i < numChunks; i++ {
		offset := i * int(chunkSize)
		limit := int(chunkSize)
		wg.Add(1)
		sem <- struct{}{}
		go func(chunkNum, offset, limit int) {
			defer wg.Done()
			defer func() { <-sem }()
			dbConn, err := pool.Acquire(ctx)
			if err != nil {
				errCh <- fmt.Errorf("[worker %d] failed to acquire DB connection: %w", chunkNum, err)
				return
			}
			defer dbConn.Release()
			q := fmt.Sprintf("SELECT * FROM %s LIMIT $1 OFFSET $2", quotePGIdentifier(table))
			if where != "" {
				q = fmt.Sprintf("SELECT * FROM %s WHERE %s LIMIT $1 OFFSET $2", quotePGIdentifier(table), where)
			}
			rowCount, err := processPostgresChunkGeneric(ctx, dbConn.Conn(), q, []interface{}{limit, offset}, table, chunkNum, partitionColumnUsed, &uploadedKeysMu, &uploadedKeys, store)
			if err != nil {
				errCh <- fmt.Errorf("[worker %d] %w", chunkNum, err)
			} else {
				progress.AddRows(rowCount)
				progress.ChunkDone()
			}
		}(i, offset, limit)
	}
	wg.Wait()
	close(errCh)

	var firstErr error
	for err := range errCh {
		if err != nil {
			fmt.Println("[ERROR]", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if firstErr != nil {
		return uploadedKeys, partitionColumnUsed, firstErr
	}
	fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n",
		table, progress.DoneRows, progress.DoneChunks)
	return uploadedKeys, partitionColumnUsed, nil
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
