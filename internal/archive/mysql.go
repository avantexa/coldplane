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

	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// tryParseTime attempts to parse a string into time.Time using multiple formats.
func tryParseTime(val string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, val); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("could not parse time: %s", val)

}

// ArchiveMySQL archives one or more tables into local Parquet files under a single backup name.
func ArchiveMySQL(ctx context.Context, dsn string, tables []string, name string, opts ArchiveOptions) error {
	fmt.Println("[archive-mysql] Starting archive process")

	baseDir := opts.LocalOutputDir
	if baseDir == "" {
		baseDir = DefaultLocalBackupDir()
	}
	backupDir := path.Join(baseDir, name)
	store := &LocalFileStore{BaseDir: backupDir}

	dbName := extractDBNameFromMySQLDSN(dsn)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("[archive-mysql] failed to connect to MySQL: %w", err)
	}
	defer db.Close()

	var manifestTables []map[string]interface{}

	for _, table := range tables {
		files, err := archiveTable(ctx, db, store, table, opts.Where, "", opts)
		if err != nil {
			return fmt.Errorf("[archive-mysql] failed to archive table %s: %w", table, err)
		}

		var tableSize int64
		for _, f := range files {
			switch v := f["size_bytes"].(type) {
			case int64:
				tableSize += v
			case int:
				tableSize += int64(v)
			case float64:
				tableSize += int64(v)
			}
		}

		manifestTables = append(manifestTables, map[string]interface{}{
			"name":       table,
			"files":      files,
			"size_bytes": tableSize,
		})
	}

	var totalSize int64
	for _, t := range manifestTables {
		switch v := t["size_bytes"].(type) {
		case int64:
			totalSize += v
		case int:
			totalSize += int64(v)
		case float64:
			totalSize += int64(v)
		}
	}

	manifest := map[string]interface{}{
		"name":       name,
		"db_type":    "mysql",
		"db_name":    dbName,
		"created_at": time.Now().UTC().Format(time.RFC3339),
		"tables":     manifestTables,
		"size_bytes": totalSize,
		"local":      true,
	}

	if err := WriteLocalManifest(backupDir, manifest); err != nil {
		return fmt.Errorf("[archive-mysql] failed to write local manifest: %w", err)
	}

	fmt.Printf("[archive-mysql] Backup complete: %s (%d bytes)\n", backupDir, totalSize)
	return nil
}

// archiveTable handles chunking, processing, and storing for a single table.
func archiveTable(ctx context.Context, db *sql.DB, store FileStore, table, where, partitionColumnFlag string, opts ArchiveOptions) ([]map[string]interface{}, error) {

	// Progress tracker for this table
	progress := &ProgressTracker{Table: table}

	// Infer partitionColumn at the very beginning
	partitionColumnUsed := partitionColumnFlag
	colRows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", quoteMySQLIdentifier(table)))
	var tableColumns []string
	var inferredPartitionColumn string
	if err == nil {
		defer colRows.Close()
		tableColumns, err = colRows.Columns()
		if err == nil && colRows.Next() {
			vals := make([]interface{}, len(tableColumns))
			valPtrs := make([]interface{}, len(tableColumns))
			for i := range tableColumns {
				valPtrs[i] = &vals[i]
			}
			if err := colRows.Scan(valPtrs...); err == nil {
				for i, v := range vals {
					var t time.Time
					switch val := v.(type) {
					case time.Time:
						t = val
					case string:
						t, _ = tryParseTime(val)
					case []byte:
						t, _ = tryParseTime(string(val))
					}
					if !t.IsZero() {
						inferredPartitionColumn = tableColumns[i]
						fmt.Printf("[INFO] Inferred partition column '%s' from first row.\n", inferredPartitionColumn)
						break
					}
				}
			}
		}
	}
	// Validate user-provided partitionColumn
	validPartition := false
	if partitionColumnUsed != "" {
		for _, col := range tableColumns {
			if col == partitionColumnUsed {
				validPartition = true
				break
			}
		}
		if !validPartition {
			fmt.Printf("[WARNING] Provided partition column '%s' not found in table. Will attempt to infer.\n", partitionColumnUsed)
			partitionColumnUsed = ""
		}
	}
	if partitionColumnUsed == "" {
		partitionColumnUsed = inferredPartitionColumn
	}
	if partitionColumnUsed == "" {
		fmt.Printf("[WARNING] No valid datetime column found in first row. Partitioning will fallback to 'unknown'.\n")
	}

	// Detect chunking column and set hasID, idColumn, hasTime, timeColumn for chunking logic
	hasID, idColumn, hasTime, timeColumn, err := DetectChunkingColumn(db, table)
	if err != nil {
		return nil, fmt.Errorf("[archive-mysql] failed to detect chunk column: %w", err)
	}

	chunkSize := opts.ChunkSize
	workerCount := opts.Workers
	errCh := make(chan error, workerCount*2)
	var wg sync.WaitGroup
	sem := make(chan struct{}, workerCount)

	// Collect all uploaded parquet keys for manifest
	var uploadedKeysMu sync.Mutex
	var uploadedKeys []map[string]interface{}

	// PK chunking
	if hasID {
		var minID, maxID int64
		idRangeQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", quoteMySQLIdentifier(idColumn), quoteMySQLIdentifier(idColumn), quoteMySQLIdentifier(table))
		if where != "" {
			idRangeQuery += " WHERE " + where
		}
		if err := db.QueryRow(idRangeQuery).Scan(&minID, &maxID); err != nil {
			return nil, fmt.Errorf("[archive-mysql] failed to get min/max id: %w", err)
		}
		if minID > maxID {
			var totalCount int
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteMySQLIdentifier(table))).Scan(&totalCount)
			if totalCount == 0 {
				fmt.Printf("[archive-mysql] Table %s is empty (0 rows)\n", table)
			} else {
				fmt.Printf("[archive-mysql] Table %s has %d rows but none match the ID range\n", table, totalCount)
			}
			fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n", table, progress.DoneRows, progress.DoneChunks)
			return uploadedKeys, nil
		}
		// Set total rows based on PK range
		progress.TotalRows = maxID - minID + 1
		fmt.Printf("[archive-mysql] Using %q for chunking (min: %d, max: %d)\n", idColumn, minID, maxID)
		cs := int64(chunkSize)
		numChunks := ((maxID - minID + 1) + cs - 1) / cs
		progress.TotalChunks = int(numChunks)
		progress.TotalRows = maxID - minID + 1
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
				if err := processMySQLChunkIndex(
					ctx, db, table, where, idColumn, startID, endID, int(chunkNum),
					partitionColumnUsed, store, &uploadedKeysMu, &uploadedKeys, progress,
				); err != nil {
					errCh <- fmt.Errorf("[worker %d] %w", chunkNum, err)
				}
			}(i, startID, endID)
		}
		wg.Wait()
		close(errCh)
		var firstErr error
		for err := range errCh {
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if firstErr != nil {
			return nil, firstErr
		}
		fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n", table, progress.DoneRows, progress.DoneChunks)
		return uploadedKeys, nil
	}

	// Time chunking
	if hasTime {
		var minTimeStr, maxTimeStr sql.NullString
		timeRangeQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", quoteMySQLIdentifier(timeColumn), quoteMySQLIdentifier(timeColumn), quoteMySQLIdentifier(table))
		if where != "" {
			timeRangeQuery += " WHERE " + where
		}
		if err := db.QueryRow(timeRangeQuery).Scan(&minTimeStr, &maxTimeStr); err != nil {
			return nil, fmt.Errorf("[archive-mysql] failed to get min/max timestamp: %w", err)
		}

		if !minTimeStr.Valid || !maxTimeStr.Valid {
			var totalCount int
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteMySQLIdentifier(table))).Scan(&totalCount)
			if totalCount == 0 {
				fmt.Printf("[archive-mysql] Table %s is empty (0 rows)\n", table)
			} else {
				fmt.Printf("[archive-mysql] Table %s has %d rows but none match the time range\n", table, totalCount)
			}
			fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n", table, progress.DoneRows, progress.DoneChunks)
			return uploadedKeys, nil
		}

		var minTime, maxTime time.Time
		var errMin, errMax error
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z",
			"2006-01-02",
		}
		for _, f := range formats {
			if minTime, errMin = time.Parse(f, minTimeStr.String); errMin == nil {
				break
			}
		}
		for _, f := range formats {
			if maxTime, errMax = time.Parse(f, maxTimeStr.String); errMax == nil {
				break
			}
		}
		if errMin != nil || errMax != nil {
			return uploadedKeys, fmt.Errorf("[archive-mysql] failed to parse min/max timestamp: min: %v max: %v", errMin, errMax)
		}

		if minTime.After(maxTime) {
			var totalCount int
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteMySQLIdentifier(table))).Scan(&totalCount)
			if totalCount == 0 {
				fmt.Printf("[archive-mysql] Table %s is empty (0 rows)\n", table)
			} else {
				fmt.Printf("[archive-mysql] Table %s has %d rows but none match the time range\n", table, totalCount)
			}
			fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n", table, progress.DoneRows, progress.DoneChunks)
			return uploadedKeys, nil
		}

		totalDuration := maxTime.Sub(minTime)
		defaultWindow := 30 * 24 * time.Hour // 1 month
		window := defaultWindow
		maxChunks := 120

		estChunks := int(totalDuration/window) + 1
		if estChunks > maxChunks {
			window = time.Duration(totalDuration.Nanoseconds() / int64(maxChunks))
			fmt.Printf("[archive-mysql] Large date range detected; increasing chunk window to %s\n", window)
			estChunks = int(totalDuration/window) + 1
		}

		progress.TotalChunks = estChunks
		fmt.Printf("[archive-mysql] Using %q for time chunking (min: %s, max: %s) with window %s (%d chunks expected)\n",
			timeColumn, minTime.Format(time.RFC3339), maxTime.Format(time.RFC3339), window, estChunks)

		chunkNum := 0
		t0 := minTime
		for t0.Before(maxTime) {
			t1 := t0.Add(window)
			// Include rows where ts == maxTime on the last chunk.
			if t1.After(maxTime) || t1.Equal(maxTime) {
				t1 = maxTime.Add(time.Second)
			}
			t0Copy := t0
			t1Copy := t1
			chunkNumCopy := chunkNum
			wg.Add(1)
			sem <- struct{}{}
			go func(chunkNum int, t0, t1 time.Time) {
				defer wg.Done()
				defer func() { <-sem }()
				if err := processMySQLChunkTime(
					ctx, db, table, where, timeColumn, t0, t1, chunkNum, partitionColumnUsed, store, &uploadedKeysMu, &uploadedKeys, progress,
				); err != nil {
					errCh <- fmt.Errorf("[worker %d] %w", chunkNum, err)
				}
			}(chunkNumCopy, t0Copy, t1Copy)
			t0 = t1
			chunkNum++
		}

		// After all chunking blocks, collect errors and write manifest if successful
		wg.Wait()
		close(errCh)
		var firstErr error
		for err := range errCh {
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if firstErr != nil {
			return uploadedKeys, firstErr
		}
		fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n", table, progress.DoneRows, progress.DoneChunks)
		return uploadedKeys, nil
	}

	// If --partition-column wasn't provided, attempt to detect a DATETIME column for partitioning
	if partitionColumnUsed == "" {
		foundDatetime := false
		rows, err := db.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", quoteMySQLIdentifier(table)))
		if err == nil {
			for rows.Next() {
				var field, colType, null, key, def, extra string
				if err := rows.Scan(&field, &colType, &null, &key, &def, &extra); err == nil {
					t := strings.ToLower(colType)
					if strings.Contains(t, "datetime") || strings.Contains(t, "timestamp") {
						partitionColumnUsed = field
						fmt.Printf("[WARN] No --partition-column provided. Detected DATETIME column '%s' for partitioning.\n", field)
						foundDatetime = true
						break
					}
				}
			}
			rows.Close()
		}
		if !foundDatetime {
			fmt.Printf("[WARN] No --partition-column or DATETIME column detected. Partitioning may default to 'unknown'.\n")
		}
	}

	// Fallback: OFFSET/LIMIT
	fmt.Println("[archive-mysql] Fallback: using OFFSET/LIMIT chunking (no PK or timestamp found)")
	var totalRows int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteMySQLIdentifier(table))
	if where != "" {
		countQuery += " WHERE " + where
	}
	if err := db.QueryRow(countQuery).Scan(&totalRows); err != nil {
		return nil, fmt.Errorf("[archive-mysql] failed to count rows: %w", err)
	}
	fmt.Printf("[archive-mysql] Row count: %d\n", totalRows)
	numChunks := (totalRows + chunkSize - 1) / chunkSize
	progress.TotalRows = int64(totalRows)
	progress.TotalChunks = numChunks
	for i := 0; i < numChunks; i++ {
		offset := i * chunkSize
		limit := chunkSize
		wg.Add(1)
		sem <- struct{}{}
		go func(chunkNum, offset, limit int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := processMySQLChunkOffset(
				ctx, db, table, where, offset, limit, chunkNum, partitionColumnUsed, store, &uploadedKeysMu, &uploadedKeys, progress,
			); err != nil {
				errCh <- fmt.Errorf("[worker %d] %w", chunkNum, err)
			}
		}(i, offset, limit)
	}
	wg.Wait()
	close(errCh)
	var firstErr error
	for err := range errCh {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	fmt.Printf("[archive-mysql] All %d chunks processed and stored (offset/limit mode).\n", numChunks)
	fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n", table, progress.DoneRows, progress.DoneChunks)
	return uploadedKeys, nil
}
func processMySQLChunkIndex(ctx context.Context, db *sql.DB, table, where, idColumn string, startID, endID int64, chunkNum int, partitionColumn string, store FileStore, uploadedKeysMu *sync.Mutex, uploadedKeys *[]map[string]interface{}, progress *ProgressTracker) error {
	quotedTable := quoteMySQLIdentifier(table)
	quotedID := quoteMySQLIdentifier(idColumn)
	query := fmt.Sprintf("SELECT * FROM %s WHERE (%s >= %d AND %s < %d)",
		quotedTable, quotedID, startID, quotedID, endID)
	if where != "" {
		query = fmt.Sprintf("SELECT * FROM %s WHERE (%s) AND (%s >= %d AND %s < %d)",
			quotedTable, where, quotedID, startID, quotedID, endID)
	}
	return processMySQLChunkGeneric(ctx, db, query, table, chunkNum, partitionColumn, store, uploadedKeysMu, uploadedKeys, progress)
}

func processMySQLChunkTime(ctx context.Context, db *sql.DB, table, where, timeColumn string, t0, t1 time.Time, chunkNum int, partitionColumn string, store FileStore, uploadedKeysMu *sync.Mutex, uploadedKeys *[]map[string]interface{}, progress *ProgressTracker) error {
	quotedTable := quoteMySQLIdentifier(table)
	quotedTime := quoteMySQLIdentifier(timeColumn)
	t0Str := t0.Format("2006-01-02 15:04:05")
	t1Str := t1.Format("2006-01-02 15:04:05")
	query := fmt.Sprintf("SELECT * FROM %s WHERE (%s >= '%s' AND %s < '%s')",
		quotedTable, quotedTime, t0Str, quotedTime, t1Str)
	if where != "" {
		query = fmt.Sprintf("SELECT * FROM %s WHERE (%s) AND (%s >= '%s' AND %s < '%s')",
			quotedTable, where, quotedTime, t0Str, quotedTime, t1Str)
	}
	return processMySQLChunkGeneric(ctx, db, query, table, chunkNum, partitionColumn, store, uploadedKeysMu, uploadedKeys, progress)
}

func processMySQLChunkOffset(ctx context.Context, db *sql.DB, table, where string, offset, limit, chunkNum int, partitionColumn string, store FileStore, uploadedKeysMu *sync.Mutex, uploadedKeys *[]map[string]interface{}, progress *ProgressTracker) error {
	query := fmt.Sprintf("SELECT * FROM %s", quoteMySQLIdentifier(table))
	if where != "" {
		query += " WHERE " + where
	}
	query += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	return processMySQLChunkGeneric(ctx, db, query, table, chunkNum, partitionColumn, store, uploadedKeysMu, uploadedKeys, progress)
}

func processMySQLChunkGeneric(ctx context.Context, db *sql.DB, query, table string, chunkNum int, partitionColumn string, store FileStore, uploadedKeysMu *sync.Mutex, uploadedKeys *[]map[string]interface{}, progress *ProgressTracker) error {
	// const maxBufferSize = 128 * 1024 * 1024 // 128MB

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("[worker %d] query error: %w", chunkNum, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("[worker %d] failed to get columns: %w", chunkNum, err)
	}
	if debug {
		fmt.Printf("[DEBUG] Detected columns: %v\n", columns)
	}

	type PartitionWriter struct {
		File     *os.File
		FilePath string
		PW       *writer.JSONWriter
		Schema   string
		Rows     int
	}

	partitionWriters := map[string]*PartitionWriter{}
	parquetFilePaths := map[string]string{} // S3 key -> temp file path

	var sampleRow []interface{}
	var schema string
	var initialized bool

	// Helper: get year/month for a row
	getPartition := func(vals []interface{}) (string, string) {
		idx := -1
		for i, name := range columns {
			if name == partitionColumn {
				idx = i
				break
			}
		}
		if idx == -1 {
			if debug {
				fmt.Printf("[DEBUG] partitionColumn '%s' not found. Columns: %v\n", partitionColumn, columns)
			}
			return "unknown", "unknown"
		}
		var t time.Time
		raw := vals[idx]
		if raw == nil {
			if debug {
				fmt.Printf("[DEBUG] getPartition: vals[%d] is nil for column '%s'\n", idx, partitionColumn)
			}
			return "unknown", "unknown"
		}
		switch v := raw.(type) {
		case time.Time:
			t = v
		case string:
			var err error
			t, err = tryParseTime(v)
			if err != nil {
				if debug {
					fmt.Printf("[DEBUG] getPartition: failed to parse string, type=%T value=%#v err=%v\n", raw, raw, err)
				}
				return "unknown", "unknown"
			}
		case []byte:
			s := string(v)
			var err error
			t, err = tryParseTime(s)
			if err != nil {
				if debug {
					fmt.Printf("[DEBUG] getPartition: failed to parse []byte, type=%T value=%#v err=%v\n", raw, raw, err)
				}
				return "unknown", "unknown"
			}
		default:
			if debug {
				fmt.Printf("[DEBUG] getPartition: unsupported type, type=%T value=%#v\n", raw, raw)
			}
			return "unknown", "unknown"
		}
		if t.IsZero() {
			if debug {
				fmt.Printf("[DEBUG] getPartition: zero time, type=%T value=%#v\n", raw, raw)
			}
			return "unknown", "unknown"
		}
		return t.Format("2006"), t.Format("01")
	}

	rowCount := 0

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("[worker %d] failed to scan row: %w", chunkNum, err)
		}
		if !initialized {
			sampleRow = make([]interface{}, len(values))
			copy(sampleRow, values)
			schema = inferParquetSchema(columns, sampleRow)
			initialized = true
		}

		year, month := getPartition(values)
		partitionKey := fmt.Sprintf("year=%s/month=%s", year, month)
		pw, ok := partitionWriters[partitionKey]
		if !ok {
			tmpFile, err := os.CreateTemp("", "chunk-*.parquet")
			if err != nil {
				return fmt.Errorf("[worker %d] failed to create temp file: %w", chunkNum, err)
			}
			jsonWriter, err := writer.NewJSONWriter(schema, writerfile.NewWriterFile(tmpFile), 4)
			if err != nil {
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				return fmt.Errorf("[worker %d] failed to create Parquet writer: %w", chunkNum, err)
			}
			// Enable ZSTD compression
			jsonWriter.CompressionType = parquet.CompressionCodec_ZSTD
			pw = &PartitionWriter{
				File:     tmpFile,
				FilePath: tmpFile.Name(),
				PW:       jsonWriter,
				Schema:   schema,
			}
			partitionWriters[partitionKey] = pw
		}

		// Map row to JSON — skip nil values so the parquet writer treats them
		// as missing (NULL) rather than trying to assert nil as a typed value.
		rowMap := make(map[string]interface{})
		for i, name := range columns {
			val := values[i]
			if val == nil {
				continue
			}
			if b, ok := val.([]byte); ok {
				rowMap[name] = string(b)
			} else {
				rowMap[name] = toParquetSafeValue(val)
			}
		}
		rowJSON, _ := json.Marshal(rowMap)
		if err := pw.PW.Write(string(rowJSON)); err != nil {
			return fmt.Errorf("[worker %d] failed to write row: %w", chunkNum, err)
		}
		pw.Rows++
		rowCount++

		if rowCount%100_000 == 0 {
			progress.AddRows(100_000)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("[worker %d] row iteration error: %w", chunkNum, err)
	}

	// Final flush for all partitions
	for partitionKey, pw := range partitionWriters {
		fileKey := path.Join(partitionKey, fmt.Sprintf("%s-chunk-%d-part-0.parquet", table, chunkNum))
		key := path.Join(table, fileKey)
		if pw.Rows == 0 {
			pw.File.Close()
			os.Remove(pw.FilePath)
			continue
		}
		if err := pw.PW.WriteStop(); err != nil {
			return fmt.Errorf("[worker %d] failed to finalize Parquet: %w", chunkNum, err)
		}
		pw.File.Sync()
		pw.File.Close()
		// Use the new fileKey for manifest and upload
		parquetFilePaths[key] = pw.FilePath
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
			uploadedKeysMu.Unlock()

			fmt.Printf("[%s] [worker %d] Stored chunk (%d rows, %.2f MB)\n",
				table, chunkNum, rowCount, float64(fileSize)/1024/1024)
		}(key, filePath)
	}
	uploadWg.Wait()
	if uploadErr != nil {
		return uploadErr
	}

	// Log if no files were uploaded for this table
	if len(*uploadedKeys) == 0 {
		fmt.Printf("[archive-mysql] Table %s had no uploaded files (empty?)\n", table)
	}

	// Final progress update for this chunk
	if rowCount%100_000 != 0 {
		progress.AddRows(rowCount % 100_000)
	}
	progress.ChunkDone()
	if debug {
		fmt.Printf("[worker %d] Done: %d rows (Parquet files written and uploaded)\n", chunkNum, rowCount)
	}
	return nil // keep error nil
}

// DetectChunkingColumn inspects the table for the best chunking column: numeric PK, then DATETIME, else fallback to offset/limit.
func DetectChunkingColumn(db *sql.DB, table string) (hasID bool, idColumn string, hasTime bool, timeColumn string, err error) {
	// 1. Try for a numeric PK
	query := fmt.Sprintf("SHOW KEYS FROM %s WHERE Key_name = 'PRIMARY'", quoteMySQLIdentifier(table))
	rows, err := db.Query(query)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var (
				tableName, nonUnique, keyName, seqInIndex, columnName, collation, cardinality, subPart, packed, null, indexType, comment, indexComment sql.NullString
			)
			// We only care about Column_name (5th field)
			_ = rows.Scan(&tableName, &nonUnique, &keyName, &seqInIndex, &columnName, &collation, &cardinality, &subPart, &packed, &null, &indexType, &comment, &indexComment)
			// Check column type
			if columnName.Valid {
				col := columnName.String
				var dataType string
				typeQuery := "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?"
				err2 := db.QueryRow(typeQuery, table, col).Scan(&dataType)
				if err2 == nil && (dataType == "int" || dataType == "bigint" || dataType == "mediumint" || dataType == "smallint" || dataType == "tinyint") {
					return true, col, false, "", nil
				}
			}
		}
	}
	// 2. Try for a datetime/timestamp column
	colRows, err := db.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", quoteMySQLIdentifier(table)))
	if err != nil {
		return false, "", false, "", err
	}
	defer colRows.Close()
	for colRows.Next() {
		var field, typ, null, key, def, extra sql.NullString
		if err := colRows.Scan(&field, &typ, &null, &key, &def, &extra); err != nil {
			continue
		}
		ft := strings.ToLower(typ.String)
		if field.Valid && (strings.Contains(ft, "datetime") || strings.Contains(ft, "timestamp")) {
			return false, "", true, field.String, nil
		}
	}
	// 3. Fallback: no id, no datetime
	return false, "", false, "", nil
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
