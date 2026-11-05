package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

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
	progress := &ProgressTracker{Table: table}
	partitionColumnUsed := mysqlPartitionColumn(db, table, partitionColumnFlag)

	hasID, idColumn, hasTime, timeColumn, err := DetectChunkingColumn(db, table)
	if err != nil {
		return nil, fmt.Errorf("[archive-mysql] failed to detect chunk column: %w", err)
	}

	var n int
	var query func(i int) string
	switch {
	case hasID:
		n, query, err = mysqlIDChunks(db, table, where, idColumn, opts.ChunkSize, progress)
	case hasTime:
		n, query, err = mysqlTimeChunks(db, table, where, timeColumn, progress)
	default:
		if partitionColumnUsed == "" {
			partitionColumnUsed = detectMySQLDatetimeColumn(db, table)
		}
		n, query, err = mysqlOffsetChunks(db, table, where, opts.ChunkSize, progress)
	}
	if err != nil {
		return nil, err
	}

	var uploadedKeysMu sync.Mutex
	var uploadedKeys []map[string]interface{}
	err = runChunks(n, opts.Workers, func(i int) error {
		return processMySQLChunkGeneric(ctx, db, query(i), table, i, partitionColumnUsed, store, &uploadedKeysMu, &uploadedKeys, progress)
	})
	if err != nil {
		return nil, err
	}
	fmt.Printf("[summary] Table %s: %d rows archived in %d chunks\n", table, progress.DoneRows, progress.DoneChunks)
	return uploadedKeys, nil
}

// mysqlPartitionColumn validates the requested partition column against the
// table, or infers one from the first datetime-looking value of the first row.
func mysqlPartitionColumn(db *sql.DB, table, requested string) string {
	columns, firstRow := mysqlSampleRow(db, table)
	if requested != "" {
		if containsString(columns, requested) {
			return requested
		}
		fmt.Printf("[WARNING] Provided partition column '%s' not found in table. Will attempt to infer.\n", requested)
	}
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

// detectMySQLDatetimeColumn returns the first DATETIME/TIMESTAMP column of the
// table by schema, for partitioning when the first row offered no hint.
func detectMySQLDatetimeColumn(db *sql.DB, table string) string {
	rows, err := db.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", quoteMySQLIdentifier(table)))
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var field, colType, null, key, def, extra sql.NullString
			if err := rows.Scan(&field, &colType, &null, &key, &def, &extra); err != nil {
				continue
			}
			t := strings.ToLower(colType.String)
			if strings.Contains(t, "datetime") || strings.Contains(t, "timestamp") {
				fmt.Printf("[WARN] No --partition-column provided. Detected DATETIME column '%s' for partitioning.\n", field.String)
				return field.String
			}
		}
	}
	fmt.Printf("[WARN] No --partition-column or DATETIME column detected. Partitioning may default to 'unknown'.\n")
	return ""
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
			t, err = parseAnyTime(v)
			if err != nil {
				if debug {
					fmt.Printf("[DEBUG] getPartition: failed to parse string, type=%T value=%#v err=%v\n", raw, raw, err)
				}
				return "unknown", "unknown"
			}
		case []byte:
			s := string(v)
			var err error
			t, err = parseAnyTime(s)
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
