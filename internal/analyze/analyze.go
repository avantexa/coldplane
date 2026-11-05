package analyze

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TableReport holds the analysis results for a single table.
type TableReport struct {
	TableName        string    `json:"table_name"`
	TotalRows        int64     `json:"total_rows"`
	SizeBytes        int64     `json:"size_bytes"`
	DateColumn       string    `json:"date_column"`
	OldestRow        time.Time `json:"oldest_row"`
	NewestRow        time.Time `json:"newest_row"`
	ArchivableRows   int64     `json:"archivable_rows"`
	ArchivablePct    int       `json:"archivable_pct"`
	EstArchivedBytes int64     `json:"est_archived_bytes"`
}

// Report holds the full analysis output.
type Report struct {
	DBType     string        `json:"db_type"`
	DBName     string        `json:"db_name"`
	OlderThan  string        `json:"older_than"`
	CutoffDate time.Time     `json:"cutoff_date"`
	Tables     []TableReport `json:"tables"`
}

// Options configures the analysis run.
type Options struct {
	DBURL        string
	DBType       string // "postgres" or "mysql"
	DBName       string
	Tables       []string
	All          bool
	OlderThan    time.Duration
	OlderThanRaw string // original flag value (e.g. "18m") for display
	DateColumn   string // optional: override auto-detected date column
}

// compressionRatio is the estimated Parquet compression factor (4:1).
const compressionRatio = 0.25

// Run connects to the database, analyzes the specified tables, and returns a report.
func Run(ctx context.Context, opts Options) (*Report, error) {
	cutoff := time.Now().Add(-opts.OlderThan)

	report := &Report{
		DBType:     opts.DBType,
		DBName:     opts.DBName,
		OlderThan:  opts.OlderThanRaw,
		CutoffDate: cutoff,
	}

	switch opts.DBType {
	case "postgres":
		return runPostgres(ctx, opts, report, cutoff)
	case "mysql":
		return runMySQL(ctx, opts, report, cutoff)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", opts.DBType)
	}
}

func runPostgres(ctx context.Context, opts Options, report *Report, cutoff time.Time) (*Report, error) {
	pool, err := pgxpool.New(ctx, opts.DBURL)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	tables := opts.Tables
	if opts.All {
		tables, err = listPostgresTables(ctx, pool)
		if err != nil {
			return nil, fmt.Errorf("list tables: %w", err)
		}
		if len(tables) == 0 {
			return nil, fmt.Errorf("no tables found in database %s", opts.DBName)
		}
	}

	for _, table := range tables {
		tr, err := analyzePostgresTable(ctx, pool, table, cutoff, opts.DateColumn)
		if err != nil {
			fmt.Printf("[analyze] Skipping table %q: %v\n", table, err)
			continue
		}
		if tr != nil {
			report.Tables = append(report.Tables, *tr)
		}
	}

	return report, nil
}

func runMySQL(ctx context.Context, opts Options, report *Report, cutoff time.Time) (*Report, error) {
	db, err := sql.Open("mysql", opts.DBURL)
	if err != nil {
		return nil, fmt.Errorf("connect to mysql: %w", err)
	}
	defer db.Close()

	tables := opts.Tables
	if opts.All {
		tables, err = listMySQLTables(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("list tables: %w", err)
		}
		if len(tables) == 0 {
			return nil, fmt.Errorf("no tables found in database %s", opts.DBName)
		}
	}

	for _, table := range tables {
		tr, err := analyzeMySQLTable(ctx, db, table, cutoff, opts.DateColumn)
		if err != nil {
			fmt.Printf("[analyze] Skipping table %q: %v\n", table, err)
			continue
		}
		if tr != nil {
			report.Tables = append(report.Tables, *tr)
		}
	}

	return report, nil
}

// ---------------------------------------------------------------------------
// PostgreSQL
// ---------------------------------------------------------------------------

func listPostgresTables(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx,
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
		 ORDER BY table_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func analyzePostgresTable(ctx context.Context, pool *pgxpool.Pool, table string, cutoff time.Time, dateColumnOverride string) (*TableReport, error) {
	quoted := quotePGIdentifier(table)

	// Use explicit date column if provided, otherwise auto-detect
	var err error
	dateCol := dateColumnOverride
	if dateCol == "" {
		dateCol, err = detectPostgresDateColumn(ctx, pool, table)
		if err != nil {
			return nil, err
		}
	}
	if dateCol == "" {
		return nil, fmt.Errorf("no date/timestamp column detected (use --date-column to specify)")
	}

	// Total rows (estimate from pg_class for speed)
	var totalRows int64
	err = pool.QueryRow(ctx,
		`SELECT COALESCE(reltuples::bigint, 0) FROM pg_class WHERE relname = $1`, table).Scan(&totalRows)
	if err != nil || totalRows <= 0 {
		// Fallback to real count
		err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoted)).Scan(&totalRows)
		if err != nil {
			return nil, fmt.Errorf("count rows: %w", err)
		}
	}

	if totalRows == 0 {
		return nil, fmt.Errorf("table is empty")
	}

	// Table size
	var sizeBytes int64
	err = pool.QueryRow(ctx,
		`SELECT pg_total_relation_size($1::regclass)`, table).Scan(&sizeBytes)
	if err != nil {
		return nil, fmt.Errorf("get table size: %w", err)
	}

	// MIN/MAX date
	var minTime, maxTime sql.NullTime
	quotedCol := quotePGIdentifier(dateCol)
	err = pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT MIN(%s), MAX(%s) FROM %s`, quotedCol, quotedCol, quoted)).Scan(&minTime, &maxTime)
	if err != nil {
		return nil, fmt.Errorf("get date range: %w", err)
	}
	if !minTime.Valid || !maxTime.Valid {
		return nil, fmt.Errorf("date column %q has no valid values", dateCol)
	}

	// Rows older than cutoff
	var archivableRows int64
	err = pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s < $1`, quoted, quotedCol), cutoff).Scan(&archivableRows)
	if err != nil {
		return nil, fmt.Errorf("count old rows: %w", err)
	}

	pct := 0
	if totalRows > 0 {
		pct = int(archivableRows * 100 / totalRows)
	}

	// Estimated archived size: proportional share * compression ratio
	estSize := int64(float64(archivableRows) / float64(totalRows) * float64(sizeBytes) * compressionRatio)

	return &TableReport{
		TableName:        table,
		TotalRows:        totalRows,
		SizeBytes:        sizeBytes,
		DateColumn:       dateCol,
		OldestRow:        minTime.Time,
		NewestRow:        maxTime.Time,
		ArchivableRows:   archivableRows,
		ArchivablePct:    pct,
		EstArchivedBytes: estSize,
	}, nil
}

func detectPostgresDateColumn(ctx context.Context, pool *pgxpool.Pool, table string) (string, error) {
	rows, err := pool.Query(ctx,
		`SELECT column_name, data_type FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = $1
		 ORDER BY ordinal_position`, table)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		var name, dtype string
		if err := rows.Scan(&name, &dtype); err != nil {
			return "", err
		}
		lower := strings.ToLower(dtype)
		if strings.Contains(lower, "timestamp") || lower == "date" {
			return name, nil
		}
	}
	return "", rows.Err()
}

// ---------------------------------------------------------------------------
// MySQL
// ---------------------------------------------------------------------------

func listMySQLTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func analyzeMySQLTable(ctx context.Context, db *sql.DB, table string, cutoff time.Time, dateColumnOverride string) (*TableReport, error) {
	quoted := quoteMySQLIdentifier(table)

	// Use explicit date column if provided, otherwise auto-detect
	var err error
	dateCol := dateColumnOverride
	if dateCol == "" {
		dateCol, err = detectMySQLDateColumn(ctx, db, table)
		if err != nil {
			return nil, err
		}
	}
	if dateCol == "" {
		return nil, fmt.Errorf("no date/timestamp column detected (use --date-column to specify)")
	}

	// Total rows + size (estimate from information_schema)
	var totalRows, sizeBytes int64
	err = db.QueryRowContext(ctx,
		`SELECT COALESCE(TABLE_ROWS, 0), COALESCE(DATA_LENGTH + INDEX_LENGTH, 0)
		 FROM information_schema.TABLES
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, table).Scan(&totalRows, &sizeBytes)
	if err != nil {
		return nil, fmt.Errorf("get table stats: %w", err)
	}

	if totalRows == 0 {
		// Fallback to real count
		err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", quoted)).Scan(&totalRows)
		if err != nil {
			return nil, fmt.Errorf("count rows: %w", err)
		}
	}

	if totalRows == 0 {
		return nil, fmt.Errorf("table is empty")
	}

	// MIN/MAX date
	quotedCol := quoteMySQLIdentifier(dateCol)
	var minStr, maxStr sql.NullString
	err = db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", quotedCol, quotedCol, quoted)).Scan(&minStr, &maxStr)
	if err != nil {
		return nil, fmt.Errorf("get date range: %w", err)
	}
	if !minStr.Valid || !maxStr.Valid {
		return nil, fmt.Errorf("date column %q has no valid values", dateCol)
	}

	minTime, err := parseMySQLTime(minStr.String)
	if err != nil {
		return nil, fmt.Errorf("parse min date: %w", err)
	}
	maxTime, err := parseMySQLTime(maxStr.String)
	if err != nil {
		return nil, fmt.Errorf("parse max date: %w", err)
	}

	// Rows older than cutoff
	var archivableRows int64
	cutoffStr := cutoff.Format("2006-01-02 15:04:05")
	err = db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s < ?", quoted, quotedCol), cutoffStr).Scan(&archivableRows)
	if err != nil {
		return nil, fmt.Errorf("count old rows: %w", err)
	}

	pct := 0
	if totalRows > 0 {
		pct = int(archivableRows * 100 / totalRows)
	}

	estSize := int64(float64(archivableRows) / float64(totalRows) * float64(sizeBytes) * compressionRatio)

	return &TableReport{
		TableName:        table,
		TotalRows:        totalRows,
		SizeBytes:        sizeBytes,
		DateColumn:       dateCol,
		OldestRow:        minTime,
		NewestRow:        maxTime,
		ArchivableRows:   archivableRows,
		ArchivablePct:    pct,
		EstArchivedBytes: estSize,
	}, nil
}

func detectMySQLDateColumn(ctx context.Context, db *sql.DB, table string) (string, error) {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SHOW COLUMNS FROM %s", quoteMySQLIdentifier(table)))
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		var field, colType, null, key string
		var def, extra sql.NullString
		if err := rows.Scan(&field, &colType, &null, &key, &def, &extra); err != nil {
			return "", err
		}
		lower := strings.ToLower(colType)
		if strings.Contains(lower, "datetime") || strings.Contains(lower, "timestamp") || lower == "date" {
			return field, nil
		}
	}
	return "", rows.Err()
}

func parseMySQLTime(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("could not parse time: %q", s)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func quotePGIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteMySQLIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// ParseDuration parses human-friendly durations like "18m", "1y", "6m", "1y6m", "90d".
// Months = 30 days, years = 365 days.
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	re := regexp.MustCompile(`(\d+)\s*(y|m|d)`)
	matches := re.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return 0, fmt.Errorf("invalid duration format %q (expected e.g. 18m, 1y, 1y6m, 90d)", s)
	}

	var total time.Duration
	for _, match := range matches {
		n, err := strconv.Atoi(match[1])
		if err != nil {
			return 0, fmt.Errorf("invalid number in duration: %s", match[1])
		}
		switch match[2] {
		case "y":
			total += time.Duration(n) * 365 * 24 * time.Hour
		case "m":
			total += time.Duration(n) * 30 * 24 * time.Hour
		case "d":
			total += time.Duration(n) * 24 * time.Hour
		}
	}

	if total <= 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	return total, nil
}

// formatDuration converts a duration back to a human-friendly string.
func formatDuration(d time.Duration) string {
	days := int(d.Hours() / 24)
	years := days / 365
	days %= 365
	months := days / 30
	days %= 30

	var parts []string
	if years > 0 {
		parts = append(parts, fmt.Sprintf("%dy", years))
	}
	if months > 0 {
		parts = append(parts, fmt.Sprintf("%dm", months))
	}
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%dd", days))
	}
	if len(parts) == 0 {
		return "0d"
	}
	return strings.Join(parts, "")
}
