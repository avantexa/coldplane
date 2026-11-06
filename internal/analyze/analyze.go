package analyze

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/avantexa/coldplane/internal/dbutil"
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
	return scanStrings(rows)
}

func analyzePostgresTable(ctx context.Context, pool *pgxpool.Pool, table string, cutoff time.Time, dateColumnOverride string) (*TableReport, error) {
	dateCol, err := resolveDateColumn(dateColumnOverride, func() (string, error) {
		return detectPostgresDateColumn(ctx, pool, table)
	})
	if err != nil {
		return nil, err
	}
	totalRows, sizeBytes, err := pgTableStats(ctx, pool, table)
	if err != nil {
		return nil, err
	}
	oldest, newest, err := pgDateRange(ctx, pool, table, dateCol)
	if err != nil {
		return nil, err
	}
	var archivable int64
	err = pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s < $1`, dbutil.QuotePG(table), dbutil.QuotePG(dateCol)), cutoff).Scan(&archivable)
	if err != nil {
		return nil, fmt.Errorf("count old rows: %w", err)
	}
	r := newTableReport(table, dateCol, totalRows, sizeBytes, oldest, newest, archivable)
	return &r, nil
}

// pgTableStats returns the row count (planner estimate, else exact) and the
// total relation size. An empty table is an error.
func pgTableStats(ctx context.Context, pool *pgxpool.Pool, table string) (totalRows, sizeBytes int64, err error) {
	err = pool.QueryRow(ctx,
		`SELECT COALESCE(reltuples::bigint, 0) FROM pg_class WHERE relname = $1`, table).Scan(&totalRows)
	if err != nil || totalRows <= 0 {
		err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, dbutil.QuotePG(table))).Scan(&totalRows)
		if err != nil {
			return 0, 0, fmt.Errorf("count rows: %w", err)
		}
	}
	if totalRows == 0 {
		return 0, 0, fmt.Errorf("table is empty")
	}
	err = pool.QueryRow(ctx, `SELECT pg_total_relation_size($1::regclass)`, table).Scan(&sizeBytes)
	if err != nil {
		return 0, 0, fmt.Errorf("get table size: %w", err)
	}
	return totalRows, sizeBytes, nil
}

// pgDateRange returns MIN and MAX of the date column.
func pgDateRange(ctx context.Context, pool *pgxpool.Pool, table, dateCol string) (time.Time, time.Time, error) {
	var minTime, maxTime sql.NullTime
	qc := dbutil.QuotePG(dateCol)
	err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT MIN(%s), MAX(%s) FROM %s`, qc, qc, dbutil.QuotePG(table))).Scan(&minTime, &maxTime)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("get date range: %w", err)
	}
	if !minTime.Valid || !maxTime.Valid {
		return time.Time{}, time.Time{}, fmt.Errorf("date column %q has no valid values", dateCol)
	}
	return minTime.Time, maxTime.Time, nil
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
	return scanStrings(rows)
}

func analyzeMySQLTable(ctx context.Context, db *sql.DB, table string, cutoff time.Time, dateColumnOverride string) (*TableReport, error) {
	dateCol, err := resolveDateColumn(dateColumnOverride, func() (string, error) {
		return detectMySQLDateColumn(ctx, db, table)
	})
	if err != nil {
		return nil, err
	}
	totalRows, sizeBytes, err := mysqlTableStats(ctx, db, table)
	if err != nil {
		return nil, err
	}
	oldest, newest, err := mysqlDateRange(ctx, db, table, dateCol)
	if err != nil {
		return nil, err
	}
	var archivable int64
	err = db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s < ?", dbutil.QuoteMySQL(table), dbutil.QuoteMySQL(dateCol)),
		cutoff.Format("2006-01-02 15:04:05")).Scan(&archivable)
	if err != nil {
		return nil, fmt.Errorf("count old rows: %w", err)
	}
	r := newTableReport(table, dateCol, totalRows, sizeBytes, oldest, newest, archivable)
	return &r, nil
}

// mysqlTableStats returns the row count (information_schema estimate, else
// exact) and the data+index size. An empty table is an error.
func mysqlTableStats(ctx context.Context, db *sql.DB, table string) (totalRows, sizeBytes int64, err error) {
	err = db.QueryRowContext(ctx,
		`SELECT COALESCE(TABLE_ROWS, 0), COALESCE(DATA_LENGTH + INDEX_LENGTH, 0)
		 FROM information_schema.TABLES
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, table).Scan(&totalRows, &sizeBytes)
	if err != nil {
		return 0, 0, fmt.Errorf("get table stats: %w", err)
	}
	if totalRows == 0 {
		err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", dbutil.QuoteMySQL(table))).Scan(&totalRows)
		if err != nil {
			return 0, 0, fmt.Errorf("count rows: %w", err)
		}
	}
	if totalRows == 0 {
		return 0, 0, fmt.Errorf("table is empty")
	}
	return totalRows, sizeBytes, nil
}

// mysqlDateRange returns MIN and MAX of the date column.
func mysqlDateRange(ctx context.Context, db *sql.DB, table, dateCol string) (time.Time, time.Time, error) {
	var minStr, maxStr sql.NullString
	qc := dbutil.QuoteMySQL(dateCol)
	err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", qc, qc, dbutil.QuoteMySQL(table))).Scan(&minStr, &maxStr)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("get date range: %w", err)
	}
	if !minStr.Valid || !maxStr.Valid {
		return time.Time{}, time.Time{}, fmt.Errorf("date column %q has no valid values", dateCol)
	}
	minTime, err := dbutil.ParseTime(minStr.String)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parse min date: %w", err)
	}
	maxTime, err := dbutil.ParseTime(maxStr.String)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parse max date: %w", err)
	}
	return minTime, maxTime, nil
}

func detectMySQLDateColumn(ctx context.Context, db *sql.DB, table string) (string, error) {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SHOW COLUMNS FROM %s", dbutil.QuoteMySQL(table)))
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

// stringRows is the part of sql.Rows and pgx.Rows needed to read one text column.
type stringRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

// scanStrings reads a single-column result set into a slice.
func scanStrings(rows stringRows) ([]string, error) {
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// resolveDateColumn uses the override when given, otherwise detects a column,
// and fails when neither yields one.
func resolveDateColumn(override string, detect func() (string, error)) (string, error) {
	col := override
	if col == "" {
		var err error
		if col, err = detect(); err != nil {
			return "", err
		}
	}
	if col == "" {
		return "", fmt.Errorf("no date/timestamp column detected (use --date-column to specify)")
	}
	return col, nil
}

// newTableReport assembles a report, deriving the archivable percentage and
// the estimated Parquet size from the counts and the table size.
func newTableReport(table, dateCol string, totalRows, sizeBytes int64, oldest, newest time.Time, archivable int64) TableReport {
	pct, est := 0, int64(0)
	if totalRows > 0 {
		pct = int(archivable * 100 / totalRows)
		est = int64(float64(archivable) / float64(totalRows) * float64(sizeBytes) * compressionRatio)
	}
	return TableReport{
		TableName:        table,
		TotalRows:        totalRows,
		SizeBytes:        sizeBytes,
		DateColumn:       dateCol,
		OldestRow:        oldest,
		NewestRow:        newest,
		ArchivableRows:   archivable,
		ArchivablePct:    pct,
		EstArchivedBytes: est,
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
