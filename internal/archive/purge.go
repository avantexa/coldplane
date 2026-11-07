package archive

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/avantexa/coldplane/internal/dbutil"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx" for batched deletes
)

// TablePurge records that a table's archived rows were removed from the source.
type TablePurge struct {
	At   string `json:"at"`
	Rows int64  `json:"rows"`
}

// PurgeOptions tunes a purge run.
type PurgeOptions struct {
	BatchSize int  // rows per DELETE statement
	Execute   bool // false is a dry run: count, print, change nothing
}

// PurgeResult is one table's outcome of a purge run.
type PurgeResult struct {
	Table string
	Rows  int64 // rows that would be, or were, deleted
}

// Purge removes the archived rows of a verified backup from its source
// database, in batches, each its own transaction. It refuses when the backup
// is not verified, when dsn names another database, or when a table's row
// count changed since verification. Without Execute it only reports.
func Purge(ctx context.Context, backupDir, dsn string, opts PurgeOptions) ([]PurgeResult, error) {
	m, err := ReadManifest(backupDir)
	if err != nil {
		return nil, err
	}
	if err := purgeRefusal(m, dbutil.DBNameFromDSN(dsn)); err != nil {
		return nil, err
	}
	db, err := openSource(m.DBType, dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var results []PurgeResult
	for i := range m.Tables {
		t := &m.Tables[i]
		current, err := countRows(ctx, db, m.DBType, t.Name, m.whereFor(*t))
		if err != nil {
			return nil, fmt.Errorf("count %s: %w", t.Name, err)
		}
		if current != t.Verified.SourceRows {
			return nil, fmt.Errorf("table %s has %d matching rows but %d were verified; the source changed, run 'backup verify' again before purging", t.Name, current, t.Verified.SourceRows)
		}
		if !opts.Execute {
			results = append(results, PurgeResult{Table: t.Name, Rows: current})
			continue
		}
		deleted, err := deleteInBatches(ctx, db, m.DBType, t.Name, m.whereFor(*t), opts.BatchSize)
		if err != nil {
			return results, fmt.Errorf("purge %s: %w", t.Name, err)
		}
		t.Purged = &TablePurge{At: time.Now().UTC().Format(time.RFC3339), Rows: deleted}
		results = append(results, PurgeResult{Table: t.Name, Rows: deleted})
	}
	if opts.Execute {
		m.PurgedAt = time.Now().UTC().Format(time.RFC3339)
		if err := writeManifest(backupDir, m); err != nil {
			return results, fmt.Errorf("record purge: %w", err)
		}
	}
	return results, nil
}

// purgeRefusal is the gate: a verified backup, every table passing, same database.
func purgeRefusal(m Manifest, dsnDBName string) error {
	if m.VerifiedAt == "" || !allVerified(m.Tables) {
		return fmt.Errorf("backup %q is not verified; run 'coldplane backup verify %s --db-url ...' first", m.Name, m.Name)
	}
	if dsnDBName != "" && m.DBName != "" && dsnDBName != m.DBName {
		return fmt.Errorf("backup %q was taken from database %q but --db-url points at %q", m.Name, m.DBName, dsnDBName)
	}
	return nil
}

// plannedPurgeRows totals the verified source rows, which a purge would delete.
func plannedPurgeRows(m Manifest) int64 {
	var n int64
	for _, t := range m.Tables {
		if t.Verified != nil {
			n += t.Verified.SourceRows
		}
	}
	return n
}

// openSource opens a database/sql handle on the backup's source engine.
func openSource(dbType, dsn string) (*sql.DB, error) {
	driver := "pgx"
	if dbType == "mysql" {
		driver = "mysql"
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", dbType, err)
	}
	return db, nil
}

func countRows(ctx context.Context, db *sql.DB, dbType, table, where string) (int64, error) {
	q := pgCountQuery(table, where)
	if dbType == "mysql" {
		q = mysqlCountQuery(table, where)
	}
	var n int64
	err := db.QueryRowContext(ctx, q).Scan(&n)
	return n, err
}

// purgeBatchSQL deletes at most batch rows matching where, in the engine's idiom.
func purgeBatchSQL(dbType, table, where string, batch int) string {
	if dbType == "mysql" {
		q := "DELETE FROM " + dbutil.QuoteMySQL(table)
		if where != "" {
			q += " WHERE " + where
		}
		return fmt.Sprintf("%s LIMIT %d", q, batch)
	}
	qt := dbutil.QuotePG(table)
	inner := "SELECT ctid FROM " + qt
	if where != "" {
		inner += " WHERE " + where
	}
	return fmt.Sprintf("DELETE FROM %s WHERE ctid IN (%s LIMIT %d)", qt, inner, batch)
}

// deleteInBatches runs purgeBatchSQL until a batch deletes nothing and returns the total.
func deleteInBatches(ctx context.Context, db *sql.DB, dbType, table, where string, batch int) (int64, error) {
	if batch <= 0 {
		batch = 5000
	}
	q := purgeBatchSQL(dbType, table, where, batch)
	var total int64
	for {
		res, err := db.ExecContext(ctx, q)
		if err != nil {
			return total, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, err
		}
		total += n
		fmt.Printf("[%s] deleted %d rows so far\n", table, total)
		if n < int64(batch) {
			return total, nil
		}
	}
}
