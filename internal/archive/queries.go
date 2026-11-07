package archive

import (
	"fmt"
	"time"

	"github.com/avantexa/coldplane/internal/dbutil"
)

// Chunk queries are native SQL for the source engine: DuckDB passes them
// through unchanged (postgres_query / mysql_query), so range bounds are
// rendered as literals here. Timestamps carry microseconds so the
// one-microsecond Postgres nudge survives; Postgres literals also carry the
// offset, MySQL literals cannot.
const (
	mysqlTimeLayout = "2006-01-02 15:04:05.000000"
	pgNaiveLayout   = "2006-01-02 15:04:05.000000"
	pgZoneLayout    = "2006-01-02 15:04:05.000000-07:00"
)

// pgTimeLiteral renders a chunk bound for Postgres as a typed literal. A
// timestamptz column compares instants, so the literal carries its offset and
// type; date and timestamp columns compare wall-clock values, so the literal
// is the UTC wall clock typed as timestamp. The explicit type matters: an
// untyped literal next to a date column would be cast to date, dropping the
// one-microsecond nudge that keeps the last row inside the final window.
func pgTimeLiteral(t time.Time, withZone bool) string {
	if withZone {
		return sqlString(t.Format(pgZoneLayout)) + "::timestamptz"
	}
	return sqlString(t.UTC().Format(pgNaiveLayout)) + "::timestamp"
}

// mysqlRangeQuery selects the rows of table where lo <= col < hi (lo and hi
// already rendered as SQL literals), optionally narrowed by a WHERE clause.
func mysqlRangeQuery(table, where, col, lo, hi string) string {
	qt, qc := dbutil.QuoteMySQL(table), dbutil.QuoteMySQL(col)
	if where != "" {
		return fmt.Sprintf("SELECT * FROM %s WHERE (%s) AND (%s >= %s AND %s < %s)", qt, where, qc, lo, qc, hi)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s >= %s AND %s < %s", qt, qc, lo, qc, hi)
}

// mysqlNullQuery selects the rows whose chunk column is NULL, which no range covers.
func mysqlNullQuery(table, where, col string) string {
	qt, qc := dbutil.QuoteMySQL(table), dbutil.QuoteMySQL(col)
	if where != "" {
		return fmt.Sprintf("SELECT * FROM %s WHERE (%s) AND %s IS NULL", qt, where, qc)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s IS NULL", qt, qc)
}

// mysqlOffsetQuery selects one LIMIT/OFFSET page of table.
func mysqlOffsetQuery(table, where string, limit, offset int) string {
	q := "SELECT * FROM " + dbutil.QuoteMySQL(table)
	if where != "" {
		q += " WHERE " + where
	}
	return fmt.Sprintf("%s LIMIT %d OFFSET %d", q, limit, offset)
}

// mysqlCountQuery counts the rows of table, optionally narrowed by a WHERE clause.
func mysqlCountQuery(table, where string) string {
	q := "SELECT COUNT(*) FROM " + dbutil.QuoteMySQL(table)
	if where != "" {
		q += " WHERE " + where
	}
	return q
}

// mysqlMinMaxQuery selects MIN(col), MAX(col) of table, optionally narrowed by a WHERE clause.
func mysqlMinMaxQuery(table, where, col string) string {
	qc := dbutil.QuoteMySQL(col)
	q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", qc, qc, dbutil.QuoteMySQL(table))
	if where != "" {
		q += " WHERE " + where
	}
	return q
}

// pgRangeQuery selects the rows of table where lo <= col < hi (literals).
func pgRangeQuery(table, where, col, lo, hi string) string {
	qt, qc := dbutil.QuotePG(table), dbutil.QuotePG(col)
	if where != "" {
		return fmt.Sprintf("SELECT * FROM %s WHERE (%s) AND (%s >= %s AND %s < %s)", qt, where, qc, lo, qc, hi)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s >= %s AND %s < %s", qt, qc, lo, qc, hi)
}

// pgNullQuery selects the rows whose chunk column is NULL, which no range covers.
func pgNullQuery(table, where, col string) string {
	qt, qc := dbutil.QuotePG(table), dbutil.QuotePG(col)
	if where != "" {
		return fmt.Sprintf("SELECT * FROM %s WHERE (%s) AND %s IS NULL", qt, where, qc)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s IS NULL", qt, qc)
}

// pgOffsetQuery selects one LIMIT/OFFSET page of table.
func pgOffsetQuery(table, where string, limit, offset int) string {
	q := "SELECT * FROM " + dbutil.QuotePG(table)
	if where != "" {
		q += " WHERE " + where
	}
	return fmt.Sprintf("%s LIMIT %d OFFSET %d", q, limit, offset)
}

// pgCountQuery counts the rows of table, optionally narrowed by a WHERE clause.
func pgCountQuery(table, where string) string {
	q := "SELECT COUNT(*) FROM " + dbutil.QuotePG(table)
	if where != "" {
		q += " WHERE " + where
	}
	return q
}

// pgMinMaxQuery selects MIN(col), MAX(col) of table, optionally narrowed by a WHERE clause.
func pgMinMaxQuery(table, where, col string) string {
	qc := dbutil.QuotePG(col)
	q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", qc, qc, dbutil.QuotePG(table))
	if where != "" {
		q += " WHERE " + where
	}
	return q
}

// sqlString renders s as a single-quoted SQL string literal.
func sqlString(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'')
		}
		out = append(out, s[i])
	}
	return string(append(out, '\''))
}
