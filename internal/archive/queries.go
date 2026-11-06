package archive

import (
	"fmt"

	"github.com/avantexa/coldplane/internal/dbutil"
)

const mysqlTimeLayout = "2006-01-02 15:04:05"

// mysqlRangeQuery selects the rows of table where ? <= col < ?, optionally
// narrowed by a WHERE clause. The bounds are bound as arguments.
func mysqlRangeQuery(table, where, col string) string {
	qt, qc := dbutil.QuoteMySQL(table), dbutil.QuoteMySQL(col)
	if where != "" {
		return fmt.Sprintf("SELECT * FROM %s WHERE (%s) AND (%s >= ? AND %s < ?)", qt, where, qc, qc)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s >= ? AND %s < ?", qt, qc, qc)
}

// mysqlOffsetQuery selects one page of table with LIMIT ? OFFSET ?.
func mysqlOffsetQuery(table, where string) string {
	q := "SELECT * FROM " + dbutil.QuoteMySQL(table)
	if where != "" {
		q += " WHERE " + where
	}
	return q + " LIMIT ? OFFSET ?"
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

// pgRangeQuery selects the rows of table where $1 <= col < $2.
func pgRangeQuery(table, where, col string) string {
	qt, qc := dbutil.QuotePG(table), dbutil.QuotePG(col)
	if where != "" {
		return fmt.Sprintf("SELECT * FROM %s WHERE (%s) AND (%s >= $1 AND %s < $2)", qt, where, qc, qc)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s >= $1 AND %s < $2", qt, qc, qc)
}

// pgOffsetQuery selects one page of table with LIMIT $1 OFFSET $2.
func pgOffsetQuery(table, where string) string {
	q := "SELECT * FROM " + dbutil.QuotePG(table)
	if where != "" {
		q += " WHERE " + where
	}
	return q + " LIMIT $1 OFFSET $2"
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
