package archive

import "fmt"

const mysqlTimeLayout = "2006-01-02 15:04:05"

// mysqlRangeQuery selects the rows of table where lo <= col < hi, with lo and
// hi already rendered as SQL literals, optionally narrowed by a WHERE clause.
func mysqlRangeQuery(table, where, col, lo, hi string) string {
	qt, qc := quoteMySQLIdentifier(table), quoteMySQLIdentifier(col)
	pred := fmt.Sprintf("(%s >= %s AND %s < %s)", qc, lo, qc, hi)
	if where != "" {
		return fmt.Sprintf("SELECT * FROM %s WHERE (%s) AND %s", qt, where, pred)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s", qt, pred)
}

// mysqlOffsetQuery selects one LIMIT/OFFSET page of table.
func mysqlOffsetQuery(table, where string, offset, limit int) string {
	q := "SELECT * FROM " + quoteMySQLIdentifier(table)
	if where != "" {
		q += " WHERE " + where
	}
	return fmt.Sprintf("%s LIMIT %d OFFSET %d", q, limit, offset)
}

// mysqlCountQuery counts the rows of table, optionally narrowed by a WHERE clause.
func mysqlCountQuery(table, where string) string {
	q := "SELECT COUNT(*) FROM " + quoteMySQLIdentifier(table)
	if where != "" {
		q += " WHERE " + where
	}
	return q
}

// mysqlMinMaxQuery selects MIN(col), MAX(col) of table, optionally narrowed by a WHERE clause.
func mysqlMinMaxQuery(table, where, col string) string {
	qc := quoteMySQLIdentifier(col)
	q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", qc, qc, quoteMySQLIdentifier(table))
	if where != "" {
		q += " WHERE " + where
	}
	return q
}

// pgRangeQuery selects the rows of table where $1 <= col < $2.
func pgRangeQuery(table, where, col string) string {
	qt, qc := quotePGIdentifier(table), quotePGIdentifier(col)
	if where != "" {
		return fmt.Sprintf("SELECT * FROM %s WHERE (%s) AND (%s >= $1 AND %s < $2)", qt, where, qc, qc)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s >= $1 AND %s < $2", qt, qc, qc)
}

// pgOffsetQuery selects one page of table with LIMIT $1 OFFSET $2.
func pgOffsetQuery(table, where string) string {
	q := "SELECT * FROM " + quotePGIdentifier(table)
	if where != "" {
		q += " WHERE " + where
	}
	return q + " LIMIT $1 OFFSET $2"
}

// pgCountQuery counts the rows of table, optionally narrowed by a WHERE clause.
func pgCountQuery(table, where string) string {
	q := "SELECT COUNT(*) FROM " + quotePGIdentifier(table)
	if where != "" {
		q += " WHERE " + where
	}
	return q
}

// pgMinMaxQuery selects MIN(col), MAX(col) of table, optionally narrowed by a WHERE clause.
func pgMinMaxQuery(table, where, col string) string {
	qc := quotePGIdentifier(col)
	q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", qc, qc, quotePGIdentifier(table))
	if where != "" {
		q += " WHERE " + where
	}
	return q
}
