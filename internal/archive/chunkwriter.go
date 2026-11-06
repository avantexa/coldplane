package archive

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// colKind is what the driver declares a column to hold. It types the Parquet
// schema before any value is seen, so a NULL in the first row cannot mistype
// a column; kindUnknown falls back to inferring from the first value.
type colKind int

const (
	kindUnknown colKind = iota
	kindInt             // any integer width -> INT64
	kindFloat           // float/double -> DOUBLE
	kindTime            // date/timestamp -> TIMESTAMP_MILLIS, stored as epoch ms
	kindText            // char/text -> UTF8
)

// rowSource is the driver-independent view of one chunk's result set.
type rowSource interface {
	// Columns returns the column names and the declared kind of each column.
	Columns() (names []string, kinds []colKind)
	Next() bool
	Values() ([]interface{}, error)
	Err() error
}

// writeChunk drains src into Parquet files partitioned by partitionColumn,
// stores them and returns their manifest entries. Progress is reported every
// 100k rows and once at the end.
func writeChunk(ctx context.Context, src rowSource, table string, chunkNum int, partitionColumn string, store FileStore, progress *ProgressTracker) ([]FileEntry, error) {
	columns, kinds := src.Columns()
	partIdx := indexOf(columns, partitionColumn)

	var pw *partitionedWriter
	rowCount := 0
	for src.Next() {
		values, err := src.Values()
		if err != nil {
			pw.discard()
			return nil, fmt.Errorf("failed to read row: %w", err)
		}
		if pw == nil {
			pw = newPartitionedWriter(inferParquetSchema(columns, values, kinds))
		}
		if err := pw.write(partitionKey(valueAt(values, partIdx)), rowToMap(columns, values, kinds)); err != nil {
			pw.discard()
			return nil, err
		}
		rowCount++
		if rowCount%100_000 == 0 {
			progress.AddRows(100_000)
		}
	}
	if err := src.Err(); err != nil {
		pw.discard()
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	entries, err := storeChunk(ctx, store, pw, table, chunkNum, rowCount)
	if err != nil {
		return nil, err
	}
	if rem := rowCount % 100_000; rem > 0 {
		progress.AddRows(rem)
	}
	progress.ChunkDone()
	if debug {
		fmt.Printf("[worker %d] Done: %d rows (Parquet files written and stored)\n", chunkNum, rowCount)
	}
	return entries, nil
}

// sqlRowSource adapts database/sql rows (MySQL).
type sqlRowSource struct {
	rows    *sql.Rows
	columns []string
	kinds   []colKind
}

func newSQLRowSource(rows *sql.Rows) (*sqlRowSource, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	src := &sqlRowSource{rows: rows, columns: make([]string, len(types)), kinds: make([]colKind, len(types))}
	for i, t := range types {
		src.columns[i] = t.Name()
		src.kinds[i] = mysqlColKind(t.DatabaseTypeName())
	}
	return src, nil
}

// mysqlColKind maps a MySQL column type name (as the driver reports it, e.g.
// "INT", "UNSIGNED BIGINT", "DATETIME", "VARCHAR") to a colKind. Types with
// no direct Parquet mapping (DECIMAL, JSON, BLOB, BIT, TIME, YEAR) are
// inferred from their first value as before.
func mysqlColKind(dataType string) colKind {
	t := strings.TrimPrefix(strings.ToUpper(dataType), "UNSIGNED ")
	switch {
	case t == "TINYINT", t == "SMALLINT", t == "MEDIUMINT", t == "INT", t == "BIGINT":
		return kindInt
	case t == "FLOAT", t == "DOUBLE":
		return kindFloat
	case t == "DATE", t == "DATETIME", t == "TIMESTAMP":
		return kindTime
	case t == "CHAR", t == "VARCHAR", t == "ENUM", t == "SET", strings.HasSuffix(t, "TEXT"):
		return kindText
	}
	return kindUnknown
}

func (s *sqlRowSource) Columns() ([]string, []colKind) { return s.columns, s.kinds }
func (s *sqlRowSource) Next() bool                     { return s.rows.Next() }
func (s *sqlRowSource) Err() error                     { return s.rows.Err() }
func (s *sqlRowSource) Values() ([]interface{}, error) {
	values := make([]interface{}, len(s.columns))
	ptrs := make([]interface{}, len(s.columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := s.rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	return values, nil
}

// pgxRowSource adapts pgx rows (PostgreSQL).
type pgxRowSource struct{ rows pgx.Rows }

func (p *pgxRowSource) Columns() ([]string, []colKind) {
	return pgColumns(p.rows.FieldDescriptions())
}
func (p *pgxRowSource) Next() bool                     { return p.rows.Next() }
func (p *pgxRowSource) Values() ([]interface{}, error) { return p.rows.Values() }
func (p *pgxRowSource) Err() error                     { return p.rows.Err() }
