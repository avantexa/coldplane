package archive

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// rowSource is the driver-independent view of one chunk's result set.
type rowSource interface {
	// Columns returns the column names and the positions of date/timestamp
	// columns that must be stored as epoch milliseconds.
	Columns() (names []string, timestampCols map[int]bool)
	Next() bool
	Values() ([]interface{}, error)
	Err() error
}

// writeChunk drains src into Parquet files partitioned by partitionColumn,
// stores them and returns their manifest entries. Progress is reported every
// 100k rows and once at the end.
func writeChunk(ctx context.Context, src rowSource, table string, chunkNum int, partitionColumn string, store FileStore, progress *ProgressTracker) ([]FileEntry, error) {
	columns, timestampCols := src.Columns()
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
			pw = newPartitionedWriter(inferParquetSchema(columns, values))
		}
		if err := pw.write(partitionKey(valueAt(values, partIdx)), rowToMap(columns, values, timestampCols)); err != nil {
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
}

func newSQLRowSource(rows *sql.Rows) (*sqlRowSource, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	return &sqlRowSource{rows: rows, columns: columns}, nil
}

func (s *sqlRowSource) Columns() ([]string, map[int]bool) { return s.columns, nil }
func (s *sqlRowSource) Next() bool                        { return s.rows.Next() }
func (s *sqlRowSource) Err() error                        { return s.rows.Err() }
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

func (p *pgxRowSource) Columns() ([]string, map[int]bool) {
	return pgColumns(p.rows.FieldDescriptions())
}
func (p *pgxRowSource) Next() bool                     { return p.rows.Next() }
func (p *pgxRowSource) Values() ([]interface{}, error) { return p.rows.Values() }
func (p *pgxRowSource) Err() error                     { return p.rows.Err() }
