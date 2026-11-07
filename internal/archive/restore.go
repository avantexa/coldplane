package archive

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/avantexa/coldplane/internal/duck"
)

// dstAlias is the name the target database is attached under in DuckDB.
const dstAlias = "dst"

// RestoreOptions selects what to restore and where.
type RestoreOptions struct {
	Tables []string // subset of the backup's tables; empty means all
	As     string   // target table name, allowed only when one table is restored
	Create bool     // create a missing target table from the archive's schema
}

// RestoreResult is one table's outcome of a restore.
type RestoreResult struct {
	Table  string // archived table
	Target string // table written in the target database
	Rows   int64
}

// Restore loads a backup's tables into the database behind targetDSN, which
// may be a different engine than the source: DuckDB attaches the target and
// inserts straight from the Parquet files, mapping types along the way.
func Restore(ctx context.Context, backupDir, targetDSN string, opts RestoreOptions) ([]RestoreResult, error) {
	m, err := ReadManifest(backupDir)
	if err != nil {
		return nil, err
	}
	selected, err := selectTables(m, opts)
	if err != nil {
		return nil, err
	}
	eng, err := duck.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer eng.Close()
	if _, err := eng.Attach(ctx, targetDSN, dstAlias, false); err != nil {
		return nil, err
	}

	var results []RestoreResult
	for _, t := range selected {
		target := t.Name
		if opts.As != "" {
			target = opts.As
		}
		rows, err := restoreTable(ctx, eng, backupDir, t, target, opts.Create)
		if err != nil {
			return results, fmt.Errorf("restore %s into %s: %w", t.Name, target, err)
		}
		results = append(results, RestoreResult{Table: t.Name, Target: target, Rows: rows})
	}
	return results, nil
}

// selectTables resolves opts.Tables against the manifest and validates --as.
func selectTables(m Manifest, opts RestoreOptions) ([]TableEntry, error) {
	if len(opts.Tables) == 0 {
		if opts.As != "" && len(m.Tables) != 1 {
			return nil, fmt.Errorf("--as needs a single table; the backup has %d, pick one with --table", len(m.Tables))
		}
		return m.Tables, nil
	}
	byName := map[string]TableEntry{}
	for _, t := range m.Tables {
		byName[t.Name] = t
	}
	var out []TableEntry
	for _, name := range opts.Tables {
		t, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("table %q is not in this backup (it has: %s)", name, strings.Join(m.TableNames(), ", "))
		}
		out = append(out, t)
	}
	if opts.As != "" && len(out) != 1 {
		return nil, fmt.Errorf("--as needs a single table, got %d", len(out))
	}
	return out, nil
}

// restoreTable creates the target table when allowed and inserts the archived rows.
func restoreTable(ctx context.Context, eng *duck.Engine, backupDir string, t TableEntry, target string, create bool) (int64, error) {
	if len(t.Files) == 0 {
		return 0, nil // nothing was archived for this table
	}
	source := duck.ParquetSQL(filepath.Join(backupDir, t.Name))
	var exists int
	if err := eng.DB().QueryRowContext(ctx, duck.TableExistsSQL(dstAlias, target)).Scan(&exists); err != nil {
		return 0, fmt.Errorf("check target table: %w", err)
	}
	if exists == 0 {
		if !create {
			return 0, fmt.Errorf("target table %q does not exist; create it (columns: %s) or pass --create", target, strings.Join(t.Columns, ", "))
		}
		if _, err := eng.DB().ExecContext(ctx, duck.CreateTableFromSQL(dstAlias, target, source)); err != nil {
			return 0, fmt.Errorf("create target table: %w", err)
		}
	}
	res, err := eng.DB().ExecContext(ctx, duck.InsertFromSQL(dstAlias, target, t.Columns, source))
	if err != nil {
		return 0, fmt.Errorf("insert rows: %w", err)
	}
	rows, _ := res.RowsAffected()
	return rows, nil
}
