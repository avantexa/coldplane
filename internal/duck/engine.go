package duck

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

// debug makes attached databases log the SQL they receive; the CLI sets it from --verbose.
var debug bool

// SetDebug toggles logging of the SQL sent to attached databases.
func SetDebug(v bool) { debug = v }

// Engine is an in-memory DuckDB with the source database extensions available.
type Engine struct {
	db  *sql.DB
	out io.Writer
}

// Open starts an in-memory DuckDB. COLDPLANE_DUCKDB_EXTENSIONS, when set,
// names the directory extensions are installed to and loaded from, for
// machines that cannot download them.
func Open(ctx context.Context) (*Engine, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	e := &Engine{db: db, out: os.Stdout}
	if dir := os.Getenv("COLDPLANE_DUCKDB_EXTENSIONS"); dir != "" {
		if _, err := db.ExecContext(ctx, "SET extension_directory = "+literal(dir)); err != nil {
			db.Close()
			return nil, fmt.Errorf("set extension directory: %w", err)
		}
	}
	return e, nil
}

// DB exposes the DuckDB connection pool.
func (e *Engine) DB() *sql.DB { return e.db }

// Close releases the DuckDB instance.
func (e *Engine) Close() error { return e.db.Close() }

// Attach connects a Postgres or MySQL database under alias and returns its
// kind. The extension is installed on first use, with a notice.
func (e *Engine) Attach(ctx context.Context, dsn, alias string, readOnly bool) (Kind, error) {
	kind, err := kindOf(dsn)
	if err != nil {
		return 0, err
	}
	if err := e.ensureExtension(ctx, kind); err != nil {
		return 0, err
	}
	connStr, err := connectionString(kind, dsn)
	if err != nil {
		return 0, err
	}
	if _, err := e.db.ExecContext(ctx, attachSQL(kind, connStr, alias, readOnly)); err != nil {
		return 0, fmt.Errorf("attach %s database: %w", kind, err)
	}
	if debug {
		if _, err := e.db.ExecContext(ctx, fmt.Sprintf("SET %s_debug_show_queries = true", kind)); err != nil {
			return 0, fmt.Errorf("enable %s query logging: %w", kind, err)
		}
	}
	return kind, nil
}

// ensureExtension loads the extension for kind, installing it first (and
// announcing the download) when it is not on disk yet. An extension that is
// not installed does not show up in duckdb_extensions(), so LOAD is the probe.
func (e *Engine) ensureExtension(ctx context.Context, kind Kind) error {
	name := kind.String()
	if _, err := e.db.ExecContext(ctx, "LOAD "+name); err == nil {
		return nil
	}
	fmt.Fprintf(e.out, "Fetching the DuckDB %s extension into %s (one time, ~40 MB). Set COLDPLANE_DUCKDB_EXTENSIONS to use a pre-installed copy.\n", name, e.extensionDir(ctx))
	if _, err := e.db.ExecContext(ctx, "INSTALL "+name); err != nil {
		return fmt.Errorf("install duckdb %s extension: %w", name, err)
	}
	if _, err := e.db.ExecContext(ctx, "LOAD "+name); err != nil {
		return fmt.Errorf("load duckdb %s extension: %w", name, err)
	}
	return nil
}

// connectionString renders dsn the way the engine's DuckDB extension expects it.
func connectionString(kind Kind, dsn string) (string, error) {
	if kind == MySQL {
		return mysqlAttachString(dsn)
	}
	return "postgres://" + strings.TrimPrefix(strings.TrimPrefix(dsn, "pg://"), "postgres://"), nil
}

func (e *Engine) extensionDir(ctx context.Context) string {
	var dir string
	if err := e.db.QueryRowContext(ctx, "SELECT current_setting('extension_directory')").Scan(&dir); err != nil || dir == "" {
		return "~/.duckdb/extensions"
	}
	return dir
}
