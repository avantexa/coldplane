package duck

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/avantexa/coldplane/internal/dbutil"
)

// Kind is a source database engine DuckDB can attach.
type Kind int

const (
	Postgres Kind = iota + 1
	MySQL
)

func (k Kind) String() string {
	switch k {
	case Postgres:
		return "postgres"
	case MySQL:
		return "mysql"
	}
	return "unknown"
}

// kindOf tells the engine from a connection string: postgres:// or pg:// URLs
// are Postgres; mysql:// URLs and Go MySQL DSNs (user:pass@tcp(host)/db) are MySQL.
func kindOf(dsn string) (Kind, error) {
	switch {
	case strings.HasPrefix(dsn, "postgres://"), strings.HasPrefix(dsn, "postgresql://"), strings.HasPrefix(dsn, "pg://"):
		return Postgres, nil
	case strings.HasPrefix(dsn, "mysql://"), goMySQLDSN.MatchString(dsn):
		return MySQL, nil
	}
	return 0, fmt.Errorf("cannot tell the database engine from %q: use postgres://, pg://, mysql:// or a Go MySQL DSN", dsn)
}

// goMySQLDSN matches [user[:pass]@][protocol[(addr)]]/dbname[?params].
var goMySQLDSN = regexp.MustCompile(`^(?:([^:@/]+)(?::([^@/]*))?@)?(?:(\w+)(?:\(([^)]*)\))?)?/([^?]*)(?:\?.*)?$`)

// mysqlAttachString converts a MySQL connection string, URL or Go DSN, into
// the key=value form the DuckDB mysql extension attaches.
func mysqlAttachString(dsn string) (string, error) {
	c, err := parseMySQLDSN(dsn)
	if err != nil {
		return "", err
	}
	if c.host == "" {
		c.host = "localhost"
	}
	if c.port == "" {
		c.port = "3306"
	}
	parts := []string{"host=" + c.host, "port=" + c.port}
	for _, kv := range []struct{ k, v string }{{"user", c.user}, {"password", c.pass}, {"database", c.db}} {
		if kv.v != "" {
			parts = append(parts, kv.k+"="+kv.v)
		}
	}
	return strings.Join(parts, " "), nil
}

// mysqlConn is a MySQL connection string taken apart.
type mysqlConn struct{ user, pass, host, port, db string }

// parseMySQLDSN accepts mysql://user:pass@host:port/db?params and the Go
// driver form user:pass@tcp(host:port)/db?params.
func parseMySQLDSN(dsn string) (mysqlConn, error) {
	if strings.HasPrefix(dsn, "mysql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return mysqlConn{}, fmt.Errorf("parse mysql url: %w", err)
		}
		pass, _ := u.User.Password()
		return mysqlConn{user: u.User.Username(), pass: pass, host: u.Hostname(), port: u.Port(), db: strings.TrimPrefix(u.Path, "/")}, nil
	}
	m := goMySQLDSN.FindStringSubmatch(dsn)
	if m == nil {
		return mysqlConn{}, fmt.Errorf("unrecognized MySQL DSN %q", dsn)
	}
	c := mysqlConn{user: m[1], pass: m[2], db: m[5]}
	if addr := m[4]; addr != "" {
		c.host, c.port, _ = strings.Cut(addr, ":")
	}
	return c, nil
}

// literal renders s as a DuckDB single-quoted string literal.
func literal(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// attachSQL attaches a database under alias, read-only when asked.
func attachSQL(kind Kind, connStr, alias string, readOnly bool) string {
	opts := "TYPE " + kind.String()
	if readOnly {
		opts += ", READ_ONLY"
	}
	return fmt.Sprintf("ATTACH %s AS %s (%s)", literal(connStr), alias, opts)
}

// remoteQuerySQL runs native SQL on the attached database and exposes the
// result as a DuckDB table function, so filtering happens on the server in
// the engine's own dialect.
func remoteQuerySQL(kind Kind, alias, nativeSQL string) string {
	return fmt.Sprintf("%s_query(%s, %s)", kind, literal(alias), literal(nativeSQL))
}

// CopyChunkSQL writes the rows of one native chunk query as Parquet under
// outDir, the table's directory. With a partition column the files land in
// year=YYYY/month=MM subdirectories, named <table>-chunk-<n>-part-<i>.parquet;
// without one they land in year=unknown/month=unknown.
func CopyChunkSQL(kind Kind, alias, nativeSQL, partitionColumn, outDir, table string, chunkNum int) string {
	source := "SELECT * FROM " + remoteQuerySQL(kind, alias, nativeSQL)
	if partitionColumn == "" {
		file := fmt.Sprintf("%s/year=unknown/month=unknown/%s-chunk-%d-part-0.parquet", outDir, table, chunkNum)
		return fmt.Sprintf("COPY (%s) TO %s (FORMAT parquet, COMPRESSION zstd)", source, literal(file))
	}
	// DuckDB quotes identifiers like Postgres. The cast to TIMESTAMP lets
	// year()/month() work on TIMESTAMPTZ and DATE columns without the ICU extension.
	pc := dbutil.QuotePG(partitionColumn) + "::TIMESTAMP"
	partitioned := fmt.Sprintf(
		"SELECT *, coalesce(year(%s)::VARCHAR, 'unknown') AS year, coalesce(lpad(month(%s)::VARCHAR, 2, '0'), 'unknown') AS month FROM %s",
		pc, pc, remoteQuerySQL(kind, alias, nativeSQL))
	pattern := fmt.Sprintf("%s-chunk-%d-part-{i}", table, chunkNum)
	return fmt.Sprintf("COPY (%s) TO %s (FORMAT parquet, PARTITION_BY (year, month), FILENAME_PATTERN %s, OVERWRITE_OR_IGNORE, COMPRESSION zstd)",
		partitioned, literal(outDir), literal(pattern))
}

// DescribeSQL yields the column names and DuckDB types of a source table.
func DescribeSQL(kind Kind, alias, table string) string {
	quoted := dbutil.QuotePG(table)
	if kind == MySQL {
		quoted = dbutil.QuoteMySQL(table)
	}
	return "DESCRIBE SELECT * FROM " + remoteQuerySQL(kind, alias, "SELECT * FROM "+quoted+" WHERE 1=0")
}

// RemoteSQL is the table function that runs native SQL on the attached
// database: use it as a FROM source.
func RemoteSQL(kind Kind, alias, nativeSQL string) string {
	return remoteQuerySQL(kind, alias, nativeSQL)
}

// ParquetSQL reads every Parquet file under a table's directory as a FROM
// source, ignoring the year=/month= directory names.
func ParquetSQL(tableDir string) string {
	return fmt.Sprintf("read_parquet(%s, hive_partitioning=false)", literal(tableDir+"/**/*.parquet"))
}

// RowChecksumSQL counts the rows of source and sums a hash of the listed
// columns per row, which is order independent and counts duplicates. With no
// column list the whole row is hashed.
func RowChecksumSQL(source string, columns []string) string {
	if len(columns) == 0 {
		return fmt.Sprintf("SELECT count(*), coalesce(sum(hash(t)::HUGEINT), 0)::VARCHAR FROM %s t", source)
	}
	quoted := make([]string, len(columns))
	for i, c := range columns {
		quoted[i] = dbutil.QuotePG(c)
	}
	return fmt.Sprintf("SELECT count(*), coalesce(sum(hash(ROW(%s))::HUGEINT), 0)::VARCHAR FROM %s", strings.Join(quoted, ", "), source)
}

// qualified names a table inside an attached catalog.
func qualified(alias, table string) string {
	return alias + "." + dbutil.QuotePG(table)
}

// CreateTableFromSQL creates an empty table in the attached catalog with the
// columns and types of source.
func CreateTableFromSQL(alias, table, source string) string {
	return fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s WHERE 1=0", qualified(alias, table), source)
}

// InsertFromSQL copies source into the attached table, by column name when
// the column list is known so the target's column order does not matter.
func InsertFromSQL(alias, table string, columns []string, source string) string {
	if len(columns) == 0 {
		return fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", qualified(alias, table), source)
	}
	quoted := make([]string, len(columns))
	for i, c := range columns {
		quoted[i] = dbutil.QuotePG(c)
	}
	cols := strings.Join(quoted, ", ")
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", qualified(alias, table), cols, cols, source)
}

// TableExistsSQL counts the attached catalog's tables with that name.
func TableExistsSQL(alias, table string) string {
	return fmt.Sprintf("SELECT count(*) FROM information_schema.tables WHERE table_catalog = %s AND table_name = %s", literal(alias), literal(table))
}
