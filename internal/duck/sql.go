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
	var user, pass, host, port, db string
	if strings.HasPrefix(dsn, "mysql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", fmt.Errorf("parse mysql url: %w", err)
		}
		user = u.User.Username()
		pass, _ = u.User.Password()
		host, port = u.Hostname(), u.Port()
		db = strings.TrimPrefix(u.Path, "/")
	} else {
		m := goMySQLDSN.FindStringSubmatch(dsn)
		if m == nil {
			return "", fmt.Errorf("unrecognized MySQL DSN %q", dsn)
		}
		user, pass, db = m[1], m[2], m[5]
		if addr := m[4]; addr != "" {
			host, port, _ = strings.Cut(addr, ":")
		}
	}
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "3306"
	}
	parts := []string{"host=" + host, "port=" + port}
	if user != "" {
		parts = append(parts, "user="+user)
	}
	if pass != "" {
		parts = append(parts, "password="+pass)
	}
	if db != "" {
		parts = append(parts, "database="+db)
	}
	return strings.Join(parts, " "), nil
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
