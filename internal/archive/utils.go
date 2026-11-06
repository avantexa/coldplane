package archive

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// debug enables verbose diagnostics; the CLI sets it from --verbose.
var debug bool

// SetDebug toggles debug output.
func SetDebug(v bool) { debug = v }

// quoteMySQLIdentifier wraps a MySQL identifier (table/column name) in backticks,
// escaping any internal backticks. Use this instead of raw string interpolation.
func quoteMySQLIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// quotePGIdentifier wraps a PostgreSQL identifier (table/column name) in double quotes,
// escaping any internal double quotes. Use this instead of raw string interpolation.
func quotePGIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// ProgressTracker tracks progress for table archiving.
type ProgressTracker struct {
	Table       string
	TotalRows   int64
	DoneRows    int64
	TotalChunks int
	DoneChunks  int
	mu          sync.Mutex
}

func (p *ProgressTracker) AddRows(n int) {
	p.mu.Lock()
	p.DoneRows += int64(n)
	fmt.Printf("[%s] Progress: %d/%s rows, %d/%d chunks\n",
		p.Table,
		p.DoneRows,
		func() string {
			if p.TotalRows == 0 {
				return "?"
			}
			return fmt.Sprintf("%d", p.TotalRows)
		}(),
		p.DoneChunks,
		p.TotalChunks,
	)
	p.mu.Unlock()
}

func (p *ProgressTracker) ChunkDone() {
	p.mu.Lock()
	p.DoneChunks++
	// Do not print rows again here — row logs are handled in AddRows
	fmt.Printf("[%s] Progress: chunks %d/%d complete\n",
		p.Table, p.DoneChunks, p.TotalChunks)
	p.mu.Unlock()
}

// NormalizeMySQLDSN transforms mysql:// URIs to Go MySQL driver format
func NormalizeMySQLDSN(input string) string {
	if !strings.HasPrefix(input, "mysql://") {
		return input
	}

	parsed, err := url.Parse(input)
	if err != nil {
		return input // fallback
	}

	user := ""
	pass := ""
	if parsed.User != nil {
		user = parsed.User.Username()
		pass, _ = parsed.User.Password()
	}

	host := parsed.Host
	db := strings.TrimPrefix(parsed.Path, "/")
	params := parsed.RawQuery

	// Compose the DSN: user:pass@tcp(host:port)/dbname?params
	auth := user
	if pass != "" {
		auth += ":" + pass
	}
	dsn := fmt.Sprintf("%s@tcp(%s)/%s", auth, host, db)
	if params != "" {
		dsn += "?" + params
	}
	return dsn
}

func inferParquetSchema(fieldNames []string, sampleRow []interface{}) string {
	tags := []string{}

	for i, name := range fieldNames {
		var tag string
		val := sampleRow[i]

		// All columns use repetitiontype=OPTIONAL so parquet-go handles
		// NULL values (missing keys in the JSON row) natively instead of
		// panicking with "interface {} is nil, not string".
		switch val.(type) {
		case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
			tag = fmt.Sprintf(`{"Tag":"name=%s, type=INT64, repetitiontype=OPTIONAL"}`, name)

		case float32, float64:
			tag = fmt.Sprintf(`{"Tag":"name=%s, type=DOUBLE, repetitiontype=OPTIONAL"}`, name)

		case time.Time:
			tag = fmt.Sprintf(`{"Tag":"name=%s, type=INT64, convertedtype=TIMESTAMP_MILLIS, repetitiontype=OPTIONAL"}`, name)

		case pgtype.Numeric:
			tag = fmt.Sprintf(`{"Tag":"name=%s, type=FIXED_LEN_BYTE_ARRAY, length=32, convertedtype=DECIMAL, scale=18, precision=38, repetitiontype=OPTIONAL"}`, name)

		case []byte, string:
			tag = fmt.Sprintf(`{"Tag":"name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"}`, name)

		default:
			fmt.Printf("[infer] Unknown type for field '%s': %T — defaulting to BYTE_ARRAY\n", name, val)
			tag = fmt.Sprintf(`{"Tag":"name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"}`, name)
		}

		tags = append(tags, tag)
	}

	schema := fmt.Sprintf(`{"Tag":"name=root","Fields":[%s]}`, strings.Join(tags, ","))
	return schema
}

func ExtractDBName(dbURL string) string {
	parts := strings.Split(dbURL, "/") // Split always yields at least one part
	return strings.Split(parts[len(parts)-1], "?")[0]
}

// toParquetSafeValue converts Go values into types that the parquet-go JSON
// writer can handle. Primitive types (int*, float*, string, nil) are passed
// through unchanged because the parquet writer maps them to INT64, DOUBLE, or
// UTF8 natively. Every other type (bool, UUID byte arrays, maps, slices,
// network types, pgtype structs, etc.) is serialised to a JSON/text string so
// the writer can store it in a BYTE_ARRAY/UTF8 column without producing empty
// pages (which would cause a panic in parquet-go's PagesToChunk).
func toParquetSafeValue(val interface{}) interface{} {
	switch v := val.(type) {
	case string, int, int32, int64, float32, float64:
		return v
	case int8, int16, uint8, uint16, uint32, uint64:
		// Narrow and unsigned integers widen to INT64.
		return reflect.ValueOf(v).Convert(reflect.TypeOf(int64(0))).Int()
	case time.Time:
		// Convert to Unix millis for INT64/TIMESTAMP_MILLIS parquet columns.
		// PostgreSQL timestamps are already converted before reaching here,
		// but MySQL timestamps (with parseTime=true) come through as time.Time.
		return v.UTC().UnixMilli()
	case bool:
		if v {
			return "true"
		}
		return "false"
	case [16]uint8:
		// UUID — format as standard hex-hyphenated string
		return fmt.Sprintf("%x-%x-%x-%x-%x", v[0:4], v[4:6], v[6:8], v[8:10], v[10:16])
	case pgtype.Numeric:
		// Numeric is handled by the parquet DECIMAL schema; pass through
		return v
	default:
		// Complex types (maps, slices, structs, net types, etc.) → JSON string
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}
