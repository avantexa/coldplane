package archive

import (
	"github.com/jackc/pgx/v5/pgtype"
	"strings"
	"testing"
	"time"
)

func TestNormalizeMySQLDSN(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "non-mysql URI passes through unchanged",
			input:    "user:pass@tcp(host:3306)/db",
			expected: "user:pass@tcp(host:3306)/db",
		},
		{
			name:     "mysql:// URI with password",
			input:    "mysql://user:pass@host:3306/mydb",
			expected: "user:pass@tcp(host:3306)/mydb",
		},
		{
			name:     "mysql:// URI without password",
			input:    "mysql://user@host:3306/mydb",
			expected: "user@tcp(host:3306)/mydb",
		},
		{
			name:     "mysql:// URI with query params",
			input:    "mysql://user:pass@host:3306/mydb?charset=utf8",
			expected: "user:pass@tcp(host:3306)/mydb?charset=utf8",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeMySQLDSN(tt.input)
			if got != tt.expected {
				t.Errorf("NormalizeMySQLDSN(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

// --- inferParquetSchema ---

func TestInferParquetSchema(t *testing.T) {
	t.Run("int column", func(t *testing.T) {
		schema := inferParquetSchema([]string{"id"}, []interface{}{int64(1)}, nil)
		if !strings.Contains(schema, "type=INT64") {
			t.Errorf("expected INT64 for int64, got: %s", schema)
		}
		if !strings.Contains(schema, "name=id") {
			t.Errorf("expected name=id, got: %s", schema)
		}
	})

	t.Run("float column", func(t *testing.T) {
		schema := inferParquetSchema([]string{"amount"}, []interface{}{float64(3.14)}, nil)
		if !strings.Contains(schema, "type=DOUBLE") {
			t.Errorf("expected DOUBLE for float64, got: %s", schema)
		}
	})

	t.Run("string column", func(t *testing.T) {
		schema := inferParquetSchema([]string{"name"}, []interface{}{"alice"}, nil)
		if !strings.Contains(schema, "type=BYTE_ARRAY") {
			t.Errorf("expected BYTE_ARRAY for string, got: %s", schema)
		}
	})

	t.Run("time column", func(t *testing.T) {
		schema := inferParquetSchema([]string{"created_at"}, []interface{}{time.Now()}, nil)
		if !strings.Contains(schema, "TIMESTAMP_MILLIS") {
			t.Errorf("expected TIMESTAMP_MILLIS for time.Time, got: %s", schema)
		}
	})

	t.Run("declared kinds beat the sample value", func(t *testing.T) {
		cases := map[colKind]string{kindInt: "type=INT64", kindFloat: "type=DOUBLE", kindTime: "TIMESTAMP_MILLIS", kindText: "convertedtype=UTF8"}
		for kind, want := range cases {
			for _, sample := range []interface{}{nil, []byte("2025-01-01 00:00:00"), "x"} {
				schema := inferParquetSchema([]string{"c"}, []interface{}{sample}, []colKind{kind})
				if !strings.Contains(schema, want) {
					t.Errorf("kind %d sample %#v: expected %s, got %s", kind, sample, want, schema)
				}
			}
		}
		// Unknown kind with a NULL sample is the one case that still defaults to bytes.
		if schema := inferParquetSchema([]string{"c"}, []interface{}{nil}, []colKind{kindUnknown}); !strings.Contains(schema, "type=BYTE_ARRAY") {
			t.Errorf("unknown+nil: %s", schema)
		}
	})

	t.Run("bool column", func(t *testing.T) {
		schema := inferParquetSchema([]string{"active"}, []interface{}{true}, nil)
		// bool falls through to default → BYTE_ARRAY
		if !strings.Contains(schema, "type=BYTE_ARRAY") {
			t.Errorf("expected BYTE_ARRAY for bool, got: %s", schema)
		}
	})

	t.Run("multiple columns", func(t *testing.T) {
		schema := inferParquetSchema(
			[]string{"id", "name", "amount"},
			[]interface{}{int64(1), "alice", float64(9.99)},
			nil,
		)
		if !strings.Contains(schema, "name=id") {
			t.Error("missing id field")
		}
		if !strings.Contains(schema, "name=name") {
			t.Error("missing name field")
		}
		if !strings.Contains(schema, "name=amount") {
			t.Error("missing amount field")
		}
		if !strings.Contains(schema, `"Tag":"name=root"`) {
			t.Error("missing root tag")
		}
	})
}

// --- toParquetSafeValue ---

func TestToParquetSafeValue(t *testing.T) {
	t.Run("string passthrough", func(t *testing.T) {
		got := toParquetSafeValue("hello")
		if got != "hello" {
			t.Errorf("got %v, want 'hello'", got)
		}
	})

	t.Run("int passthrough", func(t *testing.T) {
		got := toParquetSafeValue(int(42))
		if got != int(42) {
			t.Errorf("got %v, want 42", got)
		}
	})

	t.Run("int64 passthrough", func(t *testing.T) {
		got := toParquetSafeValue(int64(100))
		if got != int64(100) {
			t.Errorf("got %v, want 100", got)
		}
	})

	t.Run("int8 promoted to int64", func(t *testing.T) {
		got := toParquetSafeValue(int8(8))
		if got != int64(8) {
			t.Errorf("got %v (%T), want int64(8)", got, got)
		}
	})

	t.Run("uint32 promoted to int64", func(t *testing.T) {
		got := toParquetSafeValue(uint32(32))
		if got != int64(32) {
			t.Errorf("got %v (%T), want int64(32)", got, got)
		}
	})

	t.Run("time.Time to UnixMilli", func(t *testing.T) {
		ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		got := toParquetSafeValue(ts)
		want := ts.UTC().UnixMilli()
		if got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("bool true", func(t *testing.T) {
		got := toParquetSafeValue(true)
		if got != "true" {
			t.Errorf("got %v, want 'true'", got)
		}
	})

	t.Run("bool false", func(t *testing.T) {
		got := toParquetSafeValue(false)
		if got != "false" {
			t.Errorf("got %v, want 'false'", got)
		}
	})

	t.Run("complex type marshalled to JSON", func(t *testing.T) {
		got := toParquetSafeValue(map[string]int{"a": 1})
		s, ok := got.(string)
		if !ok {
			t.Fatalf("expected string, got %T", got)
		}
		if !strings.Contains(s, `"a":1`) {
			t.Errorf("expected JSON with a:1, got %s", s)
		}
	})
}

// --- DefaultOptions ---

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()
	if opts.ChunkSize != 2_500_000 {
		t.Errorf("ChunkSize = %d, want 2500000", opts.ChunkSize)
	}
	if opts.Workers != 4 {
		t.Errorf("Workers = %d, want 4", opts.Workers)
	}
	if opts.OutputDir != "" {
		t.Errorf("OutputDir should default to empty, got %q", opts.OutputDir)
	}
	if opts.Where != "" {
		t.Errorf("Where should default to empty, got %q", opts.Where)
	}
}

func TestToParquetSafeValue_MoreTypes(t *testing.T) {
	cases := map[string]struct {
		in   interface{}
		want interface{}
	}{
		"int16":  {int16(3), int64(3)},
		"uint8":  {uint8(4), int64(4)},
		"uint16": {uint16(5), int64(5)},
		"uint64": {uint64(6), int64(6)},
		"uuid":   {[16]uint8{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}, "12345678-9abc-def0-1122-334455667788"},
		"map":    {map[string]int{"a": 1}, `{"a":1}`},
		"slice":  {[]int{1, 2}, `[1,2]`},
	}
	for name, c := range cases {
		if got := toParquetSafeValue(c.in); got != c.want {
			t.Errorf("%s: got %#v, want %#v", name, got, c.want)
		}
	}
	num := pgtype.Numeric{Valid: true}
	if got := toParquetSafeValue(num); got != num {
		t.Errorf("pgtype.Numeric should pass through, got %#v", got)
	}
	if got := toParquetSafeValue(make(chan int)); got == nil || got.(string) == "" {
		t.Errorf("unmarshalable value should fall back to fmt, got %#v", got)
	}
}
