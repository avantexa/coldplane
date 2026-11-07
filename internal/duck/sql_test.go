package duck

import (
	"strings"
	"testing"
)

func TestMySQLAttachString(t *testing.T) {
	cases := map[string]string{
		"mysql://u:p@h:3307/shop?parseTime=true": "host=h port=3307 user=u password=p database=shop",
		"mysql://u@h/shop":                       "host=h port=3306 user=u database=shop",
		"u:p@tcp(h:3306)/shop?parseTime=true":    "host=h port=3306 user=u password=p database=shop",
		"u:p@tcp(10.0.0.5)/shop":                 "host=10.0.0.5 port=3306 user=u password=p database=shop",
		"u:p@/shop":                              "host=localhost port=3306 user=u password=p database=shop",
		"mysql://u:p%40ss@h/shop":                "host=h port=3306 user=u password=p@ss database=shop",
	}
	for in, want := range cases {
		got, err := mysqlAttachString(in)
		if err != nil || got != want {
			t.Errorf("mysqlAttachString(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := mysqlAttachString("not a dsn"); err == nil {
		t.Error("garbage should fail")
	}
}

func TestEngineOf(t *testing.T) {
	cases := map[string]Kind{"postgres://u@h/db": Postgres, "pg://u@h/db": Postgres, "mysql://u@h/db": MySQL, "u:p@tcp(h:3306)/db": MySQL}
	for dsn, want := range cases {
		if got, err := kindOf(dsn); err != nil || got != want {
			t.Errorf("kindOf(%q) = %v, %v; want %v", dsn, got, err, want)
		}
	}
	if _, err := kindOf("sqlite:///x"); err == nil {
		t.Error("unknown scheme should fail")
	}
}

func TestAttachSQL(t *testing.T) {
	got := attachSQL(Postgres, "postgres://u:p@h/db", "src", true)
	if got != `ATTACH 'postgres://u:p@h/db' AS src (TYPE postgres, READ_ONLY)` {
		t.Errorf("pg: %s", got)
	}
	got = attachSQL(MySQL, "host=h port=3306 user=u database=db", "dst", false)
	if got != `ATTACH 'host=h port=3306 user=u database=db' AS dst (TYPE mysql)` {
		t.Errorf("mysql: %s", got)
	}
	if got := attachSQL(Postgres, "postgres://u:it's@h/db", "src", true); !strings.Contains(got, `it''s`) {
		t.Errorf("quotes in the DSN must be escaped: %s", got)
	}
}

func TestRemoteQuerySQL(t *testing.T) {
	got := remoteQuerySQL(Postgres, "src", `SELECT * FROM "t" WHERE "ts" >= '2024-01-01'`)
	if got != `postgres_query('src', 'SELECT * FROM "t" WHERE "ts" >= ''2024-01-01''')` {
		t.Errorf("pg: %s", got)
	}
	got = remoteQuerySQL(MySQL, "src", "SELECT * FROM `t`")
	if got != "mysql_query('src', 'SELECT * FROM `t`')" {
		t.Errorf("mysql: %s", got)
	}
}

func TestCopyChunkSQL(t *testing.T) {
	native := `SELECT * FROM "orders" WHERE "id" >= 1 AND "id" < 6`
	got := CopyChunkSQL(Postgres, "src", native, "created_at", "/b/orders", "orders", 3)
	want := `COPY (SELECT *, coalesce(year("created_at"::TIMESTAMP)::VARCHAR, 'unknown') AS year, coalesce(lpad(month("created_at"::TIMESTAMP)::VARCHAR, 2, '0'), 'unknown') AS month FROM postgres_query('src', 'SELECT * FROM "orders" WHERE "id" >= 1 AND "id" < 6')) TO '/b/orders' (FORMAT parquet, PARTITION_BY (year, month), FILENAME_PATTERN 'orders-chunk-3-part-{i}', OVERWRITE_OR_IGNORE, COMPRESSION zstd)`
	if got != want {
		t.Errorf("partitioned:\n got  %s\n want %s", got, want)
	}
	got = CopyChunkSQL(MySQL, "src", "SELECT * FROM `plain` LIMIT 10 OFFSET 0", "", "/b/plain", "plain", 0)
	want = "COPY (SELECT * FROM mysql_query('src', 'SELECT * FROM `plain` LIMIT 10 OFFSET 0')) TO '/b/plain/year=unknown/month=unknown/plain-chunk-0-part-0.parquet' (FORMAT parquet, COMPRESSION zstd)"
	if got != want {
		t.Errorf("unpartitioned:\n got  %s\n want %s", got, want)
	}
	// A partition column whose values are NULL must still land somewhere readable.
	if got := CopyChunkSQL(Postgres, "src", native, "ts", "/b/t", "t", 0); !strings.Contains(got, "coalesce") {
		t.Errorf("NULL partition values should be coalesced to unknown: %s", got)
	}
}

func TestDescribeSQL(t *testing.T) {
	got := DescribeSQL(MySQL, "src", "orders")
	if got != "DESCRIBE SELECT * FROM mysql_query('src', 'SELECT * FROM `orders` WHERE 1=0')" {
		t.Errorf("%s", got)
	}
}

func TestConnectionString(t *testing.T) {
	if got, _ := connectionString(Postgres, "pg://u@h/db"); got != "postgres://u@h/db" {
		t.Errorf("pg:// should become postgres://, got %s", got)
	}
	if got, _ := connectionString(Postgres, "postgres://u@h/db"); got != "postgres://u@h/db" {
		t.Errorf("postgres:// passthrough, got %s", got)
	}
	if got, _ := connectionString(MySQL, "mysql://u@h/db"); got != "host=h port=3306 user=u database=db" {
		t.Errorf("mysql: %s", got)
	}
}
