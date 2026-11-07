package archive

import (
	"testing"
	"time"
)

func TestMySQLQueries(t *testing.T) {
	cases := map[string][2]string{
		"range":        {mysqlRangeQuery("t", "", "id", "1", "6"), "SELECT * FROM `t` WHERE `id` >= 1 AND `id` < 6"},
		"range where":  {mysqlRangeQuery("t", "x > 0", "ts", "'a'", "'b'"), "SELECT * FROM `t` WHERE (x > 0) AND (`ts` >= 'a' AND `ts` < 'b')"},
		"null":         {mysqlNullQuery("t", "", "ts"), "SELECT * FROM `t` WHERE `ts` IS NULL"},
		"null where":   {mysqlNullQuery("t", "x > 0", "ts"), "SELECT * FROM `t` WHERE (x > 0) AND `ts` IS NULL"},
		"offset":       {mysqlOffsetQuery("t", "", 10, 20), "SELECT * FROM `t` LIMIT 10 OFFSET 20"},
		"offset where": {mysqlOffsetQuery("t", "x > 0", 5, 0), "SELECT * FROM `t` WHERE x > 0 LIMIT 5 OFFSET 0"},
		"count":        {mysqlCountQuery("t", "x > 0"), "SELECT COUNT(*) FROM `t` WHERE x > 0"},
		"minmax":       {mysqlMinMaxQuery("t", "", "id"), "SELECT MIN(`id`), MAX(`id`) FROM `t`"},
		"minmax where": {mysqlMinMaxQuery("t", "x > 0", "id"), "SELECT MIN(`id`), MAX(`id`) FROM `t` WHERE x > 0"},
	}
	for name, c := range cases {
		if c[0] != c[1] {
			t.Errorf("%s:\n got  %s\n want %s", name, c[0], c[1])
		}
	}
}

func TestPGQueries(t *testing.T) {
	cases := map[string][2]string{
		"range":        {pgRangeQuery("t", "", "id", "1", "6"), `SELECT * FROM "t" WHERE "id" >= 1 AND "id" < 6`},
		"range where":  {pgRangeQuery("t", "x > 0", "ts", "'a'", "'b'"), `SELECT * FROM "t" WHERE (x > 0) AND ("ts" >= 'a' AND "ts" < 'b')`},
		"null where":   {pgNullQuery("t", "x > 0", "ts"), `SELECT * FROM "t" WHERE (x > 0) AND "ts" IS NULL`},
		"offset":       {pgOffsetQuery("t", "", 10, 20), `SELECT * FROM "t" LIMIT 10 OFFSET 20`},
		"offset where": {pgOffsetQuery("t", "x > 0", 5, 0), `SELECT * FROM "t" WHERE x > 0 LIMIT 5 OFFSET 0`},
		"count":        {pgCountQuery("t", ""), `SELECT COUNT(*) FROM "t"`},
		"minmax where": {pgMinMaxQuery("t", "x > 0", "ts"), `SELECT MIN("ts"), MAX("ts") FROM "t" WHERE x > 0`},
	}
	for name, c := range cases {
		if c[0] != c[1] {
			t.Errorf("%s:\n got  %s\n want %s", name, c[0], c[1])
		}
	}
}

func TestSQLString(t *testing.T) {
	if got := sqlString("2024-01-01 00:00:00"); got != "'2024-01-01 00:00:00'" {
		t.Errorf("plain: %s", got)
	}
	if got := sqlString("it's"); got != "'it''s'" {
		t.Errorf("quote: %s", got)
	}
}

func TestPGTimeLiteral(t *testing.T) {
	loc := time.FixedZone("minus4", -4*3600)
	ts := time.Date(2024, 11, 1, 0, 0, 0, 1000, loc) // 00:00:00.000001 at -04:00
	if got := pgTimeLiteral(ts, true); got != "'2024-11-01 00:00:00.000001-04:00'::timestamptz" {
		t.Errorf("with zone: %s", got)
	}
	// Naive columns compare wall-clock values; render the UTC wall clock without an offset.
	if got := pgTimeLiteral(ts, false); got != "'2024-11-01 04:00:00.000001'::timestamp" {
		t.Errorf("naive: %s", got)
	}
}
