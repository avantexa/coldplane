package archive

import "testing"

func TestMySQLQueries(t *testing.T) {
	cases := map[string][2]string{
		"range":        {mysqlRangeQuery("t", "", "id", "1", "6"), "SELECT * FROM `t` WHERE `id` >= 1 AND `id` < 6"},
		"range where":  {mysqlRangeQuery("t", "x > 0", "ts", "'a'", "'b'"), "SELECT * FROM `t` WHERE (x > 0) AND (`ts` >= 'a' AND `ts` < 'b')"},
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
