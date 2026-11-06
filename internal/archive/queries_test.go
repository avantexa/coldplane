package archive

import "testing"

func TestMySQLQueries(t *testing.T) {
	cases := map[string][2]string{
		"range":        {mysqlRangeQuery("t", "", "id"), "SELECT * FROM `t` WHERE `id` >= ? AND `id` < ?"},
		"range where":  {mysqlRangeQuery("t", "x > 0", "ts"), "SELECT * FROM `t` WHERE (x > 0) AND (`ts` >= ? AND `ts` < ?)"},
		"offset":       {mysqlOffsetQuery("t", ""), "SELECT * FROM `t` LIMIT ? OFFSET ?"},
		"offset where": {mysqlOffsetQuery("t", "x > 0"), "SELECT * FROM `t` WHERE x > 0 LIMIT ? OFFSET ?"},
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
		"range":        {pgRangeQuery("t", "", "id"), `SELECT * FROM "t" WHERE "id" >= $1 AND "id" < $2`},
		"range where":  {pgRangeQuery("t", "x > 0", "ts"), `SELECT * FROM "t" WHERE (x > 0) AND ("ts" >= $1 AND "ts" < $2)`},
		"offset":       {pgOffsetQuery("t", ""), `SELECT * FROM "t" LIMIT $1 OFFSET $2`},
		"offset where": {pgOffsetQuery("t", "x > 0"), `SELECT * FROM "t" WHERE x > 0 LIMIT $1 OFFSET $2`},
		"count":        {pgCountQuery("t", ""), `SELECT COUNT(*) FROM "t"`},
		"minmax where": {pgMinMaxQuery("t", "x > 0", "ts"), `SELECT MIN("ts"), MAX("ts") FROM "t" WHERE x > 0`},
	}
	for name, c := range cases {
		if c[0] != c[1] {
			t.Errorf("%s:\n got  %s\n want %s", name, c[0], c[1])
		}
	}
}
