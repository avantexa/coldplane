package archive

import "testing"

func TestIsMySQLIntegerType(t *testing.T) {
	for _, ok := range []string{"int", "INT", "bigint", "mediumint", "smallint", "tinyint"} {
		if !isMySQLIntegerType(ok) {
			t.Errorf("%s should be an integer type", ok)
		}
	}
	for _, no := range []string{"varchar", "decimal", "datetime", "float", "", "integer"} {
		if isMySQLIntegerType(no) {
			t.Errorf("%s should not be an integer type", no)
		}
	}
}

func TestIsMySQLTimeType(t *testing.T) {
	for _, yes := range []string{"DATE", "DATETIME", "TIMESTAMP", "datetime"} {
		if !isMySQLTimeType(yes) {
			t.Errorf("%s should be a time type", yes)
		}
	}
	for _, no := range []string{"TIME", "YEAR", "VARCHAR", "INT", ""} {
		if isMySQLTimeType(no) {
			t.Errorf("%s should not be a time type", no)
		}
	}
}
