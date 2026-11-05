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
