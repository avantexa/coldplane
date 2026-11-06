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

func TestMySQLColKind(t *testing.T) {
	cases := map[string]colKind{
		"INT": kindInt, "UNSIGNED BIGINT": kindInt, "tinyint": kindInt, "MEDIUMINT": kindInt,
		"FLOAT": kindFloat, "DOUBLE": kindFloat,
		"DATE": kindTime, "DATETIME": kindTime, "TIMESTAMP": kindTime,
		"VARCHAR": kindText, "CHAR": kindText, "TEXT": kindText, "LONGTEXT": kindText, "ENUM": kindText,
		"DECIMAL": kindUnknown, "JSON": kindUnknown, "BLOB": kindUnknown, "TIME": kindUnknown, "YEAR": kindUnknown, "BIT": kindUnknown, "": kindUnknown,
	}
	for in, want := range cases {
		if got := mysqlColKind(in); got != want {
			t.Errorf("mysqlColKind(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestPGColKind(t *testing.T) {
	cases := map[uint32]colKind{20: kindInt, 21: kindInt, 23: kindInt, 700: kindFloat, 701: kindFloat,
		1082: kindTime, 1114: kindTime, 1184: kindTime, 25: kindText, 1043: kindText, 1042: kindText,
		1700: kindUnknown, 16: kindUnknown, 2950: kindUnknown, 114: kindUnknown}
	for oid, want := range cases {
		if got := pgColKind(oid); got != want {
			t.Errorf("pgColKind(%d) = %d, want %d", oid, got, want)
		}
	}
}
