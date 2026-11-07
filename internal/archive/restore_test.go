package archive

import (
	"strings"
	"testing"
)

func TestSelectTables(t *testing.T) {
	m := Manifest{Tables: []TableEntry{{Name: "a"}, {Name: "b"}}}
	if got, err := selectTables(m, RestoreOptions{}); err != nil || len(got) != 2 {
		t.Errorf("all: %v %v", got, err)
	}
	if got, err := selectTables(m, RestoreOptions{Tables: []string{"b"}}); err != nil || len(got) != 1 || got[0].Name != "b" {
		t.Errorf("subset: %v %v", got, err)
	}
	if _, err := selectTables(m, RestoreOptions{Tables: []string{"zzz"}}); err == nil || !strings.Contains(err.Error(), "a, b") {
		t.Errorf("unknown table should list the available ones: %v", err)
	}
	if _, err := selectTables(m, RestoreOptions{As: "x"}); err == nil {
		t.Error("--as with two tables must be refused")
	}
	if got, err := selectTables(m, RestoreOptions{Tables: []string{"a"}, As: "x"}); err != nil || len(got) != 1 {
		t.Errorf("--as with one table: %v %v", got, err)
	}
	one := Manifest{Tables: []TableEntry{{Name: "only"}}}
	if got, err := selectTables(one, RestoreOptions{As: "x"}); err != nil || len(got) != 1 {
		t.Errorf("--as with a single-table backup: %v %v", got, err)
	}
}
