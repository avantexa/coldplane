package archive

import (
	"path/filepath"
	"testing"
	"time"
)

func TestPlanIDChunks_Edges(t *testing.T) {
	if got := planIDChunks(1, 10, 1); len(got) != 10 || got[9] != (idRange{10, 11}) {
		t.Errorf("chunk size 1: %v", got)
	}
	if got := planIDChunks(1, 10, 0); got != nil {
		t.Errorf("chunk size 0 must plan nothing, got %v", got)
	}
}

func TestTimeWindow_Boundary(t *testing.T) {
	day := 24 * time.Hour
	if w := timeWindow(119*30*day, 30*day, 120); w != 30*day {
		t.Errorf("119 windows fit: got %s", w)
	}
	total := 120*30*day + time.Hour
	if w := timeWindow(total, 30*day, 120); w != total/120 {
		t.Errorf("121 windows must widen: got %s, want %s", w, total/120)
	}
}

func TestPlanTimeChunks_ZeroWindowUsesDefault(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := planTimeChunks(t0, t0.Add(70*24*time.Hour), 0, time.Second); len(got) != 3 {
		t.Errorf("zero window should fall back to 30 days: %d chunks", len(got))
	}
}

func TestPlanOffsetChunks_Edges(t *testing.T) {
	if got := planOffsetChunks(10, 5); len(got) != 2 {
		t.Errorf("exact multiple: %v", got)
	}
	if got := planOffsetChunks(1, 1); len(got) != 1 {
		t.Errorf("single row: %v", got)
	}
	if got := planOffsetChunks(5, 0); got != nil {
		t.Errorf("chunk size 0 must plan nothing, got %v", got)
	}
}

func TestMoveByCopy_MissingSourceReturnsZero(t *testing.T) {
	n, err := moveByCopy(filepath.Join(t.TempDir(), "nope"), filepath.Join(t.TempDir(), "dst"))
	if err == nil || n != 0 {
		t.Errorf("got %d, %v", n, err)
	}
}

func TestTimeColumns_RowLongerThanColumns(t *testing.T) {
	cols := []string{"id"}
	row := []interface{}{int64(1), time.Now()}
	if got := firstTimeColumn(cols, row); got != "" {
		t.Errorf("first: %q", got)
	}
	if got := lastTimeColumn(cols, row); got != "" {
		t.Errorf("last: %q", got)
	}
}

func TestValueAtAndIndexOf(t *testing.T) {
	vals := []interface{}{"a", "b"}
	if got := valueAt(vals, len(vals)); got != nil {
		t.Errorf("index == len must be nil, got %v", got)
	}
	cols := []string{"id", "ts"}
	if indexOf(cols, "ts") != 1 || indexOf(cols, "nope") != -1 {
		t.Errorf("indexOf: %d %d", indexOf(cols, "ts"), indexOf(cols, "nope"))
	}
}

func TestEpochMillis(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	if ms, ok := epochMillis(ts); !ok || ms != ts.UnixMilli() {
		t.Errorf("time: %d %v", ms, ok)
	}
	if ms, ok := epochMillis("2025-01-01 00:00:00"); !ok || ms != ts.UnixMilli() {
		t.Errorf("string: %d %v", ms, ok)
	}
	for _, v := range []interface{}{"junk", 42, nil, []byte("x")} {
		if ms, ok := epochMillis(v); ok || ms != 0 {
			t.Errorf("%#v: %d %v", v, ms, ok)
		}
	}
}

func TestExtractDBNameFromMySQLDSN(t *testing.T) {
	cases := map[string]string{
		"u:p@tcp(h:3306)/shop?parseTime=true": "shop",
		"u@/shop":                             "shop",
		"u@/shop?x=1&y=2":                     "shop",
		"u:p@tcp(h:3306)/":                    "",
		"nodb":                                "",
		"":                                    "",
	}
	for dsn, want := range cases {
		if got := extractDBNameFromMySQLDSN(dsn); got != want {
			t.Errorf("extractDBNameFromMySQLDSN(%q) = %q, want %q", dsn, got, want)
		}
	}
}
