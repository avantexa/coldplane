package dbutil

import (
	"testing"
	"time"
)

func TestQuoteMySQL(t *testing.T) {
	cases := map[string]string{"orders": "`orders`", "we`ird": "`we``ird`", "": "``"}
	for in, want := range cases {
		if got := QuoteMySQL(in); got != want {
			t.Errorf("QuoteMySQL(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestQuotePG(t *testing.T) {
	cases := map[string]string{"orders": `"orders"`, `we"ird`: `"we""ird"`, "Mixed": `"Mixed"`}
	for in, want := range cases {
		if got := QuotePG(in); got != want {
			t.Errorf("QuotePG(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestParseTime(t *testing.T) {
	want := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	for _, in := range []string{"2025-06-15 10:30:00", "2025-06-15T10:30:00Z", "2025-06-15T10:30:00", "2025-06-15T10:30:00.000Z"} {
		got, err := ParseTime(in)
		if err != nil || !got.Equal(want) {
			t.Errorf("ParseTime(%q) = %v, %v", in, got, err)
		}
	}
	if got, err := ParseTime("2025-06-15"); err != nil || got.Day() != 15 {
		t.Errorf("date only: %v %v", got, err)
	}
	if got, err := ParseTime("2025-06-15T12:30:00+02:00"); err != nil || !got.Equal(want) {
		t.Errorf("offset should be honored: %v %v", got, err)
	}
	for _, bad := range []string{"", "yesterday", "15/06/2025"} {
		if _, err := ParseTime(bad); err == nil {
			t.Errorf("ParseTime(%q) should fail", bad)
		}
	}
}

func TestDBNameFromDSN(t *testing.T) {
	cases := map[string]string{
		"postgres://u:p@h:5432/shop?sslmode=disable": "shop",
		"u:p@tcp(h:3306)/shop?parseTime=true":        "shop",
		"u@/shop":                                    "shop",
		"u:p@tcp(h:3306)/":                           "",
		"nodb":                                       "nodb",
		"":                                           "",
	}
	for dsn, want := range cases {
		if got := DBNameFromDSN(dsn); got != want {
			t.Errorf("DBNameFromDSN(%q) = %q, want %q", dsn, got, want)
		}
	}
}
