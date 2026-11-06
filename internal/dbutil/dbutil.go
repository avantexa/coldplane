// Package dbutil holds the few SQL helpers both engines and both features
// (archive, analyze) share, so each rule lives in one place.
package dbutil

import (
	"fmt"
	"strings"
	"time"
)

// QuoteMySQL wraps an identifier in backticks, escaping embedded backticks.
func QuoteMySQL(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// QuotePG wraps an identifier in double quotes, escaping embedded quotes.
func QuotePG(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// timeLayouts are the textual timestamp forms MySQL and PostgreSQL emit:
// space-separated, RFC 3339 (with or without an offset), and date only.
var timeLayouts = []string{
	"2006-01-02 15:04:05",
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02",
}

// ParseTime parses a timestamp in any of the layouts the engines emit as text.
func ParseTime(s string) (time.Time, error) {
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("could not parse time: %q", s)
}

// DBNameFromDSN returns the database name of a connection string: the last
// path segment, without query parameters. Works for postgres://host/db?x and
// for the Go MySQL form user:pass@tcp(host)/db?x. Empty when there is none.
func DBNameFromDSN(dsn string) string {
	parts := strings.Split(dsn, "/") // Split always yields at least one part
	return strings.Split(parts[len(parts)-1], "?")[0]
}
