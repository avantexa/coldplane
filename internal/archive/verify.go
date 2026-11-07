package archive

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/avantexa/coldplane/internal/dbutil"
	"github.com/avantexa/coldplane/internal/duck"
)

// Verification outcomes recorded per table.
const (
	verifyPass             = "pass"
	verifyCountMismatch    = "count_mismatch"
	verifyChecksumMismatch = "checksum_mismatch"
	verifyError            = "error"
)

// TableVerification is the result of comparing one archived table with its source.
type TableVerification struct {
	At              string `json:"at"`
	SourceRows      int64  `json:"source_rows"`
	ArchiveRows     int64  `json:"archive_rows"`
	SourceChecksum  string `json:"source_checksum,omitempty"`
	ArchiveChecksum string `json:"archive_checksum,omitempty"`
	Status          string `json:"status"`
	Error           string `json:"error,omitempty"`
}

// VerifyOptions tunes a verification run.
type VerifyOptions struct {
	Checksum bool // compare a per-row hash as well as the row counts
}

// Verify compares every table of the backup in backupDir with the source
// database behind dsn, using the backup's own row filter, records the
// outcome per table in index.json and marks the backup verified when every
// table passes. The returned manifest carries the results.
func Verify(ctx context.Context, backupDir, dsn string, opts VerifyOptions) (Manifest, error) {
	m, err := ReadManifest(backupDir)
	if err != nil {
		return Manifest{}, err
	}
	eng, err := duck.Open(ctx)
	if err != nil {
		return Manifest{}, err
	}
	defer eng.Close()
	kind, err := eng.Attach(ctx, dsn, srcAlias, true)
	if err != nil {
		return Manifest{}, err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	for i := range m.Tables {
		v := verifyTable(ctx, eng, kind, m, m.Tables[i], backupDir, opts)
		v.At = now
		m.Tables[i].Verified = &v
	}
	m.VerifiedAt = ""
	if allVerified(m.Tables) {
		m.VerifiedAt = now
	}
	if err := writeManifest(backupDir, m); err != nil {
		return m, fmt.Errorf("record verification: %w", err)
	}
	return m, nil
}

func verifyTable(ctx context.Context, eng *duck.Engine, kind duck.Kind, m Manifest, t TableEntry, backupDir string, opts VerifyOptions) TableVerification {
	native := selectColumnsSQL(m.DBType, t.Name, t.Columns, m.whereFor(t))
	srcRows, srcSum, err := countAndChecksum(ctx, eng, duck.RemoteSQL(kind, srcAlias, native), t.Columns, opts.Checksum)
	if err != nil {
		return TableVerification{Status: verifyError, Error: "source: " + err.Error()}
	}
	var arcRows int64
	arcSum := "0"
	if len(t.Files) > 0 {
		arcRows, arcSum, err = countAndChecksum(ctx, eng, duck.ParquetSQL(filepath.Join(backupDir, t.Name)), t.Columns, opts.Checksum)
		if err != nil {
			return TableVerification{SourceRows: srcRows, Status: verifyError, Error: "archive: " + err.Error()}
		}
	}
	return TableVerification{
		SourceRows: srcRows, ArchiveRows: arcRows,
		SourceChecksum: srcSum, ArchiveChecksum: arcSum,
		Status: judgeVerification(srcRows, arcRows, srcSum, arcSum),
	}
}

// countAndChecksum runs the count (and, when asked, the row hash sum) over a DuckDB FROM source.
func countAndChecksum(ctx context.Context, eng *duck.Engine, source string, columns []string, checksum bool) (int64, string, error) {
	q := fmt.Sprintf("SELECT count(*), '' FROM %s", source)
	if checksum {
		q = duck.RowChecksumSQL(source, columns)
	}
	var rows int64
	var sum string
	if err := eng.DB().QueryRowContext(ctx, q).Scan(&rows, &sum); err != nil {
		return 0, "", err
	}
	return rows, sum, nil
}

// selectColumnsSQL is the native SELECT of the recorded columns with the backup's filter.
func selectColumnsSQL(dbType, table string, columns []string, where string) string {
	quote := dbutil.QuotePG
	if dbType == "mysql" {
		quote = dbutil.QuoteMySQL
	}
	cols := "*"
	if len(columns) > 0 {
		quoted := make([]string, len(columns))
		for i, c := range columns {
			quoted[i] = quote(c)
		}
		cols = strings.Join(quoted, ", ")
	}
	q := fmt.Sprintf("SELECT %s FROM %s", cols, quote(table))
	if where != "" {
		q += " WHERE " + where
	}
	return q
}

// judgeVerification decides a table's status: counts first, then checksums.
func judgeVerification(srcRows, arcRows int64, srcSum, arcSum string) string {
	switch {
	case srcRows != arcRows:
		return verifyCountMismatch
	case srcSum != arcSum:
		return verifyChecksumMismatch
	}
	return verifyPass
}

// allVerified reports whether every table (and at least one) passed.
func allVerified(tables []TableEntry) bool {
	if len(tables) == 0 {
		return false
	}
	for _, t := range tables {
		if t.Verified == nil || t.Verified.Status != verifyPass {
			return false
		}
	}
	return true
}

// verificationState summarizes a manifest for listings: yes, partial or no.
func verificationState(m Manifest) string {
	if m.VerifiedAt != "" {
		return "yes"
	}
	for _, t := range m.Tables {
		if t.Verified != nil {
			return "partial"
		}
	}
	return "no"
}
