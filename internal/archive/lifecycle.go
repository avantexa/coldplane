package archive

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/avantexa/coldplane/internal/dbutil"
)

// whereFor is the row filter that applies to a table: its own, else the backup's.
func (m Manifest) whereFor(t TableEntry) string {
	if t.Where != "" {
		return t.Where
	}
	return m.Where
}

// cutoffWhere renders "col < cutoff" (and "col >= since" when since is set) in
// the engine's SQL, for archiving rows older than a threshold.
func cutoffWhere(dbType string, key chunkKey, since, cutoff time.Time) string {
	col := dbutil.QuotePG(key.column)
	lit := func(t time.Time) string { return pgTimeLiteral(t, key.withZone) }
	if dbType == "mysql" {
		col = dbutil.QuoteMySQL(key.column)
		lit = func(t time.Time) string { return sqlString(t.UTC().Format(mysqlTimeLayout)) }
	}
	where := fmt.Sprintf("%s < %s", col, lit(cutoff))
	if !since.IsZero() {
		where = fmt.Sprintf("%s >= %s AND %s", col, lit(since), where)
	}
	return where
}

// lastCutoff finds the newest verified backup of the same database that
// archived table with an --older-than cutoff, so the next run can start there.
func lastCutoff(baseDir, dbName, table string) (time.Time, bool) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return time.Time{}, false
	}
	var best time.Time
	var found bool
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		m, err := ReadManifest(filepath.Join(baseDir, e.Name()))
		if err != nil || m.DBName != dbName || m.VerifiedAt == "" || m.Cutoff == "" {
			continue
		}
		cutoff, err := time.Parse(time.RFC3339, m.Cutoff)
		if err != nil {
			continue
		}
		for _, t := range m.Tables {
			if t.Name == table && cutoff.After(best) {
				best, found = cutoff, true
			}
		}
	}
	return best, found
}

// PruneOptions tunes a prune run.
type PruneOptions struct {
	IncludeUnverified bool // also remove backups that were never verified
	Execute           bool // false is a dry run
}

// selectPrunable returns the backups created before cutoff, skipping
// unverified ones unless asked, and anything without a parsable date.
func selectPrunable(infos []BackupInfo, cutoff time.Time, includeUnverified bool) []BackupInfo {
	var out []BackupInfo
	for _, b := range infos {
		created, err := time.Parse(time.RFC3339, b.CreatedAt)
		if err != nil || !created.Before(cutoff) {
			continue
		}
		if b.Verified != "yes" && !includeUnverified {
			continue
		}
		out = append(out, b)
	}
	return out
}

// Prune removes backups under baseDir ("" for the default) created before
// cutoff and returns the ones removed, or that would be in a dry run.
func Prune(baseDir string, cutoff time.Time, opts PruneOptions) ([]BackupInfo, error) {
	infos, err := ListBackups(baseDir)
	if err != nil {
		return nil, err
	}
	selected := selectPrunable(infos, cutoff, opts.IncludeUnverified)
	if !opts.Execute {
		return selected, nil
	}
	for _, b := range selected {
		if err := os.RemoveAll(b.Path); err != nil {
			return nil, fmt.Errorf("remove %s: %w", b.Path, err)
		}
	}
	return selected, nil
}
