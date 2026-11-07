package archive

import "time"

// ArchiveOptions holds tunable parameters for the archive pipeline.
type ArchiveOptions struct {
	ChunkSize int    // Rows per chunk (default: 2,500,000)
	Workers   int    // Parallel archive workers (default: 4)
	OutputDir string // Base dir for local backups (default: ~/.coldplane/backups)
	Where     string // Optional SQL WHERE clause to filter rows (e.g. "created_at < '2025-01-01'")

	// Age-based selection, used when Where is empty: rows whose time column
	// is before Cutoff (and, with Since or SinceLast, not before the lower bound).
	OlderThan string    // the flag value as typed, e.g. "18m", recorded in the manifest
	Cutoff    time.Time // now minus OlderThan
	Since     time.Time // explicit lower bound
	SinceLast bool      // lower bound from the newest verified backup of the same table

	sourceDSN string // set by the archivers for SinceLast lookups
}

// DefaultOptions returns the default archive configuration.
func DefaultOptions() ArchiveOptions {
	return ArchiveOptions{
		ChunkSize: 2_500_000,
		Workers:   4,
	}
}
