package archive

// ArchiveOptions holds tunable parameters for the archive pipeline.
type ArchiveOptions struct {
	ChunkSize int    // Rows per chunk (default: 2,500,000)
	Workers   int    // Parallel archive workers (default: 4)
	OutputDir string // Base dir for local backups (default: ~/.coldplane/backups)
	Where     string // Optional SQL WHERE clause to filter rows (e.g. "created_at < '2025-01-01'")
}

// DefaultOptions returns the default archive configuration.
func DefaultOptions() ArchiveOptions {
	return ArchiveOptions{
		ChunkSize: 2_500_000,
		Workers:   4,
	}
}
