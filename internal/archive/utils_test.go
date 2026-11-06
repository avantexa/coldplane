package archive

import (
	"testing"
)

func TestNormalizeMySQLDSN(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "non-mysql URI passes through unchanged",
			input:    "user:pass@tcp(host:3306)/db",
			expected: "user:pass@tcp(host:3306)/db",
		},
		{
			name:     "mysql:// URI with password",
			input:    "mysql://user:pass@host:3306/mydb",
			expected: "user:pass@tcp(host:3306)/mydb",
		},
		{
			name:     "mysql:// URI without password",
			input:    "mysql://user@host:3306/mydb",
			expected: "user@tcp(host:3306)/mydb",
		},
		{
			name:     "mysql:// URI with query params",
			input:    "mysql://user:pass@host:3306/mydb?charset=utf8",
			expected: "user:pass@tcp(host:3306)/mydb?charset=utf8",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeMySQLDSN(tt.input)
			if got != tt.expected {
				t.Errorf("NormalizeMySQLDSN(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

// --- inferParquetSchema ---

// --- toParquetSafeValue ---

// --- DefaultOptions ---

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()
	if opts.ChunkSize != 2_500_000 {
		t.Errorf("ChunkSize = %d, want 2500000", opts.ChunkSize)
	}
	if opts.Workers != 4 {
		t.Errorf("Workers = %d, want 4", opts.Workers)
	}
	if opts.OutputDir != "" {
		t.Errorf("OutputDir should default to empty, got %q", opts.OutputDir)
	}
	if opts.Where != "" {
		t.Errorf("Where should default to empty, got %q", opts.Where)
	}
}
