package analyze

import (
	"testing"
	"time"
)

// --- ParseDuration ---

func TestParseDuration(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"18 months", "18m", 18 * 30 * 24 * time.Hour, false},
		{"1 year", "1y", 365 * 24 * time.Hour, false},
		{"6 months", "6m", 6 * 30 * 24 * time.Hour, false},
		{"90 days", "90d", 90 * 24 * time.Hour, false},
		{"1 year 6 months", "1y6m", (365 + 6*30) * 24 * time.Hour, false},
		{"2 years 3 months", "2y3m", (2*365 + 3*30) * 24 * time.Hour, false},
		{"uppercase", "18M", 18 * 30 * 24 * time.Hour, false},
		{"with spaces", " 6m ", 6 * 30 * 24 * time.Hour, false},
		{"empty", "", 0, true},
		{"invalid", "abc", 0, true},
		{"no unit", "18", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDuration(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDuration(%q) error=%v, wantErr=%v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ParseDuration(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// --- formatDuration ---

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name  string
		input time.Duration
		want  string
	}{
		// 18*30=540 days → 1y (365) + 5m (150) + 25d
		{"18 months as days", 18 * 30 * 24 * time.Hour, "1y5m25d"},
		{"1 year", 365 * 24 * time.Hour, "1y"},
		{"1 year 6 months", (365 + 6*30) * 24 * time.Hour, "1y6m"},
		{"90 days", 90 * 24 * time.Hour, "3m"},
		{"zero", 0, "0d"},
		{"7 days", 7 * 24 * time.Hour, "7d"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDuration(tt.input)
			if got != tt.want {
				t.Errorf("formatDuration(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// --- quotePGIdentifier ---

func TestQuotePGIdentifier(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"orders", `"orders"`},
		{`table"name`, `"table""name"`},
		{"", `""`},
	}
	for _, tt := range tests {
		got := quotePGIdentifier(tt.input)
		if got != tt.want {
			t.Errorf("quotePGIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// --- quoteMySQLIdentifier ---

func TestQuoteMySQLIdentifier(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"orders", "`orders`"},
		{"table`name", "`table``name`"},
		{"", "``"},
	}
	for _, tt := range tests {
		got := quoteMySQLIdentifier(tt.input)
		if got != tt.want {
			t.Errorf("quoteMySQLIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// --- parseMySQLTime ---

func TestParseMySQLTime(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"datetime", "2025-06-15 10:30:00", false},
		{"date only", "2025-06-15", false},
		{"iso with Z", "2025-06-15T10:30:00Z", false},
		{"invalid", "not-a-date", true},
		{"empty", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMySQLTime(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseMySQLTime(%q) error=%v, wantErr=%v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got.IsZero() {
				t.Errorf("parseMySQLTime(%q) returned zero time", tt.input)
			}
		})
	}
}

// --- humanCount (via cmd package, test logic here) ---

func TestCompressionEstimate(t *testing.T) {
	// 1M rows out of 2M, table is 10GB
	totalRows := int64(2_000_000)
	archivableRows := int64(1_000_000)
	sizeBytes := int64(10 * 1024 * 1024 * 1024) // 10 GB

	est := int64(float64(archivableRows) / float64(totalRows) * float64(sizeBytes) * compressionRatio)

	// 50% of rows * 10GB * 0.25 compression = 1.25 GB
	expectedGB := 1.25
	gotGB := float64(est) / (1024 * 1024 * 1024)

	if gotGB < expectedGB-0.1 || gotGB > expectedGB+0.1 {
		t.Errorf("compression estimate = %.2f GB, want ~%.2f GB", gotGB, expectedGB)
	}
}
