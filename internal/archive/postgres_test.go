package archive

import (
	"testing"
	"time"
)

func TestParseAnyTime(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"datetime format", "2025-06-15 10:30:00", false},
		{"RFC3339 with Z", "2025-06-15T10:30:00Z", false},
		{"RFC3339 with offset", "2025-06-15T10:30:00+05:00", false},
		{"date only", "2025-06-15", false},
		{"RFC3339", "2025-06-15T10:30:00Z", false},
		{"invalid", "not-a-date", true},
		{"empty", "", true},
		{"partial", "2025-06", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseAnyTime(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseAnyTime(%q) error=%v, wantErr=%v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got.IsZero() {
				t.Errorf("parseAnyTime(%q) returned zero time", tt.input)
			}
		})
	}

	// Verify specific parsed value
	t.Run("correct value", func(t *testing.T) {
		got, err := parseAnyTime("2025-06-15 10:30:00")
		if err != nil {
			t.Fatal(err)
		}
		want := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}
