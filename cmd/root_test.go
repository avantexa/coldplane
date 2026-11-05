package cmd

import (
	"fmt"
	"strings"
	"testing"
)

// --- splitList ---

func TestSplitList(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"single item", "orders", []string{"orders"}},
		{"multiple items", "orders,users,products", []string{"orders", "users", "products"}},
		{"with whitespace", " orders , users , products ", []string{"orders", "users", "products"}},
		{"empty", "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitList(tt.input)
			if len(got) != len(tt.want) {
				t.Errorf("splitList(%q) len=%d, want %d", tt.input, len(got), len(tt.want))
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("splitList(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}

// --- normalizeDSN ---

func TestNormalizeDSN(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"pg:// to postgres://", "pg://user:pass@host:5432/db", "postgres://user:pass@host:5432/db"},
		{"postgres:// passthrough", "postgres://user:pass@host:5432/db", "postgres://user:pass@host:5432/db"},
		{"mysql:// normalized", "mysql://user:pass@host:3306/db", "user:pass@tcp(host:3306)/db"},
		{"unknown passthrough", "user:pass@tcp(host:3306)/db", "user:pass@tcp(host:3306)/db"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeDSN(tt.input)
			if got != tt.want {
				t.Errorf("normalizeDSN(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// --- humanSizeInt64 ---

func TestHumanSizeInt64(t *testing.T) {
	tests := []struct {
		name  string
		input int64
		want  string
	}{
		{"gigabytes", 2147483648, "2.00 GB"},
		{"megabytes", 5242880, "5.00 MB"},
		{"kilobytes", 2048, "2.00 KB"},
		{"bytes", 512, "512 B"},
		{"zero", 0, "0 B"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := humanSizeInt64(tt.input)
			if got != tt.want {
				t.Errorf("humanSizeInt64(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// --- humanCount ---

func TestHumanCount(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0"},
		{500, "500"},
		{999, "999"},
		{1000, "1K"},
		{1500, "2K"},
		{12345, "12K"},
		{999999, "1000K"},
		{1000000, "1.0M"},
		{1200000, "1.2M"},
		{12100000, "12.1M"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.input), func(t *testing.T) {
			got := humanCount(tt.input)
			if got != tt.want {
				t.Errorf("humanCount(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// --- detectDBType ---

func TestDetectDBType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"mysql://user:pass@host/db", "mysql"},
		{"postgres://user:pass@host/db", "postgres"},
		{"pg://user:pass@host/db", "postgres"},
		{"user:pass@tcp(host)/db", "mysql"},
		{"", "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := detectDBType(tt.input)
			if got != tt.want {
				t.Errorf("detectDBType(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// --- command surface: local only ---

func TestCloudCommandsAreGone(t *testing.T) {
	for _, args := range [][]string{{"auth"}, {"convert"}, {"backup", "describe"}, {"backup", "restore"}} {
		want := args[len(args)-1]
		c, _, _ := rootCmd.Find(args)
		if c != nil && c.Name() == want {
			t.Errorf("command %v should not exist", args)
		}
	}
}

func TestCloudFlagsAreGone(t *testing.T) {
	checks := []struct {
		cmd  string
		flag string
	}{
		{"backup create", "local"},
		{"backup list", "local"},
		{"backup list", "status"},
		{"backup list", "limit"},
		{"backup delete", "local"},
		{"query run", "local"},
		{"query run", "export"},
		{"query run", "format"},
	}
	for _, c := range checks {
		cmd, _, err := rootCmd.Find(strings.Fields(c.cmd))
		if err != nil {
			t.Fatalf("%s: %v", c.cmd, err)
		}
		if cmd.Flags().Lookup(c.flag) != nil {
			t.Errorf("%s should not have --%s", c.cmd, c.flag)
		}
	}
}
