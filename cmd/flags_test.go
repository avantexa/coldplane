package cmd

import "testing"

func TestFlagDefaults(t *testing.T) {
	if f := rootCmd.PersistentFlags().Lookup("verbose"); f == nil || f.DefValue != "false" {
		t.Errorf("--verbose should default to false, got %v", f)
	}
	cases := []struct {
		cmd       []string
		flag, def string
	}{
		{[]string{"backup", "create"}, "chunk-size", "0"},
		{[]string{"backup", "create"}, "workers", "0"},
		{[]string{"analyze"}, "all", "false"},
		{[]string{"analyze"}, "json", "false"},
		{[]string{"analyze"}, "older-than", "18m"},
		{[]string{"query", "run"}, "output", "table"},
	}
	for _, c := range cases {
		cmd, _, err := rootCmd.Find(c.cmd)
		if err != nil {
			t.Fatalf("%v: %v", c.cmd, err)
		}
		f := cmd.Flags().Lookup(c.flag)
		if f == nil || f.DefValue != c.def {
			t.Errorf("%v --%s default = %v, want %s", c.cmd, c.flag, f, c.def)
		}
	}
}

func TestHumanSizeInt64_Boundaries(t *testing.T) {
	cases := map[int64]string{1 << 10: "1024 B", 1<<10 + 1: "1.00 KB", 1 << 20: "1024.00 KB", 1<<20 + 1: "1.00 MB", 1 << 30: "1024.00 MB", 1<<30 + 1: "1.00 GB"}
	for in, want := range cases {
		if got := humanSizeInt64(in); got != want {
			t.Errorf("humanSizeInt64(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestBackupCreate_RequiresDBURLAndTables(t *testing.T) {
	run := func(args ...string) error {
		dbURL, tables = "", ""
		rootCmd.SetArgs(append([]string{"backup", "create"}, args...))
		return rootCmd.Execute()
	}
	if err := run(); err == nil || err.Error() != "you must specify --db-url" {
		t.Errorf("no flags: %v", err)
	}
	if err := run("--db-url", "postgres://x"); err == nil || err.Error() != "you must specify --tables" {
		t.Errorf("db-url only: %v", err)
	}
}
