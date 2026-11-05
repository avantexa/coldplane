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
