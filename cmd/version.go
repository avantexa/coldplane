package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

// Set via -ldflags at build time. Example:
//
//	go build -ldflags "-X github.com/avantexa/coldplane/cmd.Version=1.2.3 -X github.com/avantexa/coldplane/cmd.Commit=abc1234 -X github.com/avantexa/coldplane/cmd.BuildDate=2026-02-25T12:00:00Z"
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the coldplane version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("coldplane %s\n", Version)
		fmt.Printf("  commit:  %s\n", Commit)
		fmt.Printf("  built:   %s\n", BuildDate)
		fmt.Printf("  go:      %s\n", runtime.Version())
		fmt.Printf("  os/arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
