// Package term provides terminal color helpers: Cyan for inline status text,
// and the ColorError/ColorSuccess/ColorWarning functions for user-facing
// error and status messages.
package term

import (
	"fmt"

	"github.com/fatih/color"
)

const (
	cyan  = "\x1b[36m"
	reset = "\x1b[0m"
)

var (
	ColorError   = color.New(color.FgRed).SprintFunc()
	ColorSuccess = color.New(color.FgGreen).SprintFunc()
	ColorWarning = color.New(color.FgYellow).SprintFunc()
)

func Cyan(msg string) string {
	return fmt.Sprintf("%s%s%s", cyan, msg, reset)
}
