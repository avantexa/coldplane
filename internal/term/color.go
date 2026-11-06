// Package term provides terminal color helpers: Cyan for inline status text
// and ColorError for user-facing error messages.
package term

import (
	"fmt"

	"github.com/fatih/color"
)

const (
	cyan  = "\x1b[36m"
	reset = "\x1b[0m"
)

var ColorError = color.New(color.FgRed).SprintFunc()

func Cyan(msg string) string {
	return fmt.Sprintf("%s%s%s", cyan, msg, reset)
}
