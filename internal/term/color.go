// Package term provides terminal color helpers.
//
// Two color systems coexist:
//   - Raw ANSI helpers (Red, Green, Yellow, Cyan) — used for inline UI text
//     such as status labels and progress messages.
//   - fatih/color Sprintf-style vars (ColorError, ColorSuccess, ColorWarning) —
//     used for error/success/warning messages in fmt.Errorf and fmt.Println.
//
// Prefer the ColorError/ColorSuccess/ColorWarning vars for user-facing
// error and status messages. Use the raw ANSI helpers for decorative output.
package term

import (
	"fmt"

	"github.com/fatih/color"
)

const (
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	cyan   = "\033[36m"
	dim    = "\033[2m"
	reset  = "\033[0m"
)

var (
	ColorError   = color.New(color.FgRed).SprintFunc()
	ColorSuccess = color.New(color.FgGreen).SprintFunc()
	ColorWarning = color.New(color.FgYellow).SprintFunc()
)

func Red(msg string) string {
	return fmt.Sprintf("%s%s%s", red, msg, reset)
}

func Green(msg string) string {
	return fmt.Sprintf("%s%s%s", green, msg, reset)
}

func Yellow(msg string) string {
	return fmt.Sprintf("%s%s%s", yellow, msg, reset)
}

func Cyan(msg string) string {
	return fmt.Sprintf("%s%s%s", cyan, msg, reset)
}
