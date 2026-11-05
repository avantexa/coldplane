//go:build windows

// Enables CGO in the main package so that seekpos_shim_windows.c
// is compiled and linked into the final binary.

package main

import "C"
