//go:build !linux

package common

import "os"

// Fdatasync is a no-op on non-Linux platforms; always returns nil.
func Fdatasync(f *os.File) error { return nil }
