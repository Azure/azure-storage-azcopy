//go:build !linux

package common

import (
	"os"
)

// FadviseDontNeed is a no-op on non-Linux platforms; always returns nil.
func FadviseDontNeed(file *os.File, offset, length int64) error { return nil }
