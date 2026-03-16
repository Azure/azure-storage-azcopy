//go:build linux

package common

import (
	"os"

	"golang.org/x/sys/unix"
)

// Fdatasync attempts fdatasync on Linux; returns error if it fails.
func Fdatasync(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}
