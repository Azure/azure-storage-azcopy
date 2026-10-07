//go:build linux

package common

import (
	"os"

	"golang.org/x/sys/unix"
)

func FadviseDontNeed(file *os.File, offset, length int64) error {
	return unix.Fadvise(int(file.Fd()), offset, length, unix.FADV_DONTNEED)
}
