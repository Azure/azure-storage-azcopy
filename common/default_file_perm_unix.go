//go:build !windows

package common

import (
	"os"
	"sync"
	"syscall"
)

var (
	umask     int
	umaskOnce sync.Once
)

// GetUmask retrieves the current process's umask without permanently modifying it.
// The value is cached after the first call.
func GetUmask() int {
	umaskOnce.Do(func() {
		current := syscall.Umask(0)
		syscall.Umask(current)
		umask = current
	})
	return umask
}

// DEFAULT_FILE_PERM is 0666 masked by the process umask, matching standard
// POSIX tool behavior (e.g. cp, rsync).  With a typical umask of 022 this
// produces 0644; with 002 it produces 0664, etc.
//
// The value is computed once during package-level variable initialization
// so that every file created by AzCopy receives permissions consistent with
// the calling user's umask.
var DEFAULT_FILE_PERM = func() os.FileMode {
	return os.FileMode(0666 &^ GetUmask())
}()
