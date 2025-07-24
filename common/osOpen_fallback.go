// Be certain to add the build tags below when we use a specialized implementation.
// This file contains forwards to default, fallback implementations of os operations
//go:build !windows
// +build !windows

package common

import (
	"fmt"
	"os"
	"syscall"
)

// NOTE: OSOpenFile not safe to use on directories on Windows. See comment on the Windows version of this routine
func OSOpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

func OSStat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func EnsureRunningAsRoot() error {
	if syscall.Geteuid() != 0 {
		return fmt.Errorf("must be run as root")
	}
	return nil
}
