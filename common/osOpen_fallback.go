// Be certain to add the build tags below when we use a specialized implementation.
// This file contains forwards to default, fallback implementations of os operations
// +build !windows

package common

import (
	"os"
)

func OSOpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

func OSStat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}
