//go:build !windows
// +build !windows

package cmd

import (
	"os"
)

func WrapFolder(fullpath string, stat os.FileInfo) (os.FileInfo, error) {
	return stat, nil
}
