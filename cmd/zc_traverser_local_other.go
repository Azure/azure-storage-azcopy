// +build !windows
// +build !linux

package cmd

import (
	"os"
)

func WrapFolder(fullpath string, stat os.FileInfo) (os.FileInfo, error) {
	return stat, nil
}

func (t *localTraverser) FileRepresentsDevice() bool {
	return false
}
