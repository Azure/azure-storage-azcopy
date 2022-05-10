// +build linux

package cmd

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"golang.org/x/sys/unix"
	"os"
)

func WrapFolder(fullpath string, stat os.FileInfo) (os.FileInfo, error) { // todo: there's gotta be a better way to do this.
	return stat, nil
}

func (t *localTraverser) FileRepresentsDevice(fullpath string) bool {
	if t.posixPropertiesOption != common.EPosixPropertiesOption.SpecialFilesAndProperties() {
		return false // the user is either not processing posix properties, or wants files like this.
	}

	fi, err := os.Stat(fullpath)
	if err != nil { // this probably shouldn't fail
		return false
	}

	stat := fi.Sys().(*unix.Stat_t)
	return stat.Mode&unix.S_IFBLK == unix.S_IFBLK || stat.Mode&unix.S_IFCHR == unix.S_IFCHR
}
