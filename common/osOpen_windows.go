// +build windows

package common

import (
	"os"
)

// NOTE: this is not safe to use on directories.  It returns an os.File that points at a directory, but thinks it points to a file.
// There is no way around that in the Go SDK as at Go 1.13, because the only way to make a valid windows.File that points to a directory
// is private inside the Go SDK's Windows package.
func OSOpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	// use openwithwritethroughsetting with false writethrough, since it makes a windows syscall containing an ask
	//     for backup privileges. This allows all of our file opening to go down one route of code.
	fd, err := OpenWithWriteThroughSetting(name, flag, uint32(perm), false)

	if err != nil {
		return nil, err
	}

	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		return nil, os.ErrInvalid
	}

	return file, nil
}

func OSStat(name string) (os.FileInfo, error) {
	return os.Stat(name) // this is safe even with our --backup mode, because it uses FILE_FLAG_BACKUP_SEMANTICS (whereas os.File.Stat() does not)
}
