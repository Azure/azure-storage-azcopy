//go:build windows
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

// TODO: os.Stat(name string) already uses backup semantics.  Can/should we just use that?
//
//	That would work around the issue where here we are using OSOpen, which technically returns a bad result for
//	directories (but which we seem to get away with in the os.Stat(handle) call - which goes down the "I am a file" code
//	path, but still returns a valid result for a directory.
//	See also todo on OsOpen, and edit or remove it
func OSStat(name string) (os.FileInfo, error) {
	return os.Stat(name) // this is safe even with our --backup mode, because it uses FILE_FLAG_BACKUP_SEMANTICS (whereas os.File.Stat() does not)
}

func EnsureRunningAsRoot() error {
	return nil
}
