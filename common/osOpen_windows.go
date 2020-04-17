// +build windows

package common

import (
	"os"
)

// TODO: this is not reliable to use for directories, except inside OSStat (since the stat gets the right result)
//   (But if the raw result of this routine is used, checking its internal dir flag, that flag is wrong for directories)
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
//  That would work around the issue where here we are using OSOpen, which technically returns a bad result for
//  directories (but which we seem to get away with in the os.Stat(handle) call - which goes down the "I am a file" code
//  path, but still returns a valid result for a directory.
//  See also todo on OsOpen, and edit or remove it
func OSStat(name string) (os.FileInfo, error) {
	f, err := OSOpenFile(name, 0, 0)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	return f.Stat()
}
