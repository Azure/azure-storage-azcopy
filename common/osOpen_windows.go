// +build windows

package common

import (
	"os"
)

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
	f, err := OSOpenFile(name, 0, 0)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	return f.Stat()
}
