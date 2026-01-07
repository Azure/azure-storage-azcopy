//go:build windows
//go:build windows
// +build windows

package traverser

import (
	"os"
	"strconv"
	"syscall"
	"time"
	"unsafe"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/hillu/go-ntdll"
	"golang.org/x/sys/windows"
)

// Override the modtime from the OS stat
type folderStatWrapper struct {
	os.FileInfo
	changeTime time.Time
}

func (f *folderStatWrapper) ModTime() time.Time {
	return f.changeTime
}

func WrapFolder(fullpath string, stat os.FileInfo) (os.FileInfo, error) {
	srcPtr, err := syscall.UTF16PtrFromString(fullpath)
	if err != nil {
		return nil, err
	}

	// custom open call, because must specify FILE_FLAG_BACKUP_SEMANTICS to make --backup mode work properly (i.e. our use of SeBackupPrivilege)
	fd, err := windows.CreateFile(srcPtr,
		windows.GENERIC_READ, windows.FILE_SHARE_READ, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return nil, err
	}
	defer windows.Close(fd)

	buf := make([]byte, 1024)
	var rLen = uint32(unsafe.Sizeof(ntdll.FileBasicInformationT{}))
	if st := ntdll.CallWithExpandingBuffer(func() ntdll.NtStatus {
		var stat ntdll.IoStatusBlock
		return ntdll.NtQueryInformationFile(ntdll.Handle(fd), &stat, &buf[0], uint32(len(buf)), ntdll.FileBasicInformation)
	}, &buf, &rLen); st.IsSuccess() {
		ntdllTime := time.Date(1601, time.January, 1, 0, 0, 0, 0, time.UTC)

		// do a nasty unsafe thing and tell go that our []byte is actually a FileStandardInformationT.
		fi := (*ntdll.FileBasicInformationT)(unsafe.Pointer(&buf[0]))
		// ntdll returns times that are incremented by 100-nanosecond "instants" past the beginning of 1601.
		// time.Duration is a 64 bit integer, starting at nanoseconds.
		// It cannot hold more than 290 years. You can't add any kind of arbitrary precision number of nanoseconds either.
		// However, time.Time can handle such things on it's own.
		// So, we just add our changetime 100x. It's a little cheesy, but it does work.
		for i := 0; i < 100; i++ {
			ntdllTime = ntdllTime.Add(time.Duration(fi.ChangeTime))
		}

		return &folderStatWrapper{
			stat,
			ntdllTime,
		}, nil
	} else {
		return nil, st.Error()
	}
}

func LogHardLinkIfDefaultPolicy(fileInfo os.FileInfo, hardlinkHandling common.HardlinkHandlingType) {
}

// TODO: Add support for hardlinks on Windows later
func IsHardlink(fileInfo os.FileInfo) bool {
	return false
}

// TODO: Add support for this on Windows later
func IsRegularFile(info os.FileInfo) bool {
	return info.Mode().IsRegular()
}

func IsSymbolicLink(fileInfo os.FileInfo) bool {
	return fileInfo.Mode()&os.ModeSymlink == os.ModeSymlink
}

func getInodeString(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	handle := syscall.Handle(file.Fd())

	var info syscall.ByHandleFileInformation
	err = syscall.GetFileInformationByHandle(handle, &info)
	if err != nil {
		return "", err
	}

	// Combine high + low to form a unique file ID
	fileID := (uint64(info.FileIndexHigh) << 32) | uint64(info.FileIndexLow)

	return strconv.FormatUint(fileID, 10), nil
}
