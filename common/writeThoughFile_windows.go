// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func isDriveRoot(path string) bool {
	// VolumeName will not have trailing backslash
	if last := len(path) - 1; last >= 0 && path[last] == '\\' {
		path = path[:last]
	}

	return filepath.VolumeName(path) == path
}

func GetFileInformation(path string) (windows.ByHandleFileInformation, error) {

	if isDriveRoot(path) {
		path = ToShortPath(path)
	}

	srcPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return windows.ByHandleFileInformation{}, err
	}
	// custom open call, because must specify FILE_FLAG_BACKUP_SEMANTICS when getting information of folders (else GetFileInformationByHandle will fail)
	fd, err := windows.CreateFile(srcPtr,
		windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return windows.ByHandleFileInformation{}, err
	}
	defer windows.Close(fd)

	var info windows.ByHandleFileInformation

	err = windows.GetFileInformationByHandle(fd, &info)

	return info, err
}

func CreateFileOfSizeWithWriteThroughOption(destinationPath string, fileSize int64, writeThrough bool, tracker FolderCreationTracker, forceIfReadOnly bool) (*os.File, error) {
	const FILE_ATTRIBUTE_READONLY = windows.FILE_ATTRIBUTE_READONLY
	const FILE_ATTRIBUTE_HIDDEN = windows.FILE_ATTRIBUTE_HIDDEN

	doOpen := func() (windows.Handle, error) {
		return OpenWithWriteThroughSetting(destinationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, DEFAULT_FILE_PERM, writeThrough)
	}

	getFlagMatches := func(flags uint32) (matches uint32, allFlags uint32, retry bool) {
		fi, err := GetFileInformation(destinationPath)
		if err != nil {
			return 0, 0, false
		}
		o := fi.FileAttributes & flags
		return o, fi.FileAttributes, o != 0 // != 0 indicates we have at least one of these flags.
	}

	tryClearFlagSet := func(toClear uint32) bool {
		fi, err := GetFileInformation(destinationPath)
		if err != nil {
			return false
		}
		destPtr, err := syscall.UTF16PtrFromString(destinationPath)
		if err != nil {
			return false
		}

		// Clear the flags asked (and no others)
		// In the worst-case scenario, if this succeeds but the file open still fails,
		// we will leave the file in a state where this flag (and this flag only) has been
		// cleared. (But then, given the download implementation as at 10.3.x,
		// we'll try to clean up by deleting the file at the end of our job anyway, so we won't be
		// leaving damaged trash around if the delete works).
		// TODO: is that acceptable? Seems overkill to re-instate the attribute if the open fails....
		newAttrs := fi.FileAttributes &^ toClear
		err = windows.SetFileAttributes(destPtr, newAttrs)
		return err == nil
	}

	getIssueFlagStrings := func(flags uint32) string {
		if flags&(FILE_ATTRIBUTE_HIDDEN|FILE_ATTRIBUTE_READONLY) == (FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_READONLY) {
			return "hidden and read-only (try --force-if-read-only on the command line) flags"
		} else if flags&FILE_ATTRIBUTE_HIDDEN == FILE_ATTRIBUTE_HIDDEN {
			return "a hidden flag"
		} else if flags&FILE_ATTRIBUTE_READONLY == FILE_ATTRIBUTE_READONLY {
			return "a read-only flag (try --force-if-read-only on the command line)"
		} else {
			return fmt.Sprintf("no known flags that could cause issue (current set: %x)", flags)
		}
	}

	err := CreateParentDirectoryIfNotExist(destinationPath, tracker)
	if err != nil {
		return nil, err
	}

	fd, err := doOpen()
	if err != nil {
		// Let's check what we might need to clear, and if we should retry
		toClearFlagSet, allFlags, toRetry := getFlagMatches(FILE_ATTRIBUTE_READONLY | FILE_ATTRIBUTE_HIDDEN)

		// If we don't choose to retry, and we fail to clear the flag set, return an error
		if toRetry && tryClearFlagSet(toClearFlagSet) {
			fd, err = doOpen()
		} else {
			return nil, fmt.Errorf("destination file has "+getIssueFlagStrings(allFlags)+" and azcopy was unable to clear the flag(s), so access will be denied: %w", err)
		}
	}
	if err != nil {
		return nil, err
	}

	f := os.NewFile(uintptr(fd), destinationPath)
	if f == nil {
		return nil, os.ErrInvalid
	}

	if truncateError := f.Truncate(fileSize); truncateError != nil {
		return nil, truncateError
	}
	return f, nil
}

func makeInheritSa() *windows.SecurityAttributes {
	var sa windows.SecurityAttributes
	sa.Length = uint32(unsafe.Sizeof(sa))
	sa.InheritHandle = 1
	return &sa
}

const FILE_ATTRIBUTE_WRITE_THROUGH = 0x80000000

// Copied from syscall.open, but modified to allow setting of writeThrough option
// Also modified to conform with the windows package, to enable file backup semantics.
// Furthermore, all of the os, syscall, and windows packages line up. So, putting in os.O_RDWR or whatever of that nature into mode works fine.
// Param "perm" is unused both here and in the original Windows version of this routine.
func OpenWithWriteThroughSetting(path string, mode int, perm uint32, writeThrough bool) (fd windows.Handle, err error) {
	if len(path) == 0 {
		return windows.InvalidHandle, windows.ERROR_FILE_NOT_FOUND
	}
	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return windows.InvalidHandle, err
	}
	var access uint32
	switch mode & (windows.O_RDONLY | windows.O_WRONLY | windows.O_RDWR) {
	case windows.O_RDONLY:
		access = windows.GENERIC_READ
	case windows.O_WRONLY:
		access = windows.GENERIC_WRITE
	case windows.O_RDWR:
		access = windows.GENERIC_READ | windows.GENERIC_WRITE
	}

	if mode&windows.O_CREAT != 0 {
		access |= windows.GENERIC_WRITE
	}
	if mode&windows.O_APPEND != 0 {
		access &^= windows.GENERIC_WRITE
		access |= windows.FILE_APPEND_DATA
	}
	sharemode := uint32(windows.FILE_SHARE_READ)
	var sa *windows.SecurityAttributes
	if mode&syscall.O_CLOEXEC == 0 {
		sa = makeInheritSa()
	}
	var createmode uint32
	switch {
	case mode&(windows.O_CREAT|windows.O_EXCL) == (windows.O_CREAT | windows.O_EXCL):
		createmode = windows.CREATE_NEW
	case mode&(windows.O_CREAT|windows.O_TRUNC) == (windows.O_CREAT | windows.O_TRUNC):
		createmode = windows.CREATE_ALWAYS
	case mode&windows.O_CREAT == windows.O_CREAT:
		createmode = windows.OPEN_ALWAYS
	case mode&windows.O_TRUNC == windows.O_TRUNC:
		createmode = windows.TRUNCATE_EXISTING
	default:
		createmode = windows.OPEN_EXISTING
	}

	var attr uint32
	attr = windows.FILE_ATTRIBUTE_NORMAL | windows.FILE_FLAG_BACKUP_SEMANTICS
	if writeThrough {
		attr |= FILE_ATTRIBUTE_WRITE_THROUGH
	}
	h, e := windows.CreateFile(pathp, access, sharemode, sa, createmode, attr, 0)
	return h, e
}

// SetBackupMode optionally enables special privileges on Windows.
// For a description, see https://docs.microsoft.com/en-us/windows-hardware/drivers/ifs/privileges
// and https://superuser.com/a/1430372
// and run this: whoami /priv
// from an Administrative command prompt (where lots of privileges should exist, but be disabled)
// and compare with running the same command from a non-admin prompt, where they won't even exist.
// Note that this is particularly useful in two contexts:
// 1. Uploading data where normal file system ACLs would prevent AzCopy from reading it. Simply run
// AzCopy as an account that has SeBackupPrivilege (typically an administrator account using
// an elevated command prompt, or a member of the "Backup Operators" group)
//
//	and set the AzCopy flag for this routine to be called.
//
// 2. Downloading where you are preserving SMB permissions, and some of the permissions include
// owners that are NOT the same account as the one running AzCopy.  Again, run AzCopy
// from a elevated admin command prompt (or as a member of the "Backup Operators" group),
// and use this routine to enable SeRestorePrivilege.  Then AzCopy will be able to set the owners.
func SetBackupMode(enable bool, fromTo FromTo) error {
	if !enable {
		return nil
	}

	var privList []string
	switch {
	case fromTo.IsUpload():
		privList = []string{"SeBackupPrivilege"}
	case fromTo.IsDownload():
		// For downloads, we need both privileges.
		// This is _probably_ because restoring file times requires we open the file with FILE_WRITE_ATTRIBUTES (where there's no FILE_READ_ATTRIBUTES)
		// Thus, a read is _probably_ implied, and in scenarios where the ACL denies privileges, is denied without SeBackupPrivilege.
		privList = []string{"SeBackupPrivilege", "SeRestorePrivilege"}
	default:
		panic("unsupported fromTo in SetBackupMode")
	}

	// get process token
	procHandle := windows.CurrentProcess() // no need to close this one
	var procToken windows.Token
	err := windows.OpenProcessToken(procHandle, windows.TOKEN_ADJUST_PRIVILEGES|windows.TOKEN_QUERY, &procToken)
	if err != nil {
		return err
	}
	defer procToken.Close()

	for _, privName := range privList {
		// prepare token privs structure
		privStr, err := syscall.UTF16PtrFromString(privName)
		if err != nil {
			return err
		}
		tokenPrivs := windows.Tokenprivileges{PrivilegeCount: 1}
		tokenPrivs.Privileges[0].Attributes = windows.SE_PRIVILEGE_ENABLED
		err = windows.LookupPrivilegeValue(nil, privStr, &tokenPrivs.Privileges[0].Luid)
		if err != nil {
			return err
		}

		// Get a structure to receive the old value of every privilege that was changed.
		// This is the only way we can tell that windows.AdjustTokenPrivileges actually did anything, because
		// the underlying API can return a success result but a non-successful last error (and Go doesn't expect that,
		// so doesn't pick it up in Gos implementation of windows.AdjustTokenPrivileges.
		oldPrivs := windows.Tokenprivileges{}
		oldPrivsSize := uint32(reflect.TypeOf(oldPrivs).Size()) // it's all struct-y, with an array (not a slice) so everything is inline and size will include everything
		var requiredReturnLen uint32

		// adjust our privileges
		err = windows.AdjustTokenPrivileges(procToken, false, &tokenPrivs, oldPrivsSize, &oldPrivs, &requiredReturnLen)
		if err != nil {
			return err
		}
		if oldPrivs.PrivilegeCount != 1 {
			// Only the successful changes are returned in the old state
			// If there were none, that means it didn't work
			return errors.New("could not activate '" + BackupModeFlagName + "' mode.  Probably the account running AzCopy does not have " +
				privName + " so AzCopy could not activate that privilege. Administrators usually have that privilege, but only when they are in an elevated command prompt. " +
				"Members of the 'Backup Operators' security group also have that privilege. To check which privileges an account has, run this from a command line: whoami /priv")
		}
	}

	return nil
}

func GetExtendedProperties(path string, entityType EntityType) (ExtendedProperties, error) {
	// This function is not implemented for Windows, as it uses Statx which is a Linux-specific syscall.
	extProp, err := GetFileInformation(path)
	if err != nil {
		return nil, err
	}
	return &WindowsExtendedProperties{info: extProp}, nil
}

// WindowsExtendedProperties wraps windows.ByHandleFileInformation to implement the FileInfo interface
type WindowsExtendedProperties struct {
	info windows.ByHandleFileInformation
}

func (wfi *WindowsExtendedProperties) GetLastAccessTime() time.Time {
	return time.Unix(0, wfi.info.LastAccessTime.Nanoseconds())
}

func (wfi *WindowsExtendedProperties) GetLastWriteTime() time.Time {
	return time.Unix(0, wfi.info.LastWriteTime.Nanoseconds())
}

func (wfi *WindowsExtendedProperties) GetChangeTime() time.Time {
	// Windows doesn't have a separate change time, use default
	return time.Time{}
}
