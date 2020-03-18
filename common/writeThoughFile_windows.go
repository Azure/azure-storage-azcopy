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
	"golang.org/x/sys/windows"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

func GetFileInformation(path string) (windows.ByHandleFileInformation, error) {

	srcPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return windows.ByHandleFileInformation{}, err
	}
	// custom open call, because must specify FILE_FLAG_BACKUP_SEMANTICS when getting information of folders (else GetFileInformationByHandle will fail)
	fd, err := windows.CreateFile(srcPtr,
		windows.GENERIC_READ, windows.FILE_SHARE_READ, nil,
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
	const FILE_ATTRIBUTE_READONLY = 1

	doOpen := func() (syscall.Handle, error) {
		return OpenWithWriteThroughSetting(destinationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, DEFAULT_FILE_PERM, writeThrough)
	}

	hasReadOnlyFlag := func() bool {
		fi, err := GetFileInformation(destinationPath)
		if err != nil {
			return false
		}
		return fi.FileAttributes&FILE_ATTRIBUTE_READONLY == FILE_ATTRIBUTE_READONLY
	}

	tryClearReadOnlyFlag := func() bool {
		fi, err := GetFileInformation(destinationPath)
		if err != nil {
			return false
		}
		destPtr, err := syscall.UTF16PtrFromString(destinationPath)
		if err != nil {
			return false
		}

		// Clear the RO flag (and no others)
		// In the worst-case scenario, if this succeeds but the file open still fails,
		// we will leave the file in a state where this flag (and this flag only) has been
		// cleared. (But then, given the download implementation as at 10.3.x,
		// we'll try to clean up by deleting the file at the end of our job anyway, so we won't be
		// leaving damaged trash around if the delete works).
		// TODO: is that acceptable? Seems overkill to re-instate the attribute if the open fails...
		newAttrs := fi.FileAttributes &^ FILE_ATTRIBUTE_READONLY
		err = windows.SetFileAttributes(destPtr, newAttrs)
		return err == nil
	}

	err := CreateParentDirectoryIfNotExist(destinationPath, tracker)
	if err != nil {
		return nil, err
	}

	fd, err := doOpen()
	if err != nil && hasReadOnlyFlag() {
		if forceIfReadOnly {
			if tryClearReadOnlyFlag() {
				// do the open again
				fd, err = doOpen()
			}
		} else {
			return nil, fmt.Errorf("destination file has read-only flag so access will be denied. Try setting --force-if-read-only on command line. Error was %w", err)
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

func makeInheritSa() *syscall.SecurityAttributes {
	var sa syscall.SecurityAttributes
	sa.Length = uint32(unsafe.Sizeof(sa))
	sa.InheritHandle = 1
	return &sa
}

const FILE_ATTRIBUTE_WRITE_THROUGH = 0x80000000

// Copied from syscall.open, but modified to allow setting of writeThrough option
// Param "perm" is unused both here and in the original Windows version of this routine.
func OpenWithWriteThroughSetting(path string, mode int, perm uint32, writeThrough bool) (fd syscall.Handle, err error) {
	if len(path) == 0 {
		return syscall.InvalidHandle, syscall.ERROR_FILE_NOT_FOUND
	}
	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return syscall.InvalidHandle, err
	}
	var access uint32
	switch mode & (syscall.O_RDONLY | syscall.O_WRONLY | syscall.O_RDWR) {
	case syscall.O_RDONLY:
		access = syscall.GENERIC_READ
	case syscall.O_WRONLY:
		access = syscall.GENERIC_WRITE
	case syscall.O_RDWR:
		access = syscall.GENERIC_READ | syscall.GENERIC_WRITE
	}
	if mode&syscall.O_CREAT != 0 {
		access |= syscall.GENERIC_WRITE
	}
	if mode&syscall.O_APPEND != 0 {
		access &^= syscall.GENERIC_WRITE
		access |= syscall.FILE_APPEND_DATA
	}
	sharemode := uint32(syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE)
	var sa *syscall.SecurityAttributes
	if mode&syscall.O_CLOEXEC == 0 {
		sa = makeInheritSa()
	}
	var createmode uint32
	switch {
	case mode&(syscall.O_CREAT|syscall.O_EXCL) == (syscall.O_CREAT | syscall.O_EXCL):
		createmode = syscall.CREATE_NEW
	case mode&(syscall.O_CREAT|syscall.O_TRUNC) == (syscall.O_CREAT | syscall.O_TRUNC):
		createmode = syscall.CREATE_ALWAYS
	case mode&syscall.O_CREAT == syscall.O_CREAT:
		createmode = syscall.OPEN_ALWAYS
	case mode&syscall.O_TRUNC == syscall.O_TRUNC:
		createmode = syscall.TRUNCATE_EXISTING
	default:
		createmode = syscall.OPEN_EXISTING
	}

	var attr uint32
	attr = syscall.FILE_ATTRIBUTE_NORMAL
	if writeThrough {
		attr |= FILE_ATTRIBUTE_WRITE_THROUGH
	}
	h, e := syscall.CreateFile(pathp, access, sharemode, sa, createmode, attr, 0)
	return h, e
}

// SetBackupMode optionally enables special priviledges on Windows.
// For a description, see https://docs.microsoft.com/en-us/windows-hardware/drivers/ifs/privileges
// and run this: whoami /priv
// from an Administrative command prompt (where lots of privileges should exist, but be disabled)
// and compare with running the same command from a non-admin prompt, where they won't even exist.
// Note that this is particularly useful in two contexts:
// 1. Uploading data where normal file system ACLs would prevent AzCopy from reading it. Simply run
// AzCopy as an account that has SeBackupPrivilege (typically an administrator account using
// an elevated command prompt) and set the AzCopy flag for this routine to be called.
// 2. Downloading where you are preserving SMB permissions, and some of the permissions include
// owners that are NOT the same account as the one running AzCopy.  Again, run AzCopy
// from a elevated admin command prompt, and use this routine to enable SeRestorePrivilege.  Then
// AzCopy will be able to set the owners.
func SetBackupMode(enable bool, fromTo FromTo) error {
	if !enable {
		return nil
	}

	var privName string
	switch {
	case fromTo.IsUpload():
		privName = "SeBackupPrivilege"
	case fromTo.IsDownload():
		privName = "SeRestorePrivilege"
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
		return errors.New("could not activate " + BackupModeFlagName + " mode.  Probably the account running AzCopy does not have " +
			privName + " so AzCopy could not activate that privilege. Administrators usually have that privilege, but only when they are in an elevated command prompt. " +
			"To check which privileges an account has, run this from a command line: whoami /priv")
	}
	return nil
}
