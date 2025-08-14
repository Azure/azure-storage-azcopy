//go:build windows
// +build windows

package ste

import (
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"

	"github.com/hillu/go-ntdll"

	"github.com/Azure/azure-storage-azcopy/v10/common"

	"golang.org/x/sys/windows"

	"github.com/Azure/azure-storage-azcopy/v10/sddl"
)

// This file os-triggers the ISMBPropertyBearingSourceInfoProvider and CustomLocalOpener interfaces on a local SIP.

// getHandle obtains a windows file handle with generic read permissions & backup semantics
func (f localFileSourceInfoProvider) getHandle(path string) (ntdll.Handle, error) {
	srcPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	// custom open call, because must specify FILE_FLAG_BACKUP_SEMANTICS to make --backup mode work properly (i.e. our use of SeBackupPrivilege)
	fd, err := windows.CreateFile(srcPtr,
		windows.GENERIC_READ, windows.FILE_SHARE_READ, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return 0, err
	}

	return ntdll.Handle(fd), err
}

func (f localFileSourceInfoProvider) Open(path string) (*os.File, error) {
	fd, err := f.getHandle(path)
	if err != nil {
		return nil, err
	}

	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		return nil, os.ErrInvalid
	}

	return file, nil
}

func (f localFileSourceInfoProvider) GetSDDL() (string, error) {
	// We only need Owner, Group, and DACLs for azure files.
	fd, err := f.getHandle(f.jptm.Info().Source)
	if err != nil {
		return "", err
	}
	buf := make([]byte, 512)
	bufLen := uint32(len(buf))
	needValidate := false
	status := ntdll.CallWithExpandingBuffer(func() ntdll.NtStatus {
		status := ntdll.NtQuerySecurityObject(
			fd,
			windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
			(*ntdll.SecurityDescriptor)(unsafe.Pointer(&buf[0])),
			uint32(len(buf)),
			&bufLen)

		/*
			On certain older versions of Windows/Server and certain SAN/SMB emulator software,
			on any status but STATUS_BUFFER_TOO_SMALL, bufLen will be returned as 0.

			CallWithExpandingBuffer does not handle this correctly.
			Thus, we have to attain the real length of the security descriptor and correct the output,
			otherwise we panic due to an OOB error on the array.
		*/

		// get real buffer length, since what's returned by ntquerysecurityobject is questionable for STATUS_SUCCESS
		if status == ntdll.STATUS_SUCCESS {
			sd := (*windows.SECURITY_DESCRIPTOR)(unsafe.Pointer(&buf[0])) // ntdll.SecurityDescriptor is equivalent

			bufLen = sd.Length()
			needValidate = true
		}

		return status
	}, &buf, &bufLen)

	if status != ntdll.STATUS_SUCCESS {
		return "", fmt.Errorf("failed to query security object %s (ntstatus: %s)", f.jptm.Info().Source, status.String())
	}

	sd := (*windows.SECURITY_DESCRIPTOR)(unsafe.Pointer(&buf[0])) // ntdll.SecurityDescriptor is equivalent
	if needValidate && !sd.IsValid() {
		return "", fmt.Errorf("failed to query security object %s (invalid security descriptor returned w/ success status)", f.jptm.Info().Source)
	}
	fSDDL, err := sddl.ParseSDDL(sd.String())

	if err != nil {
		return "", err
	}

	if strings.TrimSpace(fSDDL.String()) != strings.TrimSpace(sd.String()) {
		panic("SDDL sanity check failed (parsed string output != original string.)")
	}

	return fSDDL.PortableString(), nil
}

func (f localFileSourceInfoProvider) GetSMBProperties() (TypedSMBPropertyHolder, error) {
	info, err := common.GetFileInformation(f.jptm.Info().Source)

	return HandleInfo{info}, err
}

type HandleInfo struct {
	windows.ByHandleFileInformation
}

func (hi HandleInfo) FileCreationTime() time.Time {
	return common.WinFiletimeToTime(&hi.CreationTime)
}

func (hi HandleInfo) FileLastWriteTime() time.Time {
	return common.WinFiletimeToTime(&hi.LastWriteTime)
}

func (hi HandleInfo) FileChangeTime() time.Time {
	return time.Time{} // Windows does not provide change time in ByHandleFileInformation
}

func (hi HandleInfo) FileAttributes() (*file.NTFSFileAttributes, error) {
	// Can't shorthand it because the function name overrides.
	return FileAttributesFromUint32(hi.ByHandleFileInformation.FileAttributes)
}
