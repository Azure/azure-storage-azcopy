// +build windows

package ste

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/hillu/go-ntdll"

	"github.com/Azure/azure-storage-azcopy/v10/common"

	"github.com/Azure/azure-storage-file-go/azfile"
	"golang.org/x/sys/windows"

	"github.com/Azure/azure-storage-azcopy/v10/sddl"
)

// This file os-triggers the ISMBPropertyBearingSourceInfoProvider and CustomLocalOpener interfaces on a local SIP.

func (f localFileSourceInfoProvider) GetHandle(path string) (ntdll.Handle, error) {
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
	fd, err := f.GetHandle(path)
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
	fd, err := f.GetHandle(f.jptm.Info().Source)
	if err != nil {
		return "", err
	}
	buf := make([]byte, 512)
	bufLen := uint32(len(buf))
	status := ntdll.CallWithExpandingBuffer(func() ntdll.NtStatus {
		return ntdll.NtQuerySecurityObject(
			fd,
			windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
			(*ntdll.SecurityDescriptor)(unsafe.Pointer(&buf[0])),
			uint32(len(buf)),
			&bufLen)
	}, &buf, &bufLen)

	if status != ntdll.STATUS_SUCCESS {
		return "", errors.New(fmt.Sprint("failed to query security object", f.jptm.Info().Source, "ntstatus:", status))
	}

	sd := (*windows.SECURITY_DESCRIPTOR)(unsafe.Pointer(&buf[0])) // ntdll.SecurityDescriptor is equivalent
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
	return time.Unix(0, hi.CreationTime.Nanoseconds())
}

func (hi HandleInfo) FileLastWriteTime() time.Time {
	return time.Unix(0, hi.LastWriteTime.Nanoseconds())
}

func (hi HandleInfo) FileAttributes() azfile.FileAttributeFlags {
	// Can't shorthand it because the function name overrides.
	return azfile.FileAttributeFlags(hi.ByHandleFileInformation.FileAttributes)
}
