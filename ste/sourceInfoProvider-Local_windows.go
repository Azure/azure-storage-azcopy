// +build windows

package ste

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/Azure/azure-storage-file-go/azfile"
	"golang.org/x/sys/windows"

	"github.com/Azure/azure-storage-azcopy/sddl"
)

// This file os-triggers the ISMBPropertyBearingSourceInfoProvider and CustomLocalOpener interfaces on a local SIP.

func (f localFileSourceInfoProvider) Open(path string) (*os.File, error) {
	srcPtr, err := syscall.UTF16PtrFromString(path)
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

	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		return nil, os.ErrInvalid
	}

	return file, nil
}

func (f localFileSourceInfoProvider) GetSDDL() (string, error) {
	// We only need Owner, Group, and DACLs for azure files.
	sd, err := windows.GetNamedSecurityInfo(f.jptm.Info().Source, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)

	if err != nil {
		return "", err
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
	return time.Unix(0, hi.CreationTime.Nanoseconds())
}

func (hi HandleInfo) FileLastWriteTime() time.Time {
	return time.Unix(0, hi.CreationTime.Nanoseconds())
}

func (hi HandleInfo) FileAttributes() azfile.FileAttributeFlags {
	// Can't shorthand it because the function name overrides.
	return azfile.FileAttributeFlags(hi.ByHandleFileInformation.FileAttributes)
}
