// +build windows

package ste

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/Azure/azure-storage-file-go/azfile"
	"golang.org/x/sys/windows"

	"github.com/Azure/azure-storage-azcopy/sddl"
)

// This file os-triggers the ISMBPropertyBearingSourceInfoProvider interface on a local SIP.

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

func (f localFileSourceInfoProvider) getFileInformation() (windows.ByHandleFileInformation, error) {
	tfrInfo := f.jptm.Info()
	backupSemantics := tfrInfo.EntityType == common.EEntityType.Folder() // Windows API requires that this flag is set, when reading directory properties

	sysfd, err := common.OpenWithOptions(tfrInfo.Source, windows.O_RDONLY, 0, false, backupSemantics)
	fd := castToWinHandle(sysfd)
	defer windows.Close(fd)

	if err != nil {
		return windows.ByHandleFileInformation{}, err
	}

	var info windows.ByHandleFileInformation

	err = windows.GetFileInformationByHandle(fd, &info)

	return info, err
}

func (f localFileSourceInfoProvider) GetSMBProperties() (TypedSMBPropertyHolder, error) {
	info, err := f.getFileInformation()

	return handleInfo{info}, err
}

type handleInfo struct {
	windows.ByHandleFileInformation
}

func (hi handleInfo) FileCreationTime() time.Time {
	return time.Unix(0, hi.CreationTime.Nanoseconds())
}

func (hi handleInfo) FileLastWriteTime() time.Time {
	return time.Unix(0, hi.CreationTime.Nanoseconds())
}

func (hi handleInfo) FileAttributes() azfile.FileAttributeFlags {
	// Can't shorthand it because the function name overrides.
	return azfile.FileAttributeFlags(hi.ByHandleFileInformation.FileAttributes)
}

var castToWinHandle func(handle syscall.Handle) windows.Handle

func init() {
	// check that we really can cast (safely) between these handle types
	// Because, in theory, if one was ever redefined to be a different width, Go would still allow the cast
	// but it it would be unsafe.  That's MOST unlikely to ever happen, but we may as well check it.
	var s syscall.Handle
	var w syscall.Handle
	if reflect.TypeOf(s).Bits() != reflect.TypeOf(w).Bits() {
		panic("unsafe handle cast")
	}
	castToWinHandle = func(sh syscall.Handle) windows.Handle {
		return windows.Handle(sh)
	}
}
