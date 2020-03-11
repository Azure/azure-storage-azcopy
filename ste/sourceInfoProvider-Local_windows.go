// +build windows

package ste

import (
	"strings"
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
	fd, err := windows.Open(f.jptm.Info().Source, windows.O_RDONLY, 0)
	defer windows.Close(fd)

	if err != nil {
		return windows.ByHandleFileInformation{}, err
	}

	var info windows.ByHandleFileInformation

	err = windows.GetFileInformationByHandle(fd, &info)

	return info, err
}

func (f localFileSourceInfoProvider) GetSMBProperties() (azfile.SMBPropertyHolder, error) {
	info, err := f.getFileInformation()

	return handleInfo{info}, err
}

type handleInfo struct {
	windows.ByHandleFileInformation
}

func (hi handleInfo) FileCreationTime() string {
	return time.Unix(0, hi.CreationTime.Nanoseconds()).Format(azfile.ISO8601)
}

func (hi handleInfo) FileLastWriteTime() string {
	return time.Unix(0, hi.CreationTime.Nanoseconds()).Format(azfile.ISO8601)
}

func (hi handleInfo) FileAttributes() string {
	// Can't shorthand it because the function name overrides.
	return azfile.FileAttributeFlags(hi.ByHandleFileInformation.FileAttributes).String()
}
