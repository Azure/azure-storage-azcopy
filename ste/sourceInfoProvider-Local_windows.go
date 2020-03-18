// +build windows

package ste

import (
	"github.com/Azure/azure-storage-azcopy/common"
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

func (f localFileSourceInfoProvider) GetSMBProperties() (TypedSMBPropertyHolder, error) {
	info, err := common.GetFileInformation(f.jptm.Info().Source)

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
