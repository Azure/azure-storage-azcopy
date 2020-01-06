// +build windows

package ste

import (
	"golang.org/x/sys/windows"

	"github.com/Azure/azure-storage-azcopy/sddl"
)

// This file os-triggers the ISDDLBearingSourceInfoProvider interface on a local SIP.

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

	return fSDDL.String(), nil
}
