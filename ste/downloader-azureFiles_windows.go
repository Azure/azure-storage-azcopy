// +build windows

package ste

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// This file implements the windows-triggered sddlAwareDownloader interface.

func (bd *azureFilesDownloader) PutSDDL(sip ISDDLBearingSourceInfoProvider, txInfo TransferInfo) error {
	// Let's start by getting our SDDL and parsing it.
	sddlString, err := sip.GetSDDL()
	// TODO: be better at handling these errors.
	// GetSDDL will fail on a file-level SAS token.
	if err != nil {
		return fmt.Errorf("getting source SDDL: %s", err)
	}

	// We don't need to worry about making the SDDL string portable as this is expected for persistence into Azure Files in the first place.
	// Let's have sys/x/windows parse it.
	sd, err := windows.SecurityDescriptorFromString(sddlString)
	if err != nil {
		return fmt.Errorf("parsing SDDL: %s", err)
	}

	owner, _, err := sd.Owner()
	if err != nil {
		return fmt.Errorf("reading owner property of SDDL: %s", err)
	}

	group, _, err := sd.Group()
	if err != nil {
		return fmt.Errorf("reading group property of SDDL: %s", err)
	}

	dacl, _, err := sd.DACL()
	if err != nil {
		return fmt.Errorf("reading DACL property of SDDL: %s", err)
	}

	// Then let's set the security info.
	err = windows.SetNamedSecurityInfo(txInfo.Destination,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
		owner,
		group,
		dacl,
		nil,
	)

	return err
}
