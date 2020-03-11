// +build windows

package ste

import (
	"fmt"
	"syscall"
	"time"

	"github.com/Azure/azure-storage-file-go/azfile"
	"golang.org/x/sys/windows"
)

// This file implements the windows-triggered smbPropertyAwareDownloader interface.

func (bd *azureFilesDownloader) PutFileSMBProperties(sip ISMBPropertyBearingSourceInfoProvider, txInfo TransferInfo) error {
	propHolder, err := sip.GetSMBProperties()

	if err != nil {
		return err
	}

	attribs := azfile.ParseFileAttributeFlagsString(propHolder.FileAttributes())

	destPtr, err := syscall.UTF16PtrFromString(txInfo.Destination)

	if err != nil {
		return err
	}

	// This is a safe conversion.
	err = windows.SetFileAttributes(destPtr, uint32(attribs))

	if err != nil {
		return err
	}

	// =========== set file times ===========

	smbCreation, err := time.Parse(azfile.ISO8601, propHolder.FileCreationTime())

	if err != nil {
		return err
	}

	// Should we do it here as well??
	smbLastWrite, err := time.Parse(azfile.ISO8601, propHolder.FileLastWriteTime())

	if err != nil {
		return err
	}

	fd, err := windows.Open(txInfo.Destination, windows.O_RDWR, windows.S_IWRITE)

	if err != nil {
		return err
	}

	defer windows.Close(fd)

	// windows.NsecToFileTime does the opposite of FileTime.Nanoseconds, and adjusts away the unix epoch for windows.
	smbCreationFileTime := windows.NsecToFiletime(smbCreation.UnixNano())
	smbLastWriteFileTime := windows.NsecToFiletime(smbLastWrite.UnixNano())

	err = windows.SetFileTime(fd, &smbCreationFileTime, nil, &smbLastWriteFileTime)

	return err
}

func (bd *azureFilesDownloader) PutSDDL(sip ISMBPropertyBearingSourceInfoProvider, txInfo TransferInfo) error {
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
