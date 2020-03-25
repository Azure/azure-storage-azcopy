// +build windows

package ste

import (
	"fmt"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/Azure/azure-storage-azcopy/common"

	"golang.org/x/sys/windows"
)

// This file implements the windows-triggered smbPropertyAwareDownloader interface.

// works for both folders and files
func (*azureFilesDownloader) PutSMBProperties(sip ISMBPropertyBearingSourceInfoProvider, txInfo TransferInfo) error {
	propHolder, err := sip.GetSMBProperties()
	if err != nil {
		return err
	}

	destPtr, err := syscall.UTF16PtrFromString(txInfo.Destination)
	if err != nil {
		return err
	}

	setAttributes := func() error {
		attribs := propHolder.FileAttributes()
		// This is a safe conversion.
		return windows.SetFileAttributes(destPtr, uint32(attribs))
	}

	setDates := func() error {
		smbCreation := propHolder.FileCreationTime()

		// Should we do it here as well??
		smbLastWrite := propHolder.FileLastWriteTime()

		// need custom CreateFile call because need FILE_WRITE_ATTRIBUTES
		fd, err := windows.CreateFile(destPtr,
			windows.FILE_WRITE_ATTRIBUTES, windows.FILE_SHARE_READ, nil,
			windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
		if err != nil {
			return err
		}
		defer windows.Close(fd)

		// windows.NsecToFileTime does the opposite of FileTime.Nanoseconds, and adjusts away the unix epoch for windows.
		smbCreationFileTime := windows.NsecToFiletime(smbCreation.UnixNano())
		smbLastWriteFileTime := windows.NsecToFiletime(smbLastWrite.UnixNano())

		pLastWriteTime := &smbLastWriteFileTime
		if !txInfo.ShouldTransferLastWriteTime() {
			pLastWriteTime = nil
		}

		return windows.SetFileTime(fd, &smbCreationFileTime, nil, pLastWriteTime)
	}

	// =========== set file times before we set attributes, to make sure the time-setting doesn't
	// reset archive attribute.  There's currently no risk of the attribute-setting messing with the times,
	// because we only set the last (content) "write time", not the last (metadata) "change time" =====
	err = setDates()
	if err != nil {
		return err
	}
	return setAttributes()
}

// works for both folders and files
func (a *azureFilesDownloader) PutSDDL(sip ISMBPropertyBearingSourceInfoProvider, txInfo TransferInfo) error {
	// Let's start by getting our SDDL and parsing it.
	sddlString, err := sip.GetSDDL()
	// TODO: be better at handling these errors.
	// GetSDDL will fail on a file-level SAS token.
	if err != nil {
		return fmt.Errorf("getting source SDDL: %s", err)
	}
	if sddlString == "" {
		// nothing to do (no key returned)
		return errorNoSddlFound
	}

	// We don't need to worry about making the SDDL string portable as this is expected for persistence into Azure Files in the first place.
	// Let's have sys/x/windows parse it.
	sd, err := windows.SecurityDescriptorFromString(sddlString)
	if err != nil {
		return fmt.Errorf("parsing SDDL: %s", err)
	}

	ctl, _, err := sd.Control()
	if err != nil {
		return fmt.Errorf("getting control bits: %w", err)
	}

	var securityInfoFlags windows.SECURITY_INFORMATION = windows.OWNER_SECURITY_INFORMATION | windows.GROUP_SECURITY_INFORMATION | windows.DACL_SECURITY_INFORMATION

	// remove everything down to the if statement to return to xcopy functionality
	// Obtain the destination root and figure out if we're at the top level of the transfer.
	plan := a.jptm.Plan()
	destRoot := string(plan.DestinationRoot[:plan.DestinationRootLength])
	relPath, err := filepath.Rel(destRoot, txInfo.Destination)

	if err != nil {
		// This should never ever happen.
		panic("couldn't find relative path from root")
	}

	// Golang did not cooperate with backslashes with filepath.SplitList.
	splitPath := strings.Split(relPath, common.DeterminePathSeparator(relPath))

	// Protected ACLs see no inheritance whatsoever.
	// remove the second half of the if statement to return to xcopy functionality
	if (ctl&windows.SE_DACL_PROTECTED) != 0 || len(splitPath) == 1 {
		securityInfoFlags |= windows.PROTECTED_DACL_SECURITY_INFORMATION
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
		securityInfoFlags,
		owner,
		group,
		dacl,
		nil,
	)

	if err != nil {
		return fmt.Errorf("permissions could not be restored. It may help to run from a elevated command prompt, and set the '%s' flag. Error message was: %w",
			common.BackupModeFlagName, err)
	}

	return err
}
