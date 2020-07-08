// +build windows

package ste

import (
	"fmt"
	"github.com/Azure/azure-storage-file-go/azfile"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"github.com/Azure/azure-storage-azcopy/common"

	"golang.org/x/sys/windows"
)

// This file implements the windows-triggered smbPropertyAwareDownloader interface.

// works for both folders and files
func (*azureFilesDownloader) PutSMBProperties(sip ISMBPropertyBearingSourceInfoProvider, txInfo TransferInfo) error {
	propHolder, err := sip.GetSMBProperties()
	if err != nil {
		return fmt.Errorf("failed get SMB properties: %w", err)
	}

	destPtr, err := syscall.UTF16PtrFromString(txInfo.Destination)
	if err != nil {
		return fmt.Errorf("failed convert destination string to UTF16 pointer: %w", err)
	}

	setAttributes := func() error {
		attribs := propHolder.FileAttributes()
		// This is a safe conversion.
		err := windows.SetFileAttributes(destPtr, uint32(attribs))
		if err != nil {
			return fmt.Errorf("attempted file set attributes: %w", err)
		}
		return nil
	}

	setDates := func() error {
		smbCreation := propHolder.FileCreationTime()

		// Should we do it here as well??
		smbLastWrite := propHolder.FileLastWriteTime()

		var sa windows.SecurityAttributes
		sa.Length = uint32(unsafe.Sizeof(sa))
		sa.InheritHandle = 1

		// need custom CreateFile call because need FILE_WRITE_ATTRIBUTES
		fd, err := windows.CreateFile(destPtr,
			windows.FILE_WRITE_ATTRIBUTES, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, &sa,
			windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
		if err != nil {
			return fmt.Errorf("attempted file open: %w", err)
		}
		defer windows.Close(fd)

		// windows.NsecToFileTime does the opposite of FileTime.Nanoseconds, and adjusts away the unix epoch for windows.
		smbCreationFileTime := windows.NsecToFiletime(smbCreation.UnixNano())
		smbLastWriteFileTime := windows.NsecToFiletime(smbLastWrite.UnixNano())

		pLastWriteTime := &smbLastWriteFileTime
		if !txInfo.ShouldTransferLastWriteTime() {
			pLastWriteTime = nil
		}

		err = windows.SetFileTime(fd, &smbCreationFileTime, nil, pLastWriteTime)
		if err != nil {
			err = fmt.Errorf("attempted update file times: %w", err)
		}
		return nil
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

var globalSetAclMu = &sync.Mutex{}

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

	var securityInfoFlags windows.SECURITY_INFORMATION = windows.DACL_SECURITY_INFORMATION

	// remove everything down to the if statement to return to xcopy functionality
	// Obtain the destination root and figure out if we're at the top level of the transfer.
	destRoot := a.jptm.GetDestinationRoot()
	relPath, err := filepath.Rel(destRoot, txInfo.Destination)

	if err != nil {
		// This should never ever happen.
		panic("couldn't find relative path from root")
	}

	// Golang did not cooperate with backslashes with filepath.SplitList.
	splitPath := strings.Split(relPath, common.DeterminePathSeparator(relPath))

	// To achieve robocopy like functionality, and maintain the ability to add new permissions in the middle of the copied file tree,
	//     we choose to protect both already protected files at the source, and to protect the entire root folder of the transfer.
	//     Protected files and folders experience no inheritance from their parents (but children do experience inheritance)
	//     To protect the root folder of the transfer, it's not enough to just look at "isTransferRoot" because, in the
	//     case of downloading a complete share, with strip-top-dir = false (i.e. no trailing /* on the URL), the thing at the transfer
	//     root is the share, and currently (April 2019) we can't get permissions for the share itself.  So we have to "lock"/protect
	//     the permissions one level down in that case (i.e. for its children).  But in the case of downloading from a directory (not the share root)
	//     then we DO need the check on isAtTransferRoot.
	isProtectedAtSource := (ctl & windows.SE_DACL_PROTECTED) != 0
	isAtTransferRoot := len(splitPath) == 1

	if isProtectedAtSource || isAtTransferRoot || a.parentIsShareRoot(txInfo.Source) {
		securityInfoFlags |= windows.PROTECTED_DACL_SECURITY_INFORMATION
	}

	var owner *windows.SID = nil
	var group *windows.SID = nil

	if txInfo.PreserveSMBPermissions == common.EPreservePermissionsOption.OwnershipAndACLs() {
		securityInfoFlags |= windows.OWNER_SECURITY_INFORMATION | windows.GROUP_SECURITY_INFORMATION
		owner, _, err = sd.Owner()
		if err != nil {
			return fmt.Errorf("reading owner property of SDDL: %s", err)
		}
		group, _, err = sd.Group()
		if err != nil {
			return fmt.Errorf("reading group property of SDDL: %s", err)
		}
	}

	dacl, _, err := sd.DACL()
	if err != nil {
		return fmt.Errorf("reading DACL property of SDDL: %s", err)
	}

	// Then let's set the security info.
	// We don't know or control the order in which we visit
	// elements of the tree (e.g. we don't know or care whether we are doing A/B before A/B/C).
	// Therefore we must use must use SetNamedSecurityInfo, NOT TreeSetNamedSecurityInfo.
	// (TreeSetNamedSecurityInfo, with TREE_SEC_INFO_RESET, would definitely NOT be safe to call in a situation
	// where we don't know the order in which we visit elements of the tree).
	// TODO: review and potentially remove the use of the global mutex here, once we finish drilling into issues
	//   observed when setting ACLs concurrently on a test UNC share.
	//   BTW, testing indicates no measurable perf difference, between using the mutex and not, in the cases tested.
	//   So it's safe to leave it here for now.
	globalSetAclMu.Lock()
	defer globalSetAclMu.Unlock()
	err = windows.SetNamedSecurityInfo(txInfo.Destination,
		windows.SE_FILE_OBJECT,
		securityInfoFlags,
		owner,
		group,
		dacl,
		nil,
	)

	if err != nil {
		return fmt.Errorf("permissions could not be restored. It may help to add --%s=false to the AzCopy command line (so that ACLS will be preserved but ownership will not). "+
			" Or, if you want to preserve ownership, then run from a elevated command prompt or from an account in the Backup Operators group, and set the '%s' flag."+
			" Error message was: %w",
			common.PreserveOwnerFlagName, common.BackupModeFlagName, err)
	}

	return err
}

// TODO: this method may become obsolete if/when we are able to get permissions from the share root
func (a *azureFilesDownloader) parentIsShareRoot(source string) bool {
	u, err := url.Parse(source)
	if err != nil {
		return false
	}
	f := azfile.NewFileURLParts(*u)
	path := f.DirectoryOrFilePath
	sep := common.DeterminePathSeparator(path)
	splitPath := strings.Split(strings.Trim(path, sep), sep)
	return path != "" && len(splitPath) == 1
}
