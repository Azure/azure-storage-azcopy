//go:build windows
// +build windows

package ste

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/sddl"
	"github.com/hillu/go-ntdll"

	"golang.org/x/sys/windows"
)

// This file implements the windows-triggered smbPropertyAwareDownloader interface.

// works for both folders and files
func (bd *azureFilesDownloader) PutSMBProperties(sip ISMBPropertyBearingSourceInfoProvider, txInfo *TransferInfo) error {
	if txInfo.Destination == common.Dev_Null {
		return nil // Do nothing.
	}

	propHolder, err := sip.GetSMBProperties()
	if err != nil {
		return fmt.Errorf("failed get SMB properties: %w", err)
	}

	destPtr, err := syscall.UTF16PtrFromString(txInfo.Destination)
	if err != nil {
		return fmt.Errorf("failed convert destination string to UTF16 pointer: %w", err)
	}

	fromTo := bd.jptm.FromTo()
	if fromTo.From() == common.ELocation.File() { // Files SDK can panic when the service hands it something unexpected!
		defer func() { // recover from potential panics and output raw properties for debug purposes; will cover the return call to setAttributes
			if panicerr := recover(); panicerr != nil {
				attr, _ := propHolder.FileAttributes()
				lwt := propHolder.FileLastWriteTime()
				fct := propHolder.FileCreationTime()

				err = fmt.Errorf("failed to read SMB properties (%w)! Raw data: attr: `%s` lwt: `%s`, fct: `%s`", err, attr, lwt, fct)
			}
		}()
	}

	setAttributes := func() error {
		attribs, _ := propHolder.FileAttributes()
		// This is a safe conversion.
		err = windows.SetFileAttributes(destPtr, FileAttributesToUint32(*attribs))
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
		if !txInfo.ShouldTransferLastWriteTime(bd.jptm.FromTo()) {
			pLastWriteTime = nil
		}

		err = windows.SetFileTime(fd, &smbCreationFileTime, nil, pLastWriteTime)
		if err != nil {
			err = fmt.Errorf("attempted update file times: %w", err) //nolint:staticcheck,ineffassign
			// TODO: return here on error? or ignore
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
func (a *azureFilesDownloader) PutSDDL(sip ISMBPropertyBearingSourceInfoProvider, txInfo *TransferInfo) error {
	if txInfo.Destination == common.Dev_Null {
		return nil // Do nothing.
	}

	// Let's start by getting our SDDL and parsing it.
	sddlString, err := sip.GetSDDL()

	// There's nothing to set.
	if sddlString == "" {
		return nil
	}

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
	// Obtain the destination root and figure out if  we're at the top level of the transfer.
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

	parsedSDDL, err := sddl.ParseSDDL(sddlString)

	if err != nil {
		panic(fmt.Sprintf("Sanity check; SDDL failed to parse (downloader-azureFiles_windows.go), %s", err)) // We already parsed it. This is impossible.
	}

	/*
		via Jason Shay:
		One exception is related to the "AI" flag.
		If you provide a descriptor to NtSetSecurityObject with just AI (SE_DACL_AUTO_INHERITED), it will not be stored.
		If you provide it with SE_DACL_AUTO_INHERITED AND SE_DACL_AUTO_INHERIT_REQ, then SE_DACL_AUTO_INHERITED will be stored (note the _REQ flag is never stored)

		The REST API for Azure Files will see the "AI" in the SDDL, and will do the _REQ flag work in the background for you.
	*/
	if strings.Contains(parsedSDDL.DACL.Flags, "AI") {
		// set the DACL auto-inherit flag, since Windows didn't pick it up for some reason...
		err := sd.SetControl(windows.SE_DACL_AUTO_INHERITED|windows.SE_DACL_AUTO_INHERIT_REQ, windows.SE_DACL_AUTO_INHERITED|windows.SE_DACL_AUTO_INHERIT_REQ)
		if err != nil {
			return fmt.Errorf("tried to persist auto-inherit bit: %w", err)
		}
	}

	if isProtectedAtSource || isAtTransferRoot || a.parentIsShareRoot(txInfo.Source) {
		securityInfoFlags |= windows.PROTECTED_DACL_SECURITY_INFORMATION
	}

	if txInfo.PreserveSMBPermissions == common.EPreservePermissionsOption.OwnershipAndACLs() {
		securityInfoFlags |= windows.OWNER_SECURITY_INFORMATION | windows.GROUP_SECURITY_INFORMATION
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

	destPtr, err := syscall.UTF16PtrFromString(txInfo.Destination)
	if err != nil {
		return fmt.Errorf("failed convert destination string to UTF16 pointer: %w", err)
	}

	var sa windows.SecurityAttributes
	sa.Length = uint32(unsafe.Sizeof(sa))
	sa.InheritHandle = 1

	// need WRITE_DAC and WRITE_OWNER for SDDL preservation, no need for ACCESS_SYSTEM_SECURITY, since we don't back up SACLs.
	fd, err := windows.CreateFile(destPtr, windows.FILE_GENERIC_WRITE|windows.WRITE_DAC|windows.WRITE_OWNER, windows.FILE_SHARE_WRITE, &sa,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return fmt.Errorf("attempted file open: %w", err)
	}
	defer windows.Close(fd)

	defer globalSetAclMu.Unlock()
	status := ntdll.NtSetSecurityObject(
		ntdll.Handle(fd),
		ntdll.SecurityInformationT(securityInfoFlags),
		// unsafe but actually safe conversion
		(*ntdll.SecurityDescriptor)(unsafe.Pointer(sd)),
	)

	if status != ntdll.STATUS_SUCCESS {
		return fmt.Errorf("permissions could not be restored. It may help to add --%s=false to the AzCopy command line (so that ACLS will be preserved but ownership will not). "+
			" Or, if you want to preserve ownership, then run from a elevated command prompt or from an account in the Backup Operators group, and set the '%s' flag."+
			" NT status was: %s",
			common.PreserveOwnerFlagName, common.BackupModeFlagName, status)
	}

	return err
}

// TODO: this method may become obsolete if/when we are able to get permissions from the share root
func (a *azureFilesDownloader) parentIsShareRoot(source string) bool {
	fileURLParts, err := file.ParseURL(source)
	if err != nil {
		return false
	}
	path := fileURLParts.DirectoryOrFilePath
	sep := common.DeterminePathSeparator(path)
	splitPath := strings.Split(strings.Trim(path, sep), sep)
	return path != "" && len(splitPath) == 1
}
