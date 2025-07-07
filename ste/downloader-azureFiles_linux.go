//go:build linux
// +build linux

package ste

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/sddl"

	"github.com/pkg/xattr"
	"golang.org/x/sys/unix"
)

// This file implements the linux-triggered smbPropertyAwareDownloader and nfsPropertyAwareDownloader interface.

// works for both folders and files
func (bd *azureFilesDownloader) PutSMBProperties(sip ISMBPropertyBearingSourceInfoProvider, txInfo *TransferInfo) error {
	propHolder, err := sip.GetSMBProperties()
	if err != nil {
		return fmt.Errorf("Failed to get SMB properties for %s: %w", txInfo.Destination, err)
	}

	// Set 32-bit FileAttributes for the file.
	setAttributes := func() error {
		attribs, err := propHolder.FileAttributes()
		if err != nil {
			return fmt.Errorf("attempted to read SMB properties: %w", err)
		}
		// This is a safe conversion.
		attr := FileAttributesToUint32(*attribs)
		xattrbuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(xattrbuf, attr)

		err = xattr.Set(txInfo.Destination, common.CIFS_XATTR_ATTRIB, xattrbuf)
		if err != nil {
			return fmt.Errorf("xattr.Set(%s, %s, 0x%x) failed: %w",
				txInfo.Destination, common.CIFS_XATTR_ATTRIB, attr, err)
		}

		return nil
	}

	// Set creation time and last write time for the file.
	// XXX
	// Note: It makes two SMB calls, one for setting the last write time and one for the create time.
	// XXX
	setDates := func() error {
		smbCreation := propHolder.FileCreationTime()
		smbLastWrite := propHolder.FileLastWriteTime()

		if txInfo.ShouldTransferLastWriteTime(bd.jptm.FromTo()) {
			var ts [2]unix.Timespec

			// Don't set atime.
			ts[0] = unix.Timespec{Sec: unix.UTIME_OMIT, Nsec: unix.UTIME_OMIT}

			// Set mtime to smbLastWrite.
			ts[1] = unix.NsecToTimespec(smbLastWrite.UnixNano())

			// We follow symlink (no unix.AT_SYMLINK_NOFOLLOW) just like the Windows implementation.
			err := unix.UtimesNanoAt(unix.AT_FDCWD, txInfo.Destination, ts[:], 0 /* flags */)
			if err != nil {
				return fmt.Errorf("unix.UtimesNanoAt failed to set mtime for file %s: %w",
					txInfo.Destination, err)
			}
		}

		// Convert time from "nanoseconds since Unix Epoch" to "ticks since Windows Epoch".
		smbCreationTicks := common.UnixNanoToWindowsTicks(smbCreation.UnixNano())

		xattrbuf := make([]byte, 8)
		// This is a safe conversion.
		binary.LittleEndian.PutUint64(xattrbuf, uint64(smbCreationTicks))

		err := xattr.Set(txInfo.Destination, common.CIFS_XATTR_CREATETIME, xattrbuf)
		if err != nil {
			return fmt.Errorf("xattr.Set(%s, %s, 0x%x) failed: %w",
				txInfo.Destination, common.CIFS_XATTR_CREATETIME, smbCreationTicks, err)
		}

		return nil
	}

	// =========== set file times before we set attributes, to make sure the time-setting doesn't
	// reset archive attribute.  There's currently no risk of the attribute-setting messing with the times,
	// because we only set the last (content) "write time", not the last (metadata) "change time" =====

	// TODO: Cifs client may cause the ctime to be updated. Need to think in details.

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
		return fmt.Errorf("Failed to get source SDDL for file %s: %w", txInfo.Destination, err)
	}
	if sddlString == "" {
		// nothing to do (no key returned)
		return errorNoSddlFound
	}

	// We don't need to worry about making the SDDL string portable as this is expected for persistence into Azure Files in the first place.
	sd, err := sddl.SecurityDescriptorFromString(sddlString)
	if err != nil {
		return fmt.Errorf("Failed to parse SDDL (%s) for file %s: %w", sddlString, txInfo.Destination, err)
	}

	ctl, err := sddl.GetControl(sd)
	if err != nil {
		return fmt.Errorf("Error getting control bits: %w", err)
	}

	var securityInfoFlags sddl.SECURITY_INFORMATION = sddl.DACL_SECURITY_INFORMATION

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
	// we choose to protect both already protected files at the source, and to protect the entire root folder of the transfer.
	// Protected files and folders experience no inheritance from their parents (but children do experience inheritance)
	// To protect the root folder of the transfer, it's not enough to just look at "isTransferRoot" because, in the
	// case of downloading a complete share, with strip-top-dir = false (i.e. no trailing /* on the URL), the thing at the transfer
	// root is the share, and currently (April 2019) we can't get permissions for the share itself.  So we have to "lock"/protect
	// the permissions one level down in that case (i.e. for its children).  But in the case of downloading from a directory (not the share root)
	// then we DO need the check on isAtTransferRoot.
	isProtectedAtSource := (ctl & sddl.SE_DACL_PROTECTED) != 0
	isAtTransferRoot := len(splitPath) == 1

	parsedSDDL, err := sddl.ParseSDDL(sddlString)
	if err != nil {
		panic(fmt.Sprintf("Sanity check; SDDL failed to parse (downloader-azureFiles_linux.go), %s", err)) // We already parsed it. This is impossible.
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
		err := sddl.SetControl(sd, sddl.SE_DACL_AUTO_INHERITED|sddl.SE_DACL_AUTO_INHERIT_REQ, sddl.SE_DACL_AUTO_INHERITED|sddl.SE_DACL_AUTO_INHERIT_REQ)
		if err != nil {
			return fmt.Errorf("Failed to persist auto-inherit bit: %w", err)
		}
	}

	if isProtectedAtSource || isAtTransferRoot || a.parentIsShareRoot(txInfo.Source) {
		// TODO: Is setting SE_DACL_PROTECTED control bit equivalent to passing
		//       PROTECTED_DACL_SECURITY_INFORMATION flag to NtSetSecurityObject()?
		// securityInfoFlags |= sddl.PROTECTED_DACL_SECURITY_INFORMATION
		err := sddl.SetControl(sd, sddl.SE_DACL_PROTECTED, sddl.SE_DACL_PROTECTED)
		if err != nil {
			return fmt.Errorf("Failed to set SE_DACL_PROTECTED control bit: %w", err)
		}
	}

	if txInfo.PreservePermissions == common.EPreservePermissionsOption.OwnershipAndACLs() {
		securityInfoFlags |= sddl.OWNER_SECURITY_INFORMATION | sddl.GROUP_SECURITY_INFORMATION
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

	/*
	 * XXX
	 * TODO: Why does Windows open the filehandle with InheritHandle set to 1?
	 * XXX
	 */

	defer globalSetAclMu.Unlock()

	err = sddl.SetSecurityObject(txInfo.Destination, securityInfoFlags, sd)
	if err != nil {
		return fmt.Errorf("permissions could not be restored. It may help to add --%s=false to the AzCopy command line (so that ACLS will be preserved but ownership will not). "+
			" Or, if you want to preserve ownership, then run from a elevated command prompt or from an account in the Backup Operators group, and set the '%s' flag. err=%v",
			common.PreserveOwnerFlagName, common.BackupModeFlagName, err)
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

// works for both folders and files
func (*azureFilesDownloader) PutNFSProperties(sip INFSPropertyBearingSourceInfoProvider,
	txInfo *TransferInfo) error {
	propHolder, err := sip.GetNFSProperties()
	if err != nil {
		return fmt.Errorf("Failed to get NFS properties for %s: %w", txInfo.Destination, err)
	}

	lastWriteTime := propHolder.FileLastWriteTime()

	// Convert the time to Unix timestamp (seconds and nanoseconds)
	lastModifiedTimeSec := lastWriteTime.Unix()        // Seconds part
	lastModifiedTimeNsec := lastWriteTime.Nanosecond() // Nanoseconds part

	// Convert the time to syscall.Timeval type (seconds and microseconds)
	// syscall.Timeval expects seconds and microseconds, so we convert the nanoseconds
	tv := []syscall.Timeval{
		{Sec: lastModifiedTimeSec, Usec: int64(lastModifiedTimeNsec / 1000)}, // Convert nanoseconds to microseconds
		{Sec: lastModifiedTimeSec, Usec: int64(lastModifiedTimeNsec / 1000)}, // Set both atime and mtime to the same timestamp
	}

	// Use syscall.Utimes to set modification times
	err = syscall.Utimes(txInfo.Destination, tv)
	if err != nil {
		return fmt.Errorf("Failed to set lastModifiedTime for %s. Error: %w", txInfo.Destination, err)
	}
	return nil
}

// PutNFSPermissions sets NFS permissions (owner, group, file mode) for a file or folder.
// If none of the permissions are provided, it returns errorNoNFSPermissionsFound.
func (a *azureFilesDownloader) PutNFSPermissions(sip INFSPropertyBearingSourceInfoProvider, txInfo *TransferInfo) error {
	nfsPermissions, err := sip.GetNFSPermissions()
	if err != nil {
		return fmt.Errorf("failed to get NFS permissions for %s: %w", txInfo.Destination, err)
	}

	ownerStr := nfsPermissions.GetOwner()
	groupStr := nfsPermissions.GetGroup()
	filemodeStr := nfsPermissions.GetFileMode()

	if ownerStr == nil && groupStr == nil && filemodeStr == nil {
		return errorNoNFSPermissionsFound
	}

	// Set ownership if owner or group is provided
	uid, gid := -1, -1 // -1 means "do not change" in os.Chown
	if ownerStr != nil {
		if parsedUID, err := strconv.Atoi(*ownerStr); err == nil {
			uid = parsedUID
		} else {
			return fmt.Errorf("invalid owner value for %s: %v", txInfo.Destination, err)
		}
	}
	if groupStr != nil {
		if parsedGID, err := strconv.Atoi(*groupStr); err == nil {
			gid = parsedGID
		} else {
			return fmt.Errorf("invalid group value for %s: %v", txInfo.Destination, err)
		}
	}

	if uid != -1 || gid != -1 {
		if err := os.Chown(txInfo.Destination, uid, gid); err != nil {
			return fmt.Errorf("failed to set owner/group for %s: %w", txInfo.Destination, err)
		}
	}

	// Set file mode if provided
	if filemodeStr != nil {
		parsedMode, err := strconv.ParseUint(*filemodeStr, 8, 32)
		if err != nil {
			return fmt.Errorf("invalid mode value for %s: %v", txInfo.Destination, err)
		}
		if err := os.Chmod(txInfo.Destination, os.FileMode(parsedMode)); err != nil {
			return fmt.Errorf("failed to set file mode for %s: %w", txInfo.Destination, err)
		}
	}

	return nil
}

// PutNFSDefaultPermissions sets default ownership and permissions for NFS shares
// when no explicit NFS permissions are provided by the source.
// Default: 0755 for directories, 0644 for files. Owner/group set to root (UID 0, GID 0).
func (a *azureFilesDownloader) PutNFSDefaultPermissions(sip INFSPropertyBearingSourceInfoProvider, txInfo *TransferInfo) error {
	const (
		defaultFileMode = 0644
		defaultDirMode  = 0755
		defaultUID      = 0 // root
		defaultGID      = 0 // root
	)

	// Determine file mode based on entity type
	var mode os.FileMode
	if txInfo.EntityType == common.EEntityType.Folder() {
		mode = defaultDirMode
	} else {
		mode = defaultFileMode
	}

	// Set ownership
	if err := os.Chown(txInfo.Destination, defaultUID, defaultGID); err != nil {
		return fmt.Errorf("failed to set owner/group for %s: %w", txInfo.Destination, err)
	}

	// Set permissions
	if err := os.Chmod(txInfo.Destination, mode); err != nil {
		return fmt.Errorf("failed to set permissions for %s: %w", txInfo.Destination, err)
	}

	return nil
}
