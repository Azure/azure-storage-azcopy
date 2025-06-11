//go:build linux
// +build linux

package e2etest

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"syscall"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"golang.org/x/sys/unix"
)

func (l LocalObjectResourceManager) PutNFSProperties(a Asserter, properties FileNFSProperties) {
	filePath := l.getWorkingPath()
	// for folder the lastWriteTime will be nil
	if properties.FileLastWriteTime == nil {
		return
	}
	lastWriteTime := properties.FileLastWriteTime

	// Convert the time to Unix timestamp (seconds and nanoseconds)
	lastModifiedTimeSec := lastWriteTime.Unix()        // Seconds partGetProperties
	lastModifiedTimeNsec := lastWriteTime.Nanosecond() // Nanoseconds part

	// Convert the time to syscall.Timeval type (seconds and microseconds)
	// syscall.Timeval expects seconds and microseconds, so we convert the nanoseconds
	tv := []syscall.Timeval{
		{Sec: lastModifiedTimeSec, Usec: int64(lastModifiedTimeNsec / 1000)}, // Convert nanoseconds to microseconds
		{Sec: lastModifiedTimeSec, Usec: int64(lastModifiedTimeNsec / 1000)}, // Set both atime and mtime to the same timestamp
	}

	// Use syscall.Utimes to set modification times
	err := syscall.Utimes(filePath, tv)
	a.NoError(fmt.Sprintf("Failed to set lastModifiedTime for %s", filePath), err)

	return
}

func (l LocalObjectResourceManager) PutNFSPermissions(a Asserter, permissions FileNFSPermissions) {
	filePath := l.getWorkingPath()

	ownerStr := permissions.Owner
	groupStr := permissions.Group
	filemodeStr := permissions.FileMode

	if ownerStr == nil && groupStr == nil && filemodeStr == nil {
		a.NoError(fmt.Sprintf("No permissions found"), errors.New(fmt.Sprintf("No permissions found for %s", filePath)))
		return
	}

	uid := int(-1)
	if ownerStr != nil {
		owner, err := strconv.Atoi(*ownerStr)
		if err != nil {
			a.NoError(fmt.Sprintf("Invalid Owner string for %s:", filePath), err)
			return
		}
		uid = owner
	}

	gid := int(-1)
	if groupStr != nil {
		group, err := strconv.Atoi(*groupStr)
		if err != nil {
			a.NoError(fmt.Sprintf("Invalid Group string for %s:", filePath), err)
			return
		}
		gid = group
	}

	if err := os.Chown(filePath, uid, gid); err != nil {
		a.NoError(fmt.Sprintf("failed to change owner/group for %s:", filePath), err)
		return
	}

	var mode os.FileMode
	if filemodeStr != nil {
		parsedMode, err := strconv.ParseUint(*filemodeStr, 8, 32) // Parse mode as octal
		if err != nil {
			a.NoError(fmt.Sprintf("Invalid FileMode string for %s:", filePath), err)
			return
		}
		mode = os.FileMode(parsedMode)
	}

	if err := os.Chmod(filePath, mode); err != nil {
		a.NoError(fmt.Sprintf("Invalid FileMode string for %s:", filePath), err)
		return
	}
	return
}

func (l LocalObjectResourceManager) GetNFSProperties(a Asserter) ste.TypedNFSPropertyHolder {
	filePath := l.getWorkingPath()
	info, err := common.GetFileInformation(filePath, true)
	a.NoError("get file NFS props", err)
	return ste.HandleInfo{info}
}

func (l LocalObjectResourceManager) GetNFSPermissions(a Asserter) ste.TypedNFSPermissionsHolder {
	filePath := l.getWorkingPath()
	{ // attempt to call statx, if ENOSYS is returned, statx is unavailable
		var stat unix.Statx_t

		statxFlags := unix.AT_STATX_SYNC_AS_STAT
		if l.EntityType() == common.EEntityType.Symlink() {
			statxFlags |= unix.AT_SYMLINK_NOFOLLOW
		}
		// dirfd is a null pointer, because we should only ever be passing relative paths here, and directories will be passed via transferInfo.Source.
		// AT_SYMLINK_NOFOLLOW is not used, because we automagically resolve symlinks. TODO: Add option to not follow symlinks, and use AT_SYMLINK_NOFOLLOW when resolving is disabled.
		err := unix.Statx(0, filePath,
			statxFlags,
			unix.STATX_ALL,
			&stat)

		if err != nil && err != unix.ENOSYS {
			a.NoError("get file NFS permissions", err)
		} else if err == nil {
			return ste.HandleNFSPermissions{ste.StatxTAdapter(stat)}
		}
	}

	var stat unix.Stat_t
	err := unix.Stat(filePath, &stat)
	a.NoError("get file NFS permissions", err)

	a.NoError("get file NFS permissions", err)
	return ste.HandleNFSPermissions{ste.StatTAdapter(stat)}
}

// TODO: Add SMB handling for linux later
func (l LocalObjectResourceManager) GetSDDL(a Asserter) string { return "" }

// TODO: Add SMB handling for linux later
func (l LocalObjectResourceManager) GetSMBProperties(a Asserter) ste.TypedSMBPropertyHolder {
	return nil
}

// TODO: Add SMB handling for linux later
func (l LocalObjectResourceManager) PutSMBProperties(a Asserter, properties FileProperties) {
	return
}

// TODO: Add SMB handling for linux later
func (l LocalObjectResourceManager) PutSDDL(sddlstr string, a Asserter) {}
