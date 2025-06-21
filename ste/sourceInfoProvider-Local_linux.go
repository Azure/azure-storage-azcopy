//go:build linux
// +build linux

package ste

import (
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/sddl"
	"golang.org/x/sys/unix"
)

func (f localFileSourceInfoProvider) HasUNIXProperties() bool {
	return true
}

func (f localFileSourceInfoProvider) GetUNIXProperties() (common.UnixStatAdapter, error) {
	{ // attempt to call statx, if ENOSYS is returned, statx is unavailable
		var stat unix.Statx_t

		statxFlags := unix.AT_STATX_SYNC_AS_STAT
		if f.EntityType() == common.EEntityType.Symlink() {
			statxFlags |= unix.AT_SYMLINK_NOFOLLOW
		}
		// dirfd is a null pointer, because we should only ever be passing relative paths here, and directories will be passed via transferInfo.Source.
		// AT_SYMLINK_NOFOLLOW is not used, because we automagically resolve symlinks. TODO: Add option to not follow symlinks, and use AT_SYMLINK_NOFOLLOW when resolving is disabled.
		err := unix.Statx(0, f.transferInfo.Source,
			statxFlags,
			unix.STATX_ALL,
			&stat)

		if err != nil && err != unix.ENOSYS {
			return nil, err
		} else if err == nil {
			return StatxTAdapter(stat), nil
		}
	}

	var stat unix.Stat_t
	err := unix.Stat(f.transferInfo.Source, &stat)
	if err != nil {
		return nil, err
	}

	return StatTAdapter(stat), nil
}

type StatxTAdapter unix.Statx_t

func (s StatxTAdapter) Extended() bool {
	return true
}

func (s StatxTAdapter) StatxMask() uint32 {
	return s.Mask
}

func (s StatxTAdapter) Attribute() uint64 {
	return s.Attributes
}

func (s StatxTAdapter) AttributeMask() uint64 {
	return s.Attributes_mask
}

func (s StatxTAdapter) BTime() time.Time {
	return time.Unix(s.Btime.Sec, int64(s.Btime.Nsec))
}

func (s StatxTAdapter) NLink() uint64 {
	return uint64(s.Nlink)
}

func (s StatxTAdapter) Owner() uint32 {
	return s.Uid
}

func (s StatxTAdapter) Group() uint32 {
	return s.Gid
}

func (s StatxTAdapter) FileMode() uint32 {
	return uint32(s.Mode)
}

func (s StatxTAdapter) INode() uint64 {
	return s.Ino
}

func (s StatxTAdapter) Device() uint64 {
	return unix.Mkdev(s.Dev_major, s.Dev_minor)
}

func (s StatxTAdapter) RDevice() uint64 {
	return unix.Mkdev(s.Rdev_major, s.Rdev_minor)
}

func (s StatxTAdapter) ATime() time.Time {
	return time.Unix(s.Atime.Sec, int64(s.Atime.Nsec))
}

func (s StatxTAdapter) MTime() time.Time {
	return time.Unix(s.Mtime.Sec, int64(s.Mtime.Nsec))
}

func (s StatxTAdapter) CTime() time.Time {
	return time.Unix(s.Ctime.Sec, int64(s.Ctime.Nsec))
}

type StatTAdapter unix.Stat_t

func (s StatTAdapter) Extended() bool {
	return false
}

func (s StatTAdapter) StatxMask() uint32 {
	return 0
}

func (s StatTAdapter) Attribute() uint64 {
	return 0
}

func (s StatTAdapter) AttributeMask() uint64 {
	return 0
}

func (s StatTAdapter) BTime() time.Time {
	return time.Time{}
}

func (s StatTAdapter) NLink() uint64 {
	return uint64(s.Nlink) // On amd64, this is a uint64. On arm64, this is a uint32. Do not remove this typecast.
}

func (s StatTAdapter) Owner() uint32 {
	return s.Uid
}

func (s StatTAdapter) Group() uint32 {
	return s.Gid
}

func (s StatTAdapter) FileMode() uint32 {
	return s.Mode
}

func (s StatTAdapter) INode() uint64 {
	return s.Ino
}

func (s StatTAdapter) Device() uint64 {
	return s.Dev
}

func (s StatTAdapter) RDevice() uint64 {
	return s.Rdev
}

func (s StatTAdapter) ATime() time.Time {
	return time.Unix(s.Atim.Unix())
}

func (s StatTAdapter) MTime() time.Time {
	return time.Unix(s.Mtim.Unix())
}

func (s StatTAdapter) CTime() time.Time {
	return time.Unix(s.Ctim.Unix())
}

// This file os-triggers the ISMBPropertyBearingSourceInfoProvider interface on a local SIP.
// Note: Linux SIP doesn't implement the ICustomLocalOpener since it doesn't need to do anything special, unlike
//       Windows where we need to pass FILE_FLAG_BACKUP_SEMANTICS flag for opening file.

func (f localFileSourceInfoProvider) GetSDDL() (string, error) {
	// We only need Owner, Group, and DACLs for azure files, CIFS_XATTR_CIFS_NTSD gets us that.
	const securityInfoFlags sddl.SECURITY_INFORMATION = sddl.DACL_SECURITY_INFORMATION | sddl.OWNER_SECURITY_INFORMATION | sddl.GROUP_SECURITY_INFORMATION

	// Query the Security Descriptor object for the given file.
	sd, err := sddl.QuerySecurityObject(f.jptm.Info().Source, securityInfoFlags)
	if err != nil {
		return "", fmt.Errorf("sddl.QuerySecurityObject(%s, 0x%x) failed: %w",
			f.jptm.Info().Source, securityInfoFlags, err)
	}

	// Convert the binary Security Descriptor to string in SDDL format.
	// This is the Windows equivalent of ConvertSecurityDescriptorToStringSecurityDescriptorW().
	sdStr, err := sddl.SecurityDescriptorToString(sd)
	if err != nil {
		// Panic, as it's unexpected and we would want to know.
		panic(fmt.Errorf("Cannot parse binary Security Descriptor returned by QuerySecurityObject(%s, 0x%x): %v", f.jptm.Info().Source, securityInfoFlags, err))
	}

	fSDDL, err := sddl.ParseSDDL(sdStr)
	if err != nil {
		return "", fmt.Errorf("sddl.ParseSDDL(%s) failed: %w", sdStr, err)
	}

	if strings.TrimSpace(fSDDL.String()) != strings.TrimSpace(sdStr) {
		panic("SDDL sanity check failed (parsed string output != original string)")
	}

	return fSDDL.PortableString(), nil
}

func (f localFileSourceInfoProvider) GetSMBProperties() (TypedSMBPropertyHolder, error) {
	info, err := common.GetFileInformation(f.jptm.Info().Source)

	return HandleInfo{info}, err
}

type HandleInfo struct {
	common.ByHandleFileInformation
}

func (hi HandleInfo) FileCreationTime() time.Time {
	// This returns nanoseconds since Unix Epoch.
	return time.Unix(0, hi.CreationTime.Nanoseconds())
}

func (hi HandleInfo) FileLastWriteTime() time.Time {
	// This returns nanoseconds since Unix Epoch.
	return time.Unix(0, hi.LastWriteTime.Nanoseconds())
}

func (hi HandleInfo) FileChangeTime() time.Time {
	// This returns nanoseconds since Unix Epoch.
	return time.Unix(0, hi.ChangeTime.Nanoseconds())
}

func (hi HandleInfo) FileAttributes() (*file.NTFSFileAttributes, error) {
	// Can't shorthand it because the function name overrides.
	return FileAttributesFromUint32(hi.ByHandleFileInformation.FileAttributes)
}
