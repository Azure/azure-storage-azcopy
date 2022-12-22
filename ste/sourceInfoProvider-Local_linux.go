//go:build linux
// +build linux

package ste

import (
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/aymanjarrousms/azure-storage-azcopy/v10/common"
	"github.com/aymanjarrousms/azure-storage-azcopy/v10/sddl"
	"golang.org/x/sys/unix"
)

func (f localFileSourceInfoProvider) HasUNIXProperties() bool {
	return true
}

func (f localFileSourceInfoProvider) GetUNIXProperties() (common.UnixStatAdapter, error) {
	{ // attempt to call statx, if ENOSYS is returned, statx is unavailable
		var stat unix.Statx_t
		// dirfd is a null pointer, because we should only ever be passing relative paths here, and directories will be passed via transferInfo.Source.
		// AT_SYMLINK_NOFOLLOW is not used, because we automagically resolve symlinks. TODO: Add option to not follow symlinks, and use AT_SYMLINK_NOFOLLOW when resolving is disabled.
		err := unix.Statx(0, f.transferInfo.Source,
			unix.AT_STATX_SYNC_AS_STAT,
			unix.STATX_ALL,
			&stat)

		if err != nil && err != unix.ENOSYS {
			return nil, err
		} else if err == nil {
			return statxTAdapter(stat), nil
		}
	}

	var stat unix.Stat_t
	err := unix.Stat(f.transferInfo.Source, &stat)
	if err != nil {
		return nil, err
	}

	return statTAdapter(stat), nil
}

type statxTAdapter unix.Statx_t

func (s statxTAdapter) Extended() bool {
	return true
}

func (s statxTAdapter) StatxMask() uint32 {
	return s.Mask
}

func (s statxTAdapter) Attribute() uint64 {
	return s.Attributes
}

func (s statxTAdapter) AttributeMask() uint64 {
	return s.Attributes_mask
}

func (s statxTAdapter) BTime() time.Time {
	return time.Unix(s.Btime.Sec, int64(s.Btime.Nsec))
}

func (s statxTAdapter) NLink() uint64 {
	return uint64(s.Nlink)
}

func (s statxTAdapter) Owner() uint32 {
	return s.Uid
}

func (s statxTAdapter) Group() uint32 {
	return s.Gid
}

func (s statxTAdapter) FileMode() uint32 {
	return uint32(s.Mode)
}

func (s statxTAdapter) INode() uint64 {
	return s.Ino
}

func (s statxTAdapter) Device() uint64 {
	return unix.Mkdev(s.Dev_major, s.Dev_minor)
}

func (s statxTAdapter) RDevice() uint64 {
	return unix.Mkdev(s.Rdev_major, s.Rdev_minor)
}

func (s statxTAdapter) ATime() time.Time {
	return time.Unix(s.Atime.Sec, int64(s.Atime.Nsec))
}

func (s statxTAdapter) MTime() time.Time {
	return time.Unix(s.Mtime.Sec, int64(s.Mtime.Nsec))
}

func (s statxTAdapter) CTime() time.Time {
	return time.Unix(s.Ctime.Sec, int64(s.Ctime.Nsec))
}

type statTAdapter unix.Stat_t

func (s statTAdapter) Extended() bool {
	return false
}

func (s statTAdapter) StatxMask() uint32 {
	return 0
}

func (s statTAdapter) Attribute() uint64 {
	return 0
}

func (s statTAdapter) AttributeMask() uint64 {
	return 0
}

func (s statTAdapter) BTime() time.Time {
	return time.Time{}
}

func (s statTAdapter) NLink() uint64 {
	return s.Nlink
}

func (s statTAdapter) Owner() uint32 {
	return s.Uid
}

func (s statTAdapter) Group() uint32 {
	return s.Gid
}

func (s statTAdapter) FileMode() uint32 {
	return s.Mode
}

func (s statTAdapter) INode() uint64 {
	return s.Ino
}

func (s statTAdapter) Device() uint64 {
	return s.Dev
}

func (s statTAdapter) RDevice() uint64 {
	return s.Rdev
}

func (s statTAdapter) ATime() time.Time {
	return time.Unix(s.Atim.Unix())
}

func (s statTAdapter) MTime() time.Time {
	return time.Unix(s.Mtim.Unix())
}

func (s statTAdapter) CTime() time.Time {
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

func (hi HandleInfo) FileAttributes() azfile.FileAttributeFlags {
	// Can't shorthand it because the function name overrides.
	return azfile.FileAttributeFlags(hi.ByHandleFileInformation.FileAttributes)
}
