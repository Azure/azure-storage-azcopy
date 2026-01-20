package common

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const ( // POSIX property metadata
	POSIXNlinkMeta         = "posix_nlink"
	POSIXINodeMeta         = "posix_ino"
	POSIXCTimeMeta         = "posix_ctime"
	LINUXBTimeMeta         = "linux_btime"
	POSIXBlockDeviceMeta   = "is_block_dev" // todo: read & use these
	POSIXCharDeviceMeta    = "is_char_dev"
	POSIXSocketMeta        = "is_socket"
	POSIXFIFOMeta          = "is_fifo"
	POSIXDevMeta           = "posix_dev"
	POSIXRDevMeta          = "posix_rdev"
	POSIXATimeMeta         = "posix_atime"
	POSIXFolderMeta        = "hdi_isfolder" // todo: read & use these
	POSIXSymlinkMeta       = "is_symlink"
	POSIXOwnerMeta         = "posix_owner"
	POSIXGroupMeta         = "posix_group"
	AMLFSOwnerMeta         = "owner"
	AMLFSGroupMeta         = "group"
	POSIXModeMeta          = "permissions"
	POSIXModTimeMeta       = "modtime"
	LINUXAttributeMeta     = "linux_attribute"
	LINUXAttributeMaskMeta = "linux_attribute_mask"
	LINUXStatxMaskMeta     = "linux_statx_mask"
)

var AllLinuxProperties = []string{
	POSIXNlinkMeta,
	POSIXINodeMeta,
	LINUXBTimeMeta,
	POSIXBlockDeviceMeta,
	POSIXCharDeviceMeta,
	POSIXSocketMeta,
	POSIXFIFOMeta,
	POSIXDevMeta,
	POSIXRDevMeta,
	POSIXATimeMeta,
	POSIXFolderMeta,
	POSIXSymlinkMeta,
	POSIXOwnerMeta,
	POSIXGroupMeta,
	POSIXModeMeta,
	LINUXStatxMaskMeta,
	LINUXAttributeMaskMeta,
	POSIXCTimeMeta,
	POSIXModTimeMeta,
	LINUXAttributeMeta,
	AMLFSOwnerMeta,
	AMLFSGroupMeta,
}

//goland:noinspection GoCommentStart
type UnixStatAdapter interface {
	Extended() bool // Did this call come from StatX?

	// Statx properties
	StatxMask() uint32 // Mask determines the availability of all stat/stax properties (except the device ID!). It is only used in statx though.
	Attribute() uint64 // Attribute is masked by AttributeMask.
	AttributeMask() uint64
	BTime() time.Time // BTime may not always be available on every filesystem. It's important to check Mask first!
	// ==========

	// Base Stat properties
	NLink() uint64
	Owner() uint32
	Group() uint32
	FileMode() uint32 // Mode may not always be available to check in a Statx call (though it should be, since we requested it.) Best safe than sorry; check Mask!
	INode() uint64
	Device() uint64
	RDevice() uint64 // RDevice is ONLY useful when Mode has S_IFCHR or S_IFBLK; as those determine if the file is a representitive of a block or character device.
	ATime() time.Time
	MTime() time.Time
	CTime() time.Time
}

type UnixStatContainer struct { // Created for downloads
	statx bool // Does the call contain extended properties (attributes, birthTime)?

	mask       uint32
	attributes uint64
	numLinks   uint64
	ownerUID   uint32
	groupGID   uint32
	mode       uint32

	iNode          uint64
	size           uint64
	attributesMask uint64

	accessTime time.Time // atime
	birthTime  time.Time // btime, statx only
	changeTime time.Time // ctime
	modTime    time.Time // mtime

	repDevID uint64
	devID    uint64
}

func (u UnixStatContainer) Extended() bool {
	return u.statx
}

func (u UnixStatContainer) StatxMask() uint32 {
	return u.mask
}

func (u UnixStatContainer) Attribute() uint64 {
	return u.attributes
}

func (u UnixStatContainer) AttributeMask() uint64 {
	return u.attributesMask
}

func (u UnixStatContainer) BTime() time.Time {
	return u.birthTime
}

func (u UnixStatContainer) NLink() uint64 {
	return u.numLinks
}

func (u UnixStatContainer) Owner() uint32 {
	return u.ownerUID
}

func (u UnixStatContainer) Group() uint32 {
	return u.groupGID
}

func (u UnixStatContainer) FileMode() uint32 {
	return u.mode
}

func (u UnixStatContainer) INode() uint64 {
	return u.iNode
}

func (u UnixStatContainer) Device() uint64 {
	return u.devID
}

func (u UnixStatContainer) RDevice() uint64 {
	return u.repDevID
}

func (u UnixStatContainer) ATime() time.Time {
	return u.accessTime
}

func (u UnixStatContainer) MTime() time.Time {
	return u.modTime
}

func (u UnixStatContainer) CTime() time.Time {
	return u.changeTime
}

// ReadStatFromMetadata is not fault-tolerant. If any given article does not parse,
// it will throw an error instead of continuing on, as it may be considered incorrect to attempt to persist the rest of the data.
// despite this function being used only in Downloads at the current moment, it still attempts to re-create as complete of a UnixStatAdapter as possible.
func ReadStatFromMetadata(metadata Metadata, contentLength int64) (UnixStatAdapter, error) {
	s := UnixStatContainer{size: uint64(contentLength)}

	if mask, ok := TryReadMetadata(metadata, LINUXStatxMaskMeta); ok {
		m, err := strconv.ParseUint(*mask, 10, 32)
		if err != nil {
			return s, err
		}
		s.statx = true
		s.mask = uint32(m)
	}

	// cover additional statx properties here
	if attr, ok := TryReadMetadata(metadata, LINUXAttributeMeta); ok {
		a, err := strconv.ParseUint(*attr, 10, 64)
		if err != nil {
			return s, err
		}
		s.attributes = a
	}

	if attr, ok := TryReadMetadata(metadata, LINUXAttributeMaskMeta); ok {
		a, err := strconv.ParseUint(*attr, 10, 64)
		if err != nil {
			return s, err
		}
		s.attributesMask = a
	}

	if btime, ok := TryReadMetadata(metadata, LINUXBTimeMeta); ok {
		b, err := strconv.ParseInt(*btime, 10, 64)
		if err != nil {
			return s, err
		}
		s.birthTime = time.Unix(0, b)
	}

	// base stat properties
	if nlink, ok := TryReadMetadata(metadata, POSIXNlinkMeta); ok {
		n, err := strconv.ParseUint(*nlink, 10, 64)
		if err != nil {
			return s, err
		}
		s.numLinks = n
	}

	if owner, ok := TryReadMetadata(metadata, POSIXOwnerMeta); ok {
		o, err := strconv.ParseUint(*owner, 10, 32)
		if err != nil {
			return s, err
		}
		s.ownerUID = uint32(o)
	}

	if owner, ok := TryReadMetadata(metadata, AMLFSOwnerMeta); ok {
		o, err := strconv.ParseUint(*owner, 10, 32)
		if err != nil {
			return s, err
		}
		s.ownerUID = uint32(o)
	}

	if group, ok := TryReadMetadata(metadata, POSIXGroupMeta); ok {
		g, err := strconv.ParseUint(*group, 10, 32)
		if err != nil {
			return s, err
		}
		s.groupGID = uint32(g)
	}

	if group, ok := TryReadMetadata(metadata, AMLFSGroupMeta); ok {
		g, err := strconv.ParseUint(*group, 10, 32)
		if err != nil {
			return s, err
		}
		s.groupGID = uint32(g)
	}

	// In cases, the permissions were uploaded in AMLFS style, determine what base to use
	if modeStr, ok := TryReadMetadata(metadata, POSIXModeMeta); ok {
		modeBase := 10

		// AMLFS stores permissions in octal and also sets AMLFS owner/group keys.
		amlfsStyle := false
		if _, ok := TryReadMetadata(metadata, AMLFSOwnerMeta); ok {
			amlfsStyle = true
		}
		if _, ok := TryReadMetadata(metadata, AMLFSGroupMeta); ok {
			amlfsStyle = true
		}
		// AMLFS formatter uses a leading 0 with %04o (e.g., "0755")
		if strings.HasPrefix(*modeStr, "0") {
			amlfsStyle = true
		}

		if amlfsStyle {
			modeBase = 8 // To persist AMLFS style formatting, we store in base 8
		}

		m, err := strconv.ParseUint(*modeStr, modeBase, 32)
		if err != nil {
			return s, err
		}
		s.mode = uint32(m)
	}

	if inode, ok := TryReadMetadata(metadata, POSIXINodeMeta); ok {
		ino, err := strconv.ParseUint(*inode, 10, 64)
		if err != nil {
			return s, err
		}

		s.iNode = ino
	}

	if dev, ok := TryReadMetadata(metadata, POSIXDevMeta); ok {
		d, err := strconv.ParseUint(*dev, 10, 64)
		if err != nil {
			return s, err
		}

		s.devID = d
	}

	if rdev, ok := TryReadMetadata(metadata, POSIXRDevMeta); ok {
		rd, err := strconv.ParseUint(*rdev, 10, 64)
		if err != nil {
			return s, err
		}

		s.repDevID = rd
	}

	if atime, ok := TryReadMetadata(metadata, POSIXATimeMeta); ok {
		at, err := strconv.ParseInt(*atime, 10, 64)
		if err != nil {
			return s, err
		}

		s.accessTime = time.Unix(0, at)
	}

	// ModTime can come in either standard (nanoseconds) or AMLFS (formatted string) format
	// It is stored internally as unix nanoseconds (Time.time type)
	if mtime, ok := TryReadMetadata(metadata, POSIXModTimeMeta); ok {
		mt, err := strconv.ParseInt(*mtime, 10, 64)
		if errors.Is(err, strconv.ErrSyntax) {
			amlfsTime, err := time.Parse(AMLFS_MOD_TIME_LAYOUT, *mtime)
			if err != nil {
				return s, fmt.Errorf("could not parse metadata time: %w", err)
			}
			mt = amlfsTime.UnixNano()
		} else if err != nil {
			return s, err
		}

		s.modTime = time.Unix(0, mt)
	}

	if ctime, ok := TryReadMetadata(metadata, POSIXCTimeMeta); ok {
		ct, err := strconv.ParseInt(*ctime, 10, 64)
		if err != nil {
			return s, err
		}

		s.changeTime = time.Unix(0, ct)
	}

	return s, nil
}

const ( // Values cloned from x/sys/unix to avoid dependency
	STATX_ALL             = 0xfff
	STATX_ATIME           = 0x20
	STATX_ATTR_APPEND     = 0x20
	STATX_ATTR_AUTOMOUNT  = 0x1000
	STATX_ATTR_COMPRESSED = 0x4
	STATX_ATTR_DAX        = 0x200000
	STATX_ATTR_ENCRYPTED  = 0x800
	STATX_ATTR_IMMUTABLE  = 0x10
	STATX_ATTR_MOUNT_ROOT = 0x2000
	STATX_ATTR_NODUMP     = 0x40
	STATX_ATTR_VERITY     = 0x100000
	STATX_BASIC_STATS     = 0x7ff
	STATX_BLOCKS          = 0x400
	STATX_BTIME           = 0x800
	STATX_CTIME           = 0x80
	STATX_GID             = 0x10
	STATX_INO             = 0x100
	STATX_MNT_ID          = 0x1000
	STATX_MODE            = 0x2
	STATX_MTIME           = 0x40
	STATX_NLINK           = 0x4
	STATX_SIZE            = 0x200
	STATX_TYPE            = 0x1
	STATX_UID             = 0x8

	S_IFSOCK = 0xc000
	S_IFBLK  = 0x6000
	S_IFCHR  = 0x2000
	S_IFDIR  = 0x4000
	S_IFIFO  = 0x1000
	S_IFLNK  = 0xa000
	S_IFREG  = 0x8000

	S_IRUSR = 0x400
	S_IWUSR = 0x200
	S_IXUSR = 0x100
	S_IRGRP = 0x040
	S_IWGRP = 0x020
	S_IXGRP = 0x010
	S_IROTH = 0x004
	S_IWOTH = 0x002
	S_IXOTH = 0x001

	S_ALLPERM = 0x777
)

func ClearStatFromBlobMetadata(metadata Metadata) {
	for _, v := range AllLinuxProperties {
		delete(metadata, v)
	}
}

func AddStatToBlobMetadata(s UnixStatAdapter, metadata *SafeMetadata, posixStyle PosixPropertiesStyle) {
	if s == nil {
		return
	}

	// applyMode extracts the file type (symlink, folder etc) from raw Unix file mode and adds the corresponding metadata
	applyMode := func(mode os.FileMode) {
		modes := map[uint32]string{
			S_IFCHR:  POSIXCharDeviceMeta,
			S_IFBLK:  POSIXBlockDeviceMeta,
			S_IFSOCK: POSIXSocketMeta,
			S_IFIFO:  POSIXFIFOMeta,
			S_IFDIR:  POSIXFolderMeta,
			S_IFLNK:  POSIXSymlinkMeta,
		}

		for modeToTest, metaToApply := range modes {
			if mode&os.FileMode(modeToTest) == os.FileMode(modeToTest) {
				TryAddMetadata(metadata, metaToApply, "true")
			}
		}
	}

	if s.Extended() { // try to poll the other properties
		mask := s.StatxMask()

		TryAddMetadata(metadata, LINUXStatxMaskMeta, strconv.FormatUint(uint64(mask), 10))
		TryAddMetadata(metadata, LINUXAttributeMeta, strconv.FormatUint(s.Attribute()&s.AttributeMask(), 10)) // AttributesMask indicates what attributes are supported by the filesystem
		TryAddMetadata(metadata, LINUXAttributeMaskMeta, strconv.FormatUint(s.AttributeMask(), 10))

		if StatXReturned(mask, STATX_BTIME) {
			TryAddMetadata(metadata, LINUXBTimeMeta, strconv.FormatInt(s.BTime().UnixNano(), 10))
		}

		if StatXReturned(mask, STATX_NLINK) {
			TryAddMetadata(metadata, POSIXNlinkMeta, strconv.FormatUint(s.NLink(), 10))
		}

		if StatXReturned(mask, STATX_UID) {
			if posixStyle == AMLFSPosixPropertiesStyle {
				TryAddMetadata(metadata, AMLFSOwnerMeta, strconv.FormatUint(uint64(s.Owner()), 10))
			} else {
				TryAddMetadata(metadata, POSIXOwnerMeta, strconv.FormatUint(uint64(s.Owner()), 10))
			}
		}

		if StatXReturned(mask, STATX_GID) {
			if posixStyle == AMLFSPosixPropertiesStyle {
				TryAddMetadata(metadata, AMLFSGroupMeta, strconv.FormatUint(uint64(s.Group()), 10))
			} else {
				TryAddMetadata(metadata, POSIXGroupMeta, strconv.FormatUint(uint64(s.Group()), 10))
			}
		}

		if StatXReturned(mask, STATX_MODE) {
			if posixStyle == AMLFSPosixPropertiesStyle {
				permissions := fmt.Sprintf("%04o", uint64(s.FileMode())&0777) // AMLFS uses octal and only needs permission bits. Mask off higher order bits.
				TryAddMetadata(metadata, POSIXModeMeta, permissions)
				applyMode(os.FileMode(s.FileMode()))
			} else {
				TryAddMetadata(metadata, POSIXModeMeta, strconv.FormatUint(uint64(s.FileMode()), 10))
				applyMode(os.FileMode(s.FileMode()))
			}
		}

		if StatXReturned(mask, STATX_INO) {
			TryAddMetadata(metadata, POSIXINodeMeta, strconv.FormatUint(s.INode(), 10))
		}

		// This is not optional.
		TryAddMetadata(metadata, POSIXDevMeta, strconv.FormatUint(s.Device(), 10))

		if StatXReturned(mask, STATX_MODE) && ((s.FileMode()&S_IFCHR) == S_IFCHR || (s.FileMode()&S_IFBLK) == S_IFBLK) {
			TryAddMetadata(metadata, POSIXRDevMeta, strconv.FormatUint(s.RDevice(), 10))
		}

		// Sometimes, the filesystem will return ATime, but the vfs layer will overwrite it in the mask. It's still accurate, so we can use it.
		// e.g. ext4+noatime will still return & properly store atimes, but won't be included in the statx mask.
		if StatXReturned(mask, STATX_ATIME) || s.ATime().UnixNano() > 0 {
			TryAddMetadata(metadata, POSIXATimeMeta, strconv.FormatInt(s.ATime().UnixNano(), 10))
		}

		if StatXReturned(mask, STATX_MTIME) {
			if posixStyle == AMLFSPosixPropertiesStyle {
				TryAddMetadata(metadata, POSIXModTimeMeta, s.MTime().Format(AMLFS_MOD_TIME_LAYOUT))
			} else {
				TryAddMetadata(metadata, POSIXModTimeMeta, strconv.FormatInt(s.MTime().UnixNano(), 10))
			}
		}

		if StatXReturned(mask, STATX_CTIME) {
			TryAddMetadata(metadata, POSIXCTimeMeta, strconv.FormatInt(s.CTime().UnixNano(), 10))
		}
	} else {
		TryAddMetadata(metadata, POSIXNlinkMeta, strconv.FormatUint(s.NLink(), 10))

		// For non-statx (just stat) still respect the posix style
		if posixStyle == AMLFSPosixPropertiesStyle {
			TryAddMetadata(metadata, AMLFSOwnerMeta, strconv.FormatUint(uint64(s.Owner()), 10))
			TryAddMetadata(metadata, AMLFSGroupMeta, strconv.FormatUint(uint64(s.Group()), 10))

			permissions := fmt.Sprintf("%04o", uint64(s.FileMode())&0777) // AMLFS: octal perms only
			TryAddMetadata(metadata, POSIXModeMeta, permissions)
			TryAddMetadata(metadata, POSIXModTimeMeta, s.MTime().Format(AMLFS_MOD_TIME_LAYOUT))

		} else {
			// Use standard style
			TryAddMetadata(metadata, POSIXOwnerMeta, strconv.FormatUint(uint64(s.Owner()), 10))
			TryAddMetadata(metadata, POSIXGroupMeta, strconv.FormatUint(uint64(s.Group()), 10))
			TryAddMetadata(metadata, POSIXModeMeta, strconv.FormatUint(uint64(s.FileMode()), 10))
			TryAddMetadata(metadata, POSIXModTimeMeta, strconv.FormatInt(s.MTime().UnixNano(), 10))
		}
		applyMode(os.FileMode(s.FileMode()))
		TryAddMetadata(metadata, POSIXINodeMeta, strconv.FormatUint(s.INode(), 10))
		TryAddMetadata(metadata, POSIXDevMeta, strconv.FormatUint(s.Device(), 10))

		if (s.FileMode()&S_IFCHR) == S_IFCHR || (s.FileMode()&S_IFBLK) == S_IFBLK { // this is not relevant unless the file is a block or character device.
			TryAddMetadata(metadata, POSIXRDevMeta, strconv.FormatUint(s.RDevice(), 10))
		}

		TryAddMetadata(metadata, POSIXATimeMeta, strconv.FormatInt(s.ATime().UnixNano(), 10))
		TryAddMetadata(metadata, POSIXCTimeMeta, strconv.FormatInt(s.CTime().UnixNano(), 10))
	}
}

func StatXReturned(mask uint32, want uint32) bool {
	return (mask & want) == want
}
