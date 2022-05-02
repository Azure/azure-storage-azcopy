package ste

import (
	"github.com/Azure/azure-storage-blob-go/azblob"
	"strconv"
	"time"
)

const ( // POSIX property metadata
	POSIXNlinkMeta         = "posix-nlink"
	POSIXINodeMeta         = "posix-ino"
	POSIXCTimeMeta         = "posix-ctime"
	LINUXBTimeMeta         = "linux-btime"
	POSIXBlockDeviceMeta   = "is_block_dev"
	POSIXCharDeviceMeta    = "is_char_dev"
	POSIXSocketMeta        = "is_socket"
	POSIXFIFOMeta          = "is_fifo"
	POSIXDevMeta           = "posix-dev"
	POSIXRDevMeta          = "posix-rdev"
	POSIXATimeMeta         = "posix-atime"
	POSIXFolderMeta        = "hdi_isfolder"
	POSIXSymlinkMeta       = "is_symlink"
	POSIXOwnerMeta         = "posix-owner"
	POSIXGroupMeta         = "posix-group"
	POSIXModeMeta          = "permissions"
	POSIXModTimeMeta       = "modtime"
	LINUXAttributeMeta     = "linux-attribute"
	LINUXAttributeMaskMeta = "linux-attribute-mask"
	LINUXStatxMaskMeta     = "linux-statx-mask"
)

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
	statx bool // Was the call based on unix.Stat or unix.Statx?

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
func ReadStatFromMetadata(metadata azblob.Metadata, contentLength uint) (UnixStatAdapter, error) {
	s := UnixStatContainer{}

	if mask, ok := metadata[LINUXStatxMaskMeta]; ok {
		m, err := strconv.ParseUint(mask, 10, 32)
		if err != nil {
			return s, err
		}
		s.statx = true
		s.mask = uint32(m)
	}

	if s.statx {
		// cover additional statx properties here
		if attr, ok := metadata[LINUXAttributeMeta]; ok {
			a, err := strconv.ParseUint(attr, 10, 64)
			if err != nil {
				return s, err
			}
			s.attributes = a
		}

		if attr, ok := metadata[LINUXAttributeMaskMeta]; ok {
			a, err := strconv.ParseUint(attr, 10, 64)
			if err != nil {
				return s, err
			}
			s.attributesMask = a
		}

		if btime, ok := metadata[LINUXBTimeMeta]; ok {
			b, err := strconv.ParseInt(btime, 10, 64)
			if err != nil {
				return s, err
			}
			s.birthTime = time.Unix(b, 0)
		}
	}

	if nlink, ok := metadata[POSIXNlinkMeta]; ok {
		n, err := strconv.ParseUint(nlink, 10, 64)
		if err != nil {
			return s, err
		}
		s.numLinks = n
	}

	if owner, ok := metadata[POSIXOwnerMeta]; ok {
		o, err := strconv.ParseUint(owner, 10, 32)
		if err != nil {
			return s, err
		}
		s.ownerUID = uint32(o)
	}

	if group, ok := metadata[POSIXGroupMeta]; ok {
		g, err := strconv.ParseUint(group, 10, 32)
		if err != nil {
			return s, err
		}
		s.groupGID = uint32(g)
	}

	if mode, ok := metadata[POSIXModeMeta]; ok {
		m, err := strconv.ParseUint(mode, 10, 32)
		if err != nil {
			return s, err
		}

		s.mode = uint32(m)
	}

	if inode, ok := metadata[POSIXINodeMeta]; ok {
		ino, err := strconv.ParseUint(inode, 10, 64)
		if err != nil {
			return s, err
		}

		s.iNode = ino
	}

	if dev, ok := metadata[POSIXDevMeta]; ok {
		d, err := strconv.ParseUint(dev, 10, 64)
		if err != nil {
			return s, err
		}

		s.devID = d
	}

	if rdev, ok := metadata[POSIXRDevMeta]; ok {
		rd, err := strconv.ParseUint(rdev, 10, 64)
		if err != nil {
			return s, err
		}

		s.repDevID = rd
	}

	if atime, ok := metadata[POSIXATimeMeta]; ok {
		at, err := strconv.ParseInt(atime, 10, 64)
		if err != nil {
			return s, err
		}

		s.accessTime = time.Unix(at, 0)
	}

	if mtime, ok := metadata[POSIXModTimeMeta]; ok {
		mt, err := strconv.ParseInt(mtime, 10, 64)
		if err != nil {
			return s, err
		}

		s.modTime = time.Unix(mt, 0)
	}

	if ctime, ok := metadata[POSIXCTimeMeta]; ok {
		ct, err := strconv.ParseInt(ctime, 10, 64)
		if err != nil {
			return s, err
		}

		s.changeTime = time.Unix(ct, 0)
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

	S_IFBLK = 0x6000
	S_IFCHR = 0x2000
	S_IFDIR = 0x4000
	S_IFIFO = 0x1000
	S_IFLNK = 0xa000
)

func AddStatToBlobMetadata(s UnixStatAdapter, metadata azblob.Metadata) {
	// TODO: File mode properties (hdi_isfolder, etc.)
	if s.Extended() { // try to poll the other properties
		mask := s.StatxMask()

		tryAddMetadata(metadata, LINUXStatxMaskMeta, strconv.FormatUint(uint64(mask), 10))
		tryAddMetadata(metadata, LINUXAttributeMeta, strconv.FormatUint(s.Attribute()&s.AttributeMask(), 10)) // AttributesMask indicates what attributes are supported by the filesystem
		tryAddMetadata(metadata, LINUXAttributeMaskMeta, strconv.FormatUint(s.AttributeMask(), 10))

		if statxReturned(mask, STATX_BTIME) {
			tryAddMetadata(metadata, LINUXBTimeMeta, strconv.FormatInt(s.BTime().Unix(), 10))
		}

		if statxReturned(mask, STATX_MODE) {
			tryAddMetadata(metadata, POSIXNlinkMeta, strconv.FormatUint(s.NLink(), 10))
		}

		if statxReturned(mask, STATX_UID) {
			tryAddMetadata(metadata, POSIXOwnerMeta, strconv.FormatUint(uint64(s.Owner()), 10))
		}

		if statxReturned(mask, STATX_GID) {
			tryAddMetadata(metadata, POSIXGroupMeta, strconv.FormatUint(uint64(s.Group()), 10))
		}

		if statxReturned(mask, STATX_MODE) {
			tryAddMetadata(metadata, POSIXModeMeta, strconv.FormatUint(uint64(s.FileMode()), 10))
		}

		if statxReturned(mask, STATX_INO) {
			tryAddMetadata(metadata, POSIXINodeMeta, strconv.FormatUint(s.INode(), 10))
		}

		// This is not optional.
		tryAddMetadata(metadata, POSIXDevMeta, strconv.FormatUint(s.Device(), 10))

		if statxReturned(mask, STATX_MODE) && ((s.FileMode()&S_IFCHR) == S_IFCHR || (s.FileMode()&S_IFBLK) == S_IFBLK) {
			tryAddMetadata(metadata, POSIXRDevMeta, strconv.FormatUint(s.RDevice(), 10))
		}

		if statxReturned(mask, STATX_ATIME) {
			tryAddMetadata(metadata, POSIXATimeMeta, strconv.FormatInt(s.ATime().Unix(), 10))
		}

		if statxReturned(mask, STATX_MTIME) {
			tryAddMetadata(metadata, POSIXModTimeMeta, strconv.FormatInt(s.MTime().Unix(), 10))
		}

		if statxReturned(mask, STATX_CTIME) {
			tryAddMetadata(metadata, POSIXCTimeMeta, strconv.FormatInt(s.CTime().Unix(), 10))
		}
	} else {
		tryAddMetadata(metadata, POSIXNlinkMeta, strconv.FormatUint(s.NLink(), 10))
		tryAddMetadata(metadata, POSIXOwnerMeta, strconv.FormatUint(uint64(s.Owner()), 10))
		tryAddMetadata(metadata, POSIXGroupMeta, strconv.FormatUint(uint64(s.Group()), 10))
		tryAddMetadata(metadata, POSIXModeMeta, strconv.FormatUint(uint64(s.FileMode()), 10))
		tryAddMetadata(metadata, POSIXINodeMeta, strconv.FormatUint(s.INode(), 10))
		tryAddMetadata(metadata, POSIXDevMeta, strconv.FormatUint(s.Device(), 10))

		if (s.FileMode()&S_IFCHR) == S_IFCHR || (s.FileMode()&S_IFBLK) == S_IFBLK { // this is not relevant unless the file is a block or character device.
			tryAddMetadata(metadata, POSIXRDevMeta, strconv.FormatUint(s.RDevice(), 10))
		}

		tryAddMetadata(metadata, POSIXATimeMeta, strconv.FormatInt(s.ATime().Unix(), 10))
		tryAddMetadata(metadata, POSIXModTimeMeta, strconv.FormatInt(s.MTime().Unix(), 10))
		tryAddMetadata(metadata, POSIXCTimeMeta, strconv.FormatInt(s.CTime().Unix(), 10))
	}
}

func statxReturned(mask uint32, want uint32) bool {
	return (mask & want) == want
}

func tryAddMetadata(metadata azblob.Metadata, key, value string) {
	if _, ok := metadata[key]; ok {
		return // Don't overwrite the user's metadata
	}

	metadata[key] = value
}
