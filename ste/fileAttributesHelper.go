package ste

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
)

// This is intended for easy conversion to/from local file attributes
const (
	FileAttributeNone              uint32 = 0
	FileAttributeReadonly          uint32 = 1
	FileAttributeHidden            uint32 = 2
	FileAttributeSystem            uint32 = 4
	FileAttributeArchive           uint32 = 32
	FileAttributeTemporary         uint32 = 256
	FileAttributeOffline           uint32 = 4096
	FileAttributeNotContentIndexed uint32 = 8192
	FileAttributeNoScrubData       uint32 = 131072
)

func FileAttributesFromUint32(attributes uint32) file.NTFSFileAttributes {
	attr := file.NTFSFileAttributes{}
	if attributes&FileAttributeReadonly != 0 {
		attr.ReadOnly = true
	}
	if attributes&FileAttributeHidden != 0 {
		attr.Hidden = true
	}
	if attributes&FileAttributeSystem != 0 {
		attr.System = true
	}
	if attributes&FileAttributeArchive != 0 {
		attr.Archive = true
	}
	if attributes&FileAttributeTemporary != 0 {
		attr.Temporary = true
	}
	if attributes&FileAttributeOffline != 0 {
		attr.Offline = true
	}
	if attributes&FileAttributeNotContentIndexed != 0 {
		attr.NotContentIndexed = true
	}
	if attributes&FileAttributeNoScrubData != 0 {
		attr.NoScrubData = true
	}
	return attr
}

func FileAttributesToUint32(attributes file.NTFSFileAttributes) uint32 {
	var attr uint32
	if attributes.ReadOnly {
		attr |= FileAttributeReadonly
	}
	if attributes.Hidden {
		attr |= FileAttributeHidden
	}
	if attributes.System {
		attr |= FileAttributeSystem
	}
	if attributes.Archive {
		attr |= FileAttributeArchive
	}
	if attributes.Temporary {
		attr |= FileAttributeTemporary
	}
	if attributes.Offline {
		attr |= FileAttributeOffline
	}
	if attributes.NotContentIndexed {
		attr |= FileAttributeNotContentIndexed
	}
	if attributes.NoScrubData {
		attr |= FileAttributeNoScrubData
	}
	return attr
}