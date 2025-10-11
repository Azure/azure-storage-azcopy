// Copyright © 2023 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

func FileAttributesFromUint32(attributes uint32) (*file.NTFSFileAttributes, error) {
	attr := file.NTFSFileAttributes{}
	if attributes&FileAttributeNone != 0 {
		attr.None = true
	}
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
	return &attr, nil
}

func FileAttributesToUint32(attributes file.NTFSFileAttributes) uint32 {
	var attr uint32
	if attributes.None {
		attr |= FileAttributeNone
	}
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
