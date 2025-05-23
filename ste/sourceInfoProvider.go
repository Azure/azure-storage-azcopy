// Copyright © 2017 Microsoft <wastore@microsoft.com>
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
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// ISourceInfoProvider is the abstraction of generic source info provider which provides source's properties.
type ISourceInfoProvider interface {
	// Properties returns source's properties.
	Properties() (*SrcProperties, error)

	// GetLastModifiedTime returns the source's latest last modified time.  Not used when
	// EntityType() == Folder
	GetFreshFileLastModifiedTime() (time.Time, error)

	IsLocal() bool

	EntityType() common.EntityType

	GetMD5(offset, count int64) ([]byte, error)
}

type ILocalSourceInfoProvider interface {
	ISourceInfoProvider
	OpenSourceFile() (common.CloseableReaderAt, error)
}

// IRemoteSourceInfoProvider is the abstraction of the methods needed to prepare remote copy source.
type IRemoteSourceInfoProvider interface {
	ISourceInfoProvider

	// SourceURL returns source's URL.
	PreSignedSourceURL() (string, error)

	// SourceSize returns size of source
	SourceSize() int64

	// RawSource returns raw source
	RawSource() string

	// This can be further extended, e.g. add DownloadSourceRange, which can be used to implement download+upload fashion S2S copy.
}

// IBlobSourceInfoProvider is the abstraction of the methods needed to prepare blob copy source.
type IBlobSourceInfoProvider interface {
	IRemoteSourceInfoProvider

	// BlobTier returns source's blob tier.
	BlobTier() *blob.AccessTier

	// BlobType returns source's blob type.
	BlobType() blob.BlobType
}

type TypedSMBPropertyHolder interface {
	FileCreationTime() time.Time
	FileLastWriteTime() time.Time
	FileAttributes() (*file.NTFSFileAttributes, error)
}

type ISMBPropertyBearingSourceInfoProvider interface {
	ISourceInfoProvider

	GetSDDL() (string, error)
	GetSMBProperties() (TypedSMBPropertyHolder, error)
}

type TypedNFSPropertyHolder interface {
	FileCreationTime() time.Time
	FileLastWriteTime() time.Time
}

type TypedNFSPermissionsHolder interface {
	GetOwner() *string
	GetGroup() *string
	GetFileMode() *string // Mode may not always be available to check in a Statx call (though it should be, since we requested it.) Best safe than sorry; check Mask!
}

type INFSPropertyBearingSourceInfoProvider interface {
	ISourceInfoProvider

	GetNFSProperties() (TypedNFSPropertyHolder, error)
	GetNFSPermissions() (TypedNFSPermissionsHolder, error)
	GetNFSDefaultPerms() (*string, *string, *string, error)
}

type IUNIXPropertyBearingSourceInfoProvider interface {
	ISourceInfoProvider

	GetUNIXProperties() (common.UnixStatAdapter, error)
	HasUNIXProperties() bool
}

type ISymlinkBearingSourceInfoProvider interface {
	ISourceInfoProvider

	ReadLink() (string, error)
}

type ICustomLocalOpener interface {
	ISourceInfoProvider
	Open(path string) (*os.File, error)
}

type sourceInfoProviderFactory func(jptm IJobPartTransferMgr) (ISourceInfoProvider, error)

// ///////////////////////////////////////////////////////////////////////////////////////////////
// Default copy remote source info provider which provides info sourced from transferInfo.
// It implements all methods of ISourceInfoProvider except for GetFreshLastModifiedTime.
// It's never correct to implement that based on the transfer info, because the whole point is that it should
// return FRESH (up to date) data.
type defaultRemoteSourceInfoProvider struct {
	jptm         IJobPartTransferMgr
	transferInfo *TransferInfo
}

func newDefaultRemoteSourceInfoProvider(jptm IJobPartTransferMgr) (*defaultRemoteSourceInfoProvider, error) {
	return &defaultRemoteSourceInfoProvider{jptm: jptm, transferInfo: jptm.Info()}, nil
}

func (p *defaultRemoteSourceInfoProvider) Properties() (*SrcProperties, error) {
	return &SrcProperties{
		SrcHTTPHeaders: p.transferInfo.SrcHTTPHeaders,
		SrcMetadata:    p.transferInfo.SrcMetadata,
		SrcBlobTags:    p.transferInfo.SrcBlobTags,
	}, nil
}

func (p *defaultRemoteSourceInfoProvider) IsLocal() bool {
	return false
}

func (p *defaultRemoteSourceInfoProvider) SourceSize() int64 {
	return p.transferInfo.SourceSize
}

func (p *defaultRemoteSourceInfoProvider) EntityType() common.EntityType {
	return p.transferInfo.EntityType
}

// formatHTTPRange converts an offset and count to its header format.
func formatHTTPRange(offset, count int64) *string {
	if offset == 0 && count == 0 {
		return nil // No specified range
	}
	endOffset := "" // if count == CountToEnd (0)
	if count > 0 {
		endOffset = strconv.FormatInt((offset+count)-1, 10)
	}
	dataRange := fmt.Sprintf("bytes=%v-%s", offset, endOffset)
	return &dataRange
}
