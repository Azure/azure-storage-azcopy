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
	"net/url"
	"os"
	"time"

	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-storage-azcopy/common"

	"github.com/Azure/azure-storage-blob-go/azblob"
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
}

type ILocalSourceInfoProvider interface {
	ISourceInfoProvider
	OpenSourceFile() (common.CloseableReaderAt, error)
}

// IRemoteSourceInfoProvider is the abstraction of the methods needed to prepare remote copy source.
type IRemoteSourceInfoProvider interface {
	ISourceInfoProvider

	// SourceURL returns source's URL.
	PreSignedSourceURL() (*url.URL, error)

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
	BlobTier() azblob.AccessTierType

	// BlobType returns source's blob type.
	BlobType() azblob.BlobType
}

type TypedSMBPropertyHolder interface {
	FileCreationTime() time.Time
	FileLastWriteTime() time.Time
	FileAttributes() azfile.FileAttributeFlags
}

type ISMBPropertyBearingSourceInfoProvider interface {
	ISourceInfoProvider

	GetSDDL() (string, error)
	GetSMBProperties() (TypedSMBPropertyHolder, error)
}

type ICustomLocalOpener interface {
	ISourceInfoProvider
	Open(path string) (*os.File, error)
}

type sourceInfoProviderFactory func(jptm IJobPartTransferMgr) (ISourceInfoProvider, error)

/////////////////////////////////////////////////////////////////////////////////////////////////
// Default copy remote source info provider which provides info sourced from transferInfo.
// It implements all methods of ISourceInfoProvider except for GetFreshLastModifiedTime.
// It's never correct to implement that based on the transfer info, because the whole point is that it should
// return FRESH (up to date) data.
type defaultRemoteSourceInfoProvider struct {
	jptm         IJobPartTransferMgr
	transferInfo TransferInfo
}

func newDefaultRemoteSourceInfoProvider(jptm IJobPartTransferMgr) (*defaultRemoteSourceInfoProvider, error) {
	return &defaultRemoteSourceInfoProvider{jptm: jptm, transferInfo: jptm.Info()}, nil
}

func (p *defaultRemoteSourceInfoProvider) PreSignedSourceURL() (*url.URL, error) {
	srcURL, err := url.Parse(p.transferInfo.Source)
	if err != nil {
		return nil, err
	}

	return srcURL, nil
}

func (p *defaultRemoteSourceInfoProvider) Properties() (*SrcProperties, error) {
	return &SrcProperties{
		SrcHTTPHeaders: p.transferInfo.SrcHTTPHeaders,
		SrcMetadata:    p.transferInfo.SrcMetadata,
	}, nil
}

func (p *defaultRemoteSourceInfoProvider) IsLocal() bool {
	return false
}

func (p *defaultRemoteSourceInfoProvider) SourceSize() int64 {
	return p.transferInfo.SourceSize
}

func (p *defaultRemoteSourceInfoProvider) RawSource() string {
	return p.transferInfo.Source
}

func (p *defaultRemoteSourceInfoProvider) EntityType() common.EntityType {
	return p.transferInfo.EntityType
}
