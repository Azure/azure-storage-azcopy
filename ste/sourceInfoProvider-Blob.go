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
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/common"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

// Source info provider for Azure blob
type blobSourceInfoProvider struct {
	defaultRemoteSourceInfoProvider
}

func (p *blobSourceInfoProvider) IsDFSSource() bool {
	fromTo := p.jptm.FromTo()
	return fromTo.From() == common.ELocation.BlobFS()
}

func (p *blobSourceInfoProvider) internalPresignedURL(useHNS bool) (*url.URL, error) {
	uri, err := p.defaultRemoteSourceInfoProvider.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	// This will have no real effect on non-standard endpoints (e.g. emulator, stack), and *may* work, but probably won't.
	// However, Stack/Emulator don't support HNS, so, this won't get use.
	bURLParts := azblob.NewBlobURLParts(*uri)
	if useHNS {
		bURLParts.Host = strings.Replace(bURLParts.Host, ".blob", ".dfs", 1)

		if bURLParts.BlobName != "" {
			bURLParts.BlobName = strings.TrimSuffix(bURLParts.BlobName, "/") // BlobFS doesn't handle folders correctly like this.
		} else {
			bURLParts.ContainerName += "/" // container level perms MUST have a /
		}
	} else {
		bURLParts.Host = strings.Replace(bURLParts.Host, ".dfs", ".blob", 1)
	}
	out := bURLParts.URL()

	return &out, nil
}

func (p *blobSourceInfoProvider) PreSignedSourceURL() (*url.URL, error) {
	return p.internalPresignedURL(false) // prefer to return the blob URL; data can be read from either endpoint.
}

func (p *blobSourceInfoProvider) ReadLink() (string, error) {
	uri, err := p.internalPresignedURL(false)
	if err != nil {
		return "", err
	}

	pl := p.jptm.SourceProviderPipeline()
	ctx := p.jptm.Context()

	blobURL := azblob.NewBlockBlobURL(*uri, pl)

	clientProvidedKey := azblob.ClientProvidedKeyOptions{}
	if p.jptm.IsSourceEncrypted() {
		clientProvidedKey = common.ToClientProvidedKeyOptions(p.jptm.CpkInfo(), p.jptm.CpkScopeInfo())
	}

	resp, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, clientProvidedKey)
	if err != nil {
		return "", err
	}

	symlinkBuf, err := io.ReadAll(resp.Body(azblob.RetryReaderOptions{
		MaxRetryRequests: 5,
		NotifyFailedRead: common.NewReadLogFunc(p.jptm, uri),
	}))
	if err != nil {
		return "", err
	}

	return string(symlinkBuf), nil
}

func (p *blobSourceInfoProvider) GetUNIXProperties() (common.UnixStatAdapter, error) {
	prop, err := p.Properties()
	if err != nil {
		return nil, err
	}

	return common.ReadStatFromMetadata(prop.SrcMetadata.ToAzBlobMetadata(), p.SourceSize())
}

func (p *blobSourceInfoProvider) HasUNIXProperties() bool {
	prop, err := p.Properties()
	if err != nil {
		return false // This transfer is probably going to fail anyway.
	}

	for _, v := range common.AllLinuxProperties {
		_, ok := prop.SrcMetadata[v]
		if ok {
			return true
		}
	}

	return false
}

func newBlobSourceInfoProvider(jptm IJobPartTransferMgr) (ISourceInfoProvider, error) {
	base, err := newDefaultRemoteSourceInfoProvider(jptm)
	if err != nil {
		return nil, err
	}

	return &blobSourceInfoProvider{defaultRemoteSourceInfoProvider: *base}, nil
}

func (p *blobSourceInfoProvider) AccessControl() (azbfs.BlobFSAccessControl, error) {
	// We can only get access control via HNS, so we MUST switch here.
	presignedURL, err := p.internalPresignedURL(true)
	if err != nil {
		return azbfs.BlobFSAccessControl{}, err
	}

	fURL := azbfs.NewFileURL(*presignedURL, p.jptm.SecondarySourceProviderPipeline())
	return fURL.GetAccessControl(p.jptm.Context())
}

func (p *blobSourceInfoProvider) BlobTier() azblob.AccessTierType {
	return p.transferInfo.S2SSrcBlobTier
}

func (p *blobSourceInfoProvider) BlobType() azblob.BlobType {
	return p.transferInfo.SrcBlobType
}

func (p *blobSourceInfoProvider) GetFreshFileLastModifiedTime() (time.Time, error) {
	// We can't set a custom LMT on HNS, so it doesn't make sense to swap here.
	presignedURL, err := p.internalPresignedURL(false)
	if err != nil {
		return time.Time{}, err
	}

	blobURL := azblob.NewBlobURL(*presignedURL, p.jptm.SourceProviderPipeline())
	clientProvidedKey := azblob.ClientProvidedKeyOptions{}
	if p.jptm.IsSourceEncrypted() {
		clientProvidedKey = common.ToClientProvidedKeyOptions(p.jptm.CpkInfo(), p.jptm.CpkScopeInfo())
	}

	properties, err := blobURL.GetProperties(p.jptm.Context(), azblob.BlobAccessConditions{}, clientProvidedKey)
	if err != nil {
		return time.Time{}, err
	}

	return properties.LastModified(), nil
}
