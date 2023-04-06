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
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
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

func (p *blobSourceInfoProvider) ReadLink() (string, error) {
	source, err := p.PreSignedSourceURL()
	if err != nil {
		return "", err
	}
	blobClient, err := common.CreateBlobClient(source, p.jptm.S2SSourceCredentialInfo(), p.jptm.CredentialOpOptions(), p.jptm.S2SSourceClientOptions())

	ctx := p.jptm.Context()

	resp, err := blobClient.DownloadStream(ctx, &blob.DownloadStreamOptions{
		CPKInfo:      p.jptm.CpkInfo(),
		CPKScopeInfo: p.jptm.CpkScopeInfo(),
	})
	if err != nil {
		return "", err
	}

	symlinkBuf, err := io.ReadAll(resp.NewRetryReader(ctx, &blob.RetryReaderOptions{
		MaxRetries:   5,
		OnFailedRead: common.NewReadLogFunc(p.jptm, source),
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

// AccessControl should ONLY get called when we know for a fact it is a blobFS->blobFS tranfser.
// It *assumes* that the source is actually a HNS account.
func (p *blobSourceInfoProvider) AccessControl() (azbfs.BlobFSAccessControl, error) {
	presignedURL, err := p.PreSignedSourceURL()
	if err != nil {
		return azbfs.BlobFSAccessControl{}, err
	}
	sourceURL, err := url.Parse(presignedURL)
	if err != nil {
		return azbfs.BlobFSAccessControl{}, err
	}

	bURLParts := azblob.NewBlobURLParts(*sourceURL)
	bURLParts.Host = strings.ReplaceAll(bURLParts.Host, ".blob", ".dfs")
	if bURLParts.BlobName != "" {
		bURLParts.BlobName = strings.TrimSuffix(bURLParts.BlobName, "/") // BlobFS doesn't handle folders correctly like this.
	} else {
		bURLParts.BlobName = "/" // container level perms MUST have a /
	}

	// todo: jank, and violates the principle of interfaces
	fURL := azbfs.NewFileURL(bURLParts.URL(), p.jptm.(*jobPartTransferMgr).jobPartMgr.(*jobPartMgr).secondarySourceProviderPipeline)
	return fURL.GetAccessControl(p.jptm.Context())
}

func (p *blobSourceInfoProvider) BlobTier() blob.AccessTier {
	return p.transferInfo.S2SSrcBlobTier
}

func (p *blobSourceInfoProvider) BlobType() blob.BlobType {
	return p.transferInfo.SrcBlobType
}

func (p *blobSourceInfoProvider) GetFreshFileLastModifiedTime() (time.Time, error) {
	source, err := p.PreSignedSourceURL()
	if err != nil {
		return time.Time{}, err
	}

	blobClient, err := common.CreateBlobClient(source, p.jptm.S2SSourceCredentialInfo(), p.jptm.CredentialOpOptions(), p.jptm.S2SSourceClientOptions())
	if err != nil {
		return time.Time{}, err
	}

	properties, err := blobClient.GetProperties(p.jptm.Context(), &blob.GetPropertiesOptions{CPKInfo: p.jptm.CpkInfo()})
	if err != nil {
		return time.Time{}, err
	}
	return common.IffNotNil(properties.LastModified, time.Time{}), nil
}
