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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io"
	"strings"
	"time"
)

// Source info provider for Azure blob
type blobSourceInfoProvider struct {
	defaultRemoteSourceInfoProvider
}

func (p *blobSourceInfoProvider) IsDFSSource() bool {
	return p.jptm.FromTo().From() == common.ELocation.BlobFS()
}

func (p *blobSourceInfoProvider) internalPresignedURL(useHNS bool) (string, error) {
	uri, err := p.defaultRemoteSourceInfoProvider.PreSignedSourceURL()
	if err != nil {
		return "", err
	}

	// This will have no real effect on non-standard endpoints (e.g. emulator, stack), and *may* work, but probably won't.
	// However, Stack/Emulator don't support HNS, so, this won't get use.
	bURLParts, err := blob.ParseURL(uri)
	if err != nil {
		return "", err
	}
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

	return bURLParts.String(), nil
}

func (p *blobSourceInfoProvider) PreSignedSourceURL() (string, error) {
	return p.internalPresignedURL(false) // prefer to return the blob URL; data can be read from either endpoint.
}

func (p *blobSourceInfoProvider) ReadLink() (string, error) {
	source, err := p.internalPresignedURL(false)
	if err != nil {
		return "", err
	}
	blobClient := common.CreateBlobClient(source, p.jptm.S2SSourceCredentialInfo(), p.jptm.CredentialOpOptions(), p.jptm.S2SSourceClientOptions())

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
		OnFailedRead: common.NewBlobReadLogFunc(p.jptm, source),
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

	return common.ReadStatFromMetadata(prop.SrcMetadata, p.SourceSize())
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

func (p *blobSourceInfoProvider) AccessControl() (*string, error) {
	// We can only get access control via HNS, so we MUST switch here.
	presignedURL, err := p.internalPresignedURL(true)
	if err != nil {
		return nil, err
	}
	parsedURL, err := blob.ParseURL(presignedURL)
	if err != nil {
		return nil, err
	}
	parsedURL.Host = strings.ReplaceAll(parsedURL.Host, ".blob", ".dfs")
	if parsedURL.BlobName != "" {
		parsedURL.BlobName = strings.TrimSuffix(parsedURL.BlobName, "/") // BlobFS doesn't handle folders correctly like this.
	} else {
		parsedURL.BlobName = "/" // container level perms MUST have a /
	}
	fileClient := common.CreateDatalakeFileClient(parsedURL.String(), p.jptm.CredentialInfo(), p.jptm.CredentialOpOptions(), p.jptm.ClientOptions())
	resp, err := fileClient.GetAccessControl(p.jptm.Context(), nil)
	if err != nil {
		return nil, err
	}
	return resp.ACL, nil
}

func (p *blobSourceInfoProvider) BlobTier() *blob.AccessTier {
	return to.Ptr(p.transferInfo.S2SSrcBlobTier)
}

func (p *blobSourceInfoProvider) BlobType() blob.BlobType {
	return p.transferInfo.SrcBlobType
}

func (p *blobSourceInfoProvider) GetFreshFileLastModifiedTime() (time.Time, error) {
	// We can't set a custom LMT on HNS, so it doesn't make sense to swap here.
	source, err := p.internalPresignedURL(false)
	if err != nil {
		return time.Time{}, err
	}

	blobClient := common.CreateBlobClient(source, p.jptm.S2SSourceCredentialInfo(), p.jptm.CredentialOpOptions(), p.jptm.S2SSourceClientOptions())

	properties, err := blobClient.GetProperties(p.jptm.Context(), &blob.GetPropertiesOptions{CPKInfo: p.jptm.CpkInfo()})
	if err != nil {
		return time.Time{}, err
	}
	return common.IffNotNil(properties.LastModified, time.Time{}), nil
}
