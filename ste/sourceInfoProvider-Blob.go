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
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// Source info provider for Azure blob
type blobSourceInfoProvider struct {
	defaultRemoteSourceInfoProvider
	source *blob.Client
	ctx    context.Context
}

func (p *blobSourceInfoProvider) IsDFSSource() bool {
	return p.jptm.FromTo().From() == common.ELocation.BlobFS()
}

func (p *blobSourceInfoProvider) PreSignedSourceURL() (string, error) {
	return p.source.URL(), nil // prefer to return the blob URL; data can be read from either endpoint.
}

func (p *blobSourceInfoProvider) RawSource() string {
	return p.source.URL()
}

func (p *blobSourceInfoProvider) GetObjectRange(offset, length int64) (io.ReadCloser, error) {
	return nil, fmt.Errorf("GetObjectRange not implemented for blobSourceInfoProvider")
}

func (p *blobSourceInfoProvider) ReadLink() (string, error) {
	resp, err := p.source.DownloadStream(p.ctx, &blob.DownloadStreamOptions{
		CPKInfo:      p.jptm.CpkInfo(),
		CPKScopeInfo: p.jptm.CpkScopeInfo(),
	})
	if err != nil {
		return "", err
	}

	symlinkBuf, err := io.ReadAll(resp.NewRetryReader(p.ctx, &blob.RetryReaderOptions{
		MaxRetries:   5,
		OnFailedRead: common.NewBlobReadLogFunc(p.jptm, p.jptm.Info().Source),
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

	var ret = &blobSourceInfoProvider{
		defaultRemoteSourceInfoProvider: *base,
	}

	bsc, err := jptm.SrcServiceClient().BlobServiceClient()
	if err != nil {
		return nil, err
	}

	blobClient := bsc.NewContainerClient(jptm.Info().SrcContainer).NewBlobClient(jptm.Info().SrcFilePath)

	if jptm.Info().VersionID != "" {
		blobClient, err = blobClient.WithVersionID(jptm.Info().VersionID)
		if err != nil {
			return nil, err
		}
	} else if jptm.Info().SnapshotID != "" {
		blobClient, err = blobClient.WithSnapshot(jptm.Info().SnapshotID)
		if err != nil {
			return nil, err
		}
	}

	ret.source = blobClient

	ctx := jptm.Context()
	ctx = withPipelineNetworkStats(ctx, nil)
	ret.ctx = ctx

	return ret, nil
}

func (p *blobSourceInfoProvider) AccessControl() (*string, error) {
	dsc, err := p.jptm.SrcServiceClient().DatalakeServiceClient()
	if err != nil {
		return nil, err
	}

	sourceDatalakeClient := dsc.NewFileSystemClient(p.jptm.Info().SrcContainer).NewFileClient(p.jptm.Info().SrcFilePath)

	resp, err := sourceDatalakeClient.GetAccessControl(p.ctx, nil)
	if err != nil {
		return nil, err
	}
	return resp.ACL, nil
}

func (p *blobSourceInfoProvider) BlobTier() *blob.AccessTier {
	if p.transferInfo.S2SSrcBlobTier == "" {
		return nil
	}
	return to.Ptr(p.transferInfo.S2SSrcBlobTier)
}

func (p *blobSourceInfoProvider) BlobType() blob.BlobType {
	return p.transferInfo.SrcBlobType
}

func (p *blobSourceInfoProvider) GetFreshFileLastModifiedTime() (time.Time, error) {
	// We can't set a custom LMT on HNS, so it doesn't make sense to swap here.
	properties, err := p.source.GetProperties(p.ctx, &blob.GetPropertiesOptions{CPKInfo: p.jptm.CpkInfo()})
	if err != nil {
		return time.Time{}, err
	}
	return common.IffNotNil(properties.LastModified, time.Time{}), nil
}

func (p *blobSourceInfoProvider) GetMD5(offset, count int64) ([]byte, error) {
	var rangeGetContentMD5 *bool
	if count <= common.MaxRangeGetSize {
		rangeGetContentMD5 = to.Ptr(true)
	}
	response, err := p.source.DownloadStream(p.ctx,
		&blob.DownloadStreamOptions{
			Range:              blob.HTTPRange{Offset: offset, Count: count},
			RangeGetContentMD5: rangeGetContentMD5,
			CPKInfo:            p.jptm.CpkInfo(),
			CPKScopeInfo:       p.jptm.CpkScopeInfo(),
		})
	if err != nil {
		return nil, err
	}
	if len(response.ContentMD5) > 0 {
		return response.ContentMD5, nil
	} else {
		// compute md5
		body := response.NewRetryReader(p.ctx, &blob.RetryReaderOptions{MaxRetries: MaxRetryPerDownloadBody})
		defer body.Close()
		h := md5.New()
		if _, err = io.Copy(h, body); err != nil {
			return nil, err
		}
		return h.Sum(nil), nil
	}
}
