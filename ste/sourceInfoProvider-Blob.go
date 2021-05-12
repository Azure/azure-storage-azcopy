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
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// Source info provider for Azure blob
type blobSourceInfoProvider struct {
	defaultRemoteSourceInfoProvider
	needsSAS bool
}

func newBlobSourceInfoProvider(jptm IJobPartTransferMgr) (ISourceInfoProvider, error) {
	base, err := newDefaultRemoteSourceInfoProvider(jptm)
	if err != nil {
		return nil, err
	}

	fromTo := jptm.FromTo()

	result, err := base.PreSignedSourceURL()

	if err != nil {
		return nil, err
	}

	bURLParts := azblob.NewBlobURLParts(*result)

	return &blobSourceInfoProvider{
		defaultRemoteSourceInfoProvider: *base,
		needsSAS:                        fromTo.IsS2S() && fromTo.From() == common.ELocation.Blob() && bURLParts.SAS.Encode() == "",
	}, nil
}

func (p *blobSourceInfoProvider) BlobTier() azblob.AccessTierType {
	return p.transferInfo.S2SSrcBlobTier
}

func (p *blobSourceInfoProvider) BlobType() azblob.BlobType {
	return p.transferInfo.SrcBlobType
}

func (p *blobSourceInfoProvider) GetFreshFileLastModifiedTime() (time.Time, error) {
	presignedURL, err := p.PreSignedSourceURL()
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

func (p *blobSourceInfoProvider) PreSignedSourceURL() (*url.URL, error) {
	udamInstance := p.jptm.GetUserDelegationAuthenticationManager()

	// result needs to dereference so we don't modify the actual URL. This enables proper refreshing.
	result, err := p.defaultRemoteSourceInfoProvider.PreSignedSourceURL()

	if err != nil {
		return result, err
	}

	// needsSAS is only set if it's a blob-* s2s transfer, and no SAS is present on the source.
	// As a result, we generate one.
	if p.needsSAS {
		bURLParts := azblob.NewBlobURLParts(*result)
		result.RawQuery, err = udamInstance.GetUserDelegationSASForURL(bURLParts)
	}

	return result, err
}
