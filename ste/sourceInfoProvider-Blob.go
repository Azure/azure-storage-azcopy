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
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/Azure/azure-storage-azcopy/common"
)

// Source info provider for Azure blob
type blobSourceInfoProvider struct {
	defaultRemoteSourceInfoProvider
	udamInstance    *userDelegationAuthenticationManager
	stashedURLNoSAS *url.URL
	needsSAS        bool
}

func newBlobSourceInfoProvider(jptm IJobPartTransferMgr) (ISourceInfoProvider, error) {
	b, err := newDefaultRemoteSourceInfoProvider(jptm)
	if err != nil {
		return nil, err
	}

	base := b.(*defaultRemoteSourceInfoProvider)
	fromTo := jptm.FromTo()

	return &blobSourceInfoProvider{
		defaultRemoteSourceInfoProvider: *base,
		udamInstance:                    jptm.GetUserDelegationAuthenticationManagerInstance(),
		needsSAS:                        fromTo.IsS2S() && fromTo.From() == common.ELocation.Blob(),
	}, nil
}

func (p *blobSourceInfoProvider) PreSignedSourceURL() (*url.URL, error) {
	if p.stashedURLNoSAS == nil {
		var err error
		p.stashedURLNoSAS, err = p.defaultRemoteSourceInfoProvider.PreSignedSourceURL()

		if err != nil {
			return p.stashedURLNoSAS, err
		}
	}

	result := p.stashedURLNoSAS
	if p.needsSAS {
		bURLParts := azblob.NewBlobURLParts(*p.stashedURLNoSAS)

		// If there's not already a SAS on the source, append it!
		if bURLParts.SAS.Encode() == "" {
			result.RawQuery, _ = p.udamInstance.GetUserDelegationSASForURL(bURLParts)
		} else {
			p.needsSAS = false
		}
	}

	return result, nil
}

func (p *blobSourceInfoProvider) BlobTier() azblob.AccessTierType {
	return p.transferInfo.S2SSrcBlobTier
}

func (p *blobSourceInfoProvider) BlobType() azblob.BlobType {
	return p.transferInfo.SrcBlobType
}

func (p *blobSourceInfoProvider) GetLastModifiedTime() (time.Time, error) {
	presignedURL, err := p.PreSignedSourceURL()
	if err != nil {
		return time.Time{}, err
	}

	blobURL := azblob.NewBlobURL(*presignedURL, p.jptm.SourceProviderPipeline())
	properties, err := blobURL.GetProperties(p.jptm.Context(), azblob.BlobAccessConditions{})
	if err != nil {
		return time.Time{}, err
	}

	return properties.LastModified(), nil
}
