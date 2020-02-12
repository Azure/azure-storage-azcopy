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
	"time"

	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-storage-azcopy/common"
)

// Source info provider for Azure blob
type fileSourceInfoProvider struct {
	ctx                 context.Context
	cachedPermissionKey string
	defaultRemoteSourceInfoProvider
}

func newFileSourceInfoProvider(jptm IJobPartTransferMgr) (ISourceInfoProvider, error) {
	b, err := newDefaultRemoteSourceInfoProvider(jptm)
	if err != nil {
		return nil, err
	}

	base := b.(*defaultRemoteSourceInfoProvider)

	// due to the REST parity feature added in 2019-02-02, the File APIs are no longer backward compatible
	// so we must use the latest SDK version to stay safe
	ctx := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azfile.ServiceVersion)

	return &fileSourceInfoProvider{defaultRemoteSourceInfoProvider: *base, ctx: ctx}, nil
}

func (p *fileSourceInfoProvider) GetSDDL() (string, error) {
	presigned, err := p.PreSignedSourceURL()

	if err != nil {
		return "", err
	}

	// Get the key for SIPM
	key := p.cachedPermissionKey

	if key == "" {
		fileURL := azfile.NewFileURL(*presigned, p.jptm.SourceProviderPipeline())
		props, err := fileURL.GetProperties(p.ctx)

		if err != nil {
			return "", err
		}

		key = props.FilePermissionKey()
	}

	// Call into SIPM and grab our SDDL string.
	sipm := p.jptm.SecurityInfoPersistenceManager()

	// fURLParts := common.NewGenericResourceURLParts(*presigned, common.ELocation.File())
	fURLParts := azfile.NewFileURLParts(*presigned)
	fURLParts.DirectoryOrFilePath = ""
	shareURL := azfile.NewShareURL(fURLParts.URL(), p.jptm.SourceProviderPipeline())

	sddlString, err := sipm.GetSDDLFromID(key, shareURL)

	return sddlString, err
}

func (p *fileSourceInfoProvider) Properties() (*SrcProperties, error) {
	srcProperties, err := p.defaultRemoteSourceInfoProvider.Properties()
	if err != nil {
		return nil, err
	}

	// Get properties in backend.
	if p.transferInfo.S2SGetPropertiesInBackend {
		presignedURL, err := p.PreSignedSourceURL()
		if err != nil {
			return nil, err
		}

		fileURL := azfile.NewFileURL(*presignedURL, p.jptm.SourceProviderPipeline())
		properties, err := fileURL.GetProperties(p.ctx)
		if err != nil {
			return nil, err
		}

		// We cache this as getting the SDDL is a separate operation.
		p.cachedPermissionKey = properties.FilePermissionKey()

		srcProperties = &SrcProperties{
			SrcHTTPHeaders: common.ResourceHTTPHeaders{
				ContentType:        properties.ContentType(),
				ContentEncoding:    properties.ContentEncoding(),
				ContentDisposition: properties.ContentDisposition(),
				ContentLanguage:    properties.ContentLanguage(),
				CacheControl:       properties.CacheControl(),
				ContentMD5:         properties.ContentMD5(),
			},
			SrcMetadata: common.FromAzFileMetadataToCommonMetadata(properties.NewMetadata()),
		}
	}

	return srcProperties, nil
}

func (p *fileSourceInfoProvider) GetLastModifiedTime() (time.Time, error) {
	presignedURL, err := p.PreSignedSourceURL()
	if err != nil {
		return time.Time{}, err
	}

	fileURL := azfile.NewFileURL(*presignedURL, p.jptm.SourceProviderPipeline())
	properties, err := fileURL.GetProperties(p.ctx)
	if err != nil {
		return time.Time{}, err
	}

	return properties.LastModified(), nil
}
