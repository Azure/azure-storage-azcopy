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
	"sync"
	"time"

	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-storage-azcopy/common"
)

// Source info provider for Azure blob
type fileSourceInfoProvider struct {
	ctx                 context.Context
	cachedPermissionKey string
	cacheOnce           *sync.Once
	cachedProperties    *azfile.FileGetPropertiesResponse
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

	return &fileSourceInfoProvider{defaultRemoteSourceInfoProvider: *base, ctx: ctx, cacheOnce: &sync.Once{}}, nil
}

// for reviewers: should we worry about caching these properties?
func (p *fileSourceInfoProvider) getCachedProperties() (*azfile.FileGetPropertiesResponse, error) {
	presigned, err := p.PreSignedSourceURL()

	if err != nil {
		return nil, err
	}

	p.cacheOnce.Do(func() {
		fileURL := azfile.NewFileURL(*presigned, p.jptm.SourceProviderPipeline())

		p.cachedProperties, err = fileURL.GetProperties(p.ctx)
	})

	if err != nil {
		return p.cachedProperties, err
	}

	return p.cachedProperties, nil
}

func (p *fileSourceInfoProvider) GetFileSMBCreationTime() (time.Time, error) {
	props, err := p.getCachedProperties()

	if err != nil {
		return time.Time{}, err
	}

	timeAdapter := azfile.SMBTimeAdapter{PropertySource: props}

	return timeAdapter.FileCreationTime(), nil
}

func (p *fileSourceInfoProvider) GetFileSMBAttributes() (azfile.FileAttributeFlags, error) {
	props, err := p.getCachedProperties()

	if err != nil {
		return 0, err
	}

	return azfile.ParseFileAttributeFlagsString(props.FileAttributes()), nil
}

// for reviewers: should we worry about proactively getting the latest version of this?
// If not, then we shouldn't worry about whether this is newer than the REST last write time
func (p *fileSourceInfoProvider) GetFileSMBLastWriteTime() (time.Time, error) {
	// Because this is subject to change quite often, it's worthwhile to get a not-cached version for this.
	presigned, err := p.PreSignedSourceURL()

	if err != nil {
		return time.Time{}, err
	}

	fileUrl := azfile.NewFileURL(*presigned, p.jptm.SourceProviderPipeline())

	props, err := fileUrl.GetProperties(p.ctx)

	if err != nil {
		return time.Time{}, err
	}

	timeAdapter := azfile.SMBTimeAdapter{PropertySource: props}

	return timeAdapter.FileLastWriteTime(), nil
}

func (p *fileSourceInfoProvider) GetSDDL() (string, error) {
	presigned, err := p.PreSignedSourceURL()

	if err != nil {
		return "", err
	}

	// Get the key for SIPM
	props, err := p.getCachedProperties()

	if err != nil {
		return "", err
	}

	key := props.FilePermissionKey()

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

		switch p.EntityType() {
		case common.EEntityType.File():
			properties, err := p.getCachedProperties()
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
		case common.EEntityType.Folder():
			dirURL := azfile.NewDirectoryURL(*presignedURL, p.jptm.SourceProviderPipeline())
			properties, err := dirURL.GetProperties(p.ctx)
			if err != nil {
				return nil, err
			}

			p.cachedPermissionKey = properties.FilePermissionKey()

			srcProperties = &SrcProperties{
				SrcHTTPHeaders: common.ResourceHTTPHeaders{}, // no contentType etc for folders
				SrcMetadata:    common.FromAzFileMetadataToCommonMetadata(properties.NewMetadata()),
			}
		default:
			panic("unsupported entity type")
		}
	}

	return srcProperties, nil
}

func (p *fileSourceInfoProvider) GetFileLastModifiedTime() (time.Time, error) {
	if p.EntityType() != common.EEntityType.File() {
		panic("unsupported. Cannot get modification time on non-file object") // nothing should ever call this for a non-file
	}

	presignedURL, err := p.PreSignedSourceURL()
	if err != nil {
		return time.Time{}, err
	}

	fileURL := azfile.NewFileURL(*presignedURL, p.jptm.SourceProviderPipeline())
	properties, err := fileURL.GetProperties(p.ctx)
	if err != nil {
		return time.Time{}, err
	}

	// for reviewers: Should we worry about this here?
	smbLastWrite, err := p.GetFileSMBLastWriteTime()
	if err != nil {
		return time.Time{}, err
	}

	if properties.LastModified().After(smbLastWrite) {
		return properties.LastModified(), nil
	} else {
		return smbLastWrite, nil
	}
}
