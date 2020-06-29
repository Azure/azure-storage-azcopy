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

type richSMBPropertyHolder interface {
	azfile.SMBPropertyHolder
	FilePermissionKey() string
	NewMetadata() azfile.Metadata
	LastModified() time.Time
}

type contentPropsProvider interface {
	CacheControl() string
	ContentDisposition() string
	ContentEncoding() string
	ContentLanguage() string
	ContentType() string
	ContentMD5() []byte
}

// Source info provider for Azure blob
type fileSourceInfoProvider struct {
	ctx                 context.Context
	cachedPermissionKey string
	cacheOnce           *sync.Once
	cachedProperties    richSMBPropertyHolder // use interface because may be file or directory properties
	defaultRemoteSourceInfoProvider
}

func newFileSourceInfoProvider(jptm IJobPartTransferMgr) (ISourceInfoProvider, error) {
	base, err := newDefaultRemoteSourceInfoProvider(jptm)
	if err != nil {
		return nil, err
	}

	// due to the REST parity feature added in 2019-02-02, the File APIs are no longer backward compatible
	// so we must use the latest SDK version to stay safe
	ctx := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azfile.ServiceVersion)

	return &fileSourceInfoProvider{defaultRemoteSourceInfoProvider: *base, ctx: ctx, cacheOnce: &sync.Once{}}, nil
}

func (p *fileSourceInfoProvider) getFreshProperties() (richSMBPropertyHolder, error) {
	presigned, err := p.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	switch p.EntityType() {
	case common.EEntityType.File():
		fileURL := azfile.NewFileURL(*presigned, p.jptm.SourceProviderPipeline())
		return fileURL.GetProperties(p.ctx)
	case common.EEntityType.Folder():
		dirURL := azfile.NewDirectoryURL(*presigned, p.jptm.SourceProviderPipeline())
		return dirURL.GetProperties(p.ctx)
	default:
		panic("unexpected case")
	}
}

// cached because we use it for both GetSMBProperties and GetSDDL, and in some cases (e.g. small files,
// or enough transactions that transaction costs matter) saving IOPS matters
func (p *fileSourceInfoProvider) getCachedProperties() (richSMBPropertyHolder, error) {
	var err error

	p.cacheOnce.Do(func() {
		p.cachedProperties, err = p.getFreshProperties()
	})

	return p.cachedProperties, err
}

func (p *fileSourceInfoProvider) GetSMBProperties() (TypedSMBPropertyHolder, error) {
	cachedProps, err := p.getCachedProperties()

	return &azfile.SMBPropertyAdapter{PropertySource: cachedProps}, err
}

func (p *fileSourceInfoProvider) GetSDDL() (string, error) {
	// Get the key for SIPM
	props, err := p.getCachedProperties()
	if err != nil {
		return "", err
	}
	key := props.FilePermissionKey()
	if key == "" {
		return "", nil
	}

	// Call into SIPM and grab our SDDL string.
	sipm := p.jptm.SecurityInfoPersistenceManager()
	presigned, err := p.PreSignedSourceURL()
	if err != nil {
		return "", err
	}
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

		properties, err := p.getCachedProperties()
		if err != nil {
			return nil, err
		}
		// TODO: is it OK that this does not get set if s2sGetPropertiesInBackend is false?  Probably yes, because it's only a cached value, and getPropertiesInBackend is always false of AzFiles anyway at present (early 2020)
		p.cachedPermissionKey = properties.FilePermissionKey() // We cache this as getting the SDDL is a separate operation.

		switch p.EntityType() {
		case common.EEntityType.File():
			fileProps := properties.(contentPropsProvider)
			srcProperties = &SrcProperties{
				SrcHTTPHeaders: common.ResourceHTTPHeaders{
					ContentType:        fileProps.ContentType(),
					ContentEncoding:    fileProps.ContentEncoding(),
					ContentDisposition: fileProps.ContentDisposition(),
					ContentLanguage:    fileProps.ContentLanguage(),
					CacheControl:       fileProps.CacheControl(),
					ContentMD5:         fileProps.ContentMD5(),
				},
				SrcMetadata: common.FromAzFileMetadataToCommonMetadata(properties.NewMetadata()),
			}
		case common.EEntityType.Folder():
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

func (p *fileSourceInfoProvider) GetFreshFileLastModifiedTime() (time.Time, error) {
	if p.EntityType() != common.EEntityType.File() {
		panic("unsupported. Cannot get modification time on non-file object") // nothing should ever call this for a non-file
	}

	properties, err := p.getFreshProperties()
	if err != nil {
		return time.Time{}, err
	}

	// We ignore smblastwrite because otherwise the tx will fail s2s
	return properties.LastModified(), nil
}
