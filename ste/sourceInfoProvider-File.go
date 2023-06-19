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
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"sync"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type shareFilePropertyProvider interface {
	FileCreationTime() time.Time
	FileLastWriteTime() time.Time
	FileAttributes() (*file.NTFSFileAttributes, error)
	FilePermissionKey() string
	Metadata() map[string]*string
	LastModified() time.Time
	CacheControl() string
	ContentDisposition() string
	ContentEncoding() string
	ContentLanguage() string
	ContentType() string
	ContentMD5() []byte
}

type fileGetPropertiesAdapter struct {
	GetProperties file.GetPropertiesResponse
}

func (f fileGetPropertiesAdapter) CacheControl() string {
	return common.IffNotNil(f.GetProperties.CacheControl, "")
}

func (f fileGetPropertiesAdapter) ContentDisposition() string {
	return common.IffNotNil(f.GetProperties.ContentDisposition, "")
}

func (f fileGetPropertiesAdapter) ContentEncoding() string {
	return common.IffNotNil(f.GetProperties.ContentEncoding, "")
}

func (f fileGetPropertiesAdapter) ContentLanguage() string {
	return common.IffNotNil(f.GetProperties.ContentLanguage, "")
}

func (f fileGetPropertiesAdapter) ContentType() string {
	return common.IffNotNil(f.GetProperties.ContentType, "")
}

func (f fileGetPropertiesAdapter) ContentMD5() []byte {
	return f.GetProperties.ContentMD5
}

func (f fileGetPropertiesAdapter) FileCreationTime() time.Time {
	return common.IffNotNil(f.GetProperties.FileCreationTime, time.Time{})
}

func (f fileGetPropertiesAdapter) FileLastWriteTime() time.Time {
	return common.IffNotNil(f.GetProperties.FileLastWriteTime, time.Time{})
}

func (f fileGetPropertiesAdapter) FileAttributes() (*file.NTFSFileAttributes, error) {
	return file.ParseNTFSFileAttributes(f.GetProperties.FileAttributes)
}

func (f fileGetPropertiesAdapter) FilePermissionKey() string {
	return common.IffNotNil(f.GetProperties.FilePermissionKey, "")
}

func (f fileGetPropertiesAdapter) Metadata() map[string]*string {
	return f.GetProperties.Metadata
}

func (f fileGetPropertiesAdapter) LastModified() time.Time {
	return common.IffNotNil(f.GetProperties.LastModified, time.Time{})
}

type directoryGetPropertiesAdapter struct {
	GetProperties directory.GetPropertiesResponse
}

func (d directoryGetPropertiesAdapter) CacheControl() string {
	return ""
}

func (d directoryGetPropertiesAdapter) ContentDisposition() string {
	return ""
}

func (d directoryGetPropertiesAdapter) ContentEncoding() string {
	return ""
}

func (d directoryGetPropertiesAdapter) ContentLanguage() string {
	return ""
}

func (d directoryGetPropertiesAdapter) ContentType() string {
	return ""
}

func (d directoryGetPropertiesAdapter) ContentMD5() []byte {
	return make([]byte, 0)
}

func (d directoryGetPropertiesAdapter) FileCreationTime() time.Time {
	return common.IffNotNil(d.GetProperties.FileCreationTime, time.Time{})
}

func (d directoryGetPropertiesAdapter) FileLastWriteTime() time.Time {
	return common.IffNotNil(d.GetProperties.FileLastWriteTime, time.Time{})
}

func (d directoryGetPropertiesAdapter) FileAttributes() (*file.NTFSFileAttributes, error) {
	return file.ParseNTFSFileAttributes(d.GetProperties.FileAttributes)
}

func (d directoryGetPropertiesAdapter) FilePermissionKey() string {
	return common.IffNotNil(d.GetProperties.FilePermissionKey, "")
}

func (d directoryGetPropertiesAdapter) Metadata() map[string]*string {
	return d.GetProperties.Metadata
}

func (d directoryGetPropertiesAdapter) LastModified() time.Time {
	return common.IffNotNil(d.GetProperties.LastModified, time.Time{})
}

// Source info provider for Azure blob
type fileSourceInfoProvider struct {
	ctx                 context.Context
	cachedPermissionKey string
	cacheOnce           *sync.Once
	cachedProperties    shareFilePropertyProvider // use interface because may be file or directory properties
	defaultRemoteSourceInfoProvider
}

func newFileSourceInfoProvider(jptm IJobPartTransferMgr) (ISourceInfoProvider, error) {
	base, err := newDefaultRemoteSourceInfoProvider(jptm)
	if err != nil {
		return nil, err
	}

	// due to the REST parity feature added in 2019-02-02, the File APIs are no longer backward compatible
	// so we must use the latest SDK version to stay safe
	//TODO: Should we do that?
	return &fileSourceInfoProvider{defaultRemoteSourceInfoProvider: *base, ctx: jptm.Context(), cacheOnce: &sync.Once{}}, nil
}

func (p *fileSourceInfoProvider) getFreshProperties() (shareFilePropertyProvider, error) {
	source, err := p.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	switch p.EntityType() {
	case common.EEntityType.File():
		fileClient := common.CreateShareFileClient(source, p.jptm.S2SSourceCredentialInfo(), p.jptm.CredentialOpOptions(), p.jptm.S2SSourceClientOptions())
		props, err := fileClient.GetProperties(p.ctx, nil)
		return &fileGetPropertiesAdapter{props}, err
	case common.EEntityType.Folder():
		directoryClient := common.CreateShareDirectoryClient(source, p.jptm.S2SSourceCredentialInfo(), p.jptm.CredentialOpOptions(), p.jptm.S2SSourceClientOptions())
		props, err := directoryClient.GetProperties(p.ctx, nil)
		return &directoryGetPropertiesAdapter{props}, err
	default:
		panic("unexpected case")
	}
}

// cached because we use it for both GetSMBProperties and GetSDDL, and in some cases (e.g. small files,
// or enough transactions that transaction costs matter) saving IOPS matters
func (p *fileSourceInfoProvider) getCachedProperties() (shareFilePropertyProvider, error) {
	var err error

	p.cacheOnce.Do(func() {
		p.cachedProperties, err = p.getFreshProperties()
	})

	return p.cachedProperties, err
}

func (p *fileSourceInfoProvider) GetSMBProperties() (TypedSMBPropertyHolder, error) {
	return p.getCachedProperties()
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
	source, err := p.PreSignedSourceURL()
	if err != nil {
		return "", err
	}
	fURLParts, err := file.ParseURL(source)
	if err != nil {
		return "", err
	}
	fURLParts.DirectoryOrFilePath = ""
	sddlString, err := sipm.GetSDDLFromID(key, fURLParts.String(), p.jptm.S2SSourceCredentialInfo(), p.jptm.CredentialOpOptions(), p.jptm.S2SSourceClientOptions())

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
			srcProperties = &SrcProperties{
				SrcHTTPHeaders: common.ResourceHTTPHeaders{
					ContentType:        properties.ContentType(),
					ContentEncoding:    properties.ContentEncoding(),
					ContentDisposition: properties.ContentDisposition(),
					ContentLanguage:    properties.ContentLanguage(),
					CacheControl:       properties.CacheControl(),
					ContentMD5:         properties.ContentMD5(),
				},
				SrcMetadata: properties.Metadata(),
			}
		case common.EEntityType.Folder():
			srcProperties = &SrcProperties{
				SrcHTTPHeaders: common.ResourceHTTPHeaders{}, // no contentType etc for folders
				SrcMetadata:    properties.Metadata(),
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
