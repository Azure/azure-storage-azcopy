// Copyright © Microsoft <wastore@microsoft.com>
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

package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/Azure/azure-storage-azcopy/v10/sddl"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

// Our resource adapters convert from objectProperties to the metadata and other objects for Blob/File etc
// They don't share any common interface, because blob/file etc don't share a common interface.
// The reverse conversion, from the remote format back to objectProperties, is in resourceManager.getAllProperties.

// Adapts testObject to blob.
// Doesn't need to deal with anything except contentHeaders and metadata, because that's all Blob supports
type blobResourceAdapter struct {
	obj *testObject
}

func (a blobResourceAdapter) toHeaders() *blob.HTTPHeaders {
	props := a.obj.creationProperties.contentHeaders
	if props == nil {
		return nil
	}
	return &blob.HTTPHeaders{
		BlobContentType:        props.contentType,
		BlobContentMD5:         props.contentMD5,
		BlobContentEncoding:    props.contentEncoding,
		BlobContentLanguage:    props.contentLanguage,
		BlobContentDisposition: props.contentDisposition,
		BlobCacheControl:       props.cacheControl,
	}
}

type filesResourceAdapter struct {
	obj *testObject
}

func (a filesResourceAdapter) toSMBProperties(c asserter) *file.SMBProperties {
	return &file.SMBProperties{
		Attributes:    a.toAttributes(c),
		LastWriteTime: a.obj.creationProperties.lastWriteTime,
	}
}

func (a filesResourceAdapter) toAttributes(c asserter) *file.NTFSFileAttributes {
	if a.obj.creationProperties.smbAttributes != nil {
		attr, err := ste.FileAttributesFromUint32(*a.obj.creationProperties.smbAttributes)
		c.AssertNoErr(err)
		return attr
	}
	return nil
}

func (a filesResourceAdapter) toPermissions(c asserter, shareClient *share.Client) *file.Permissions {
	if a.obj.creationProperties.smbPermissionsSddl != nil {
		permissions := file.Permissions{}
		parsedSDDL, err := sddl.ParseSDDL(*a.obj.creationProperties.smbPermissionsSddl)
		c.AssertNoErr(err, "Failed to parse SDDL")

		var permKey string

		if len(parsedSDDL.PortableString()) > 8000 {
			createPermResp, err := shareClient.CreatePermission(ctx, parsedSDDL.PortableString(), nil)
			c.AssertNoErr(err)

			permKey = *createPermResp.FilePermissionKey
		}

		if permKey != "" {
			permissions.PermissionKey = &permKey
		} else {
			perm := parsedSDDL.PortableString()
			permissions.Permission = &perm
		}
		return &permissions
	}
	return nil
}

func (a filesResourceAdapter) toHeaders() *file.HTTPHeaders {
	props := a.obj.creationProperties.contentHeaders
	if props == nil {
		return nil
	}

	return &file.HTTPHeaders{
		ContentType:        props.contentType,
		ContentMD5:         props.contentMD5,
		ContentEncoding:    props.contentEncoding,
		ContentLanguage:    props.contentLanguage,
		ContentDisposition: props.contentDisposition,
		CacheControl:       props.cacheControl,
	}
}
