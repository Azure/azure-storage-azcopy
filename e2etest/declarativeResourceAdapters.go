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

import "github.com/Azure/azure-storage-blob-go/azblob"

func sval(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// Our resource adapters adapt between blob/File etc and objectProperties
// They don't share any common interface

// Adapts testObject to blob.
// Note that lastWriteTime and creationTime are read only for blob.
// While smbAttributes and smbPermissionsSddl don't exist in blob and will be ignored
type blobResourceAdapter struct {
	obj *testObject
}

func (a blobResourceAdapter) toHeaders() azblob.BlobHTTPHeaders {
	props := a.obj.creationProperties.contentHeaders
	if props == nil {
		return azblob.BlobHTTPHeaders{}
	}
	return azblob.BlobHTTPHeaders{
		ContentType:        sval(props.contentType),
		ContentMD5:         props.contentMD5,
		ContentEncoding:    sval(props.contentEncoding),
		ContentLanguage:    sval(props.contentLanguage),
		ContentDisposition: sval(props.contentDisposition),
		CacheControl:       sval(props.cacheControl),
	}
}

func (a blobResourceAdapter) toMetadata() azblob.Metadata {
	if a.obj.creationProperties.nameValueMetadata == nil {
		return azblob.Metadata{}
	}
	return *a.obj.creationProperties.nameValueMetadata
}
