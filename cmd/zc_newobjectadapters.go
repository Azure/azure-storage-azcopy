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

package cmd

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

var noContentProps = emptyPropertiesAdapter{}
var noBlobProps = emptyPropertiesAdapter{}
var noMetdata common.Metadata = nil

// emptyPropertiesAdapter supplies empty (zero-like) values
// for all methods in contentPropsProvider and blobPropsProvider
type emptyPropertiesAdapter struct{}

func (e emptyPropertiesAdapter) CacheControl() string {
	return ""
}

func (e emptyPropertiesAdapter) ContentDisposition() string {
	return ""
}

func (e emptyPropertiesAdapter) ContentEncoding() string {
	return ""
}

func (e emptyPropertiesAdapter) ContentLanguage() string {
	return ""
}

func (e emptyPropertiesAdapter) ContentType() string {
	return ""
}

func (e emptyPropertiesAdapter) ContentMD5() []byte {
	return make([]byte, 0)
}

func (e emptyPropertiesAdapter) BlobType() azblob.BlobType {
	return azblob.BlobNone
}

func (e emptyPropertiesAdapter) AccessTier() azblob.AccessTierType {
	return azblob.AccessTierNone
}

// md5OnlyAdapter is like emptyProperties adapter, except for the ContentMD5
// method, for which it returns a real value
type md5OnlyAdapter struct {
	emptyPropertiesAdapter
	md5 []byte
}

func (m md5OnlyAdapter) ContentMD5() []byte {
	return m.md5
}

// blobPropertiesResponseAdapter adapts a BlobGetPropertiesResponse to the blobPropsProvider interface
type blobPropertiesResponseAdapter struct {
	*azblob.BlobGetPropertiesResponse
}

func (a blobPropertiesResponseAdapter) AccessTier() azblob.AccessTierType {
	return azblob.AccessTierType(a.BlobGetPropertiesResponse.AccessTier())
}

// blobPropertiesAdapter adapts a BlobProperties object to both the
// contentPropsProvider and blobPropsProvider interfaces
type blobPropertiesAdapter struct {
	azblob.BlobProperties
}

func (a blobPropertiesAdapter) CacheControl() string {
	return common.IffStringNotNil(a.BlobProperties.CacheControl, "")
}

func (a blobPropertiesAdapter) ContentDisposition() string {
	return common.IffStringNotNil(a.BlobProperties.ContentDisposition, "")
}

func (a blobPropertiesAdapter) ContentEncoding() string {
	return common.IffStringNotNil(a.BlobProperties.ContentEncoding, "")
}

func (a blobPropertiesAdapter) ContentLanguage() string {
	return common.IffStringNotNil(a.BlobProperties.ContentLanguage, "")
}

func (a blobPropertiesAdapter) ContentType() string {
	return common.IffStringNotNil(a.BlobProperties.ContentType, "")
}

func (a blobPropertiesAdapter) ContentMD5() []byte {
	return a.BlobProperties.ContentMD5
}

func (a blobPropertiesAdapter) BlobType() azblob.BlobType {
	return a.BlobProperties.BlobType
}

func (a blobPropertiesAdapter) AccessTier() azblob.AccessTierType {
	return a.BlobProperties.AccessTier
}
