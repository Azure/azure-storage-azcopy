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
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/Azure/azure-storage-azcopy/v10/common"
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

func (e emptyPropertiesAdapter) BlobType() blob.BlobType {
	return ""
}

func (e emptyPropertiesAdapter) AccessTier() blob.AccessTier {
	return ""
}

func (e emptyPropertiesAdapter) ArchiveStatus() blob.ArchiveStatus {
	return ""
}

func (e emptyPropertiesAdapter) LeaseDuration() lease.DurationType {
	return ""
}

func (e emptyPropertiesAdapter) LeaseState() lease.StateType {
	return ""
}

func (e emptyPropertiesAdapter) LeaseStatus() lease.StatusType {
	return ""
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
	*blob.GetPropertiesResponse
}

func (a blobPropertiesResponseAdapter) CacheControl() string {
	return common.IffNotNil(a.GetPropertiesResponse.CacheControl, "")
}

func (a blobPropertiesResponseAdapter) ContentDisposition() string {
	return common.IffNotNil(a.GetPropertiesResponse.ContentDisposition, "")
}

func (a blobPropertiesResponseAdapter) ContentEncoding() string {
	return common.IffNotNil(a.GetPropertiesResponse.ContentEncoding, "")
}

func (a blobPropertiesResponseAdapter) ContentLanguage() string {
	return common.IffNotNil(a.GetPropertiesResponse.ContentLanguage, "")
}

func (a blobPropertiesResponseAdapter) ContentType() string {
	return common.IffNotNil(a.GetPropertiesResponse.ContentType, "")
}

func (a blobPropertiesResponseAdapter) ContentMD5() []byte {
	return a.GetPropertiesResponse.ContentMD5
}

// TODO (gapra) : In a future PR use iffNotNil
func (a blobPropertiesResponseAdapter) BlobType() blob.BlobType {
	if a.GetPropertiesResponse.BlobType == nil {
		return ""
	}
	return *a.GetPropertiesResponse.BlobType
}

func (a blobPropertiesResponseAdapter) AccessTier() blob.AccessTier {
	if a.GetPropertiesResponse.AccessTier == nil {
		return ""
	}
	return blob.AccessTier(*a.GetPropertiesResponse.AccessTier)
}

func (a blobPropertiesResponseAdapter) ArchiveStatus() blob.ArchiveStatus {
	if a.GetPropertiesResponse.ArchiveStatus == nil {
		return ""
	}
	return blob.ArchiveStatus(*a.GetPropertiesResponse.ArchiveStatus)
}

// LeaseDuration returns the value for header x-ms-lease-duration.
func (a blobPropertiesResponseAdapter) LeaseDuration() lease.DurationType {
	if a.GetPropertiesResponse.LeaseDuration == nil {
		return ""
	}
	return *a.GetPropertiesResponse.LeaseDuration
}

// LeaseState returns the value for header x-ms-lease-state.
func (a blobPropertiesResponseAdapter) LeaseState() lease.StateType {
	if a.GetPropertiesResponse.LeaseState == nil {
		return ""
	}
	return *a.GetPropertiesResponse.LeaseState
}

// LeaseStatus returns the value for header x-ms-lease-status.
func (a blobPropertiesResponseAdapter) LeaseStatus() lease.StatusType {
	if a.GetPropertiesResponse.LeaseStatus == nil {
		return ""
	}
	return *a.GetPropertiesResponse.LeaseStatus
}

// blobPropertiesAdapter adapts a BlobProperties object to both the
// contentPropsProvider and blobPropsProvider interfaces
type blobPropertiesAdapter struct {
	BlobProperties *container.BlobProperties
}

func (a blobPropertiesAdapter) CacheControl() string {
	return common.IffNotNil(a.BlobProperties.CacheControl, "")
}

func (a blobPropertiesAdapter) ContentDisposition() string {
	return common.IffNotNil(a.BlobProperties.ContentDisposition, "")
}

func (a blobPropertiesAdapter) ContentEncoding() string {
	return common.IffNotNil(a.BlobProperties.ContentEncoding, "")
}

func (a blobPropertiesAdapter) ContentLanguage() string {
	return common.IffNotNil(a.BlobProperties.ContentLanguage, "")
}

func (a blobPropertiesAdapter) ContentType() string {
	return common.IffNotNil(a.BlobProperties.ContentType, "")
}

func (a blobPropertiesAdapter) ContentMD5() []byte {
	return a.BlobProperties.ContentMD5
}

func (a blobPropertiesAdapter) BlobType() blob.BlobType {
	if a.BlobProperties.BlobType == nil {
		return ""
	}
	return *a.BlobProperties.BlobType
}

func (a blobPropertiesAdapter) AccessTier() blob.AccessTier {
	if a.BlobProperties.AccessTier == nil {
		return ""
	}
	return *a.BlobProperties.AccessTier
}

// LeaseDuration returns the value for header x-ms-lease-duration.
func (a blobPropertiesAdapter) LeaseDuration() lease.DurationType {
	if a.BlobProperties.LeaseDuration == nil {
		return ""
	}
	return *a.BlobProperties.LeaseDuration
}

// LeaseState returns the value for header x-ms-lease-state.
func (a blobPropertiesAdapter) LeaseState() lease.StateType {
	if a.BlobProperties.LeaseState == nil {
		return ""
	}
	return *a.BlobProperties.LeaseState
}

// LeaseStatus returns the value for header x-ms-lease-status.
func (a blobPropertiesAdapter) LeaseStatus() lease.StatusType {
	if a.BlobProperties.LeaseStatus == nil {
		return ""
	}
	return *a.BlobProperties.LeaseStatus
}

func (a blobPropertiesAdapter) ArchiveStatus() blob.ArchiveStatus {
	if a.BlobProperties.ArchiveStatus == nil {
		return ""
	}
	return *a.BlobProperties.ArchiveStatus
}
