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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	sharedirectory "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	sharefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var noContentProps = emptyPropertiesAdapter{}
var noBlobProps = emptyPropertiesAdapter{}
var noMetadata common.Metadata = nil

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

func (e emptyPropertiesAdapter) ContentLength() int64 {
	return 0
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

func (e emptyPropertiesAdapter) LastModified() time.Time {
	return time.Time{}
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

// blobPropertiesResponseAdapter adapts a BlobGetPropertiesResponse to the blobPropsProvider interface
type blobPropertiesResponseAdapter struct {
	*blob.GetPropertiesResponse
}

func (a blobPropertiesResponseAdapter) LastModified() time.Time {
	return common.IffNotNil(a.GetPropertiesResponse.LastModified, time.Time{})
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

func (a blobPropertiesResponseAdapter) ContentLength() int64 {
	return common.IffNotNil(a.GetPropertiesResponse.ContentLength, 0)
}

func (a blobPropertiesResponseAdapter) BlobType() blob.BlobType {
	return common.IffNotNil(a.GetPropertiesResponse.BlobType, "")
}

func (a blobPropertiesResponseAdapter) AccessTier() blob.AccessTier {
	return blob.AccessTier(common.IffNotNil(a.GetPropertiesResponse.AccessTier, ""))
}

func (a blobPropertiesResponseAdapter) ArchiveStatus() blob.ArchiveStatus {
	return blob.ArchiveStatus(common.IffNotNil(a.GetPropertiesResponse.ArchiveStatus, ""))
}

// LeaseDuration returns the value for header x-ms-lease-duration.
func (a blobPropertiesResponseAdapter) LeaseDuration() lease.DurationType {
	return common.IffNotNil(a.GetPropertiesResponse.LeaseDuration, "")
}

// LeaseState returns the value for header x-ms-lease-state.
func (a blobPropertiesResponseAdapter) LeaseState() lease.StateType {
	return common.IffNotNil(a.GetPropertiesResponse.LeaseState, "")
}

// LeaseStatus returns the value for header x-ms-lease-status.
func (a blobPropertiesResponseAdapter) LeaseStatus() lease.StatusType {
	return common.IffNotNil(a.GetPropertiesResponse.LeaseStatus, "")
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

func (a blobPropertiesAdapter) ContentLength() int64 {
	return common.IffNotNil(a.BlobProperties.ContentLength, 0)
}

func (a blobPropertiesAdapter) BlobType() blob.BlobType {
	return common.IffNotNil(a.BlobProperties.BlobType, "")
}

func (a blobPropertiesAdapter) AccessTier() blob.AccessTier {
	return common.IffNotNil(a.BlobProperties.AccessTier, "")
}

// LeaseDuration returns the value for header x-ms-lease-duration.
func (a blobPropertiesAdapter) LeaseDuration() lease.DurationType {
	return common.IffNotNil(a.BlobProperties.LeaseDuration, "")
}

// LeaseState returns the value for header x-ms-lease-state.
func (a blobPropertiesAdapter) LeaseState() lease.StateType {
	return common.IffNotNil(a.BlobProperties.LeaseState, "")
}

// LeaseStatus returns the value for header x-ms-lease-status.
func (a blobPropertiesAdapter) LeaseStatus() lease.StatusType {
	return common.IffNotNil(a.BlobProperties.LeaseStatus, "")
}

func (a blobPropertiesAdapter) ArchiveStatus() blob.ArchiveStatus {
	return common.IffNotNil(a.BlobProperties.ArchiveStatus, "")
}

func (a blobPropertiesAdapter) LastModified() time.Time {
	return common.IffNotNil(a.BlobProperties.LastModified, time.Time{})
}

type shareFilePropertiesAdapter struct {
	*sharefile.GetPropertiesResponse
}

func (a shareFilePropertiesAdapter) Metadata() common.Metadata {
	return a.GetPropertiesResponse.Metadata
}

func (a shareFilePropertiesAdapter) LastModified() time.Time {
	return common.IffNotNil(a.GetPropertiesResponse.LastModified, time.Time{})
}

func (a shareFilePropertiesAdapter) FileLastWriteTime() time.Time {
	return common.IffNotNil(a.GetPropertiesResponse.FileLastWriteTime, time.Time{})
}

func (a shareFilePropertiesAdapter) CacheControl() string {
	return common.IffNotNil(a.GetPropertiesResponse.CacheControl, "")
}

func (a shareFilePropertiesAdapter) ContentDisposition() string {
	return common.IffNotNil(a.GetPropertiesResponse.ContentDisposition, "")
}

func (a shareFilePropertiesAdapter) ContentEncoding() string {
	return common.IffNotNil(a.GetPropertiesResponse.ContentEncoding, "")
}

func (a shareFilePropertiesAdapter) ContentLanguage() string {
	return common.IffNotNil(a.GetPropertiesResponse.ContentLanguage, "")
}

func (a shareFilePropertiesAdapter) ContentType() string {
	return common.IffNotNil(a.GetPropertiesResponse.ContentType, "")
}

func (a shareFilePropertiesAdapter) ContentMD5() []byte {
	return a.GetPropertiesResponse.ContentMD5
}

func (a shareFilePropertiesAdapter) ContentLength() int64 {
	return common.IffNotNil(a.GetPropertiesResponse.ContentLength, 0)
}

func (a shareFilePropertiesAdapter) LinkCount() int64 {
	return common.IffNotNil(a.GetPropertiesResponse.LinkCount, 0)
}

type shareDirectoryPropertiesAdapter struct {
	*sharedirectory.GetPropertiesResponse
}

func (a shareDirectoryPropertiesAdapter) Metadata() common.Metadata {
	return a.GetPropertiesResponse.Metadata
}

func (a shareDirectoryPropertiesAdapter) LastModified() time.Time {
	return common.IffNotNil(a.GetPropertiesResponse.LastModified, time.Time{})
}

func (a shareDirectoryPropertiesAdapter) FileLastWriteTime() time.Time {
	return common.IffNotNil(a.GetPropertiesResponse.FileLastWriteTime, time.Time{})
}

func (a shareDirectoryPropertiesAdapter) CacheControl() string {
	return ""
}

func (a shareDirectoryPropertiesAdapter) ContentDisposition() string {
	return ""
}

func (a shareDirectoryPropertiesAdapter) ContentEncoding() string {
	return ""
}

func (a shareDirectoryPropertiesAdapter) ContentLanguage() string {
	return ""
}

func (a shareDirectoryPropertiesAdapter) ContentType() string {
	return ""
}

func (a shareDirectoryPropertiesAdapter) ContentMD5() []byte {
	return make([]byte, 0)
}

func (a shareDirectoryPropertiesAdapter) ContentLength() int64 {
	return 0
}

func (a shareDirectoryPropertiesAdapter) LinkCount() int64 {
	return 0
}
