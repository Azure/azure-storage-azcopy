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

package cmd

import (
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// Design explanation:
/*
Blob type exclusion is required as a part of the copy enumerators refactor. This would be used in Download and S2S scenarios.
This map is used effectively as a hash set. If an item exists in the set, it does not pass the filter.
*/
type excludeBlobTypeFilter struct {
	blobTypes map[blob.BlobType]bool
}

func (f *excludeBlobTypeFilter) DoesSupportThisOS() (msg string, supported bool) {
	return "", true
}

func (f *excludeBlobTypeFilter) AppliesOnlyToFiles() bool {
	return true // there aren't any (real) folders in Blob Storage
}

func (f *excludeBlobTypeFilter) DoesPass(object traverser.StoredObject) bool {
	if _, ok := f.blobTypes[object.BlobType]; !ok {
		// For readability purposes, focus on returning false.
		// Basically, the statement says "If the blob type is not present in the list, the object passes the filters."
		return true
	}

	return false
}

////////

// includeAfterDateFilter includes files with Last Modified Times >= the specified threshold
// Used for copy, but doesn't make conceptual sense for sync
type IncludeAfterDateFilter struct {
	Threshold time.Time
}

func (f *IncludeAfterDateFilter) DoesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *IncludeAfterDateFilter) AppliesOnlyToFiles() bool {
	return false
}

func (f *IncludeAfterDateFilter) DoesPass(storedObject traverser.StoredObject) bool {
	zeroTime := time.Time{}
	if storedObject.LastModifiedTime == zeroTime {
		panic("cannot use IncludeAfterDateFilter on an object for which no Last Modified Time has been retrieved")
	}

	return storedObject.LastModifiedTime.After(f.Threshold) ||
		storedObject.LastModifiedTime.Equal(f.Threshold) // >= is easier for users to understand than >
}

// IncludeBeforeDateFilter includes files with Last Modified Times <= the specified Threshold
// Used for copy, but doesn't make conceptual sense for sync
type IncludeBeforeDateFilter struct {
	Threshold time.Time
}

func (f *IncludeBeforeDateFilter) DoesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *IncludeBeforeDateFilter) AppliesOnlyToFiles() bool {
	return false
}

func (f *IncludeBeforeDateFilter) DoesPass(storedObject traverser.StoredObject) bool {
	zeroTime := time.Time{}
	if storedObject.LastModifiedTime == zeroTime {
		panic("cannot use IncludeBeforeDateFilter on an object for which no Last Modified Time has been retrieved")
	}

	return storedObject.LastModifiedTime.Before(f.Threshold) ||
		storedObject.LastModifiedTime.Equal(f.Threshold) // <= is easier for users to understand than <
}

type permDeleteFilter struct {
	deleteSnapshots bool
	deleteVersions  bool
}

func (s *permDeleteFilter) DoesSupportThisOS() (msg string, supported bool) {
	return "", true
}

func (s *permDeleteFilter) AppliesOnlyToFiles() bool {
	return false
}

func (s *permDeleteFilter) DoesPass(storedObject traverser.StoredObject) bool {
	if (s.deleteVersions && s.deleteSnapshots) && storedObject.BlobDeleted && (storedObject.BlobVersionID != "" || storedObject.BlobSnapshotID != "") {
		return true
	} else if s.deleteSnapshots && storedObject.BlobDeleted && storedObject.BlobSnapshotID != "" {
		return true
	} else if s.deleteVersions && storedObject.BlobDeleted && storedObject.BlobVersionID != "" {
		return true
	}
	return false
}

func buildIncludeSoftDeleted(permanentDeleteOption common.PermanentDeleteOption) []traverser.ObjectFilter {
	filters := make([]traverser.ObjectFilter, 0)
	switch permanentDeleteOption {
	case common.EPermanentDeleteOption.Snapshots():
		filters = append(filters, &permDeleteFilter{deleteSnapshots: true})
	case common.EPermanentDeleteOption.Versions():
		filters = append(filters, &permDeleteFilter{deleteVersions: true})
	case common.EPermanentDeleteOption.SnapshotsAndVersions():
		filters = append(filters, &permDeleteFilter{deleteSnapshots: true, deleteVersions: true})
	}
	return filters
}
