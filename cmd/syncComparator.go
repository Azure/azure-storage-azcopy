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
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"reflect"
	"strings"
)

// with the help of an objectIndexer containing the source objects
// find out the destination objects that should be transferred
// in other words, this should be used when destination is being enumerated secondly
type syncDestinationComparator struct {
	// the rejected objects would be passed to the destinationCleaner
	destinationCleaner objectProcessor

	// the processor responsible for scheduling copy transfers
	copyTransferScheduler objectProcessor

	// storing the source objects
	sourceIndex *objectIndexer

	comparisonHashType common.SyncHashType

	disableComparison bool
}

func newSyncDestinationComparator(i *objectIndexer, copyScheduler, cleaner objectProcessor, comparisonHashType common.SyncHashType, disableComparison bool) *syncDestinationComparator {
	return &syncDestinationComparator{sourceIndex: i, copyTransferScheduler: copyScheduler, destinationCleaner: cleaner, disableComparison: disableComparison, comparisonHashType: comparisonHashType}
}

// it will only schedule transfers for destination objects that are present in the indexer but stale compared to the entry in the map
// if the destinationObject is not at the source, it will be passed to the destinationCleaner
// ex: we already know what the source contains, now we are looking at objects at the destination
// if file x from the destination exists at the source, then we'd only transfer it if it is considered stale compared to its counterpart at the source
// if file x does not exist at the source, then it is considered extra, and will be deleted
func (f *syncDestinationComparator) processIfNecessary(destinationObject StoredObject) error {
	sourceObjectInMap, present := f.sourceIndex.indexMap[destinationObject.relativePath]
	if !present && f.sourceIndex.isDestinationCaseInsensitive {
		lcRelativePath := strings.ToLower(destinationObject.relativePath)
		sourceObjectInMap, present = f.sourceIndex.indexMap[lcRelativePath]
	}

	// if the destinationObject is present at source and stale, we transfer the up-to-date version from source
	if present {
		defer delete(f.sourceIndex.indexMap, destinationObject.relativePath)

		if f.disableComparison {
			return f.copyTransferScheduler(sourceObjectInMap)
		}

		if f.comparisonHashType != common.ESyncHashType.None() {
			switch f.comparisonHashType {
			case common.ESyncHashType.MD5():
				if !reflect.DeepEqual(sourceObjectInMap.md5, destinationObject.md5) {
					// hash inequality = source "newer" in this model.
					return f.copyTransferScheduler(sourceObjectInMap)
				}
			default:
				panic("sanity check: unsupported hash type " + f.comparisonHashType.String())
			}
		} else if sourceObjectInMap.isMoreRecentThan(destinationObject) {
			return f.copyTransferScheduler(sourceObjectInMap)
		}
	} else {
		// purposefully ignore the error from destinationCleaner
		// it's a tolerable error, since it just means some extra destination object might hang around a bit longer
		_ = f.destinationCleaner(destinationObject)
	}

	return nil
}

// with the help of an objectIndexer containing the destination objects
// filter out the source objects that should be transferred
// in other words, this should be used when source is being enumerated secondly
type syncSourceComparator struct {
	// the processor responsible for scheduling copy transfers
	copyTransferScheduler objectProcessor

	// storing the destination objects
	destinationIndex *objectIndexer

	comparisonHashType common.SyncHashType

	disableComparison bool
}

func newSyncSourceComparator(i *objectIndexer, copyScheduler objectProcessor, comparisonHashType common.SyncHashType, disableComparison bool) *syncSourceComparator {
	return &syncSourceComparator{destinationIndex: i, copyTransferScheduler: copyScheduler, disableComparison: disableComparison, comparisonHashType: comparisonHashType}
}

// it will only transfer source items that are:
//  1. not present in the map
//  2. present but is more recent than the entry in the map
//
// note: we remove the StoredObject if it is present so that when we have finished
// the index will contain all objects which exist at the destination but were NOT seen at the source
func (f *syncSourceComparator) processIfNecessary(sourceObject StoredObject) error {
	relPath := sourceObject.relativePath

	if f.destinationIndex.isDestinationCaseInsensitive {
		relPath = strings.ToLower(relPath)
	}

	destinationObjectInMap, present := f.destinationIndex.indexMap[relPath]

	if present {
		defer delete(f.destinationIndex.indexMap, relPath)

		if f.disableComparison {
			return f.copyTransferScheduler(sourceObject)
		}

		if f.comparisonHashType != common.ESyncHashType.None() {
			switch f.comparisonHashType {
			case common.ESyncHashType.MD5():
				if !reflect.DeepEqual(sourceObject.md5, destinationObjectInMap.md5) {
					// hash inequality = source "newer" in this model.
					return f.copyTransferScheduler(sourceObject)
				}
			default:
				panic("sanity check: unsupported hash type " + f.comparisonHashType.String())
			}
		} else if sourceObject.isMoreRecentThan(destinationObjectInMap) {
			// if destination is stale, schedule source
			return f.copyTransferScheduler(sourceObject)
		}
		// skip if dest is more recent
		return nil
	}

	// if source does not exist at the destination, then schedule it for transfer
	return f.copyTransferScheduler(sourceObject)
}
