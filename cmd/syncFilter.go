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

// with the help of an objectIndexer containing the source objects
// filter out the destination objects that should be transferred
// in other words, this should be used when destination is being enumerated secondly
type syncDestinationFilter struct {
	// the rejected objects would be passed to the destinationCleaner
	destinationCleaner objectProcessor

	// storing the source objects
	sourceIndex *objectIndexer
}

func newSyncDestinationFilter(i *objectIndexer, recyclers objectProcessor) objectFilter {
	return &syncDestinationFilter{sourceIndex: i, destinationCleaner: recyclers}
}

// it will only pass destination objects that are present in the indexer but stale compared to the entry in the map
// if the destinationObject is not present at all, it will be passed to the destinationCleaner
// ex: we already know what the source contains, now we are looking at objects at the destination
// if file x from the destination exists at the source, then we'd only transfer it if it is considered stale compared to its counterpart at the source
// if file x does not exist at the source, then it is considered extra, and will be deleted
func (f *syncDestinationFilter) doesPass(destinationObject storedObject) bool {
	storedObjectInMap, present := f.sourceIndex.indexMap[destinationObject.relativePath]

	// if the destinationObject is present and stale, we let it pass
	if present {
		defer delete(f.sourceIndex.indexMap, destinationObject.relativePath)

		if storedObjectInMap.isMoreRecentThan(destinationObject) {
			return true
		}
	} else {
		// purposefully ignore the error from destinationCleaner
		// it's a tolerable error, since it just means some extra destination object might hang around a bit longer
		_ = f.destinationCleaner(destinationObject)
	}

	return false
}

// with the help of an objectIndexer containing the destination objects
// filter out the source objects that should be transferred
// in other words, this should be used when source is being enumerated secondly
type syncSourceFilter struct {

	// storing the destination objects
	destinationIndex *objectIndexer
}

func newSyncSourceFilter(i *objectIndexer) objectFilter {
	return &syncSourceFilter{destinationIndex: i}
}

// it will only pass items that are:
//	1. not present in the map
//  2. present but is more recent than the entry in the map
// note: we remove the storedObject if it is present so that when we have finished
// the index will contain all objects which exist at the destination but were NOT passed to this routine
func (f *syncSourceFilter) doesPass(sourceObject storedObject) bool {
	storedObjectInMap, present := f.destinationIndex.indexMap[sourceObject.relativePath]

	// if the sourceObject is more recent, we let it pass
	if present {
		defer delete(f.destinationIndex.indexMap, sourceObject.relativePath)

		if sourceObject.isMoreRecentThan(storedObjectInMap) {
			return true
		}

		return false
	}

	return true
}
