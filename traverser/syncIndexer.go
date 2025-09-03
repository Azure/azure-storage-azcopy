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

package traverser

import (
	"strings"
)

// the ObjectIndexer is essential for the generic sync enumerator to work
// it can serve as a:
//  1. objectProcessor: accumulate a lookup map with given StoredObjects
//  2. resourceTraverser: go through the entities in the map like a traverser
type ObjectIndexer struct {
	IndexMap map[string]StoredObject
	counter  int

	// IsDestinationCaseInsensitive is true when the destination is case-insensitive
	// In Windows, both paths D:\path\to\dir and D:\Path\TO\DiR point to the same resource.
	// Apple File System (APFS) can be configured to be case-sensitive or case-insensitive.
	// So for such locations, the key in the IndexMap will be lowercase to avoid infinite syncing.
	IsDestinationCaseInsensitive bool
}

func NewObjectIndexer() *ObjectIndexer {
	return &ObjectIndexer{IndexMap: make(map[string]StoredObject)}
}

// process the given stored object by indexing it using its relative path
func (i *ObjectIndexer) Store(storedObject StoredObject) (err error) {
	// TODO we might buffer too much data in memory, figure out whether we should limit the max number of files
	// TODO previously we used 10M as the max, but it was proven to be too small for some users

	// It is safe to index all StoredObjects just by relative path, regardless of their entity type, because
	// no filesystem allows a file and a folder to have the exact same full path.  This is true of
	// Linux file systems, Windows, Azure Files and ADLS Gen 2 (and logically should be true of all file systems).
	if i.IsDestinationCaseInsensitive {
		lcRelativePath := strings.ToLower(storedObject.RelativePath)
		i.IndexMap[lcRelativePath] = storedObject
	} else {
		i.IndexMap[storedObject.RelativePath] = storedObject
	}
	i.counter += 1
	return
}

// go through the remaining stored objects in the map to process them
func (i *ObjectIndexer) Traverse(processor ObjectProcessor, filters []ObjectFilter) (err error) {
	for _, value := range i.IndexMap {
		err = ProcessIfPassedFilters(filters, value, processor)
		_, err = GetProcessingError(err)
		if err != nil {
			return
		}
	}
	return
}
