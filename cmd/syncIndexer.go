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
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type limitedStoredObject struct {
	relativeCapitalization []bool
	lmt                    time.Time
	entityType             common.EntityType
}

func (o limitedStoredObject) ToStoredObject(p string) StoredObject {
	return StoredObject{
		entityType:       o.entityType,
		lastModifiedTime: o.lmt,
		relativePath:     o.GetCapitalizedRelPath(p),
	}
}

func (s *limitedStoredObject) isMoreRecentThan(storedObject2 StoredObject) bool {
	return s.lmt.After(storedObject2.lastModifiedTime)
}

func (o *limitedStoredObject) StoreCapitalization(p string) {
	o.relativeCapitalization = make([]bool, len(p))

	for k, v := range p {
		o.relativeCapitalization[k] = strings.ToLower(string(v)) != string(v)
	}
}

func (o limitedStoredObject) GetCapitalizedRelPath(p string) string {
	if o.relativeCapitalization == nil {
		return p // it's already capitalized.
	}

	out := ""

	for k, v := range p {
		if o.relativeCapitalization[k] {
			out += strings.ToUpper(string(v))
		} else {
			out += string(v)
		}
	}

	return out
}

// the objectIndexer is essential for the generic sync enumerator to work
// it can serve as a:
// 		1. objectProcessor: accumulate a lookup map with given StoredObjects
//		2. resourceTraverser: go through the entities in the map like a traverser
type objectIndexer struct {
	indexMap map[string]limitedStoredObject
	counter  int

	// isDestinationCaseInsensitive is true when the destination is case-insensitive
	// In Windows, both paths D:\path\to\dir and D:\Path\TO\DiR point to the same resource.
	// Apple File System (APFS) can be configured to be case-sensitive or case-insensitive.
	// So for such locations, the key in the indexMap will be lowercase to avoid infinite syncing.
	isDestinationCaseInsensitive bool
}

func newObjectIndexer() *objectIndexer {
	return &objectIndexer{indexMap: make(map[string]limitedStoredObject)}
}

// process the given stored object by indexing it using its relative path
func (i *objectIndexer) store(storedObject StoredObject) (err error) {
	// TODO we might buffer too much data in memory, figure out whether we should limit the max number of files
	// TODO previously we used 10M as the max, but it was proven to be too small for some users

	// It is safe to index all StoredObjects just by relative path, regardless of their entity type, because
	// no filesystem allows a file and a folder to have the exact same full path.  This is true of
	// Linux file systems, Windows, Azure Files and ADLS Gen 2 (and logically should be true of all file systems).
	if i.isDestinationCaseInsensitive {
		lcRelativePath := strings.ToLower(storedObject.relativePath)
		tmp := limitedStoredObject{
			lmt:        storedObject.lastModifiedTime,
			entityType: storedObject.entityType,
		}

		tmp.StoreCapitalization(storedObject.relativePath)

		i.indexMap[lcRelativePath] = tmp
	} else {
		i.indexMap[storedObject.relativePath] = limitedStoredObject{
			lmt:        storedObject.lastModifiedTime,
			entityType: storedObject.entityType,
		}
	}
	i.counter += 1
	return
}

// go through the remaining stored objects in the map to process them
func (i *objectIndexer) traverse(processor objectProcessor, filters []ObjectFilter) (err error) {
	for relPath, value := range i.indexMap { // todo: can we preserve casing while
		err = processIfPassedFilters(filters, value.ToStoredObject(relPath), processor)
		_, err = getProcessingError(err)
		if err != nil {
			return
		}
	}
	return
}
