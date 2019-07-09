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
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"strings"
	"time"
)

// -------------------------------------- Component Definitions -------------------------------------- \\
// the following interfaces and structs allow the sync enumerator
// to be generic and has as little duplicated code as possible

// represent a local or remote resource object (ex: local file, blob, etc.)
// we can add more properties if needed, as this is easily extensible
type storedObject struct {
	name             string
	lastModifiedTime time.Time
	size             int64
	md5              []byte
	blobType         azblob.BlobType // will be "None" when unknown or not applicable

	// partial path relative to its root directory
	// example: rootDir=/var/a/b/c fullPath=/var/a/b/c/d/e/f.pdf => relativePath=d/e/f.pdf name=f.pdf
	// note that sometimes the rootDir given by the user turns out to be a single file
	// example: rootDir=/var/a/b/c/d/e/f.pdf fullPath=/var/a/b/c/d/e/f.pdf => relativePath=""
	// in this case, since rootDir already points to the file, relatively speaking the path is nothing.
	relativePath string
}

const (
	blobTypeNA = azblob.BlobNone // some things, e.g. local files, aren't blobs so they don't have their own blob type so we use this "not applicable" constant
)

func (storedObject *storedObject) isMoreRecentThan(storedObject2 storedObject) bool {
	return storedObject.lastModifiedTime.After(storedObject2.lastModifiedTime)
}

// a constructor is used so that in case the storedObject has to change, the callers would get a compilation error
func newStoredObject(name string, relativePath string, lmt time.Time, size int64, md5 []byte, blobType azblob.BlobType) storedObject {
	return storedObject{
		name:             name,
		relativePath:     relativePath,
		lastModifiedTime: lmt,
		size:             size,
		md5:              md5,
		blobType:         blobType,
	}
}

// capable of traversing a structured resource like container or local directory
// pass each storedObject to the given objectProcessor if it passes all the filters
type resourceTraverser interface {
	traverse(processor objectProcessor, filters []objectFilter) error
}

// given a storedObject, process it accordingly
type objectProcessor func(storedObject storedObject) error

// given a storedObject, verify if it satisfies the defined conditions
// if yes, return true
type objectFilter interface {
	doesSupportThisOS() (msg string, supported bool)
	doesPass(storedObject storedObject) bool
}

// -------------------------------------- Generic Enumerators -------------------------------------- \\
// the following enumerators must be instantiated with configurations
// they define the work flow in the most generic terms

type syncEnumerator struct {
	// these allow us to go through the source and destination
	// there is flexibility in which side we scan first, it could be either the source or the destination
	primaryTraverser   resourceTraverser
	secondaryTraverser resourceTraverser

	// the results from the primary traverser would be stored here
	objectIndexer *objectIndexer

	// general filters apply to both the primary and secondary traverser
	filters []objectFilter

	// the processor that apply only to the secondary traverser
	// it processes objects as scanning happens
	// based on the data from the primary traverser stored in the objectIndexer
	objectComparator objectProcessor

	// a finalizer that is always called if the enumeration finishes properly
	finalize func() error
}

func newSyncEnumerator(primaryTraverser, secondaryTraverser resourceTraverser, indexer *objectIndexer,
	filters []objectFilter, comparator objectProcessor, finalize func() error) *syncEnumerator {
	return &syncEnumerator{
		primaryTraverser:   primaryTraverser,
		secondaryTraverser: secondaryTraverser,
		objectIndexer:      indexer,
		filters:            filters,
		objectComparator:   comparator,
		finalize:           finalize,
	}
}

func (e *syncEnumerator) enumerate() (err error) {
	// enumerate the primary resource and build lookup map
	err = e.primaryTraverser.traverse(e.objectIndexer.store, e.filters)
	if err != nil {
		return
	}

	// enumerate the secondary resource and as the objects pass the filters
	// they will be passed to the object comparator
	// which can process given objects based on what's already indexed
	// note: transferring can start while scanning is ongoing
	err = e.secondaryTraverser.traverse(e.objectComparator, e.filters)
	if err != nil {
		return
	}

	// execute the finalize func which may perform useful clean up steps
	err = e.finalize()
	if err != nil {
		return
	}

	return
}

type copyEnumerator struct {
	traverser resourceTraverser

	// general filters apply to the objects returned by the traverser
	filters []objectFilter

	// receive objects from the traverser and dispatch them for transferring
	objectDispatcher objectProcessor

	// a finalizer that is always called if the enumeration finishes properly
	finalize func() error
}

func newCopyEnumerator(traverser resourceTraverser, filters []objectFilter, objectDispatcher objectProcessor, finalizer func() error) *copyEnumerator {
	return &copyEnumerator{
		traverser:        traverser,
		filters:          filters,
		objectDispatcher: objectDispatcher,
		finalize:         finalizer,
	}
}

func (e *copyEnumerator) enumerate() (err error) {
	err = e.traverser.traverse(e.objectDispatcher, e.filters)
	if err != nil {
		return
	}

	// execute the finalize func which may perform useful clean up steps
	return e.finalize()
}

// -------------------------------------- Helper Funcs -------------------------------------- \\

func passedFilters(filters []objectFilter, storedObject storedObject) bool {
	if filters != nil && len(filters) > 0 {
		// loop through the filters, if any of them fail, then return false
		for _, filter := range filters {
			msg, supported := filter.doesSupportThisOS()
			if !supported {
				glcm.Info(msg)
			}
			if !filter.doesPass(storedObject) {
				return false
			}
		}
	}

	return true
}

func processIfPassedFilters(filters []objectFilter, storedObject storedObject, processor objectProcessor) (err error) {
	if passedFilters(filters, storedObject) {
		err = processor(storedObject)
	}

	return
}

// storedObject names are useful for filters
func getObjectNameOnly(fullPath string) (nameOnly string) {
	lastPathSeparator := strings.LastIndex(fullPath, common.AZCOPY_PATH_SEPARATOR_STRING)

	// if there is a path separator and it is not the last character
	if lastPathSeparator > 0 && lastPathSeparator != len(fullPath)-1 {
		// then we separate out the name of the storedObject
		nameOnly = fullPath[lastPathSeparator+1:]
	} else {
		nameOnly = fullPath
	}

	return
}
