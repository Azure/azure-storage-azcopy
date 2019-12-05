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
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
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

	// all of these will be empty when unknown or not applicable.
	contentDisposition string
	cacheControl       string
	contentLanguage    string
	contentEncoding    string
	contentType        string

	// partial path relative to its root directory
	// example: rootDir=/var/a/b/c fullPath=/var/a/b/c/d/e/f.pdf => relativePath=d/e/f.pdf name=f.pdf
	// note that sometimes the rootDir given by the user turns out to be a single file
	// example: rootDir=/var/a/b/c/d/e/f.pdf fullPath=/var/a/b/c/d/e/f.pdf => relativePath=""
	// in this case, since rootDir already points to the file, relatively speaking the path is nothing.
	relativePath string
	// container source, only included by account traversers.
	containerName string
	// destination container name. Included in the processor after resolving container names.
	dstContainerName string
	// access tier, only included by blob traverser.
	blobAccessTier azblob.AccessTierType
	// metadata, included in S2S transfers
	Metadata common.Metadata
}

const (
	blobTypeNA = azblob.BlobNone // some things, e.g. local files, aren't blobs so they don't have their own blob type so we use this "not applicable" constant
)

func (storedObject *storedObject) isMoreRecentThan(storedObject2 storedObject) bool {
	return storedObject.lastModifiedTime.After(storedObject2.lastModifiedTime)
}

// a constructor is used so that in case the storedObject has to change, the callers would get a compilation error
func newStoredObject(morpher objectMorpher, name string, relativePath string, lmt time.Time, size int64, md5 []byte, blobType azblob.BlobType, containerName string) storedObject {
	obj := storedObject{
		name:             name,
		relativePath:     relativePath,
		lastModifiedTime: lmt,
		size:             size,
		md5:              md5,
		blobType:         blobType,
		containerName:    containerName,
	}

	// in some cases we may be supplied with a func that will perform some modification on the basic object
	if morpher != nil {
		morpher(&obj)
	}

	return obj
}

// capable of traversing a structured resource like container or local directory
// pass each storedObject to the given objectProcessor if it passes all the filters
type resourceTraverser interface {
	traverse(preprocessor objectMorpher, processor objectProcessor, filters []objectFilter) error
	isDirectory(isSource bool) bool
	// isDirectory has an isSource flag for a single exception to blob.
	// Blob should ONLY check remote if it's a source.
	// On destinations, because blobs and virtual directories can share names, we should support placing in both ways.
	// Thus, we only check the directory syntax on blob destinations. On sources, we check both syntax and remote, if syntax isn't a directory.
}

type accountTraverser interface {
	resourceTraverser
	listContainers() ([]string, error)
}

// basically rename a function and change the order of inputs just to make what's happening clearer
func containerNameMatchesPattern(containerName, pattern string) (bool, error) {
	return filepath.Match(pattern, containerName)
}

// newContainerDecorator constructs an objectMorpher that adds the given container name to storedObjects
func newContainerDecorator(containerName string) objectMorpher {
	return func(object *storedObject) {
		object.containerName = containerName
	}
}

const accountTraversalInherentlyRecursiveError = "account copies are an inherently recursive operation, and thus --recursive is required"
const httpsRecommendedNotice = "NOTE: HTTP is in use for one or more location(s). The use of HTTP is not recommended due to security concerns."

var httpsRecommendationOnce sync.Once

func recommendHttpsIfNecessary(url url.URL) {
	if strings.EqualFold(url.Scheme, "http") {
		httpsRecommendationOnce.Do(func() {
			glcm.Info(httpsRecommendedNotice)
		})
	}
}

// source, location, recursive, and incrementEnumerationCounter are always required.
// ctx, pipeline are only required for remote resources.
// followSymlinks is only required for local resources (defaults to false)
// errorOnDirWOutRecursive is used by copy.
func initResourceTraverser(resource string, location common.Location, ctx *context.Context, credential *common.CredentialInfo, followSymlinks *bool, listofFilesChannel chan string, recursive, getProperties bool, incrementEnumerationCounter func()) (resourceTraverser, error) {
	var output resourceTraverser
	var p *pipeline.Pipeline

	// Clean up the resource if it's a local path
	if location == common.ELocation.Local() {
		resource = cleanLocalPath(resource)
	}

	// Initialize the pipeline if creds and ctx is provided
	if ctx != nil && credential != nil {
		tmppipe, err := initPipeline(*ctx, location, *credential)

		if err != nil {
			return nil, err
		}

		p = &tmppipe
	}

	toFollow := false
	if followSymlinks != nil {
		toFollow = *followSymlinks
	}

	// Feed list of files channel into new list traverser, separate SAS.
	if listofFilesChannel != nil {
		sas := ""
		if location.IsRemote() {
			var err error
			resource, sas, err = SplitAuthTokenFromResource(resource, location)

			if err != nil {
				return nil, err
			}
		} else {
			// First, ignore all escaped stars. Stars can be valid characters on many platforms (out of the 3 we support though, Windows is the only that cannot support it).
			// In the future, should we end up supporting another OS that does not treat * as a valid character, we should turn these checks into a map-check against runtime.GOOS.
			tmpResource := common.IffString(runtime.GOOS == "windows", resource, strings.ReplaceAll(resource, `\*`, ``))
			// check for remaining stars. We can't combine list traversers, and wildcarded list traversal occurs below.
			if strings.Contains(tmpResource, "*") {
				return nil, errors.New("cannot combine local wildcards with include-path or list-of-files")
			}
		}

		output = newListTraverser(resource, sas, location, credential, ctx, recursive, toFollow, getProperties, listofFilesChannel, incrementEnumerationCounter)
		return output, nil
	}

	switch location {
	case common.ELocation.Local():
		_, err := os.Stat(resource)

		// If wildcard is present and this isn't an existing file/folder, glob and feed the globbed list into a list enum.
		if strings.Index(resource, "*") != -1 && err != nil {
			basePath := getPathBeforeFirstWildcard(resource)
			matches, err := filepath.Glob(resource)

			if err != nil {
				return nil, fmt.Errorf("failed to glob: %s", err)
			}

			globChan := make(chan string)

			go func() {
				defer close(globChan)
				for _, v := range matches {
					globChan <- strings.TrimPrefix(v, basePath)
				}
			}()

			output = newListTraverser(cleanLocalPath(basePath), "", location, nil, nil, recursive, toFollow, getProperties, globChan, incrementEnumerationCounter)
		} else {
			output = newLocalTraverser(resource, recursive, toFollow, incrementEnumerationCounter)
		}
	case common.ELocation.Benchmark():
		ben, err := newBenchmarkTraverser(resource, incrementEnumerationCounter)
		if err != nil {
			return nil, err
		}
		output = ben

	case common.ELocation.Blob():
		resourceURL, err := url.Parse(resource)
		if err != nil {
			return nil, err
		}

		recommendHttpsIfNecessary(*resourceURL)

		if ctx == nil || p == nil {
			return nil, errors.New("a valid credential and context must be supplied to create a blob traverser")
		}

		burl := azblob.NewBlobURLParts(*resourceURL)

		if burl.ContainerName == "" || strings.Contains(burl.ContainerName, "*") {

			if !recursive {
				return nil, errors.New(accountTraversalInherentlyRecursiveError)
			}

			output = newBlobAccountTraverser(resourceURL, *p, *ctx, incrementEnumerationCounter)
		} else {
			output = newBlobTraverser(resourceURL, *p, *ctx, recursive, incrementEnumerationCounter)
		}
	case common.ELocation.File():
		resourceURL, err := url.Parse(resource)
		if err != nil {
			return nil, err
		}

		recommendHttpsIfNecessary(*resourceURL)

		if ctx == nil || p == nil {
			return nil, errors.New("a valid credential and context must be supplied to create a file traverser")
		}

		furl := azfile.NewFileURLParts(*resourceURL)

		if furl.ShareName == "" || strings.Contains(furl.ShareName, "*") {
			if !recursive {
				return nil, errors.New(accountTraversalInherentlyRecursiveError)
			}

			output = newFileAccountTraverser(resourceURL, *p, *ctx, getProperties, incrementEnumerationCounter)
		} else {
			output = newFileTraverser(resourceURL, *p, *ctx, recursive, getProperties, incrementEnumerationCounter)
		}
	case common.ELocation.BlobFS():
		resourceURL, err := url.Parse(resource)
		if err != nil {
			return nil, err
		}

		// check if credential is also nil here (would never trigger) to tame syntax highlighting.
		// As a precondition to pipeline p, credential must not be nil anyway.
		if ctx == nil || p == nil || credential == nil {
			return nil, errors.New("a valid credential and context must be supplied to create a blobFS traverser")
		}

		recommendHttpsIfNecessary(*resourceURL)

		bfsURL := azbfs.NewBfsURLParts(*resourceURL)

		if bfsURL.FileSystemName == "" || strings.Contains(bfsURL.FileSystemName, "*") {
			// TODO service traverser

			if !recursive {
				return nil, errors.New(accountTraversalInherentlyRecursiveError)
			}

			output = newBlobFSAccountTraverser(resourceURL, *p, *ctx, incrementEnumerationCounter)
		} else {
			output = newBlobFSTraverser(resourceURL, *p, *ctx, recursive, incrementEnumerationCounter)
		}
	case common.ELocation.S3():
		resourceURL, err := url.Parse(resource)
		if err != nil {
			return nil, err
		}

		recommendHttpsIfNecessary(*resourceURL)

		s3URLParts, err := common.NewS3URLParts(*resourceURL)
		if err != nil {
			return nil, err
		}

		if ctx == nil {
			return nil, errors.New("a valid context must be supplied to create a S3 traverser")
		}

		if s3URLParts.BucketName == "" || strings.Contains(s3URLParts.BucketName, "*") {
			// TODO convert to path style URL

			if !recursive {
				return nil, errors.New(accountTraversalInherentlyRecursiveError)
			}

			output, err = newS3ServiceTraverser(resourceURL, *ctx, getProperties, incrementEnumerationCounter)

			if err != nil {
				return nil, err
			}
		} else {
			output, err = newS3Traverser(resourceURL, *ctx, recursive, getProperties, incrementEnumerationCounter)

			if err != nil {
				return nil, err
			}
		}
	default:
		return nil, errors.New("could not choose a traverser from currently available traversers")
	}

	if output == nil {
		panic("sanity check: somehow didn't spawn a traverser")
	}

	return output, nil
}

func appendSASIfNecessary(rawURL string, sasToken string) (string, error) {
	if sasToken != "" {
		parsedURL, err := url.Parse(rawURL)

		if err != nil {
			return rawURL, err
		}

		parsedURL = copyHandlerUtil{}.appendQueryParamToUrl(parsedURL, sasToken)
		return parsedURL.String(), nil
	}

	return rawURL, nil
}

// given a storedObject, process it accordingly. Used for the "real work" of, say, creating a copyTransfer from the object
type objectProcessor func(storedObject storedObject) error

// TODO: consider making objectMorpher an interface, not a func, and having newStoredObject take an array of them, instead of just one
//   Might be easier to debug
// modifies a storedObject, but does NOT process it.  Used for modifications, such as pre-pending a parent path
type objectMorpher func(storedObject *storedObject)

// FollowedBy returns a new objectMorpher, which performs the action of existing followed by the action of additional.
// Use this so that we always chain pre-processors, never replace them (this is so we avoid making any assumptions about
// whether an old processor actually does anything)
func (existing objectMorpher) FollowedBy(additional objectMorpher) objectMorpher {
	switch {
	case existing == nil:
		return additional
	case additional == nil:
		return existing
	default:
		return func(obj *storedObject) {
			existing(obj)
			additional(obj)
		}
	}
}

// noPreProcessor is used at the top level, when we start traversal because at that point we have no morphing to apply
// Morphing only becomes necessary as we drill down through a tree of nested traversers, at which point the children
// add their morphers with FollowedBy()
var noPreProccessor objectMorpher = nil

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
	err = e.primaryTraverser.traverse(noPreProccessor, e.objectIndexer.store, e.filters)
	if err != nil {
		return
	}

	// enumerate the secondary resource and as the objects pass the filters
	// they will be passed to the object comparator
	// which can process given objects based on what's already indexed
	// note: transferring can start while scanning is ongoing
	err = e.secondaryTraverser.traverse(noPreProccessor, e.objectComparator, e.filters)
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

func LogStdoutAndJobLog(toLog string) {
	glcm.Info(toLog)
	if ste.JobsAdmin != nil {
		ste.JobsAdmin.LogToJobLog(toLog)
	}
}

func (e *copyEnumerator) enumerate() (err error) {
	err = e.traverser.traverse(noPreProccessor, e.objectDispatcher, e.filters)
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
				glcm.Error(msg)
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
