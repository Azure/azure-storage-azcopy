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
// ** DO NOT instantiate directly, always use newStoredObject ** (to make sure its fully populated and any preprocessor method runs)
type storedObject struct {
	name             string
	entityType       common.EntityType
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
	// Note 1: sometimes the rootDir given by the user turns out to be a single file
	// example: rootDir=/var/a/b/c/d/e/f.pdf fullPath=/var/a/b/c/d/e/f.pdf => relativePath=""
	// in this case, since rootDir already points to the file, relatively speaking the path is nothing.
	// In this case isSingleSourceFile returns true.
	// Note 2: The other unusual case is the storedObject representing the folder properties of the root dir
	// (if the source is folder-aware). In this case relativePath is also empty.
	// In this case isSourceRootFolder returns true.
	relativePath string
	// container source, only included by account traversers.
	containerName string
	// destination container name. Included in the processor after resolving container names.
	dstContainerName string
	// access tier, only included by blob traverser.
	blobAccessTier azblob.AccessTierType
	// metadata, included in S2S transfers
	Metadata      common.Metadata
	blobVersionID string
}

const (
	blobTypeNA = azblob.BlobNone // some things, e.g. local files, aren't blobs so they don't have their own blob type so we use this "not applicable" constant
)

func (s *storedObject) isMoreRecentThan(storedObject2 storedObject) bool {
	return s.lastModifiedTime.After(storedObject2.lastModifiedTime)
}

func (s *storedObject) isSingleSourceFile() bool {
	return s.relativePath == "" && s.entityType == common.EEntityType.File()
}

func (s *storedObject) isSourceRootFolder() bool {
	return s.relativePath == "" && s.entityType == common.EEntityType.Folder()
}

// isCompatibleWithFpo serves as our universal filter for filtering out folders in the cases where we should not
// process them. (If we didn't have a filter like this, we'd have to put the filtering into
// every enumerator, which would complicated them.)
// We can't just implement this filtering in ToNewCopyTransfer, because delete transfers (from sync)
// do not pass through that routine.  So we need to make the filtering available in a separate function
// so that the sync deletion code path(s) can access it.
func (s *storedObject) isCompatibleWithFpo(fpo common.FolderPropertyOption) bool {
	if s.entityType == common.EEntityType.File() {
		return true
	} else if s.entityType == common.EEntityType.Folder() {
		switch fpo {
		case common.EFolderPropertiesOption.NoFolders():
			return false
		case common.EFolderPropertiesOption.AllFoldersExceptRoot():
			return !s.isSourceRootFolder()
		case common.EFolderPropertiesOption.AllFolders():
			return true
		default:
			panic("undefined folder properties option")
		}
	} else {
		panic("undefined entity type")
	}
}

// Returns a func that only calls inner if storedObject isCompatibleWithFpo
// We use this, so that we can easily test for compatibility in the sync deletion code (which expects an objectProcessor)
func newFpoAwareProcessor(fpo common.FolderPropertyOption, inner objectProcessor) objectProcessor {
	return func(s storedObject) error {
		if s.isCompatibleWithFpo(fpo) {
			return inner(s)
		} else {
			return nil // nothing went wrong, because we didn't do anything
		}
	}
}

func (s *storedObject) ToNewCopyTransfer(
	steWillAutoDecompress bool,
	Source string,
	Destination string,
	preserveBlobTier bool,
	folderPropertiesOption common.FolderPropertyOption) (transfer common.CopyTransfer, shouldSendToSte bool) {

	if !s.isCompatibleWithFpo(folderPropertiesOption) {
		return common.CopyTransfer{}, false
	}

	if steWillAutoDecompress {
		Destination = stripCompressionExtension(Destination, s.contentEncoding)
	}

	t := common.CopyTransfer{
		Source:             Source,
		Destination:        Destination,
		EntityType:         s.entityType,
		LastModifiedTime:   s.lastModifiedTime,
		SourceSize:         s.size,
		ContentType:        s.contentType,
		ContentEncoding:    s.contentEncoding,
		ContentDisposition: s.contentDisposition,
		ContentLanguage:    s.contentLanguage,
		CacheControl:       s.cacheControl,
		ContentMD5:         s.md5,
		Metadata:           s.Metadata,
		BlobType:           s.blobType,
		BlobVersionID:      s.blobVersionID,
		// set this below, conditionally: BlobTier
	}

	if preserveBlobTier {
		t.BlobTier = s.blobAccessTier
	}

	return t, true
}

// stripCompressionExtension strips any file extension that corresponds to the
// compression indicated by the encoding type.
// Why remove this extension here, at enumeration time, instead of just doing it
// in the STE when we are about to save the file?
// Because by doing it here we get the accurate name in things that
// directly read the Plan files, like the jobs show command
func stripCompressionExtension(dest string, contentEncoding string) string {
	// Ignore error getting compression type. We can't easily report it now, and we don't need to know about the error
	// cases here when deciding renaming.  STE will log error on the error cases
	ct, _ := common.GetCompressionType(contentEncoding)
	ext := strings.ToLower(filepath.Ext(dest))
	stripGzip := ct == common.ECompressionType.GZip() && (ext == ".gz" || ext == ".gzip")
	stripZlib := ct == common.ECompressionType.ZLib() && ext == ".zz" // "standard" extension for zlib-wrapped files, according to pigz doc and Stack Overflow
	if stripGzip || stripZlib {
		return strings.TrimSuffix(dest, filepath.Ext(dest))
	}
	return dest
}

// interfaces for standard properties of storedObjects
type contentPropsProvider interface {
	CacheControl() string
	ContentDisposition() string
	ContentEncoding() string
	ContentLanguage() string
	ContentType() string
	ContentMD5() []byte
}
type blobPropsProvider interface {
	BlobType() azblob.BlobType
	AccessTier() azblob.AccessTierType
}

// a constructor is used so that in case the storedObject has to change, the callers would get a compilation error
// and it forces all necessary properties to be always supplied and not forgotten
func newStoredObject(morpher objectMorpher, name string, relativePath string, entityType common.EntityType, lmt time.Time, size int64, props contentPropsProvider, blobProps blobPropsProvider, meta common.Metadata, containerName string) storedObject {
	obj := storedObject{
		name:               name,
		relativePath:       relativePath,
		entityType:         entityType,
		lastModifiedTime:   lmt,
		size:               size,
		cacheControl:       props.CacheControl(),
		contentDisposition: props.ContentDisposition(),
		contentEncoding:    props.ContentEncoding(),
		contentLanguage:    props.ContentLanguage(),
		contentType:        props.ContentType(),
		md5:                props.ContentMD5(),
		blobType:           blobProps.BlobType(),
		blobAccessTier:     blobProps.AccessTier(),
		Metadata:           meta,
		containerName:      containerName,
	}

	// Folders don't have size, and root ones shouldn't have names in the storedObject. Ensure those rules are consistently followed
	if entityType == common.EEntityType.Folder() {
		obj.size = 0
		if obj.isSourceRootFolder() {
			obj.name = "" // make these consistent, even from enumerators that pass in an actual name for these (it doesn't really make sense to pass an actual name)
		}
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

type enumerationCounterFunc func(entityType common.EntityType)

// source, location, recursive, and incrementEnumerationCounter are always required.
// ctx, pipeline are only required for remote resources.
// followSymlinks is only required for local resources (defaults to false)
// errorOnDirWOutRecursive is used by copy.
func initResourceTraverser(resource common.ResourceString, location common.Location, ctx *context.Context, credential *common.CredentialInfo,
	followSymlinks *bool, listOfFilesChannel chan string, recursive, getProperties, includeDirectoryStubs bool, incrementEnumerationCounter enumerationCounterFunc, listOfVersionIds chan string) (resourceTraverser, error) {
	var output resourceTraverser
	var p *pipeline.Pipeline

	// Clean up the resource if it's a local path
	if location == common.ELocation.Local() {
		resource = common.ResourceString{Value: cleanLocalPath(resource.ValueLocal())}
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

	// Feed list of files channel into new list traverser
	if listOfFilesChannel != nil {
		if location.IsLocal() {
			// First, ignore all escaped stars. Stars can be valid characters on many platforms (out of the 3 we support though, Windows is the only that cannot support it).
			// In the future, should we end up supporting another OS that does not treat * as a valid character, we should turn these checks into a map-check against runtime.GOOS.
			tmpResource := common.IffString(runtime.GOOS == "windows", resource.ValueLocal(), strings.ReplaceAll(resource.ValueLocal(), `\*`, ``))
			// check for remaining stars. We can't combine list traversers, and wildcarded list traversal occurs below.
			if strings.Contains(tmpResource, "*") {
				return nil, errors.New("cannot combine local wildcards with include-path or list-of-files")
			}
		}

		output = newListTraverser(resource, location, credential, ctx, recursive, toFollow, getProperties, listOfFilesChannel, includeDirectoryStubs, incrementEnumerationCounter)
		return output, nil
	}

	switch location {
	case common.ELocation.Local():
		_, err := common.OSStat(resource.ValueLocal())

		// If wildcard is present and this isn't an existing file/folder, glob and feed the globbed list into a list enum.
		if strings.Index(resource.ValueLocal(), "*") != -1 && err != nil {
			basePath := getPathBeforeFirstWildcard(resource.ValueLocal())
			matches, err := filepath.Glob(resource.ValueLocal())

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

			baseResource := resource.CloneWithValue(cleanLocalPath(basePath))
			output = newListTraverser(baseResource, location, nil, nil, recursive, toFollow, getProperties, globChan, includeDirectoryStubs, incrementEnumerationCounter)
		} else {
			output = newLocalTraverser(resource.ValueLocal(), recursive, toFollow, incrementEnumerationCounter)
		}
	case common.ELocation.Benchmark():
		ben, err := newBenchmarkTraverser(resource.Value, incrementEnumerationCounter)
		if err != nil {
			return nil, err
		}
		output = ben

	case common.ELocation.Blob():
		resourceURL, err := resource.FullURL()
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

			output = newBlobAccountTraverser(resourceURL, *p, *ctx, includeDirectoryStubs, incrementEnumerationCounter)
		} else if listOfVersionIds != nil {
			output = newBlobVersionsTraverser(resourceURL, *p, *ctx, recursive, includeDirectoryStubs, incrementEnumerationCounter, listOfVersionIds)
		} else {
			output = newBlobTraverser(resourceURL, *p, *ctx, recursive, includeDirectoryStubs, incrementEnumerationCounter)
		}
	case common.ELocation.File():
		resourceURL, err := resource.FullURL()
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
		resourceURL, err := resource.FullURL()
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
		resourceURL, err := resource.FullURL()
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
	appliesOnlyToFiles() bool
}

type preFilterProvider interface {
	getEnumerationPreFilter() string
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

func WarnStdoutAndJobLog(toLog string) {
	glcm.Info(toLog)
	if ste.JobsAdmin != nil {
		ste.JobsAdmin.LogToJobLog(toLog, pipeline.LogWarning)
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

			if filter.appliesOnlyToFiles() && storedObject.entityType != common.EEntityType.File() {
				// don't pass folders to filters that only know how to deal with files
				// As at Feb 2020, we have separate logic to weed out folder properties (and not even send them)
				// if any filter applies only to files... but that logic runs after this point, so we need this
				// protection here, just to make sure we don't pass the filter logic an object that it can't handle.
				continue
			}

			if !filter.doesPass(storedObject) {
				return false
			}
		}
	}

	return true
}

// This error should be treated as a flag, that we didn't fail processing, but instead, we just didn't process it.
// Currently, this is only really used for symlink processing, but it _is_ an error, so it must be handled in all new traversers.
// Basically, anywhere processIfPassedFilters is called, additionally call getProcessingError.
var ignoredError = errors.New("FileIgnored")

func getProcessingError(errin error) (ignored bool, err error) {
	if errin == ignoredError {
		return true, nil
	}

	return false, err
}

func processIfPassedFilters(filters []objectFilter, storedObject storedObject, processor objectProcessor) (err error) {
	if passedFilters(filters, storedObject) {
		err = processor(storedObject)
	} else {
		err = ignoredError
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
