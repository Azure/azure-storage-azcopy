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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var EnumerationParallelism = 1
var EnumerationParallelStatFiles = false

// -------------------------------------- Component Definitions -------------------------------------- \\
// the following interfaces and structs allow the sync enumerator
// to be generic and has as little duplicated code as possible

// represent a local or remote resource object (ex: local file, blob, etc.)
// we can add more properties if needed, as this is easily extensible
// ** DO NOT instantiate directly, always use newStoredObject ** (to make sure its fully populated and any preprocessor method runs)
type StoredObject struct {
	Name                string
	EntityType          common.EntityType
	LastModifiedTime    time.Time
	smbLastModifiedTime time.Time
	Size                int64
	Md5                 []byte
	BlobType            blob.BlobType // will be "None" when unknown or not applicable

	// all of these will be empty when unknown or not applicable.
	ContentDisposition string
	CacheControl       string
	ContentLanguage    string
	ContentEncoding    string
	ContentType        string

	// partial path relative to its root directory
	// example: rootDir=/var/a/b/c fullPath=/var/a/b/c/d/e/f.pdf => RelativePath=d/e/f.pdf Name=f.pdf
	// Note 1: sometimes the rootDir given by the user turns out to be a single file
	// example: rootDir=/var/a/b/c/d/e/f.pdf fullPath=/var/a/b/c/d/e/f.pdf => RelativePath=""
	// in this case, since rootDir already points to the file, relatively speaking the path is nothing.
	// In this case isSingleSourceFile returns true.
	// Note 2: The other unusual case is the StoredObject representing the folder properties of the root dir
	// (if the source is folder-aware). In this case RelativePath is also empty.
	// In this case isSourceRootFolder returns true.
	RelativePath string
	// container source, only included by account traversers.
	ContainerName string
	// destination container name. Included in the processor after resolving container names.
	DstContainerName string
	// access tier, only included by blob traverser.
	BlobAccessTier blob.AccessTier
	ArchiveStatus  blob.ArchiveStatus
	// metadata, included in S2S transfers
	Metadata       common.Metadata
	BlobVersionID  string
	BlobTags       common.BlobTags
	BlobSnapshotID string
	BlobDeleted    bool

	// Lease information
	LeaseState    lease.StateType
	LeaseStatus   lease.StatusType
	LeaseDuration lease.DurationType
}

func (s *StoredObject) IsMoreRecentThan(storedObject2 StoredObject, preferSMBTime bool) bool {
	lmtA := s.LastModifiedTime
	if preferSMBTime && !s.smbLastModifiedTime.IsZero() {
		lmtA = s.smbLastModifiedTime
	}
	lmtB := storedObject2.LastModifiedTime
	if preferSMBTime && !storedObject2.smbLastModifiedTime.IsZero() {
		lmtB = storedObject2.smbLastModifiedTime
	}

	return lmtA.After(lmtB)
}

func (s *StoredObject) IsSingleSourceFile() bool {
	return s.RelativePath == "" && (s.EntityType == common.EEntityType.File() || s.EntityType == common.EEntityType.Hardlink())
}

func (s *StoredObject) IsSourceRootFolder() bool {
	return s.RelativePath == "" && s.EntityType == common.EEntityType.Folder()
}

// TODO : (gapra) Investigate if this can just be a filter?
// IsCompatibleWithEntitySettings serves as our universal filter for filtering out folders in the cases where we should not
// process them. (If we didn't have a filter like this, we'd have to put the filtering into
// every enumerator, which would complicated them.)
// We can't just implement this filtering in ToNewCopyTransfer, because delete transfers (from sync)
// do not pass through that routine.  So we need to make the filtering available in a separate function
// so that the sync deletion code path(s) can access it.
func (s *StoredObject) IsCompatibleWithEntitySettings(fpo common.FolderPropertyOption, sht common.SymlinkHandlingType, pho common.HardlinkHandlingType) bool {
	if s.EntityType == common.EEntityType.File() {
		return true
	} else if s.EntityType == common.EEntityType.Folder() {
		switch fpo {
		case common.EFolderPropertiesOption.NoFolders():
			return false
		case common.EFolderPropertiesOption.AllFoldersExceptRoot():
			return !s.IsSourceRootFolder()
		case common.EFolderPropertiesOption.AllFolders():
			return true
		default:
			panic("undefined folder properties option")
		}
	} else if s.EntityType == common.EEntityType.Symlink() {
		return sht == common.ESymlinkHandlingType.Preserve()
	} else if s.EntityType == common.EEntityType.Hardlink() {
		return pho == common.EHardlinkHandlingType.Follow()
	} else if s.EntityType == common.EEntityType.Other() {
		return false
	} else {
		panic("undefined entity type")
	}
}

// ErrorNoHashPresent , ErrorHashNoLongerValid, and ErrorHashNotCompatible indicate a hash is not present, not obtainable, and/or not usable.
// For the sake of best-effort, when these errors are emitted, depending on the sync hash policy
var ErrorNoHashPresent = errors.New("no hash present on file")
var ErrorHashNoLongerValid = errors.New("attached hash no longer valid")
var ErrorHashNotCompatible = errors.New("hash types do not match")

// ErrorHashAsyncCalculation is not a strict "the hash is unobtainable", but a "the hash is not currently present".
// In effect, when it is returned, it indicates we have placed the target onto a queue to be handled later.
// It can be treated like a promise, and the item can cease processing in the immediate term.
// This option is only used locally on sync-downloads when the user has specified that azcopy should create a new hash.
var ErrorHashAsyncCalculation = errors.New("hash is calculating asynchronously")

// Returns a func that only calls inner if StoredObject isCompatibleWithFpo
// We use this, so that we can easily test for compatibility in the sync deletion code (which expects an ObjectProcessor)
func NewFpoAwareProcessor(fpo common.FolderPropertyOption, inner ObjectProcessor) ObjectProcessor {
	return func(s StoredObject) error {
		if s.IsCompatibleWithEntitySettings(fpo, common.ESymlinkHandlingType.Skip(), common.EHardlinkHandlingType.Follow()) {
			return inner(s)
		} else {
			return nil // nothing went wrong, because we didn't do anything
		}
	}
}

func (s *StoredObject) ToNewCopyTransfer(steWillAutoDecompress bool,
	Source string,
	Destination string,
	preserveBlobTier bool,
	folderPropertiesOption common.FolderPropertyOption,
	symlinkHandlingType common.SymlinkHandlingType,
	hardlinkHandlingType common.HardlinkHandlingType) (transfer common.CopyTransfer, shouldSendToSte bool) {

	if !s.IsCompatibleWithEntitySettings(folderPropertiesOption, symlinkHandlingType, hardlinkHandlingType) {
		return common.CopyTransfer{}, false
	}

	if steWillAutoDecompress {
		Destination = stripCompressionExtension(Destination, s.ContentEncoding)
	}

	t := common.CopyTransfer{
		Source:             Source,
		Destination:        Destination,
		EntityType:         s.EntityType,
		LastModifiedTime:   s.LastModifiedTime,
		SourceSize:         s.Size,
		ContentType:        s.ContentType,
		ContentEncoding:    s.ContentEncoding,
		ContentDisposition: s.ContentDisposition,
		ContentLanguage:    s.ContentLanguage,
		CacheControl:       s.CacheControl,
		ContentMD5:         s.Md5,
		Metadata:           s.Metadata,
		BlobType:           s.BlobType,
		BlobVersionID:      s.BlobVersionID,
		BlobTags:           s.BlobTags,
		BlobSnapshotID:     s.BlobSnapshotID,
	}

	if preserveBlobTier {
		t.BlobTier = s.BlobAccessTier
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

// interfaces for standard properties of StoredObjects
type contentPropsProvider interface {
	CacheControl() string
	ContentDisposition() string
	ContentEncoding() string
	ContentLanguage() string
	ContentType() string
	ContentMD5() []byte
}
type blobPropsProvider interface {
	BlobType() blob.BlobType
	AccessTier() blob.AccessTier
	LeaseStatus() lease.StatusType
	LeaseDuration() lease.DurationType
	LeaseState() lease.StateType
	ArchiveStatus() blob.ArchiveStatus
	LastModified() time.Time
	ContentLength() int64
}
type filePropsProvider interface {
	contentPropsProvider
	Metadata() common.Metadata
	LastModified() time.Time
	FileLastWriteTime() time.Time
	ContentLength() int64
	NFSFileType() string
	LinkCount() int64
	FileID() string
}

// a constructor is used so that in case the StoredObject has to change, the callers would get a compilation error
// and it forces all necessary properties to be always supplied and not forgotten
func NewStoredObject(morpher objectMorpher, name string, relativePath string, entityType common.EntityType, lmt time.Time, size int64, props contentPropsProvider, blobProps blobPropsProvider, meta common.Metadata, containerName string) StoredObject {
	obj := StoredObject{
		Name:               name,
		RelativePath:       relativePath,
		EntityType:         entityType,
		LastModifiedTime:   lmt,
		Size:               size,
		CacheControl:       props.CacheControl(),
		ContentDisposition: props.ContentDisposition(),
		ContentEncoding:    props.ContentEncoding(),
		ContentLanguage:    props.ContentLanguage(),
		ContentType:        props.ContentType(),
		Md5:                props.ContentMD5(),
		BlobType:           blobProps.BlobType(),
		BlobAccessTier:     blobProps.AccessTier(),
		ArchiveStatus:      blobProps.ArchiveStatus(),
		Metadata:           meta,
		ContainerName:      containerName,
		// Additional lease properties. To be used in listing
		LeaseStatus:   blobProps.LeaseStatus(),
		LeaseState:    blobProps.LeaseState(),
		LeaseDuration: blobProps.LeaseDuration(),
	}

	// Folders don't have size, and root ones shouldn't have names in the StoredObject. Ensure those rules are consistently followed
	if entityType == common.EEntityType.Folder() {
		obj.Size = 0
		if obj.IsSourceRootFolder() {
			obj.Name = "" // make these consistent, even from enumerators that pass in an actual name for these (it doesn't really make sense to pass an actual name)
		}
	}

	// in some cases we may be supplied with a func that will perform some modification on the basic object
	if morpher != nil {
		morpher(&obj)
	}

	return obj
}

// capable of traversing a structured resource like container or local directory
// pass each StoredObject to the given objectProcessor if it passes all the filters
type ResourceTraverser interface {
	Traverse(preprocessor objectMorpher, processor ObjectProcessor, filters []ObjectFilter) error
	IsDirectory(isSource bool) (isDirectory bool, err error)
	// isDirectory has an isSource flag for a single exception to blob.
	// Blob should ONLY check remote if it's a source.
	// On destinations, because blobs and virtual directories can share names, we should support placing in both ways.
	// Thus, we only check the directory syntax on blob destinations. On sources, we check both syntax and remote, if syntax isn't a directory.
}

type AccountTraverser interface {
	ResourceTraverser
	ListContainers() ([]string, error)
}

// basically rename a function and change the order of inputs just to make what's happening clearer
func containerNameMatchesPattern(containerName, pattern string) (bool, error) {
	return filepath.Match(pattern, containerName)
}

// newContainerDecorator constructs an objectMorpher that adds the given container name to StoredObjects
func newContainerDecorator(containerName string) objectMorpher {
	return func(object *StoredObject) {
		object.ContainerName = containerName
	}
}

const accountTraversalInherentlyRecursiveError = "account copies are an inherently recursive operation, and thus --recursive is required"
const httpsRecommendedNotice = "NOTE: HTTP is in use for one or more location(s). The use of HTTP is not recommended due to security concerns."

var httpsRecommendationOnce sync.Once

func recommendHttpsIfNecessary(url url.URL) {
	if strings.EqualFold(url.Scheme, "http") {
		httpsRecommendationOnce.Do(func() {
			common.GetLifecycleMgr().Info(httpsRecommendedNotice)
		})
	}
}

type enumerationCounterFunc func(entityType common.EntityType)

var enumerationCounterFuncNoop enumerationCounterFunc = func(entityType common.EntityType) {}

type InitResourceTraverserOptions struct {
	DestResourceType *common.Location // Used by Azure Files

	//Credential           *common.CredentialInfo // Required for most remote traversers
	Client         *common.ServiceClient // For Azure storage traversers
	CredentialType common.CredentialType // For S3 traversers. TODO : (gapra) One day we should roll s3/gcp clients into common.ServiceClient

	IncrementEnumeration enumerationCounterFunc

	ListOfFiles      <-chan string        // Creates a list of files traverser
	ListOfVersionIDs <-chan string        // Used by Blob/DFS
	ErrorChannel     chan<- ErrorFileInfo // Used by local traverser

	CpkOptions common.CpkOptions // Used by Blob

	PreservePermissions common.PreservePermissionsOption // Blob, BlobFS
	SymlinkHandling     common.SymlinkHandlingType       // Local
	PermanentDelete     common.PermanentDeleteOption     // Blob, BlobFS
	SyncHashType        common.SyncHashType              // Local
	TrailingDotOption   common.TrailingDotOption         // Files

	Recursive               bool // All resources
	GetPropertiesInFrontend bool // Files, GCP, S3
	IncludeDirectoryStubs   bool // Blob, BlobFS
	PreserveBlobTags        bool // Blob, BlobFS
	StripTopDir             bool // Local

	ExcludeContainers []string // Blob account
	ListVersions      bool     // Blob
	HardlinkHandling  common.HardlinkHandlingType
}

func (o *InitResourceTraverserOptions) PerformChecks() error {
	if o.IncrementEnumeration == nil {
		o.IncrementEnumeration = enumerationCounterFuncNoop
	}

	return nil
}

func InitResourceTraverser(resource common.ResourceString, resourceLocation common.Location, ctx context.Context, opts InitResourceTraverserOptions) (ResourceTraverser, error) {
	if ctx == nil {
		return nil, errors.New("a valid context must be supplied to create a traverser")
	}

	err := opts.PerformChecks()
	if err != nil {
		return nil, err
	}

	var (
		output ResourceTraverser
	)

	// Clean up the resource if it's a local path
	if resourceLocation == common.ELocation.Local() {
		resource = common.ResourceString{Value: common.CleanLocalPath(resource.ValueLocal())}
	}

	// Feed list of files channel into new list traverser
	if opts.ListOfFiles != nil {
		if resourceLocation.IsLocal() {
			// First, ignore all escaped stars. Stars can be valid characters on many platforms (out of the 3 we support though, Windows is the only that cannot support it).
			// In the future, should we end up supporting another OS that does not treat * as a valid character, we should turn these checks into a map-check against runtime.GOOS.
			tmpResource := common.Iff(runtime.GOOS == "windows", resource.ValueLocal(), strings.ReplaceAll(resource.ValueLocal(), `\*`, ``))
			// check for remaining stars. We can't combine list traversers, and wildcarded list traversal occurs below.
			if strings.Contains(tmpResource, "*") {
				return nil, errors.New("cannot combine local wildcards with include-path or list-of-files")
			}
		}

		output = newListTraverser(resource, resourceLocation, ctx, opts)
		return output, nil
	}

	switch resourceLocation {
	case common.ELocation.Local():
		_, err := common.OSStat(resource.ValueLocal())

		// If wildcard is present and this isn't an existing file/folder, glob and feed the globbed list into a list enum.
		if strings.Contains(resource.ValueLocal(), "*") && (opts.StripTopDir || err != nil) {
			basePath := GetPathBeforeFirstWildcard(resource.ValueLocal())
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

			opts.ListOfFiles = globChan

			baseResource := resource.CloneWithValue(common.CleanLocalPath(basePath))
			output = newListTraverser(baseResource, resourceLocation, ctx, opts)
		} else {
			output, _ = NewLocalTraverser(resource.ValueLocal(), ctx, opts)
		}
	case common.ELocation.Benchmark():
		ben, err := newBenchmarkTraverser(resource.Value, opts.IncrementEnumeration)
		if err != nil {
			return nil, err
		}
		output = ben

	case common.ELocation.Blob():
		// TODO (last service migration) : Remove dependency on URLs.
		resourceURL, err := resource.FullURL()
		if err != nil {
			return nil, err
		}

		recommendHttpsIfNecessary(*resourceURL)

		r := resourceURL.String()

		blobURLParts, err := blob.ParseURL(r)
		if err != nil {
			return nil, err
		}
		containerName := blobURLParts.ContainerName
		bsc, err := opts.Client.BlobServiceClient()
		if err != nil {
			return nil, err
		}

		if containerName == "" || strings.Contains(containerName, "*") {
			if !opts.Recursive {
				return nil, errors.New(accountTraversalInherentlyRecursiveError)
			}
			output = NewBlobAccountTraverser(bsc, containerName, ctx, opts)
		} else if opts.ListOfVersionIDs != nil {
			output = newBlobVersionsTraverser(r, bsc, ctx, opts)
		} else {
			output = NewBlobTraverser(r, bsc, ctx, opts)
		}
	case common.ELocation.File(), common.ELocation.FileNFS():
		// TODO (last service migration) : Remove dependency on URLs.
		resourceURL, err := resource.FullURL()
		if err != nil {
			return nil, err
		}

		recommendHttpsIfNecessary(*resourceURL)

		r := resourceURL.String()

		fileURLParts, err := file.ParseURL(r)
		if err != nil {
			return nil, err
		}
		shareName := fileURLParts.ShareName
		fsc, err := opts.Client.FileServiceClient()
		if err != nil {
			return nil, err
		}

		if shareName == "" || strings.Contains(shareName, "*") {
			if !opts.Recursive {
				return nil, errors.New(accountTraversalInherentlyRecursiveError)
			}
			output = NewFileAccountTraverser(fsc, shareName, ctx, opts)
		} else {
			output = NewFileTraverser(r, fsc, ctx, opts)
		}
	case common.ELocation.BlobFS():
		resourceURL, err := resource.FullURL()
		if err != nil {
			return nil, err
		}

		recommendHttpsIfNecessary(*resourceURL)

		r := resourceURL.String()
		r = strings.Replace(r, ".dfs", ".blob", 1)
		blobURLParts, err := blob.ParseURL(r)
		if err != nil {
			return nil, err
		}
		containerName := blobURLParts.ContainerName
		bsc, err := opts.Client.BlobServiceClient()
		if err != nil {
			return nil, err
		}

		opts.IncludeDirectoryStubs = true // DFS is supposed to feed folders in
		if containerName == "" || strings.Contains(containerName, "*") {
			if !opts.Recursive {
				return nil, errors.New(accountTraversalInherentlyRecursiveError)
			}
			output = NewBlobAccountTraverser(bsc, containerName, ctx, opts, BlobTraverserOptions{IsDFS: to.Ptr(true)})
		} else if opts.ListOfVersionIDs != nil {
			output = newBlobVersionsTraverser(r, bsc, ctx, opts)
		} else {
			output = NewBlobTraverser(r, bsc, ctx, opts, BlobTraverserOptions{IsDFS: to.Ptr(true)})
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

		if s3URLParts.BucketName == "" || strings.Contains(s3URLParts.BucketName, "*") {
			// TODO convert to path style URL

			if !opts.Recursive {
				return nil, errors.New(accountTraversalInherentlyRecursiveError)
			}

			output, err = NewS3ServiceTraverser(resourceURL, ctx, opts)

			if err != nil {
				return nil, err
			}
		} else {
			output, err = NewS3Traverser(resourceURL, ctx, opts)

			if err != nil {
				return nil, err
			}
		}
	case common.ELocation.GCP():
		resourceURL, err := resource.FullURL()
		if err != nil {
			return nil, err
		}

		recommendHttpsIfNecessary(*resourceURL)

		gcpURLParts, err := common.NewGCPURLParts(*resourceURL)
		if err != nil {
			return nil, err
		}

		if gcpURLParts.BucketName == "" || strings.Contains(gcpURLParts.BucketName, "*") {
			if !opts.Recursive {
				return nil, errors.New(accountTraversalInherentlyRecursiveError)
			}

			output, err = NewGCPServiceTraverser(resourceURL, ctx, opts)
			if err != nil {
				return nil, err
			}
		} else {
			output, err = NewGCPTraverser(resourceURL, ctx, opts)
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

// given a StoredObject, process it accordingly. Used for the "real work" of, say, creating a copyTransfer from the object
type ObjectProcessor func(storedObject StoredObject) error

// TODO: consider making objectMorpher an interface, not a func, and having NewStoredObject take an array of them, instead of just one
//
//	Might be easier to debug
//
// modifies a StoredObject, but does NOT process it.  Used for modifications, such as prepending a parent path
type objectMorpher func(storedObject *StoredObject)

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
		return func(obj *StoredObject) {
			existing(obj)
			additional(obj)
		}
	}
}

// noPreProcessor is used at the top level, when we start traversal because at that point we have no morphing to apply
// Morphing only becomes necessary as we drill down through a tree of nested traversers, at which point the children
// add their morphers with FollowedBy()
var NoPreProccessor objectMorpher = nil

// given a StoredObject, verify if it satisfies the defined conditions
// if yes, return true
type ObjectFilter interface {
	DoesSupportThisOS() (msg string, supported bool)
	DoesPass(storedObject StoredObject) bool
	AppliesOnlyToFiles() bool
}

type PreFilterProvider interface {
	GetEnumerationPreFilter() string
}

// -------------------------------------- Generic Enumerators -------------------------------------- \\
// the following enumerators must be instantiated with configurations
// they define the work flow in the most generic terms

type SyncEnumerator struct {
	// these allow us to go through the source and destination
	// there is flexibility in which side we scan first, it could be either the source or the destination
	primaryTraverser   ResourceTraverser
	secondaryTraverser ResourceTraverser

	// the results from the primary traverser would be stored here
	objectIndexer *ObjectIndexer

	// general filters apply to both the primary and secondary traverser
	filters []ObjectFilter

	// the processor that apply only to the secondary traverser
	// it processes objects as scanning happens
	// based on the data from the primary traverser stored in the objectIndexer
	objectComparator ObjectProcessor

	// a finalizer that is always called if the enumeration finishes properly
	finalize func() error
}

func NewSyncEnumerator(primaryTraverser, secondaryTraverser ResourceTraverser, indexer *ObjectIndexer,
	filters []ObjectFilter, comparator ObjectProcessor, finalize func() error) *SyncEnumerator {
	return &SyncEnumerator{
		primaryTraverser:   primaryTraverser,
		secondaryTraverser: secondaryTraverser,
		objectIndexer:      indexer,
		filters:            filters,
		objectComparator:   comparator,
		finalize:           finalize,
	}
}

func (e *SyncEnumerator) Enumerate() (err error) {
	handleAcceptableErrors := func() {
		switch {
		case err == nil: // don't do any error checking
		case fileerror.HasCode(err, fileerror.ResourceNotFound),
			datalakeerror.HasCode(err, datalakeerror.ResourceNotFound),
			bloberror.HasCode(err, bloberror.BlobNotFound),
			strings.Contains(err.Error(), "The system cannot find the"),
			errors.Is(err, os.ErrNotExist):
			err = nil // Oh no! Oh well. We'll create it later.
		}
	}

	// Enumerate the primary resource and build lookup map
	err = e.primaryTraverser.Traverse(NoPreProccessor, e.objectIndexer.Store, e.filters)
	handleAcceptableErrors()
	if err != nil {
		return err
	}

	// Enumerate the secondary resource and as the objects pass the filters
	// they will be passed to the object comparator
	// which can process given objects based on what's already indexed
	// note: transferring can start while scanning is ongoing
	err = e.secondaryTraverser.Traverse(NoPreProccessor, e.objectComparator, e.filters)
	handleAcceptableErrors()
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

type CopyEnumerator struct {
	Traverser ResourceTraverser

	// general filters apply to the objects returned by the traverser
	Filters []ObjectFilter

	// receive objects from the traverser and dispatch them for transferring
	ObjectDispatcher ObjectProcessor

	// a finalizer that is always called if the enumeration finishes properly
	Finalize func() error
}

func NewCopyEnumerator(traverser ResourceTraverser, filters []ObjectFilter, objectDispatcher ObjectProcessor, finalizer func() error) *CopyEnumerator {
	return &CopyEnumerator{
		Traverser:        traverser,
		Filters:          filters,
		ObjectDispatcher: objectDispatcher,
		Finalize:         finalizer,
	}
}

func WarnStdoutAndScanningLog(toLog string) {
	common.GetLifecycleMgr().Info(toLog)
	if common.AzcopyScanningLogger != nil {
		// ste.JobsAdmin.LogToJobLog(toLog, pipeline.LogWarning)
		common.AzcopyScanningLogger.Log(common.LogWarning, toLog)
	}
}

func (e *CopyEnumerator) Enumerate() (err error) {
	err = e.Traverser.Traverse(NoPreProccessor, e.ObjectDispatcher, e.Filters)
	if err != nil {
		return
	}

	// execute the finalize func which may perform useful clean up steps
	return e.Finalize()
}

// -------------------------------------- Helper Funcs -------------------------------------- \\

func passedFilters(filters []ObjectFilter, storedObject StoredObject) (bool, error) {
	if len(filters) > 0 {
		// loop through the filters, if any of them fail, then return false
		for _, filter := range filters {
			msg, supported := filter.DoesSupportThisOS()
			if !supported {
				return false, errors.New(msg)
			}

			if filter.AppliesOnlyToFiles() && (storedObject.EntityType != common.EEntityType.File() && storedObject.EntityType != common.EEntityType.Hardlink()) {
				// don't pass folders to filters that only know how to deal with files
				// As at Feb 2020, we have separate logic to weed out folder properties (and not even send them)
				// if any filter applies only to files... but that logic runs after this point, so we need this
				// protection here, just to make sure we don't pass the filter logic an object that it can't handle.
				continue
			}

			if !filter.DoesPass(storedObject) {
				return false, nil
			}
		}
	}

	return true, nil
}

// This error should be treated as a flag, that we didn't fail processing, but instead, we just didn't process it.
// Currently, this is only really used for symlink processing, but it _is_ an error, so it must be handled in all new traversers.
// Basically, anywhere ProcessIfPassedFilters is called, additionally call GetProcessingError.
var IgnoredError = errors.New("FileIgnored")

func GetProcessingError(errin error) (ignored bool, err error) {
	if errin == IgnoredError {
		return true, nil
	}

	return false, errin
}

func ProcessIfPassedFilters(filters []ObjectFilter, storedObject StoredObject, processor ObjectProcessor) (err error) {
	pass, err := passedFilters(filters, storedObject)
	if err != nil {
		return err
	}
	if pass {
		err = processor(storedObject)
	} else {
		err = IgnoredError
	}

	return
}

// StoredObject names are useful for filters
func getObjectNameOnly(fullPath string) (nameOnly string) {
	lastPathSeparator := strings.LastIndex(fullPath, common.AZCOPY_PATH_SEPARATOR_STRING)

	// if there is a path separator and it is not the last character
	if lastPathSeparator > 0 && lastPathSeparator != len(fullPath)-1 {
		// then we separate out the name of the StoredObject
		nameOnly = fullPath[lastPathSeparator+1:]
	} else {
		nameOnly = fullPath
	}

	return
}
