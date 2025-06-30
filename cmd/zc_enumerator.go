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

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
)

// -------------------------------------- Component Definitions -------------------------------------- \\
// the following interfaces and structs allow the sync enumerator
// to be generic and has as little duplicated code as possible

// represent a local or remote resource object (ex: local file, blob, etc.)
// we can add more properties if needed, as this is easily extensible
// ** DO NOT instantiate directly, always use newStoredObject ** (to make sure its fully populated and any preprocessor method runs)
type StoredObject struct {
	name             string
	entityType       common.EntityType
	lastModifiedTime time.Time

	// For SMB, this is last write time and change time.
	// For NFS, this is populated from POSIX properties - posix_ctime, modtime
	lastWriteTime time.Time
	changeTime    time.Time

	size     int64
	md5      []byte
	blobType blob.BlobType // will be "None" when unknown or not applicable

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
	// Note 2: The other unusual case is the StoredObject representing the folder properties of the root dir
	// (if the source is folder-aware). In this case relativePath is also empty.
	// In this case isSourceRootFolder returns true.
	relativePath string
	// container source, only included by account traversers.
	ContainerName string
	// destination container name. Included in the processor after resolving container names.
	DstContainerName string
	// access tier, only included by blob traverser.
	blobAccessTier blob.AccessTier
	archiveStatus  blob.ArchiveStatus
	// metadata, included in S2S transfers
	Metadata       common.Metadata
	blobVersionID  string
	blobTags       common.BlobTags
	blobSnapshotID string
	blobDeleted    bool

	// Lease information
	leaseState    lease.StateType
	leaseStatus   lease.StatusType
	leaseDuration lease.DurationType
}

func (s *StoredObject) isMoreRecentThan(storedObject2 StoredObject, preferSMBTime bool) bool {
	lmtA := s.lastModifiedTime
	if preferSMBTime && !s.lastWriteTime.IsZero() {
		lmtA = s.lastWriteTime
	}
	lmtB := storedObject2.lastModifiedTime
	if preferSMBTime && !storedObject2.lastWriteTime.IsZero() {
		lmtB = storedObject2.lastWriteTime
	}

	if buildmode.IsMover {
		return lmtA.Compare(lmtB) != 0
	} else {
		return lmtA.After(lmtB)
	}
}

func (s *StoredObject) isSingleSourceFile() bool {
	return s.relativePath == "" && s.entityType == common.EEntityType.File()
}

func (s *StoredObject) isSourceRootFolder() bool {
	return s.relativePath == "" && s.entityType == common.EEntityType.Folder()
}

// isCompatibleWithEntitySettings serves as our universal filter for filtering out folders in the cases where we should not
// process them. (If we didn't have a filter like this, we'd have to put the filtering into
// every enumerator, which would complicated them.)
// We can't just implement this filtering in ToNewCopyTransfer, because delete transfers (from sync)
// do not pass through that routine.  So we need to make the filtering available in a separate function
// so that the sync deletion code path(s) can access it.
func (s *StoredObject) isCompatibleWithEntitySettings(fpo common.FolderPropertyOption, sht common.SymlinkHandlingType) bool {
	if s.entityType == common.EEntityType.File() || s.entityType == common.EEntityType.FileProperties() {
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
	} else if s.entityType == common.EEntityType.Symlink() {
		return sht == common.ESymlinkHandlingType.Preserve()
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
// We use this, so that we can easily test for compatibility in the sync deletion code (which expects an objectProcessor)
func newFpoAwareProcessor(fpo common.FolderPropertyOption, inner objectProcessor) objectProcessor {
	return func(s StoredObject) error {
		if s.isCompatibleWithEntitySettings(fpo, common.ESymlinkHandlingType.Skip()) {
			return inner(s)
		} else {
			return nil // nothing went wrong, because we didn't do anything
		}
	}
}

func (s *StoredObject) ToNewCopyTransfer(steWillAutoDecompress bool, Source string, Destination string, preserveBlobTier bool, folderPropertiesOption common.FolderPropertyOption, symlinkHandlingType common.SymlinkHandlingType) (transfer common.CopyTransfer, shouldSendToSte bool) {

	if !s.isCompatibleWithEntitySettings(folderPropertiesOption, symlinkHandlingType) {
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
		BlobTags:           s.blobTags,
		BlobSnapshotID:     s.blobSnapshotID,
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
	LastWriteTime() time.Time
	ChangeTime() time.Time
	ContentLength() int64
}

// a constructor is used so that in case the StoredObject has to change, the callers would get a compilation error
// and it forces all necessary properties to be always supplied and not forgotten
func newStoredObject(morpher objectMorpher, name string, relativePath string, entityType common.EntityType, lmt time.Time, size int64, props contentPropsProvider, blobProps blobPropsProvider, meta common.Metadata, containerName string) StoredObject {
	obj := StoredObject{
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
		archiveStatus:      blobProps.ArchiveStatus(),
		Metadata:           meta,
		ContainerName:      containerName,
		// Additional lease properties. To be used in listing
		leaseStatus:   blobProps.LeaseStatus(),
		leaseState:    blobProps.LeaseState(),
		leaseDuration: blobProps.LeaseDuration(),
	}

	// Folders don't have size, and root ones shouldn't have names in the StoredObject. Ensure those rules are consistently followed
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

// updateTimestamps updates the lastWriteTime and changeTime fields of a StoredObject
func (so *StoredObject) updateTimestamps(lastWriteTime, changeTime time.Time) {
	so.lastWriteTime = lastWriteTime
	so.changeTime = changeTime
}

// tryUpdateTimestampsFromMetadata updates the lastWriteTime and changeTime fields of a StoredObject
func (so *StoredObject) tryUpdateTimestampsFromMetadata(meta common.Metadata) {
	so.lastWriteTime, _, _ = common.TryReadModTimeFromMetadata(meta)
	so.changeTime, _, _ = common.TryReadCTimeFromMetadata(meta)
}

type ResourceTraverserTemplate struct {
	location                    common.Location
	credential                  *common.CredentialInfo
	symlinkHandling             common.SymlinkHandlingType
	listOfFilesChannel          chan string
	recursive                   bool
	getProperties               bool
	includeDirectoryStubs       bool
	permanentDeleteOption       common.PermanentDeleteOption
	incrementEnumerationCounter enumerationCounterFunc
	listOfVersionIds            chan string
	s2sPreserveBlobTags         bool
	syncHashType                common.SyncHashType
	preservePermissions         common.PreservePermissionsOption
	logLevel                    common.LogLevel
	cpkOptions                  common.CpkOptions
	errorChannel                chan TraverserErrorItemInfo
	stripTopDir                 bool
	trailingDot                 common.TrailingDotOption
	destination                 *common.Location
	excludeContainerNames       []string
	includeVersionsList         bool
	incrementNotTransferred     enumerationCounterFunc
}

// capable of traversing a structured resource like container or local directory
// pass each StoredObject to the given objectProcessor if it passes all the filters
type ResourceTraverser interface {
	Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error
	IsDirectory(isSource bool) (isDirectory bool, err error)
	// isDirectory has an isSource flag for a single exception to blob.
	// Blob should ONLY check remote if it's a source.
	// On destinations, because blobs and virtual directories can share names, we should support placing in both ways.
	// Thus, we only check the directory syntax on blob destinations. On sources, we check both syntax and remote, if syntax isn't a directory.
}

type AccountTraverser interface {
	ResourceTraverser
	listContainers() ([]string, error)
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
			glcm.Info(httpsRecommendedNotice)
		})
	}
}

type enumerationCounterFunc func(entityType common.EntityType)

// source, location, recursive, and incrementEnumerationCounter are always required.
// ctx, pipeline are only required for remote resources.
// symlinkHandling is only required for local resources (defaults to false)
// errorOnDirWOutRecursive is used by copy.
// If errorChannel is non-nil, all errors encountered during enumeration will be conveyed through this channel.
// To avoid slowdowns, use a buffered channel of enough capacity.
func InitResourceTraverser(
	resource common.ResourceString,
	location common.Location,
	ctx *context.Context,
	credential *common.CredentialInfo,
	symlinkHandling common.SymlinkHandlingType,
	listOfFilesChannel chan string,
	recursive,
	getProperties,
	includeDirectoryStubs bool,
	permanentDeleteOption common.PermanentDeleteOption,
	incrementEnumerationCounter enumerationCounterFunc,
	listOfVersionIds chan string,
	s2sPreserveBlobTags bool,
	syncHashType common.SyncHashType,
	preservePermissions common.PreservePermissionsOption,
	logLevel common.LogLevel,
	cpkOptions common.CpkOptions,
	errorChannel chan TraverserErrorItemInfo,
	stripTopDir bool,
	trailingDot common.TrailingDotOption,
	destination *common.Location,
	excludeContainerNames []string,
	includeVersionsList bool) (ResourceTraverser, error) {

	var output ResourceTraverser

	var includeDeleted bool
	var includeSnapshot bool
	var includeVersion bool
	switch permanentDeleteOption {
	case common.EPermanentDeleteOption.Snapshots():
		includeDeleted = true
		includeSnapshot = true
	case common.EPermanentDeleteOption.Versions():
		includeDeleted = true
		includeVersion = true
	case common.EPermanentDeleteOption.SnapshotsAndVersions():
		includeDeleted = true
		includeSnapshot = true
		includeVersion = true
	}

	// print out version id when using azcopy list
	if includeVersionsList {
		includeVersion = true
	}
	// Clean up the resource if it's a local path
	if location == common.ELocation.Local() {
		resource = common.ResourceString{Value: cleanLocalPath(resource.ValueLocal())}
	}

	// Feed list of files channel into new list traverser
	if listOfFilesChannel != nil {
		if location.IsLocal() {
			// First, ignore all escaped stars. Stars can be valid characters on many platforms (out of the 3 we support though, Windows is the only that cannot support it).
			// In the future, should we end up supporting another OS that does not treat * as a valid character, we should turn these checks into a map-check against runtime.GOOS.
			tmpResource := common.Iff(runtime.GOOS == "windows", resource.ValueLocal(), strings.ReplaceAll(resource.ValueLocal(), `\*`, ``))
			// check for remaining stars. We can't combine list traversers, and wildcarded list traversal occurs below.
			if strings.Contains(tmpResource, "*") {
				return nil, errors.New("cannot combine local wildcards with include-path or list-of-files")
			}
		}

		output = newListTraverser(resource, location, credential, ctx, recursive, symlinkHandling, getProperties,
			listOfFilesChannel, includeDirectoryStubs, incrementEnumerationCounter, s2sPreserveBlobTags, logLevel,
			cpkOptions, syncHashType, preservePermissions, trailingDot, destination)
		return output, nil
	}

	var reauthTok *common.ScopedAuthenticator
	if credential != nil {
		if at, ok := credential.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok {
			// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
			reauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
		}
	}

	options := CreateClientOptions(azcopyScanningLogger, nil, reauthTok)

	switch location {
	case common.ELocation.Local():
		_, err := common.OSStat(resource.ValueLocal())

		// If wildcard is present and this isn't an existing file/folder, glob and feed the globbed list into a list enum.
		if strings.Contains(resource.ValueLocal(), "*") && (stripTopDir || err != nil) {
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
			output = newListTraverser(baseResource, location, nil, nil, recursive, symlinkHandling, getProperties,
				globChan, includeDirectoryStubs, incrementEnumerationCounter, s2sPreserveBlobTags, logLevel,
				cpkOptions, syncHashType, preservePermissions, trailingDot, destination)
		} else {
			if ctx != nil {
				output, _ = newLocalTraverser(*ctx, resource.ValueLocal(), recursive, stripTopDir, symlinkHandling,
					syncHashType, incrementEnumerationCounter, errorChannel)
			} else {
				output, _ = newLocalTraverser(context.TODO(), resource.ValueLocal(), recursive, stripTopDir,
					symlinkHandling, syncHashType, incrementEnumerationCounter, errorChannel)
			}
		}
	case common.ELocation.Benchmark():
		ben, err := newBenchmarkTraverser(resource.Value, incrementEnumerationCounter)
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

		if ctx == nil {
			return nil, errors.New("a valid context must be supplied to create a blob traverser")
		}
		r := resourceURL.String()

		blobURLParts, err := blob.ParseURL(r)
		if err != nil {
			return nil, err
		}
		containerName := blobURLParts.ContainerName
		// Strip any non-service related things away
		blobURLParts.ContainerName = ""
		blobURLParts.BlobName = ""
		blobURLParts.Snapshot = ""
		blobURLParts.VersionID = ""

		res, err := SplitResourceString(blobURLParts.String(), common.ELocation.Blob())
		if err != nil {
			return nil, err
		}

		c, err := common.GetServiceClientForLocation(common.ELocation.Blob(), res, credential.CredentialType, credential.OAuthTokenInfo.TokenCredential, &options, nil)
		if err != nil {
			return nil, err
		}
		bsc, err := c.BlobServiceClient()
		if err != nil {
			return nil, err
		}

		if containerName == "" || strings.Contains(containerName, "*") {
			if !recursive {
				return nil, errors.New(accountTraversalInherentlyRecursiveError)
			}
			output = newBlobAccountTraverser(bsc, containerName, *ctx, includeDirectoryStubs, incrementEnumerationCounter, s2sPreserveBlobTags,
				cpkOptions, preservePermissions, false, excludeContainerNames)
		} else if listOfVersionIds != nil {
			output = newBlobVersionsTraverser(r, bsc, *ctx, includeDirectoryStubs, incrementEnumerationCounter, listOfVersionIds, cpkOptions)
		} else {
			output = newBlobTraverser(r, bsc, *ctx, recursive, includeDirectoryStubs, incrementEnumerationCounter, s2sPreserveBlobTags, cpkOptions,
				includeDeleted, includeSnapshot, includeVersion, preservePermissions, false, errorChannel)
		}
	case common.ELocation.File():
		// TODO (last service migration) : Remove dependency on URLs.
		resourceURL, err := resource.FullURL()
		if err != nil {
			return nil, err
		}

		recommendHttpsIfNecessary(*resourceURL)

		if ctx == nil {
			return nil, errors.New("a valid context must be supplied to create a file traverser")
		}
		r := resourceURL.String()

		fileURLParts, err := file.ParseURL(r)
		if err != nil {
			return nil, err
		}
		shareName := fileURLParts.ShareName
		// Strip any non-service related things away
		fileURLParts.ShareName = ""
		fileURLParts.ShareSnapshot = ""
		fileURLParts.DirectoryOrFilePath = ""
		fileOptions := &common.FileClientOptions{
			AllowTrailingDot: trailingDot.IsEnabled(),
		}

		res, err := SplitResourceString(fileURLParts.String(), common.ELocation.File())
		if err != nil {
			return nil, err
		}

		c, err := common.GetServiceClientForLocation(common.ELocation.File(), res, credential.CredentialType, credential.OAuthTokenInfo.TokenCredential, &options, fileOptions)
		if err != nil {
			return nil, err
		}
		fsc, err := c.FileServiceClient()
		if err != nil {
			return nil, err
		}

		if shareName == "" || strings.Contains(shareName, "*") {
			if !recursive {
				return nil, errors.New(accountTraversalInherentlyRecursiveError)
			}
			output = newFileAccountTraverser(fsc, shareName, *ctx, getProperties, incrementEnumerationCounter, trailingDot, destination)
		} else {
			output = newFileTraverser(r, fsc, *ctx, recursive, getProperties, incrementEnumerationCounter, trailingDot, destination)
		}
	case common.ELocation.BlobFS():
		resourceURL, err := resource.FullURL()
		if err != nil {
			return nil, err
		}

		recommendHttpsIfNecessary(*resourceURL)

		if ctx == nil {
			return nil, errors.New("a valid context must be supplied to create a blob traverser")
		}
		r := resourceURL.String()
		r = strings.Replace(r, ".dfs", ".blob", 1)
		blobURLParts, err := blob.ParseURL(r)
		if err != nil {
			return nil, err
		}
		containerName := blobURLParts.ContainerName
		// Strip any non-service related things away
		blobURLParts.ContainerName = ""
		blobURLParts.BlobName = ""
		blobURLParts.Snapshot = ""
		blobURLParts.VersionID = ""

		res, err := SplitResourceString(blobURLParts.String(), common.ELocation.Blob())
		if err != nil {
			return nil, err
		}

		c, err := common.GetServiceClientForLocation(common.ELocation.Blob(), res, credential.CredentialType, credential.OAuthTokenInfo.TokenCredential, &options, nil)
		if err != nil {
			return nil, err
		}
		bsc, err := c.BlobServiceClient()
		if err != nil {
			return nil, err
		}

		includeDirectoryStubs = true // DFS is supposed to feed folders in
		if containerName == "" || strings.Contains(containerName, "*") {
			if !recursive {
				return nil, errors.New(accountTraversalInherentlyRecursiveError)
			}
			output = newBlobAccountTraverser(bsc, containerName, *ctx, includeDirectoryStubs, incrementEnumerationCounter, s2sPreserveBlobTags,
				cpkOptions, preservePermissions, true, excludeContainerNames)
		} else if listOfVersionIds != nil {
			output = newBlobVersionsTraverser(r, bsc, *ctx, includeDirectoryStubs, incrementEnumerationCounter, listOfVersionIds, cpkOptions)
		} else {
			output = newBlobTraverser(r, bsc, *ctx, recursive, includeDirectoryStubs, incrementEnumerationCounter, s2sPreserveBlobTags,
				cpkOptions, includeDeleted, includeSnapshot, includeVersion, preservePermissions, true, errorChannel)
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
			output, err = newS3Traverser(credential.CredentialType, resourceURL, *ctx, recursive, getProperties, incrementEnumerationCounter)

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

		if ctx == nil {
			return nil, errors.New("a valid context must be supplied to create a GCP traverser")
		}

		if gcpURLParts.BucketName == "" || strings.Contains(gcpURLParts.BucketName, "*") {
			if !recursive {
				return nil, errors.New(accountTraversalInherentlyRecursiveError)
			}

			output, err = newGCPServiceTraverser(resourceURL, *ctx, getProperties, incrementEnumerationCounter)
			if err != nil {
				return nil, err
			}
		} else {
			output, err = newGCPTraverser(resourceURL, *ctx, recursive, getProperties, incrementEnumerationCounter)
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
type objectProcessor func(storedObject StoredObject) error

// TODO: consider making objectMorpher an interface, not a func, and having newStoredObject take an array of them, instead of just one
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
var noPreProccessor objectMorpher = nil

// given a StoredObject, verify if it satisfies the defined conditions
// if yes, return true
type ObjectFilter interface {
	DoesSupportThisOS() (msg string, supported bool)
	DoesPass(storedObject StoredObject) bool
	AppliesOnlyToFiles() bool
}

type preFilterProvider interface {
	getEnumerationPreFilter() string
}

// -------------------------------------- Generic Enumerators -------------------------------------- \\
// the following enumerators must be instantiated with configurations
// they define the work flow in the most generic terms

type syncEnumerator struct {
	primaryTraverserTemplate   ResourceTraverserTemplate
	secondaryTraverserTemplate ResourceTraverserTemplate

	// these allow us to go through the source and destination
	// there is flexibility in which side we scan first, it could be either the source or the destination
	primaryTraverser   ResourceTraverser
	secondaryTraverser ResourceTraverser

	// the results from the primary traverser would be stored here
	objectIndexer *objectIndexer

	// general filters apply to both the primary and secondary traverser
	filters []ObjectFilter

	// the processor that apply only to the secondary traverser
	// it processes objects as scanning happens
	// based on the data from the primary traverser stored in the objectIndexer
	objectComparator objectProcessor

	ctp *copyTransferProcessor

	// a finalizer that is always called if the enumeration finishes properly
	finalize func() error

	orchestratorOptions *syncOrchestratorOptions
}

func newSyncEnumerator(primaryTemplate ResourceTraverserTemplate, secondaryTemplate ResourceTraverserTemplate, primaryTraverser ResourceTraverser, secondaryTraverser ResourceTraverser, indexer *objectIndexer,
	filters []ObjectFilter, comparator objectProcessor, finalize func() error, ctp *copyTransferProcessor, orchestratorOptions *syncOrchestratorOptions) *syncEnumerator {
	return &syncEnumerator{
		primaryTraverserTemplate:   primaryTemplate,
		secondaryTraverserTemplate: secondaryTemplate,
		primaryTraverser:           primaryTraverser,
		secondaryTraverser:         secondaryTraverser,
		objectIndexer:              indexer,
		filters:                    filters,
		objectComparator:           comparator,
		finalize:                   finalize,
		ctp:                        ctp,
		orchestratorOptions:        orchestratorOptions,
	}
}

func (e *syncEnumerator) Enumerate() (err error) {
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

	// enumerate the primary resource and build lookup map
	err = e.primaryTraverser.Traverse(noPreProccessor, e.objectIndexer.store, e.filters)
	handleAcceptableErrors()
	if err != nil {
		return err
	}

	glcm.Info(fmt.Sprintf("Object indexer map size: %d", e.objectIndexer.counter))
	// enumerate the secondary resource and as the objects pass the filters
	// they will be passed to the object comparator
	// which can process given objects based on what's already indexed
	// note: transferring can start while scanning is ongoing
	err = e.secondaryTraverser.Traverse(noPreProccessor, e.objectComparator, e.filters)
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
	ObjectDispatcher objectProcessor

	// a finalizer that is always called if the enumeration finishes properly
	Finalize func() error
}

func NewCopyEnumerator(traverser ResourceTraverser, filters []ObjectFilter, objectDispatcher objectProcessor, finalizer func() error) *CopyEnumerator {
	return &CopyEnumerator{
		Traverser:        traverser,
		Filters:          filters,
		ObjectDispatcher: objectDispatcher,
		Finalize:         finalizer,
	}
}

func WarnStdoutAndScanningLog(toLog string) {
	glcm.Info(toLog)
	if azcopyScanningLogger != nil {
		// ste.JobsAdmin.LogToJobLog(toLog, pipeline.LogWarning)
		azcopyScanningLogger.Log(common.LogWarning, toLog)
	}

	if buildmode.IsMover {
		fmt.Sprintf("[AzCopy] " + toLog)
	}
}

func (e *CopyEnumerator) enumerate() (err error) {
	err = e.Traverser.Traverse(noPreProccessor, e.ObjectDispatcher, e.Filters)
	if err != nil {
		return
	}

	// execute the finalize func which may perform useful clean up steps
	return e.Finalize()
}

// -------------------------------------- Helper Funcs -------------------------------------- \\

func passedFilters(filters []ObjectFilter, storedObject StoredObject) bool {
	if len(filters) > 0 {
		// loop through the filters, if any of them fail, then return false
		for _, filter := range filters {
			msg, supported := filter.DoesSupportThisOS()
			if !supported {
				glcm.Error(msg)
			}

			if filter.AppliesOnlyToFiles() && storedObject.entityType != common.EEntityType.File() {
				// don't pass folders to filters that only know how to deal with files
				// As at Feb 2020, we have separate logic to weed out folder properties (and not even send them)
				// if any filter applies only to files... but that logic runs after this point, so we need this
				// protection here, just to make sure we don't pass the filter logic an object that it can't handle.
				continue
			}

			if !filter.DoesPass(storedObject) {
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

	return false, errin
}

func processIfPassedFilters(filters []ObjectFilter, storedObject StoredObject, processor objectProcessor) (err error) {
	if passedFilters(filters, storedObject) {
		err = processor(storedObject)
	} else {
		err = ignoredError
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
