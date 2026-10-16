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
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
	"github.com/Azure/azure-storage-azcopy/v10/ste"

	"github.com/pkg/errors"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
)

// allow us to iterate through a path pointing to the blob endpoint
type blobTraverser struct {
	rawURL        string
	serviceClient *service.Client
	ctx           context.Context
	recursive     bool

	// parallel listing employs the hierarchical listing API which is more expensive
	// cx should have the option to disable this optimization in the name of saving costs
	parallelListing     bool
	flatListDiagnostics bool
	flatListArrow       bool

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc

	s2sPreserveSourceTags bool

	cpkOptions common.CpkOptions

	preservePermissions common.PreservePermissionsOption

	include common.BlobTraverserIncludeOption

	isDFS bool

	destResourceType *common.Location

	errorChannel chan<- TraverserErrorItemInfo

	// isSyncDestination indicates this traverser is enumerating the destination side of a sync job.
	isSyncDestination  bool
	suppressCrawlStats bool

	// includeDirectoryOrPrefix is used to determine if we should enqueue directories or prefixes
	// in the traversal process. If true, prefixes will be enqueued as well even if location
	// is not folder aware.
	includeDirectoryOrPrefix bool
}

// ErrorFileInfo holds information about files and folders that failed enumeration.
type ErrorBlobInfo struct {
	BlobPath             string
	BlobSize             int64
	BlobName             string
	BlobLastModifiedTime time.Time
	Error                error
	Dir                  bool
}

// Compile-time check to ensure ErrorFileInfo implements TraverserErrorItemInfo
var _ TraverserErrorItemInfo = (*ErrorBlobInfo)(nil)

// START - Implementing methods defined in TraverserErrorItemInfo

func (e ErrorBlobInfo) FullPath() string {
	return e.BlobPath
}

func (e ErrorBlobInfo) Name() string {
	return e.BlobName
}

func (e ErrorBlobInfo) Size() int64 {
	return e.BlobSize
}

func (e ErrorBlobInfo) LastModifiedTime() time.Time {
	return e.BlobLastModifiedTime
}

func (e ErrorBlobInfo) IsDir() bool {
	return e.Dir
}

func (e ErrorBlobInfo) ErrorMessage() error {
	return e.Error
}

func (e ErrorBlobInfo) Location() common.Location {
	return common.ELocation.Blob()
}

// END - Implementing methods defined in TraverserErrorItemInfo

// destIsBlobFS reports whether the destination is a BlobFS (HNS) endpoint.
// Returns false when destResourceType is nil, since the container-root ACL path
// only succeeds against an explicitly-known HNS destination.
func (t *blobTraverser) destIsBlobFS() bool {
	return t.destResourceType != nil && *t.destResourceType == common.ELocation.BlobFS()
}

func (t *blobTraverser) writeToBlobErrorChannel(err ErrorBlobInfo) {
	if t.errorChannel != nil {
		// Defense-in-depth: the orchestrator drains all traversers before the caller closes this
		// channel, but recover in case a send still races a close during cancellation.
		defer func() {
			if r := recover(); r != nil {
				WarnStdoutAndScanningLog(fmt.Sprintf("Error channel closed, could not send error: %v", err.ErrorMessage()))
			}
		}()
		select {
		case t.errorChannel <- err:
		default:
			// Channel might be full, log the error instead
			WarnStdoutAndScanningLog(fmt.Sprintf("Failed to send error to channel: %v", err.ErrorMessage()))
		}
	}
}

var NonErrorDirectoryStubOverlappable = errors.New("The directory stub exists, and can overlap.")

func (t *blobTraverser) IsDirectory(isSource bool) (isDirectory bool, err error) {
	isDirDirect := copyHandlerUtil{}.urlIsContainerOrVirtualDirectory(t.rawURL)

	blobURLParts, err := blob.ParseURL(t.rawURL)
	if err != nil {
		return false, err
	}

	// Skip the single blob check if we're checking a destination.
	// This is an individual exception for blob because blob supports virtual directories and blobs sharing the same name.
	// On HNS accounts, we would still perform this test. The user may have provided directory name without path-separator
	if isDirDirect { // a container or a path ending in '/' is always directory
		if blobURLParts.ContainerName != "" && blobURLParts.BlobName == "" {
			// If it's a container, let's ensure that container exists. Listing is a safe assumption to be valid, because how else would we enumerate?
			containerClient := t.serviceClient.NewContainerClient(blobURLParts.ContainerName)
			p := containerClient.NewListBlobsFlatPager(nil)
			_, err = p.NextPage(t.ctx)

			if bloberror.HasCode(err, bloberror.AuthorizationPermissionMismatch) {
				// Maybe we don't have the ability to list? Can we get container properties as a fallback?
				_, propErr := containerClient.GetProperties(t.ctx, nil)
				err = ternary.Iff(propErr == nil, nil, err)
			}
		}

		return true, err
	}
	if !isSource && !t.isDFS {
		// destination on blob endpoint. If it does not end in '/' it is a file
		return false, nil
	}

	// All sources and DFS-destinations we'll look further
	// This call is fine, because there is no trailing / here-- If there's a trailing /, this is surely referring
	_, _, isDirStub, _, blobErr := t.getPropertiesIfSingleBlob()

	// We know for sure this is a single blob still, let it walk on through to the traverser.
	if bloberror.HasCode(blobErr, bloberror.BlobUsesCustomerSpecifiedEncryption) {
		return false, nil
	}

	if blobErr == nil {
		return isDirStub, nil
	}

	containerClient := t.serviceClient.NewContainerClient(blobURLParts.ContainerName)
	searchPrefix := strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING) + common.AZCOPY_PATH_SEPARATOR_STRING
	maxResults := int32(1)
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &searchPrefix, MaxResults: &maxResults})
	resp, err := pager.NextPage(t.ctx)
	if err != nil {
		if azcopyScanningLogger != nil {
			msg := fmt.Sprintf("Failed to check if the destination is a folder or a file (Azure Files). Assuming the destination is a file: %s", err)
			azcopyScanningLogger.Log(common.LogError, msg)
		}
		return false, nil
	}

	if len(resp.Segment.BlobItems) == 0 {
		// Not a directory, but there was also no file on site. Therefore, there's nothing.
		return false, errors.New(common.FILE_NOT_FOUND)
	}

	return true, nil
}

func (t *blobTraverser) getPropertiesIfSingleBlob() (response *blob.GetPropertiesResponse, isBlob bool, isDirStub bool, blobName string, err error) {
	// trim away the trailing slash before we check whether it's a single blob
	// so that we can detect the directory stub in case there is one
	blobURLParts, err := blob.ParseURL(t.rawURL)
	if err != nil {
		return nil, false, false, "", err
	}

	if blobURLParts.BlobName == "" {
		// This is a container, which needs to be given a proper listing.
		return nil, false, false, "", nil
	}

	/*
		If the user specified a trailing /, they may mean:
		A) `folder/` with `hdi_isfolder`, this is intentional.
		B) `folder` with `hdi_isfolder`
		C) a virtual directory with children, but no stub
	*/

retry:
	blobClient, err := createBlobClientFromServiceClient(blobURLParts, t.serviceClient)
	if err != nil {
		return nil, false, false, blobURLParts.BlobName, err
	}
	props, err := blobClient.GetProperties(t.ctx, &blob.GetPropertiesOptions{CPKInfo: t.cpkOptions.GetCPKInfo()})

	if err != nil && strings.HasSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING) {
		// Trim & retry, maybe the directory stub is DFS style.
		blobURLParts.BlobName = strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)
		goto retry
	} else if err == nil {
		// We found the target blob, great! Let's return the details.
		isDir := gCopyUtil.doesBlobRepresentAFolder(props.Metadata)
		return &props, !isDir, isDir, blobURLParts.BlobName, nil
	}

	// We found nothing.
	return nil, false, false, "", err
}

func (t *blobTraverser) getBlobTags() (common.BlobTags, error) {
	blobURLParts, err := blob.ParseURL(t.rawURL)
	if err != nil {
		return nil, err
	}
	blobURLParts.BlobName = strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)

	// perform the check
	blobClient, err := createBlobClientFromServiceClient(blobURLParts, t.serviceClient)
	if err != nil {
		return nil, err
	}
	blobTagsMap := make(common.BlobTags)
	blobGetTagsResp, err := blobClient.GetTags(t.ctx, nil)
	if err != nil {
		return blobTagsMap, err
	}

	for _, blobTag := range blobGetTagsResp.BlobTagSet {
		blobTagsMap[url.QueryEscape(*blobTag.Key)] = url.QueryEscape(*blobTag.Value)
	}
	return blobTagsMap, nil
}

func (t *blobTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	blobURLParts, err := blob.ParseURL(t.rawURL)
	if err != nil {
		return err
	}

	// check if the url points to a single blob
	blobProperties, isBlob, isDirStub, blobName, err := t.getPropertiesIfSingleBlob()

	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		errorBlobInfo := ErrorBlobInfo{
			BlobPath: blobURLParts.BlobName,
			Error:    err,
		}

		// Don't error out unless it's a CPK error just yet
		// If it's a CPK error, we know it's a single blob and that we can't get the properties on it anyway.
		if respErr.ErrorCode == string(bloberror.BlobUsesCustomerSpecifiedEncryption) {
			t.writeToBlobErrorChannel(errorBlobInfo)
			return errors.New("this blob uses customer provided encryption keys (CPK). At the moment, AzCopy does not support CPK-encrypted blobs. " +
				"If you wish to make use of this blob, we recommend using one of the Azure Storage SDKs")
		}
		if respErr.RawResponse == nil {
			t.writeToBlobErrorChannel(errorBlobInfo)
			return fmt.Errorf("cannot list files due to reason %s", respErr)
		} else if respErr.StatusCode == 403 { // Some nature of auth error-- Whatever the user is pointing at, they don't have access to, regardless of whether it's a file or a dir stub.
			t.writeToBlobErrorChannel(errorBlobInfo)
			return fmt.Errorf("cannot list files due to reason %s", respErr)
		} else if t.isSyncDestination && t.isDFS && respErr.StatusCode == 404 {
			// If this is a sync destination traversal and we get a 404, it means the blob doesn't exist
			// in the destination. We need to explicitly return the error so the sync orchestrator
			// can handle it gracefully (e.g. set isDestinationPresent = false).
			return fmt.Errorf("blob %s not found in destination. Err %s", blobName, respErr)
		}
	}

	// schedule the blob in two cases:
	// 	1. either we are targeting a single blob and the URL wasn't explicitly pointed to a virtual dir
	//	2. either we are scanning recursively with includeDirectoryStubs set to true,
	//	   then we add the stub blob that represents the directory
	if (isBlob && !strings.HasSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)) ||
		(t.include.DirStubs() && isDirStub && t.recursive) {
		// sanity checking so highlighting doesn't highlight things we're not worried about.
		if blobProperties == nil {
			panic("isBlob should never be set if getting properties is an error")
		}

		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(common.LogDebug, "Detected the root as a blob.")
			azcopyScanningLogger.Log(common.LogDebug, fmt.Sprintf("Root entity type: %s", getEntityType(blobProperties.Metadata)))
		}

		relPath := ""
		if strings.HasSuffix(blobName, "/") {
			relPath = "\x00" // Because the ste will trim the / suffix from our source, or we may not already have it.
		}

		blobPropsAdapter := blobPropertiesResponseAdapter{blobProperties}
		storedObject := newStoredObject(
			preprocessor,
			getObjectNameOnly(blobName),
			relPath,
			getEntityType(blobPropsAdapter.Metadata),
			blobPropsAdapter.LastModified(),
			blobPropsAdapter.ContentLength(),
			blobPropsAdapter,
			blobPropsAdapter,
			blobPropsAdapter.Metadata,
			blobURLParts.ContainerName,
		)
		storedObject.tryUpdateTimestampsFromMetadata(blobPropsAdapter.Metadata)

		if t.s2sPreserveSourceTags {
			blobTagsMap, err := t.getBlobTags()
			if err != nil {
				panic("Couldn't fetch blob tags due to error: " + err.Error())
			}
			if len(blobTagsMap) > 0 {
				storedObject.blobTags = blobTagsMap
			}
		}
		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(storedObject.entityType)
		}

		err := processIfPassedFilters(filters, storedObject, processor)
		_, err = getProcessingError(err)

		// short-circuit if we don't have anything else to scan and permanent delete is not on
		if !t.include.Deleted() && (isBlob || err != nil) {
			return err
		}
	} else if blobURLParts.BlobName == "" && (t.preservePermissions.IsTruthy() || (t.isDFS && t.destIsBlobFS())) {
		// If the root is a container and we're copying "folders", we should persist the ACLs there too.
		// For DFS, include the container root only when the destination is BlobFS — the container-root
		// ACL path (blobFolderSender.SetContainerACL) hardcodes the DFS endpoint, so only HNS→HNS can
		// actually land the ACL
		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(common.LogDebug, "Detected the root as a container.")
		}

		storedObject := newStoredObject(
			preprocessor,
			"",
			"",
			common.EEntityType.Folder(),
			time.Now(),
			0,
			noContentProps,
			noBlobProps,
			common.Metadata{},
			blobURLParts.ContainerName,
		)
		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.Folder())
		}

		err := processIfPassedFilters(filters, storedObject, processor)
		_, err = getProcessingError(err)
		if err != nil {
			return err
		}
	}

	// get the container URL so that we can list the blobs
	containerClient := t.serviceClient.NewContainerClient(blobURLParts.ContainerName)

	// get the search prefix to aid in the listing
	// example: for a url like https://test.blob.core.windows.net/test/foo/bar/bla
	// the search prefix would be foo/bar/bla
	searchPrefix := blobURLParts.BlobName

	// append a slash if it is not already present
	// example: foo/bar/bla becomes foo/bar/bla/ so that we only list children of the virtual directory
	if searchPrefix != "" && !strings.HasSuffix(searchPrefix, common.AZCOPY_PATH_SEPARATOR_STRING) && !t.include.Snapshots() && !t.include.Deleted() {
		searchPrefix += common.AZCOPY_PATH_SEPARATOR_STRING
	}

	// as a performance optimization, get an extra prefix to do pre-filtering. It's typically the start portion of a blob name.
	extraSearchPrefix := FilterSet(filters).GetEnumerationPreFilter(t.recursive)

	if t.parallelListing {
		return t.parallelList(containerClient, blobURLParts.ContainerName, searchPrefix, extraSearchPrefix, preprocessor, processor, filters)
	}

	return t.serialList(containerClient, blobURLParts.ContainerName, searchPrefix, extraSearchPrefix, preprocessor, processor, filters)
}

func (t *blobTraverser) parallelList(containerClient *container.Client, containerName string, searchPrefix string,
	extraSearchPrefix string, preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error {
	emptyPrefix := true

	// Define how to enumerate its contents
	// This func must be thread safe/goroutine safe
	enumerateOneDir := func(dir parallel.Directory, enqueueDir func(parallel.Directory), enqueueOutput func(parallel.DirectoryEntry, error)) error {
		currentDirPath := dir.(string)

		pager := containerClient.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
			Prefix:  &currentDirPath,
			Include: container.ListBlobsInclude{Metadata: true, Tags: t.s2sPreserveSourceTags, Deleted: t.include.Deleted(), Snapshots: t.include.Snapshots(), Versions: t.include.Versions()},
		})
		var marker *string

		// The traverser emits objects in raw service order (files and sub-dirs as the hierarchy
		// listing returns them) via enqueueOutput. Any ordering the streaming merge-join needs is
		// done in its own producer (folders-only heap), so the traverser does no per-page sorting.

		for pager.More() {
			lResp, err := pager.NextPage(t.ctx)
			if err != nil {
				return fmt.Errorf("cannot list files due to reason %s", err)
			}
			emptyPrefix = emptyPrefix && len(lResp.Segment.BlobPrefixes) == 0 && len(lResp.Segment.BlobItems) == 0
			// queue up the sub virtual directories if recursive is true or if enqueueDirorPrefix is true
			if t.recursive || t.includeDirectoryOrPrefix {
				for _, virtualDir := range lResp.Segment.BlobPrefixes {
					if t.recursive {
						enqueueDir(*virtualDir.Name)
						if azcopyScanningLogger != nil {
							azcopyScanningLogger.Log(common.LogDebug, fmt.Sprintf("Enqueuing sub-directory %s for enumeration.", *virtualDir.Name))
						}
					}

					enqueuedDirAsOutput := false // Reset the flag for each directory processed

					// TODO: shnayak to check the impact of removing t.includeDirectoryOrPrefix for OnPrem scenario.
					if t.include.DirStubs() {
						// try to get properties on the directory itself, since it's not listed in BlobItems
						dName := strings.TrimSuffix(*virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING)
						blobClient := containerClient.NewBlobClient(dName)
					altNameCheck:
						pResp, err := blobClient.GetProperties(t.ctx, nil)
						if err == nil {
							if !t.doesBlobRepresentAFolder(pResp.Metadata) { // We've picked up on a file *named* the folder, not the folder itself. Does folder/ exist?
								if !strings.HasSuffix(dName, "/") {
									blobClient = containerClient.NewBlobClient(dName + common.AZCOPY_PATH_SEPARATOR_STRING) // Tack on the path separator, check.
									dName += common.AZCOPY_PATH_SEPARATOR_STRING
									goto altNameCheck // "foo" is a file, what about "foo/"?
								}

								goto skipDirAdd // We shouldn't add a blob that isn't a folder as a folder. You either have the folder metadata, or you don't.
							}

							pbPropAdapter := blobPropertiesResponseAdapter{&pResp}
							folderRelativePath := strings.TrimPrefix(dName, searchPrefix)

							storedObject := newStoredObject(
								preprocessor,
								getObjectNameOnly(dName),
								folderRelativePath,
								common.EEntityType.Folder(),
								pbPropAdapter.LastModified(),
								pbPropAdapter.ContentLength(),
								pbPropAdapter,
								pbPropAdapter,
								pbPropAdapter.Metadata,
								containerName,
							)
							storedObject.tryUpdateTimestampsFromMetadata(pbPropAdapter.Metadata)

							if t.s2sPreserveSourceTags {
								tResp, err := blobClient.GetTags(t.ctx, nil)

								if err == nil {
									blobTagsMap := common.BlobTags{}
									for _, blobTag := range tResp.BlobTagSet {
										blobTagsMap[url.QueryEscape(*blobTag.Key)] = url.QueryEscape(*blobTag.Value)
									}
									storedObject.blobTags = blobTagsMap
								}
							}

							enqueueOutput(storedObject, err)
							enqueuedDirAsOutput = true
						} else {
							// There was nothing there, but is there folder/?
							if !strings.HasSuffix(dName, "/") {
								blobClient = containerClient.NewBlobClient(dName + common.AZCOPY_PATH_SEPARATOR_STRING) // Tack on the path separator, check.
								dName += common.AZCOPY_PATH_SEPARATOR_STRING
								goto altNameCheck // "foo" is a file, what about "foo/"?
							}
						}
					skipDirAdd:
					}

					if t.includeDirectoryOrPrefix && !enqueuedDirAsOutput {
						// For HNS accounts, we will include the virtual directory as a stub if it does not have a blob with the folder metadata in the above section.
						// However, for non-HNS accounts, we will not enqueue the virtual directory as a stub if it does not have a blob with the folder metadata.
						//
						// If we didn't enqueue the directory, it means that it is a non-HNS account and we didn't find a blob with the folder metadata.
						// We still want to enqueue the virtual directory as a stub, so that we can compare it with the source during local -> blob sync.
						// This blob prefix enqueue is necessary to track extra objects in target during mirror sync
						// operations, so that we can delete them if they are not present in source.
						// Note: We can skip this if deleteDestination is set to false but we don't have that option for blob traverser.
						dName := strings.TrimSuffix(*virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING)
						folderRelativePath := strings.TrimPrefix(dName, searchPrefix)
						storedObject := newStoredObject(
							preprocessor,
							getObjectNameOnly(dName),
							folderRelativePath,
							common.EEntityType.Folder(),
							time.Time{},
							0,
							noContentProps,
							noBlobProps,
							noMetadata,
							containerName,
						)
						storedObject.isVirtualPrefix = true
						enqueueOutput(storedObject, err)
					}
				}
			}

			// process the blobs returned in this result segment
			for _, blobInfo := range lResp.Segment.BlobItems {
				// if the blob represents a hdi folder, then skip it
				if t.doesBlobRepresentAFolder(blobInfo.Metadata) {
					continue
				}

				storedObject := t.createStoredObjectForBlob(preprocessor, blobInfo, strings.TrimPrefix(*blobInfo.Name, searchPrefix), containerName)

				// edge case, blob name happens to be the same as root and ends in /
				if storedObject.relativePath == "" && strings.HasSuffix(storedObject.name, "/") {
					storedObject.relativePath = "\x00" // Short circuit, letting the backend know we *really* meant root/.
				}

				if t.s2sPreserveSourceTags && blobInfo.BlobTags != nil {
					blobTagsMap := common.BlobTags{}
					for _, blobTag := range blobInfo.BlobTags.BlobTagSet {
						blobTagsMap[url.QueryEscape(*blobTag.Key)] = url.QueryEscape(*blobTag.Value)
					}
					storedObject.blobTags = blobTagsMap
				}

				enqueueOutput(storedObject, nil)
			}

			// if debug mode is on, note down the result, this is not going to be fast
			if azcopyScanningLogger != nil && azcopyScanningLogger.ShouldLog(common.LogDebug) {
				tokenValue := "NONE"
				if marker != nil {
					tokenValue = *marker
				}

				var vdirListBuilder strings.Builder
				for _, virtualDir := range lResp.Segment.BlobPrefixes {
					fmt.Fprintf(&vdirListBuilder, " %s,", *virtualDir.Name)
				}
				var fileListBuilder strings.Builder
				for _, blobInfo := range lResp.Segment.BlobItems {
					fmt.Fprintf(&fileListBuilder, " %s,", *blobInfo.Name)
				}
				msg := fmt.Sprintf("Enumerating %s with token %s. Sub-dirs:%s Files:%s", currentDirPath,
					tokenValue, vdirListBuilder.String(), fileListBuilder.String())
				azcopyScanningLogger.Log(common.LogDebug, msg)
			}
			marker = lResp.NextMarker
		}
		return nil
	}

	// initiate parallel scanning, starting at the root path
	workerContext, cancelWorkers := context.WithCancel(t.ctx)
	defer cancelWorkers()
	// Random dequeue is only used for mover-high-perf Blob/BlobFS<->Blob/BlobFS transfers: it helps
	// avoid partition hotspotting when writing to Azure Blob Storage. The source side here is always
	// Blob or BlobFS (this is the blob traverser); check the destination side too so we don't affect
	// Blob/BlobFS -> Local, File, S3, etc.
	randomDequeue := buildmode.HighPerf() && t.destResourceType != nil &&
		(*t.destResourceType == common.ELocation.Blob() || *t.destResourceType == common.ELocation.BlobFS())
	cCrawled, crawlStats := parallel.CrawlWithStats(workerContext, searchPrefix+extraSearchPrefix, enumerateOneDir, EnumerationParallelism,
		parallel.CrawlOptions{RandomDequeue: randomDequeue, MaxQueueDirectories: resolveHighPerfMaxQueueDirs()})
	if !t.suppressCrawlStats {
		stopStats := startBlobCrawlStats(workerContext, crawlStats, EnumerationParallelism, randomDequeue)
		defer stopStats()
	}
	for x := range cCrawled {
		item, workerError := x.Item()
		if workerError != nil {
			t.writeToBlobErrorChannel(ErrorBlobInfo{Error: workerError})
			return workerError
		}

		object := item.(StoredObject)

		if t.incrementEnumerationCounter != nil {
			if UseSyncOrchestrator && !object.isVirtualPrefix {
				t.incrementEnumerationCounter(object.entityType)
			} else if !UseSyncOrchestrator {
				// XDM: Retaining the old behavior but this seems like a bug.
				t.incrementEnumerationCounter(common.EEntityType.File())
			}
		}

		processErr := processIfPassedFilters(filters, object, processor)
		_, processErr = getProcessingError(processErr)
		if processErr != nil {
			t.writeToBlobErrorChannel(ErrorBlobInfo{
				BlobName:             object.name,
				BlobPath:             object.relativePath,
				BlobLastModifiedTime: object.lastModifiedTime,
				Dir:                  object.entityType == common.EEntityType.Folder(),
				Error:                processErr})
			return processErr
		}
	}

	if t.isSyncDestination && !t.isDFS && emptyPrefix {
		// In case of sync destination traversal, we want to let the orchestrator know that the prefix was not found
		// for a flat blob destination. This will help in optimizing the sync process by avoiding redundant
		// target traversals.
		return fmt.Errorf("blob %s not found in destination. Err %s", t.rawURL, bloberror.BlobNotFound)
	}

	return nil
}

func startBlobCrawlStats(ctx context.Context, stats *parallel.CrawlStats, parallelism int, randomDequeue bool) func() {
	if !buildmode.HighPerf() {
		return func() {}
	}
	syncOrchestratorLog(common.LogInfo, fmt.Sprintf(
		"[CrawlConfig] mode=blob-parallel, crawlParallelism=%d, maxQueueDirectories=%d, randomDequeue=%t; queue limit is a worker-shedding threshold, not a hard queue or memory cap",
		parallelism, stats.MaxQueueDirectories, randomDequeue), true)
	statsCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		logStats := func(final bool) {
			syncOrchestratorLog(common.LogInfo, fmt.Sprintf(
				"[CrawlStats] mode=blob-parallel, activeWorkers=%d/%d, queuedDirs=%d, goroutines=%d, maxQueueDirectories=%d, peakQueuedDirs=%d, final=%t",
				atomic.LoadInt64(&stats.ActiveWorkers), parallelism, atomic.LoadInt64(&stats.QueuedDirs),
				runtime.NumGoroutine(), stats.MaxQueueDirectories, atomic.LoadInt64(&stats.PeakQueuedDirs), final), true)
		}
		for {
			select {
			case <-statsCtx.Done():
				logStats(true)
				return
			case <-ticker.C:
				logStats(false)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func getEntityType(metadata map[string]*string) common.EntityType {
	// Note: We are just checking keys here, not their corresponding values. Is that safe?
	if folderValue, isFolder := common.TryReadMetadata(metadata, common.POSIXFolderMeta); isFolder && folderValue != nil && strings.ToLower(*folderValue) == "true" {
		return common.EEntityType.Folder()
	} else if symlinkValue, isSymlink := common.TryReadMetadata(metadata, common.POSIXSymlinkMeta); isSymlink && symlinkValue != nil && strings.ToLower(*symlinkValue) == "true" {
		return common.EEntityType.Symlink()
	}
	return common.EEntityType.File()
}

func (t *blobTraverser) createStoredObjectForBlob(preprocessor objectMorpher, blobInfo *container.BlobItem, relativePath string, containerName string) StoredObject {
	adapter := blobPropertiesAdapter{blobInfo.Properties}

	if azcopyScanningLogger != nil {
		azcopyScanningLogger.Log(common.LogDebug, fmt.Sprintf("Blob %s entity type: %s", relativePath, getEntityType(blobInfo.Metadata)))
	}

	object := newStoredObject(
		preprocessor,
		getObjectNameOnly(*blobInfo.Name),
		relativePath,
		getEntityType(blobInfo.Metadata),
		adapter.LastModified(),
		*adapter.BlobProperties.ContentLength,
		adapter,
		adapter, // adapter satisfies both interfaces
		blobInfo.Metadata,
		containerName,
	)
	object.tryUpdateTimestampsFromMetadata(blobInfo.Metadata)

	object.blobDeleted = ternary.IffNotNil(blobInfo.Deleted, false)
	if t.include.Deleted() && t.include.Snapshots() {
		object.blobSnapshotID = ternary.IffNotNil(blobInfo.Snapshot, "")
	} else if t.include.Versions() && blobInfo.VersionID != nil {
		object.blobVersionID = ternary.IffNotNil(blobInfo.VersionID, "")
	}
	return object
}

func (t *blobTraverser) doesBlobRepresentAFolder(metadata map[string]*string) bool {
	util := copyHandlerUtil{}
	return util.doesBlobRepresentAFolder(metadata) // We should ignore these, because we pick them up in other ways.
}

type flatListTimingKey struct{}

type flatListPoolKey struct{}

type flatListHTTPKey struct{}

var flatListDiagnosticSequence atomic.Uint64

type flatListHTTPValues struct {
	status                                                  int
	requestID, clientRequestID, contentType, version, proto string
	transportTime, connWait, dnsTime, tcpTime, tlsTime      time.Duration
	writeTime, ttfb, bodyRead, bodyWall                     time.Duration
	newConns, reusedConns, bodyReads                        int
	bodyBytes                                               int64
	transportFailed, bodyFailed, bodyEOF, bodyClosed        bool
}

type flatListHTTPMetrics struct {
	mu sync.Mutex
	flatListHTTPValues
	headersAt time.Time
}

func (m *flatListHTTPMetrics) update(f func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f()
}

func (m *flatListHTTPMetrics) snapshot() flatListHTTPValues {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flatListHTTPValues
}

func (m *flatListHTTPMetrics) trace() *httptrace.ClientTrace {
	var acquiring, acquired, dns, tlsStart, written, firstByte time.Time
	var connects map[[2]string]time.Time
	updateTTFB := func() {
		if !written.IsZero() && !firstByte.IsZero() && !firstByte.Before(written) {
			m.ttfb = firstByte.Sub(written)
		}
	}
	return &httptrace.ClientTrace{
		GetConn: func(string) {
			m.update(func() { acquiring = time.Now() })
		},
		GotConn: func(info httptrace.GotConnInfo) {
			m.update(func() {
				acquired = time.Now()
				if !acquiring.IsZero() {
					m.connWait += acquired.Sub(acquiring)
				}
				if info.Reused {
					m.reusedConns++
				} else {
					m.newConns++
				}
			})
		},
		DNSStart: func(httptrace.DNSStartInfo) {
			m.update(func() { dns = time.Now() })
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			m.update(func() {
				if !dns.IsZero() {
					m.dnsTime += time.Since(dns)
				}
			})
		},
		ConnectStart: func(network, addr string) {
			m.update(func() {
				if connects == nil {
					connects = make(map[[2]string]time.Time)
				}
				connects[[2]string{network, addr}] = time.Now()
			})
		},
		ConnectDone: func(network, addr string, _ error) {
			m.update(func() {
				key := [2]string{network, addr}
				if start, ok := connects[key]; ok {
					m.tcpTime += time.Since(start)
					delete(connects, key)
				}
			})
		},
		TLSHandshakeStart: func() {
			m.update(func() { tlsStart = time.Now() })
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
			m.update(func() {
				if !tlsStart.IsZero() {
					m.tlsTime += time.Since(tlsStart)
				}
			})
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			m.update(func() {
				if info.Err == nil {
					written = time.Now()
					if !acquired.IsZero() {
						m.writeTime += written.Sub(acquired)
					}
					updateTTFB()
				}
			})
		},
		GotFirstResponseByte: func() {
			m.update(func() {
				firstByte = time.Now()
				updateTTFB()
			})
		},
	}
}

// Only requests carrying a flat-page context use the isolated pool; STE and other calls keep theirs.
type flatListPoolTransport struct {
	shared  *http.Client
	once    sync.Once
	listing *http.Client
	err     error
}

func (t *flatListPoolTransport) Do(req *http.Request) (*http.Response, error) {
	if req.Context().Value(flatListPoolKey{}) == nil {
		return t.shared.Do(req)
	}
	t.once.Do(func() {
		t.listing, t.err = common.NewIsolatedHTTPClient(t.shared, 4)
		if t.err == nil {
			syncOrchestratorLog(common.LogInfo, "[FlatListPool] mode=dedicated pools=1 max_connections_per_host=4", true)
		}
	})
	if t.err != nil {
		return nil, t.err
	}
	return t.listing.Do(req)
}

// Delegate to the selected pooled client; wrap before SDK buffering or streaming decode.
type flatListDiagnosticTransport struct{ inner policy.Transporter }

func (t flatListDiagnosticTransport) Do(req *http.Request) (*http.Response, error) {
	m, _ := req.Context().Value(flatListHTTPKey{}).(*flatListHTTPMetrics)
	if m == nil {
		return t.inner.Do(req)
	}
	start := time.Now()
	resp, err := t.inner.Do(req.WithContext(httptrace.WithClientTrace(req.Context(), m.trace())))
	m.update(func() {
		m.headersAt = time.Now()
		m.transportTime = m.headersAt.Sub(start)
		m.transportFailed = err != nil
		m.clientRequestID = req.Header.Get("x-ms-client-request-id")
		if resp != nil {
			m.status = resp.StatusCode
			m.requestID = resp.Header.Get("x-ms-request-id")
			m.contentType = resp.Header.Get("Content-Type")
			m.version = resp.Header.Get("x-ms-version")
			m.proto = resp.Proto
		}
	})
	if resp != nil && resp.Body != nil {
		resp.Body = &flatListDiagnosticBody{ReadCloser: resp.Body, metrics: m}
	}
	return resp, err
}

type flatListDiagnosticBody struct {
	io.ReadCloser
	metrics *flatListHTTPMetrics
}

func (b *flatListDiagnosticBody) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := b.ReadCloser.Read(p)
	end := time.Now()
	b.metrics.update(func() {
		b.metrics.bodyRead += end.Sub(start)
		b.metrics.bodyWall = end.Sub(b.metrics.headersAt)
		b.metrics.bodyBytes += int64(n)
		b.metrics.bodyReads++
		b.metrics.bodyEOF = b.metrics.bodyEOF || err == io.EOF
		b.metrics.bodyFailed = b.metrics.bodyFailed || (err != nil && err != io.EOF)
	})
	return n, err
}

func (b *flatListDiagnosticBody) Close() error {
	err := b.ReadCloser.Close()
	b.metrics.update(func() {
		b.metrics.bodyClosed = true
		b.metrics.bodyFailed = b.metrics.bodyFailed || err != nil
	})
	return err
}

type flatListPageTiming struct {
	attempts, throttles                     int
	attemptTime, retryGap                   time.Duration
	lastAttemptEnd                          time.Time
	details                                 bool
	http                                    []*flatListHTTPMetrics
	started                                 time.Time
	nextTime, processingTime, processorTime time.Duration
	waitTime, handoffTime                   time.Duration
	blobs                                   int
}

type flatListPage struct {
	response container.ListBlobsFlatResponse
	timing   flatListPageTiming
	err      error
	number   int
}

type flatListTimingPolicy struct{}

func (flatListTimingPolicy) Do(req *policy.Request) (*http.Response, error) {
	timing, _ := req.Raw().Context().Value(flatListTimingKey{}).(*flatListPageTiming)
	if timing == nil {
		return req.Next()
	}
	start := time.Now()
	if !timing.lastAttemptEnd.IsZero() {
		timing.retryGap += start.Sub(timing.lastAttemptEnd)
	}
	timing.attempts++
	if timing.details {
		m := &flatListHTTPMetrics{}
		timing.http = append(timing.http, m)
		req = req.WithContext(context.WithValue(req.Raw().Context(), flatListHTTPKey{}, m))
	}
	resp, err := req.Next()
	timing.lastAttemptEnd = time.Now()
	// XML buffers here; Arrow requests stream bodies later, including XML fallback.
	timing.attemptTime += timing.lastAttemptEnd.Sub(start)
	if resp != nil && (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable) {
		timing.throttles++
	}
	return resp, err
}

func flatListDiagnosticsEnabled() bool {
	return buildmode.HighPerf() && (strings.EqualFold(os.Getenv("MOVER_FLAT_LIST_DIAGNOSTICS"), "true") ||
		flatListPageDiagnosticsEnabled())
}

// Detailed logs emit one entry per HTTP attempt and page, in addition to summaries.
func flatListPageDiagnosticsEnabled() bool {
	return buildmode.HighPerf() && strings.EqualFold(os.Getenv("MOVER_FLAT_LIST_PAGE_DIAGNOSTICS"), "true")
}

func flatListMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func (p *flatListPageTiming) logDetails(listing uint64, page int, destination bool, stage string, failed bool) time.Duration {
	if !p.details {
		return 0
	}
	start := time.Now()
	// With prefetch this is page residence time, not the interval between completed pages.
	loopTime := start.Sub(p.started)
	var transportTime, bodyRead time.Duration
	observed := 0
	for i, m := range p.http {
		v := m.snapshot()
		transportTime += v.transportTime
		bodyRead += v.bodyRead
		if v.transportTime > 0 {
			observed++
		}
		// Connection/DNS/TCP/TLS and body_wall are inclusive spans, not additive buckets.
		syncOrchestratorLog(common.LogInfo, fmt.Sprintf(
			"[FlatListHTTP] listing=%d page=%d attempt=%d status=%d request_id=%q client_request_id=%q content_type=%q service_version=%q protocol=%q new_connections=%d reused_connections=%d transport_headers_ms=%.3f conn_acquire_ms=%.3f dns_ms=%.3f tcp_ms=%.3f tls_ms=%.3f request_write_ms=%.3f ttfb_ms=%.3f body_bytes=%d body_reads=%d body_read_ms=%.3f body_wall_ms=%.3f body_eof=%t body_closed=%t transport_failed=%t body_failed=%t",
			listing, page, i+1, v.status, v.requestID, v.clientRequestID, v.contentType, v.version, v.proto,
			v.newConns, v.reusedConns, flatListMillis(v.transportTime), flatListMillis(v.connWait),
			flatListMillis(v.dnsTime), flatListMillis(v.tcpTime), flatListMillis(v.tlsTime),
			flatListMillis(v.writeTime), flatListMillis(v.ttfb), v.bodyBytes, v.bodyReads,
			flatListMillis(v.bodyRead), flatListMillis(v.bodyWall), v.bodyEOF, v.bodyClosed, v.transportFailed, v.bodyFailed), true)
	}
	// Residual includes SDK decode/mapping, policy work, allocation/GC and instrumentation, not pure decode CPU.
	other := p.nextTime - p.retryGap - transportTime - bodyRead
	syncOrchestratorLog(common.LogInfo, fmt.Sprintf(
		"[FlatListPage] listing=%d page=%d destination=%t stage=%q failed=%t blobs=%d attempts=%d observed_http_attempts=%d throttles=%d next_ms=%.3f transport_headers_ms=%.3f body_read_ms=%.3f between_attempt_ms=%.3f next_other_ms=%.3f processing_ms=%.3f processor_ms=%.3f loop_before_log_ms=%.3f page_wait_ms=%.3f handoff_ms=%.3f",
		listing, page, destination, stage, failed, p.blobs, p.attempts, observed, p.throttles,
		flatListMillis(p.nextTime), flatListMillis(transportTime), flatListMillis(bodyRead), flatListMillis(p.retryGap),
		flatListMillis(other), flatListMillis(p.processingTime), flatListMillis(p.processorTime), flatListMillis(loopTime),
		flatListMillis(p.waitTime), flatListMillis(p.handoffTime)), true)
	return time.Since(start)
}

type flatListTimingStats struct {
	started, lastLog                                    time.Time
	pages, blobs, minPage, maxPage, attempts, throttles int
	arrowPages, xmlPages                                int
	nextTime, attemptTime, retryGap, processingTime     time.Duration
	detailLogTime                                       time.Duration
	waitTime                                            time.Duration
	prefetch                                            bool
	discardedPages                                      int
}

func (s *flatListTimingStats) log(destination, final bool) {
	now := time.Now()
	if !final && now.Sub(s.lastLog) < 30*time.Second {
		return
	}
	s.lastLog = now
	syncOrchestratorLog(common.LogInfo, fmt.Sprintf(
		"[FlatListTiming] destination=%t final=%t pages=%d blobs=%d minPage=%d maxPage=%d attempts=%d throttles=%d elapsed_ms=%d next_ms=%d attempt_ms=%d between_attempt_ms=%d outside_attempt_ms=%d processing_ms=%d arrow_pages=%d xml_pages=%d detail_log_ms=%d prefetch=%t page_wait_ms=%d discarded_pages=%d",
		destination, final, s.pages, s.blobs, s.minPage, s.maxPage, s.attempts, s.throttles,
		now.Sub(s.started).Milliseconds(), s.nextTime.Milliseconds(), s.attemptTime.Milliseconds(),
		s.retryGap.Milliseconds(), (s.nextTime-s.attemptTime-s.retryGap).Milliseconds(), s.processingTime.Milliseconds(),
		s.arrowPages, s.xmlPages, s.detailLogTime.Milliseconds(), s.prefetch, s.waitTime.Milliseconds(), s.discardedPages), true)
}

func (t *blobTraverser) serialList(containerClient *container.Client, containerName string, searchPrefix string,
	extraSearchPrefix string, preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error {

	diagnostics := t.flatListDiagnostics
	pageDiagnostics := diagnostics && flatListPageDiagnosticsEnabled()
	var listing uint64
	if pageDiagnostics {
		listing = flatListDiagnosticSequence.Add(1)
	}
	prefetch := t.flatListArrow
	timings := flatListTimingStats{prefetch: prefetch}
	if diagnostics {
		timings.started = time.Now()
		timings.lastLog = timings.started
		defer timings.log(t.isSyncDestination, true)
	}
	// see the TO DO in GetEnumerationPreFilter if/when we make this more directory-aware
	// TODO optimize for the case where recursive is off
	prefix := searchPrefix + extraSearchPrefix
	responseFormat := container.StorageResponseFormatAuto
	listCtx := t.ctx
	if buildmode.HighPerf() {
		listCtx = context.WithValue(listCtx, flatListPoolKey{}, struct{}{})
	}
	if t.flatListArrow {
		responseFormat = container.StorageResponseFormatArrow
		// Scope the newer contract to listing; the shared client and transfer context stay unchanged.
		listCtx = context.WithValue(listCtx, ste.ServiceAPIVersionOverride, "2026-06-06")
	}
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix:         &prefix,
		Include:        container.ListBlobsInclude{Metadata: true, Tags: t.s2sPreserveSourceTags, Deleted: t.include.Deleted(), Snapshots: t.include.Snapshots(), Versions: t.include.Versions()},
		ResponseFormat: responseFormat,
	})
	pageNumber := 0
	fetchPage := func(ctx context.Context) (flatListPage, bool) {
		if !pager.More() {
			return flatListPage{}, false
		}
		pageNumber++
		page := flatListPage{number: pageNumber, timing: flatListPageTiming{details: pageDiagnostics}}
		if diagnostics {
			ctx = context.WithValue(ctx, flatListTimingKey{}, &page.timing)
			page.timing.started = time.Now()
		}
		page.response, page.err = pager.NextPage(ctx)
		if diagnostics {
			page.timing.nextTime = time.Since(page.timing.started)
		}
		return page, true
	}
	recordFetch := func(page *flatListPage) {
		timings.nextTime += page.timing.nextTime
		timings.attempts += page.timing.attempts
		timings.throttles += page.timing.throttles
		timings.attemptTime += page.timing.attemptTime
		timings.retryGap += page.timing.retryGap
	}
	nextPage := func() (flatListPage, bool) { return fetchPage(listCtx) }
	if prefetch {
		ctx, cancel := context.WithCancel(listCtx)
		pages := make(chan flatListPage)
		// The unbuffered handoff permits only one page ahead, including in-flight work.
		// Only this goroutine accesses the pager; the consumer retains ordered processing.
		go func() {
			defer close(pages)
			for ctx.Err() == nil {
				page, more := fetchPage(ctx)
				if !more {
					return
				}
				pages <- page
				if page.err != nil {
					return
				}
			}
		}()
		nextPage = func() (flatListPage, bool) {
			page, more := <-pages
			return page, more
		}
		defer func() {
			cancel()
			// Drain and join even on processor failure, preserving metrics for unused prefetches.
			for page := range pages {
				if diagnostics {
					recordFetch(&page)
					timings.discardedPages++
					timings.detailLogTime += page.timing.logDetails(listing, page.number, t.isSyncDestination, "prefetch-discarded", page.err != nil)
				}
			}
		}()
		syncOrchestratorLog(common.LogInfo, "[FlatListPrefetch] enabled=true max_pages_ahead=1", true)
	}
	lastContentType := "<not received>"
	for {
		if prefetch && t.ctx.Err() != nil {
			return t.ctx.Err()
		}
		var waitStart time.Time
		if diagnostics {
			waitStart = time.Now()
		}
		page, more := nextPage()
		if !more {
			break
		}
		pageTiming := &page.timing
		if diagnostics {
			pageTiming.waitTime = time.Since(waitStart)
			if prefetch {
				pageTiming.handoffTime = time.Since(pageTiming.started) - pageTiming.nextTime
			}
			timings.waitTime += pageTiming.waitTime
			recordFetch(&page)
		}
		resp, err := page.response, page.err
		if err != nil {
			timings.detailLogTime += pageTiming.logDetails(listing, page.number, t.isSyncDestination, "next", true)
			return fmt.Errorf("cannot list blobs. Failed with error %w", err)
		}
		contentType := ternary.DerefOrZero(resp.ContentType)
		if t.flatListArrow && contentType != lastContentType {
			syncOrchestratorLog(common.LogInfo, fmt.Sprintf(
				"[FlatListFormat] destination=%t requested=Arrow content_type=%q service_version=%q",
				t.isSyncDestination, contentType, ternary.DerefOrZero(resp.Version)), true)
			lastContentType = contentType
		}
		var processingStart time.Time
		if diagnostics {
			if strings.HasPrefix(contentType, "application/vnd.apache.arrow.stream") {
				timings.arrowPages++
			} else if strings.HasPrefix(contentType, "application/xml") || strings.HasPrefix(contentType, "text/xml") {
				timings.xmlPages++
			}
			processingStart = time.Now()
			count := len(resp.Segment.BlobItems)
			pageTiming.blobs = count
			timings.pages++
			timings.blobs += count
			if timings.pages == 1 || count < timings.minPage {
				timings.minPage = count
			}
			if count > timings.maxPage {
				timings.maxPage = count
			}
		}
		pageProcessor := processor
		if pageDiagnostics {
			pageProcessor = func(object StoredObject) error {
				start := time.Now()
				err := processor(object)
				pageTiming.processorTime += time.Since(start)
				return err
			}
		}
		// process the blobs returned in this result segment
		for _, blobInfo := range resp.Segment.BlobItems {
			if prefetch && t.ctx.Err() != nil {
				if diagnostics {
					pageTiming.processingTime = time.Since(processingStart)
					timings.processingTime += pageTiming.processingTime
				}
				timings.detailLogTime += pageTiming.logDetails(listing, page.number, t.isSyncDestination, "processing", true)
				return t.ctx.Err()
			}
			// if the blob represents a hdi folder, then skip it
			if t.doesBlobRepresentAFolder(blobInfo.Metadata) {
				continue
			}

			relativePath := strings.TrimPrefix(*blobInfo.Name, searchPrefix)
			// if recursive
			if !t.recursive && strings.Contains(relativePath, common.AZCOPY_PATH_SEPARATOR_STRING) {
				continue
			}

			storedObject := t.createStoredObjectForBlob(preprocessor, blobInfo, relativePath, containerName)

			// edge case, blob name happens to be the same as root and ends in /
			if storedObject.relativePath == "" && strings.HasSuffix(storedObject.name, "/") {
				storedObject.relativePath = "\x00" // Short circuit, letting the backend know we *really* meant root/.
			}

			// Setting blob tags
			if t.s2sPreserveSourceTags && blobInfo.BlobTags != nil {
				blobTagsMap := common.BlobTags{}
				for _, blobTag := range blobInfo.BlobTags.BlobTagSet {
					blobTagsMap[url.QueryEscape(*blobTag.Key)] = url.QueryEscape(*blobTag.Value)
				}
				storedObject.blobTags = blobTagsMap
			}

			if t.incrementEnumerationCounter != nil {
				t.incrementEnumerationCounter(common.EEntityType.File())
			}

			processErr := processIfPassedFilters(filters, storedObject, pageProcessor)
			_, processErr = getProcessingError(processErr)
			if processErr != nil {
				if diagnostics {
					pageTiming.processingTime = time.Since(processingStart)
					timings.processingTime += pageTiming.processingTime
				}
				timings.detailLogTime += pageTiming.logDetails(listing, page.number, t.isSyncDestination, "processing", true)
				return processErr
			}
		}
		if diagnostics {
			pageTiming.processingTime = time.Since(processingStart)
			timings.processingTime += pageTiming.processingTime
			timings.detailLogTime += pageTiming.logDetails(listing, page.number, t.isSyncDestination, "complete", false)
			timings.log(t.isSyncDestination, false)
		}
	}

	if prefetch {
		return t.ctx.Err()
	}
	return nil
}

type BlobTraverserOptions struct {
	isDFS *bool
}

func newBlobTraverser(rawURL string, serviceClient *service.Client, ctx context.Context, opts InitResourceTraverserOptions, blobOpts ...BlobTraverserOptions) (t *blobTraverser) {
	t = &blobTraverser{
		rawURL:                      rawURL,
		serviceClient:               serviceClient,
		ctx:                         ctx,
		recursive:                   opts.Recursive,
		include:                     common.EBlobTraverserIncludeOption.FromInputs(opts.PermanentDelete, opts.ListVersions, opts.IncludeDirectoryStubs),
		incrementEnumerationCounter: opts.IncrementEnumeration,
		parallelListing:             true,
		flatListDiagnostics:         opts.FlatListing && flatListDiagnosticsEnabled(),
		s2sPreserveSourceTags:       opts.PreserveBlobTags,
		cpkOptions:                  opts.CpkOptions,
		preservePermissions:         opts.PreservePermissions,
		isDFS:                       ternary.DerefOrZero(ternary.FirstOrZero(blobOpts).isDFS),
		destResourceType:            opts.DestResourceType,
		errorChannel:                opts.ErrorChannel,
		isSyncDestination:           opts.IsSyncDestination,
		suppressCrawlStats:          opts.SuppressCrawlStats,
	}

	t.includeDirectoryOrPrefix = UseSyncOrchestrator && !t.recursive

	disableHierarchicalScanning := strings.ToLower(enum.EEnvironmentVariable.DisableHierarchicalScanning().Get())

	// disableHierarchicalScanning should be true for permanent delete
	if (disableHierarchicalScanning == "false" || disableHierarchicalScanning == "") && t.include.Deleted() && (t.include.Snapshots() || t.include.Versions()) {
		t.parallelListing = false
		fmt.Println("AZCOPY_DISABLE_HIERARCHICAL_SCAN has been set to true to permanently delete soft-deleted snapshots/versions.")
	}

	if disableHierarchicalScanning == "true" {
		// TODO log to frontend log that parallel listing was disabled, once the frontend log PR is merged
		t.parallelListing = false
	}
	if opts.FlatListing && !t.isDFS && opts.DestResourceType != nil && *opts.DestResourceType == common.ELocation.Blob() {
		t.parallelListing = false
		t.flatListArrow = true
	}
	return
}

func createBlobClientFromServiceClient(blobURLParts blob.URLParts, client *service.Client) (*blob.Client, error) {
	containerClient := client.NewContainerClient(blobURLParts.ContainerName)
	blobClient := containerClient.NewBlobClient(blobURLParts.BlobName)
	if blobURLParts.Snapshot != "" {
		return blobClient.WithSnapshot(blobURLParts.Snapshot)
	}
	if blobURLParts.VersionID != "" {
		return blobClient.WithVersionID(blobURLParts.VersionID)
	}
	return blobClient, nil
}
