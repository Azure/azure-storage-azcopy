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
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"

	"github.com/pkg/errors"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// allow us to iterate through a path pointing to the blob endpoint
type BlobTraverser struct {
	RawURL        string
	ServiceClient *service.Client
	ctx           context.Context
	recursive     bool

	// parallel listing employs the hierarchical listing API which is more expensive
	// cx should have the option to disable this optimization for the sake of saving costs
	ParallelListing bool

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc

	s2sPreserveSourceTags bool

	cpkOptions common.CpkOptions

	preservePermissions common.PreservePermissionsOption

	include common.BlobTraverserIncludeOption

	isDFS bool

	includeRoot bool
}

var NonErrorDirectoryStubOverlappable = errors.New("The directory stub exists, and can overlap.")

func (t *BlobTraverser) IsDirectory(isSource bool) (isDirectory bool, err error) {
	isDirDirect := UrlIsContainerOrVirtualDirectory(t.RawURL)

	blobURLParts, err := blob.ParseURL(t.RawURL)
	if err != nil {
		return false, err
	}

	// Skip the single blob check if we're checking a destination.
	// This is an individual exception for blob because blob supports virtual directories and blobs sharing the same name.
	// On HNS accounts, we would still perform this test. The user may have provided directory name without path-separator
	if isDirDirect { // a container or a path ending in '/' is always directory
		if blobURLParts.ContainerName != "" && blobURLParts.BlobName == "" {
			// If it's a container, let's ensure that container exists. Listing is a safe assumption to be valid, because how else would we enumerate?
			containerClient := t.ServiceClient.NewContainerClient(blobURLParts.ContainerName)
			p := containerClient.NewListBlobsFlatPager(nil)
			_, err = p.NextPage(t.ctx)

			if bloberror.HasCode(err, bloberror.AuthorizationPermissionMismatch) {
				// Maybe we don't have the ability to list? Can we get container properties as a fallback?
				_, propErr := containerClient.GetProperties(t.ctx, nil)
				err = common.Iff(propErr == nil, nil, err)
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

	containerClient := t.ServiceClient.NewContainerClient(blobURLParts.ContainerName)
	searchPrefix := strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING) + common.AZCOPY_PATH_SEPARATOR_STRING
	maxResults := int32(1)
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &searchPrefix, MaxResults: &maxResults})
	resp, err := pager.NextPage(t.ctx)
	if err != nil {
		if common.AzcopyScanningLogger != nil {
			msg := fmt.Sprintf("Failed to check if the destination is a folder or a file (Azure Files). Assuming the destination is a file: %s", err)
			common.AzcopyScanningLogger.Log(common.LogError, msg)
		}
		return false, nil
	}

	if len(resp.Segment.BlobItems) == 0 {
		// Not a directory, but there was also no file on site. Therefore, there's nothing.
		return false, errors.New(common.FILE_NOT_FOUND)
	}

	return true, nil
}

func (t *BlobTraverser) getPropertiesIfSingleBlob() (response *blob.GetPropertiesResponse, isBlob bool, isDirStub bool, blobName string, err error) {
	// trim away the trailing slash before we check whether it's a single blob
	// so that we can detect the directory stub in case there is one
	blobURLParts, err := blob.ParseURL(t.RawURL)
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
	blobClient, err := createBlobClientFromServiceClient(blobURLParts, t.ServiceClient)
	if err != nil {
		return nil, false, false, blobURLParts.BlobName, err
	}
	cpkInfo, err := t.cpkOptions.GetCPKInfo()
	if err != nil {
		return nil, false, false, blobURLParts.BlobName, err
	}
	props, err := blobClient.GetProperties(t.ctx, &blob.GetPropertiesOptions{CPKInfo: cpkInfo})

	if err != nil && strings.HasSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING) {
		// Trim & retry, maybe the directory stub is DFS style.
		blobURLParts.BlobName = strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)
		goto retry
	} else if err == nil {
		// We found the target blob, great! Let's return the details.
		isDir := DoesBlobRepresentAFolder(props.Metadata)
		return &props, !isDir, isDir, blobURLParts.BlobName, nil
	}

	// We found nothing.
	return nil, false, false, "", err
}

func (t *BlobTraverser) getBlobTags() (common.BlobTags, error) {
	blobURLParts, err := blob.ParseURL(t.RawURL)
	if err != nil {
		return nil, err
	}
	blobURLParts.BlobName = strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)

	// perform the check
	blobClient, err := createBlobClientFromServiceClient(blobURLParts, t.ServiceClient)
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

func (t *BlobTraverser) Traverse(preprocessor objectMorpher, processor ObjectProcessor, filters []ObjectFilter) (err error) {
	blobURLParts, err := blob.ParseURL(t.RawURL)
	if err != nil {
		return err
	}

	// check if the url points to a single blob
	blobProperties, isBlob, isDirStub, blobName, err := t.getPropertiesIfSingleBlob()

	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		// Don't error out unless it's a CPK error just yet
		// If it's a CPK error, we know it's a single blob and that we can't get the properties on it anyway.
		if respErr.ErrorCode == string(bloberror.BlobUsesCustomerSpecifiedEncryption) {
			return errors.New("this blob uses customer provided encryption keys (CPK). At the moment, AzCopy does not support CPK-encrypted blobs. " +
				"If you wish to make use of this blob, we recommend using one of the Azure Storage SDKs")
		}
		if respErr.RawResponse == nil {
			return fmt.Errorf("cannot list files due to reason %w", respErr)
		} else if respErr.StatusCode == 403 { // Some nature of auth error-- Whatever the user is pointing at, they don't have access to, regardless of whether it's a file or a dir stub.
			return fmt.Errorf("cannot list files due to reason %s", respErr)
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

		if common.AzcopyScanningLogger != nil {
			common.AzcopyScanningLogger.Log(common.LogDebug, "Detected the root as a blob.")
			common.AzcopyScanningLogger.Log(common.LogDebug, fmt.Sprintf("Root entity type: %s", GetEntityType(blobProperties.Metadata)))
		}

		relPath := ""
		if strings.HasSuffix(blobName, "/") {
			relPath = "\x00" // Because the ste will trim the / suffix from our source, or we may not already have it.
		}

		blobPropsAdapter := BlobPropertiesResponseAdapter{blobProperties}
		storedObject := NewStoredObject(
			preprocessor,
			getObjectNameOnly(blobName),
			relPath,
			GetEntityType(blobPropsAdapter.Metadata),
			blobPropsAdapter.LastModified(),
			blobPropsAdapter.ContentLength(),
			blobPropsAdapter,
			blobPropsAdapter,
			blobPropsAdapter.Metadata,
			blobURLParts.ContainerName,
		)

		if t.s2sPreserveSourceTags {
			blobTagsMap, err := t.getBlobTags()
			if err != nil {
				panic("Couldn't fetch blob tags due to error: " + err.Error())
			}
			if len(blobTagsMap) > 0 {
				storedObject.BlobTags = blobTagsMap
			}
		}
		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(storedObject.EntityType, common.SymlinkHandlingType(0), common.DefaultHardlinkHandlingType)
		}

		err := ProcessIfPassedFilters(filters, storedObject, processor)
		_, err = getProcessingError(err)

		// short-circuit if we don't have anything else to scan and permanent delete is not on
		if !t.include.Deleted() && (isBlob || err != nil) {
			return err
		}
	} else if blobURLParts.BlobName == "" && (t.preservePermissions.IsTruthy() || t.isDFS) {
		// If the root is a container and we're copying "folders", we should persist the ACLs there too.
		// For DFS, we should always include the container root.
		if common.AzcopyScanningLogger != nil {
			common.AzcopyScanningLogger.Log(common.LogDebug, "Detected the root as a container.")
		}

		storedObject := NewStoredObject(
			preprocessor,
			"",
			"",
			common.EEntityType.Folder(),
			time.Now(),
			0,
			NoContentProps,
			NoBlobProps,
			common.Metadata{},
			blobURLParts.ContainerName,
		)

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.Folder(), common.SymlinkHandlingType(0), common.DefaultHardlinkHandlingType)
		}

		err := ProcessIfPassedFilters(filters, storedObject, processor)
		_, err = getProcessingError(err)
		if err != nil {
			return err
		}
	} else if blobURLParts.BlobName != "" && isDirStub && t.isDFS && t.includeRoot {
		// Handle enumerating folder roots for BlobFS (HNS enabled only)
		var dirName string
		if strings.HasSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING) {
			dirName = strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)
		}
		if common.AzcopyScanningLogger != nil {
			common.AzcopyScanningLogger.Log(common.LogDebug, fmt.Sprintf("Detected the root as a folder %s.", dirName))
		}

		// Use props from previous call to create the root object
		if blobProperties != nil && DoesBlobRepresentAFolder(blobProperties.Metadata) {
			dirPropsAdapter := BlobPropertiesResponseAdapter{blobProperties}
			storedObject := NewStoredObject(
				preprocessor,
				"", // empty for root
				"", // empty for root
				common.EEntityType.Folder(),
				dirPropsAdapter.LastModified(),
				0, // folders have no size
				dirPropsAdapter,
				dirPropsAdapter,
				dirPropsAdapter.Metadata,
				blobURLParts.ContainerName,
			)

			if t.incrementEnumerationCounter != nil {
				t.incrementEnumerationCounter(common.EEntityType.Folder(),
					common.SymlinkHandlingType(0), common.DefaultHardlinkHandlingType)
			}

			err = ProcessIfPassedFilters(filters, storedObject, processor)
			_, err = getProcessingError(err)
			if err != nil {
				return err
			}
		}
	}

	// get the container URL so that we can list the blobs
	containerClient := t.ServiceClient.NewContainerClient(blobURLParts.ContainerName)

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

	if t.ParallelListing {
		return t.parallelList(containerClient, blobURLParts.ContainerName, searchPrefix, extraSearchPrefix, preprocessor, processor, filters)
	}

	return t.serialList(containerClient, blobURLParts.ContainerName, searchPrefix, extraSearchPrefix, preprocessor, processor, filters)
}

func (t *BlobTraverser) parallelList(containerClient *container.Client, containerName string, searchPrefix string,
	extraSearchPrefix string, preprocessor objectMorpher, processor ObjectProcessor, filters []ObjectFilter) error {
	// Define how to enumerate its contents
	// This func must be thread safe/goroutine safe
	enumerateOneDir := func(dir parallel.Directory, enqueueDir func(parallel.Directory), enqueueOutput func(parallel.DirectoryEntry, error)) error {
		currentDirPath := dir.(string)

		pager := containerClient.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
			Prefix:  &currentDirPath,
			Include: container.ListBlobsInclude{Metadata: true, Tags: t.s2sPreserveSourceTags, Deleted: t.include.Deleted(), Snapshots: t.include.Snapshots(), Versions: t.include.Versions()},
		})
		var marker *string
		for pager.More() {
			lResp, err := pager.NextPage(t.ctx)
			if err != nil {
				return fmt.Errorf("cannot list files due to reason %w", err)
			}
			// queue up the sub virtual directories if recursive is true
			if t.recursive {
				for _, virtualDir := range lResp.Segment.BlobPrefixes {
					enqueueDir(*virtualDir.Name)
					if common.AzcopyScanningLogger != nil {
						common.AzcopyScanningLogger.Log(common.LogDebug, fmt.Sprintf("Enqueuing sub-directory %s for enumeration.", *virtualDir.Name))
					}

					if t.include.DirStubs() {
						// try to get properties on the directory itself, since it's not listed in BlobItems
						dName := strings.TrimSuffix(*virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING)
						blobClient := containerClient.NewBlobClient(dName)
					altNameCheck:
						pResp, err := blobClient.GetProperties(t.ctx, nil)
						if err == nil {
							if !DoesBlobRepresentAFolder(pResp.Metadata) { // We've picked up on a file *named* the folder, not the folder itself. Does folder/ exist?
								if !strings.HasSuffix(dName, "/") {
									blobClient = containerClient.NewBlobClient(dName + common.AZCOPY_PATH_SEPARATOR_STRING) // Tack on the path separator, check.
									dName += common.AZCOPY_PATH_SEPARATOR_STRING
									goto altNameCheck // "foo" is a file, what about "foo/"?
								}

								goto skipDirAdd // We shouldn't add a blob that isn't a folder as a folder. You either have the folder metadata, or you don't.
							}

							pbPropAdapter := BlobPropertiesResponseAdapter{&pResp}
							folderRelativePath := strings.TrimPrefix(dName, searchPrefix)

							storedObject := NewStoredObject(
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

							if t.s2sPreserveSourceTags {
								tResp, err := blobClient.GetTags(t.ctx, nil)

								if err == nil {
									blobTagsMap := common.BlobTags{}
									for _, blobTag := range tResp.BlobTagSet {
										blobTagsMap[url.QueryEscape(*blobTag.Key)] = url.QueryEscape(*blobTag.Value)
									}
									storedObject.BlobTags = blobTagsMap
								}
							}

							enqueueOutput(storedObject, err)
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
				}
			}

			// process the blobs returned in this result segment
			for _, blobInfo := range lResp.Segment.BlobItems {
				// if the blob represents a hdi folder, then skip it
				if DoesBlobRepresentAFolder(blobInfo.Metadata) {
					continue
				}

				storedObject := t.createStoredObjectForBlob(preprocessor, blobInfo, strings.TrimPrefix(*blobInfo.Name, searchPrefix), containerName)

				// edge case, blob name happens to be the same as root and ends in /
				if storedObject.RelativePath == "" && strings.HasSuffix(storedObject.Name, "/") {
					storedObject.RelativePath = "\x00" // Short circuit, letting the backend know we *really* meant root/.
				}

				if t.s2sPreserveSourceTags && blobInfo.BlobTags != nil {
					blobTagsMap := common.BlobTags{}
					for _, blobTag := range blobInfo.BlobTags.BlobTagSet {
						blobTagsMap[url.QueryEscape(*blobTag.Key)] = url.QueryEscape(*blobTag.Value)
					}
					storedObject.BlobTags = blobTagsMap
				}

				enqueueOutput(storedObject, nil)
			}

			// if debug mode is on, note down the result, this is not going to be fast
			if common.AzcopyScanningLogger != nil && common.AzcopyScanningLogger.ShouldLog(common.LogDebug) {
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
				common.AzcopyScanningLogger.Log(common.LogDebug, msg)
			}
			marker = lResp.NextMarker
		}
		return nil
	}

	// initiate parallel scanning, starting at the root path
	workerContext, cancelWorkers := context.WithCancel(t.ctx)
	defer cancelWorkers()
	cCrawled := parallel.Crawl(workerContext, searchPrefix+extraSearchPrefix, enumerateOneDir, EnumerationParallelism)

	for x := range cCrawled {
		item, workerError := x.Item()
		if workerError != nil {
			return workerError
		}

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.File(), common.SymlinkHandlingType(0), common.DefaultHardlinkHandlingType)
		}

		object := item.(StoredObject)
		processErr := ProcessIfPassedFilters(filters, object, processor)
		_, processErr = getProcessingError(processErr)
		if processErr != nil {
			return processErr
		}
	}

	return nil
}

func GetEntityType(metadata map[string]*string) common.EntityType {
	// Note: We are just checking keys here, not their corresponding values. Is that safe?
	if folderValue, isFolder := common.TryReadMetadata(metadata, common.POSIXFolderMeta); isFolder && folderValue != nil && strings.ToLower(*folderValue) == "true" {
		return common.EEntityType.Folder()
	} else if symlinkValue, isSymlink := common.TryReadMetadata(metadata, common.POSIXSymlinkMeta); isSymlink && symlinkValue != nil && strings.ToLower(*symlinkValue) == "true" {
		return common.EEntityType.Symlink()
	}
	return common.EEntityType.File()
}

func (t *BlobTraverser) createStoredObjectForBlob(preprocessor objectMorpher, blobInfo *container.BlobItem, relativePath string, containerName string) StoredObject {
	adapter := blobPropertiesAdapter{blobInfo.Properties}

	if common.AzcopyScanningLogger != nil {
		common.AzcopyScanningLogger.Log(common.LogDebug, fmt.Sprintf("Blob %s entity type: %s", relativePath, GetEntityType(blobInfo.Metadata)))
	}

	object := NewStoredObject(
		preprocessor,
		getObjectNameOnly(*blobInfo.Name),
		relativePath,
		GetEntityType(blobInfo.Metadata),
		adapter.LastModified(),
		*adapter.BlobProperties.ContentLength,
		adapter,
		adapter, // adapter satisfies both interfaces
		blobInfo.Metadata,
		containerName,
	)

	object.blobDeleted = common.IffNotNil(blobInfo.Deleted, false)
	if t.include.Deleted() && t.include.Snapshots() {
		object.BlobSnapshotID = common.IffNotNil(blobInfo.Snapshot, "")
	} else if t.include.Versions() && blobInfo.VersionID != nil {
		object.BlobVersionID = common.IffNotNil(blobInfo.VersionID, "")
	}
	return object
}

func (t *BlobTraverser) serialList(containerClient *container.Client, containerName string, searchPrefix string,
	extraSearchPrefix string, preprocessor objectMorpher, processor ObjectProcessor, filters []ObjectFilter) error {

	// see the TO DO in GetEnumerationPreFilter if/when we make this more directory-aware
	// TODO optimize for the case where recursive is off
	prefix := searchPrefix + extraSearchPrefix
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: container.ListBlobsInclude{Metadata: true, Tags: t.s2sPreserveSourceTags, Deleted: t.include.Deleted(), Snapshots: t.include.Snapshots(), Versions: t.include.Versions()},
	})
	for pager.More() {
		resp, err := pager.NextPage(t.ctx)
		if err != nil {
			return fmt.Errorf("cannot list blobs. Failed with error %w", err)
		}
		// process the blobs returned in this result segment
		for _, blobInfo := range resp.Segment.BlobItems {
			// if the blob represents a hdi folder, then skip it
			if DoesBlobRepresentAFolder(blobInfo.Metadata) {
				continue
			}

			relativePath := strings.TrimPrefix(*blobInfo.Name, searchPrefix)
			// if recursive
			if !t.recursive && strings.Contains(relativePath, common.AZCOPY_PATH_SEPARATOR_STRING) {
				continue
			}

			storedObject := t.createStoredObjectForBlob(preprocessor, blobInfo, relativePath, containerName)

			// edge case, blob name happens to be the same as root and ends in /
			if storedObject.RelativePath == "" && strings.HasSuffix(storedObject.Name, "/") {
				storedObject.RelativePath = "\x00" // Short circuit, letting the backend know we *really* meant root/.
			}

			// Setting blob tags
			if t.s2sPreserveSourceTags && blobInfo.BlobTags != nil {
				blobTagsMap := common.BlobTags{}
				for _, blobTag := range blobInfo.BlobTags.BlobTagSet {
					blobTagsMap[url.QueryEscape(*blobTag.Key)] = url.QueryEscape(*blobTag.Value)
				}
				storedObject.BlobTags = blobTagsMap
			}

			if t.incrementEnumerationCounter != nil {
				t.incrementEnumerationCounter(common.EEntityType.File(), common.SymlinkHandlingType(0), common.DefaultHardlinkHandlingType)
			}

			processErr := ProcessIfPassedFilters(filters, storedObject, processor)
			_, processErr = getProcessingError(processErr)
			if processErr != nil {
				return processErr
			}
		}
	}

	return nil
}

type BlobTraverserOptions struct {
	IsDFS *bool
}

func NewBlobTraverser(rawURL string, serviceClient *service.Client, ctx context.Context, opts InitResourceTraverserOptions, blobOpts ...BlobTraverserOptions) (t *BlobTraverser) {
	t = &BlobTraverser{
		RawURL:                      rawURL,
		ServiceClient:               serviceClient,
		ctx:                         ctx,
		recursive:                   opts.Recursive,
		include:                     common.EBlobTraverserIncludeOption.FromInputs(opts.PermanentDelete, opts.ListVersions, opts.IncludeDirectoryStubs),
		incrementEnumerationCounter: opts.IncrementEnumeration,
		ParallelListing:             true,
		s2sPreserveSourceTags:       opts.PreserveBlobTags,
		cpkOptions:                  opts.CpkOptions,
		preservePermissions:         opts.PreservePermissions,
		isDFS:                       common.DerefOrZero(common.FirstOrZero(blobOpts).IsDFS),
		includeRoot:                 opts.IncludeRoot,
	}

	disableHierarchicalScanning := strings.ToLower(common.GetEnvironmentVariable(common.EEnvironmentVariable.DisableHierarchicalScanning()))

	// disableHierarchicalScanning should be true for permanent delete
	if (disableHierarchicalScanning == "false" || disableHierarchicalScanning == "") && t.include.Deleted() && (t.include.Snapshots() || t.include.Versions()) {
		t.ParallelListing = false
		fmt.Println("AZCOPY_DISABLE_HIERARCHICAL_SCAN has been set to true to permanently delete soft-deleted snapshots/versions.")
	}

	if disableHierarchicalScanning == "true" {
		// TODO log to frontend log that parallel listing was disabled, once the frontend log PR is merged
		t.ParallelListing = false
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
