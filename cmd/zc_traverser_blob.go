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
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"

	"github.com/pkg/errors"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// allow us to iterate through a path pointing to the blob endpoint
type blobTraverser struct {
	rawURL        string
	serviceClient *service.Client
	ctx           context.Context
	recursive     bool

	// parallel listing employs the hierarchical listing API which is more expensive
	// cx should have the option to disable this optimization in the name of saving costs
	parallelListing bool

	// whether to include blobs that have metadata 'hdi_isfolder = true'
	includeDirectoryStubs bool

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc

	s2sPreserveSourceTags bool

	cpkOptions common.CpkOptions

	preservePermissions common.PreservePermissionsOption

	includeDeleted bool

	includeSnapshot bool

	includeVersion bool

	isDFS bool
}

func (t *blobTraverser) IsDirectory(isSource bool) (bool, error) {
	isDirDirect := copyHandlerUtil{}.urlIsContainerOrVirtualDirectory(t.rawURL)

	// Skip the single blob check if we're checking a destination.
	// This is an individual exception for blob because blob supports virtual directories and blobs sharing the same name.
	// On HNS accounts, we would still perform this test. The user may have provided directory name without path-separator
	if isDirDirect { // a container or a path ending in '/' is always directory
		return true, nil
	}
	if !isSource && !t.isDFS {
		// destination on blob endpoint. If it does not end in '/' it is a file
		return false, nil
	}

	// All sources and DFS-destinations we'll look further

	_, _, isDirStub, blobErr := t.getPropertiesIfSingleBlob()

	// We know for sure this is a single blob still, let it walk on through to the traverser.
	if bloberror.HasCode(blobErr, bloberror.BlobUsesCustomerSpecifiedEncryption) {
		return false, nil
	}

	if blobErr == nil {
		return isDirStub, nil
	}

	blobURLParts, err := blob.ParseURL(t.rawURL)
	if err != nil {
		return false, err
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
		// Not a directory
		// If the blob is not found return the error to throw
		if bloberror.HasCode(blobErr, bloberror.BlobNotFound) {
			return false, errors.New(common.FILE_NOT_FOUND)
		}
		return false, blobErr
	}

	return true, nil
}

func (t *blobTraverser) getPropertiesIfSingleBlob() (response *blob.GetPropertiesResponse, isBlob bool, isDirStub bool, err error) {
	// trim away the trailing slash before we check whether it's a single blob
	// so that we can detect the directory stub in case there is one
	blobURLParts, err := blob.ParseURL(t.rawURL)
	if err != nil {
		return nil, false, false, err
	}
	blobURLParts.BlobName = strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)

	if blobURLParts.BlobName == "" {
		// This is a container, which needs to be given a proper listing.
		return nil, false, false, nil
	}

	blobClient, err := createBlobClientFromServiceClient(blobURLParts, t.serviceClient)
	if err != nil {
		return nil, false, false, err
	}
	props, err := blobClient.GetProperties(t.ctx, &blob.GetPropertiesOptions{CPKInfo: t.cpkOptions.GetCPKInfo()})

	// if there was no problem getting the properties, it means that we are looking at a single blob
	if err == nil {
		if gCopyUtil.doesBlobRepresentAFolder(props.Metadata) {
			return &props, false, true, nil
		}

		return &props, true, false, err
	}

	return nil, false, false, err
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
	blobProperties, isBlob, isDirStub, err := t.getPropertiesIfSingleBlob()

	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		// Don't error out unless it's a CPK error just yet
		// If it's a CPK error, we know it's a single blob and that we can't get the properties on it anyway.
		if respErr.ErrorCode == string(bloberror.BlobUsesCustomerSpecifiedEncryption) {
			return errors.New("this blob uses customer provided encryption keys (CPK). At the moment, AzCopy does not support CPK-encrypted blobs. " +
				"If you wish to make use of this blob, we recommend using one of the Azure Storage SDKs")
		}
		if respErr.RawResponse == nil {
			return fmt.Errorf("cannot list files due to reason %s", respErr)
		} else if respErr.StatusCode == 403 { // Some nature of auth error-- Whatever the user is pointing at, they don't have access to, regardless of whether it's a file or a dir stub.
			return fmt.Errorf("cannot list files due to reason %s", respErr)
		}
	}

	// schedule the blob in two cases:
	// 	1. either we are targeting a single blob and the URL wasn't explicitly pointed to a virtual dir
	//	2. either we are scanning recursively with includeDirectoryStubs set to true,
	//	   then we add the stub blob that represents the directory
	if (isBlob && !strings.HasSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)) ||
		(t.includeDirectoryStubs && isDirStub && t.recursive) {
		// sanity checking so highlighting doesn't highlight things we're not worried about.
		if blobProperties == nil {
			panic("isBlob should never be set if getting properties is an error")
		}

		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(common.LogDebug, "Detected the root as a blob.")
			azcopyScanningLogger.Log(common.LogDebug, fmt.Sprintf("Root entity type: %s", getEntityType(blobProperties.Metadata)))
		}

		storedObject := newStoredObject(
			preprocessor,
			getObjectNameOnly(strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)),
			"",
			getEntityType(blobProperties.Metadata),
			*blobProperties.LastModified,
			*blobProperties.ContentLength,
			blobPropertiesResponseAdapter{blobProperties},
			blobPropertiesResponseAdapter{blobProperties},
			blobProperties.Metadata,
			blobURLParts.ContainerName,
		)

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
		if !t.includeDeleted && (isBlob || err != nil) {
			return err
		}
	} else if blobURLParts.BlobName == "" && t.preservePermissions.IsTruthy() {
		// if the root is a container and we're copying "folders", we should persist the ACLs there too.
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
	if searchPrefix != "" && !strings.HasSuffix(searchPrefix, common.AZCOPY_PATH_SEPARATOR_STRING) && !t.includeSnapshot && !t.includeDeleted {
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
	// Define how to enumerate its contents
	// This func must be thread safe/goroutine safe
	enumerateOneDir := func(dir parallel.Directory, enqueueDir func(parallel.Directory), enqueueOutput func(parallel.DirectoryEntry, error)) error {
		currentDirPath := dir.(string)

		pager := containerClient.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
			Prefix:  &currentDirPath,
			Include: container.ListBlobsInclude{Metadata: true, Tags: t.s2sPreserveSourceTags, Deleted: t.includeDeleted, Snapshots: t.includeSnapshot, Versions: t.includeVersion},
		})
		var marker *string
		for pager.More() {
			lResp, err := pager.NextPage(t.ctx)
			if err != nil {
				return fmt.Errorf("cannot list files due to reason %s", err)
			}
			// queue up the sub virtual directories if recursive is true
			if t.recursive {
				for _, virtualDir := range lResp.Segment.BlobPrefixes {
					enqueueDir(*virtualDir.Name)
					if azcopyScanningLogger != nil {
						azcopyScanningLogger.Log(common.LogDebug, fmt.Sprintf("Enqueuing sub-directory %s for enumeration.", *virtualDir.Name))
					}

					if t.includeDirectoryStubs {
						// try to get properties on the directory itself, since it's not listed in BlobItems
						blobClient := containerClient.NewBlobClient(strings.TrimSuffix(*virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING))
						pResp, err := blobClient.GetProperties(t.ctx, nil)
						folderRelativePath := strings.TrimSuffix(*virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING)
						folderRelativePath = strings.TrimPrefix(folderRelativePath, searchPrefix)
						if err == nil {
							storedObject := newStoredObject(
								preprocessor,
								getObjectNameOnly(strings.TrimSuffix(*virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING)),
								folderRelativePath,
								common.EEntityType.Folder(),
								*pResp.LastModified,
								*pResp.ContentLength,
								blobPropertiesResponseAdapter{&pResp},
								blobPropertiesResponseAdapter{&pResp},
								pResp.Metadata,
								containerName,
							)

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
						}
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
	cCrawled := parallel.Crawl(workerContext, searchPrefix+extraSearchPrefix, enumerateOneDir, EnumerationParallelism)

	for x := range cCrawled {
		item, workerError := x.Item()
		if workerError != nil {
			return workerError
		}

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.File())
		}

		object := item.(StoredObject)
		processErr := processIfPassedFilters(filters, object, processor)
		_, processErr = getProcessingError(processErr)
		if processErr != nil {
			return processErr
		}
	}

	return nil
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
		*blobInfo.Properties.LastModified,
		*blobInfo.Properties.ContentLength,
		adapter,
		adapter, // adapter satisfies both interfaces
		blobInfo.Metadata,
		containerName,
	)

	object.blobDeleted = common.IffNotNil(blobInfo.Deleted, false)
	if t.includeDeleted && t.includeSnapshot {
		object.blobSnapshotID = common.IffNotNil(blobInfo.Snapshot, "")
	} else if t.includeDeleted && t.includeVersion && blobInfo.VersionID != nil {
		object.blobVersionID = common.IffNotNil(blobInfo.VersionID, "")
	}
	return object
}

func (t *blobTraverser) doesBlobRepresentAFolder(metadata map[string]*string) bool {
	util := copyHandlerUtil{}
	return util.doesBlobRepresentAFolder(metadata) && !(t.includeDirectoryStubs && t.recursive)
}

func (t *blobTraverser) serialList(containerClient *container.Client, containerName string, searchPrefix string,
	extraSearchPrefix string, preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error {

	// see the TO DO in GetEnumerationPreFilter if/when we make this more directory-aware
	// TODO optimize for the case where recursive is off
	prefix := searchPrefix + extraSearchPrefix
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: container.ListBlobsInclude{Metadata: true, Tags: t.s2sPreserveSourceTags, Deleted: t.includeDeleted, Snapshots: t.includeSnapshot, Versions: t.includeVersion},
	})
	for pager.More() {
		resp, err := pager.NextPage(t.ctx)
		if err != nil {
			return fmt.Errorf("cannot list blobs. Failed with error %s", err.Error())
		}
		// process the blobs returned in this result segment
		for _, blobInfo := range resp.Segment.BlobItems {
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

			processErr := processIfPassedFilters(filters, storedObject, processor)
			_, processErr = getProcessingError(processErr)
			if processErr != nil {
				return processErr
			}
		}
	}

	return nil
}

func newBlobTraverser(rawURL string, serviceClient *service.Client, ctx context.Context, recursive, includeDirectoryStubs bool, incrementEnumerationCounter enumerationCounterFunc, s2sPreserveSourceTags bool, cpkOptions common.CpkOptions, includeDeleted, includeSnapshot, includeVersion bool, preservePermissions common.PreservePermissionsOption, isDFS bool) (t *blobTraverser) {
	t = &blobTraverser{
		rawURL:                      rawURL,
		serviceClient:               serviceClient,
		ctx:                         ctx,
		recursive:                   recursive,
		includeDirectoryStubs:       includeDirectoryStubs,
		incrementEnumerationCounter: incrementEnumerationCounter,
		parallelListing:             true,
		s2sPreserveSourceTags:       s2sPreserveSourceTags,
		cpkOptions:                  cpkOptions,
		includeDeleted:              includeDeleted,
		includeSnapshot:             includeSnapshot,
		includeVersion:              includeVersion,
		preservePermissions:         preservePermissions,
		isDFS:                       isDFS,
	}

	disableHierarchicalScanning := strings.ToLower(glcm.GetEnvironmentVariable(common.EEnvironmentVariable.DisableHierarchicalScanning()))

	// disableHierarchicalScanning should be true for permanent delete
	if (disableHierarchicalScanning == "false" || disableHierarchicalScanning == "") && includeDeleted && (includeSnapshot || includeVersion) {
		t.parallelListing = false
		fmt.Println("AZCOPY_DISABLE_HIERARCHICAL_SCAN has been set to true to permanently delete soft-deleted snapshots/versions.")
	}

	if disableHierarchicalScanning == "true" {
		// TODO log to frontend log that parallel listing was disabled, once the frontend log PR is merged
		t.parallelListing = false
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
