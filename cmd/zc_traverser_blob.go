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
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
	"net/url"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// allow us to iterate through a path pointing to the blob endpoint
type blobTraverser struct {
	rawURL    *url.URL
	p         pipeline.Pipeline
	ctx       context.Context
	recursive bool

	// parallel listing employs the hierarchical listing API which is more expensive
	// cx should have the option to disable this optimization in the name of saving costs
	parallelListing bool

	// whether to include blobs that have metadata 'hdi_isfolder = true'
	includeDirectoryStubs bool

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc

	s2sPreserveSourceTags bool

	cpkOptions common.CpkOptions

	includeDeleted bool

	includeSnapshot bool

	includeVersion bool
}

func (t *blobTraverser) IsDirectory(isSource bool) bool {
	isDirDirect := copyHandlerUtil{}.urlIsContainerOrVirtualDirectory(t.rawURL)

	// Skip the single blob check if we're checking a destination.
	// This is an individual exception for blob because blob supports virtual directories and blobs sharing the same name.
	if isDirDirect || !isSource {
		return isDirDirect
	}

	_, _, isDirStub, err := t.detectRootUsingGetProperties()

	if stgErr, ok := err.(azblob.StorageError); ok {
		// We know for sure this is a single blob still, let it walk on through to the traverser.
		if stgErr.ServiceCode() == common.CPK_ERROR_SERVICE_CODE {
			return false
		}
	}

	if err == nil {
		return isDirStub
	}

	blobURLParts := azblob.NewBlobURLParts(*t.rawURL)
	containerRawURL := copyHandlerUtil{}.getContainerUrl(blobURLParts)
	containerURL := azblob.NewContainerURL(containerRawURL, t.p)
	searchPrefix := strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING) + common.AZCOPY_PATH_SEPARATOR_STRING
	resp, err := containerURL.ListBlobsFlatSegment(t.ctx, azblob.Marker{}, azblob.ListBlobsSegmentOptions{Prefix: searchPrefix, MaxResults: 1})
	if err != nil {
		if azcopyScanningLogger != nil {
			msg := fmt.Sprintf("Failed to check if the destination is a folder or a file (Azure Files). Assuming the destination is a file: %s", err)
			azcopyScanningLogger.Log(pipeline.LogError, msg)
		}
		return false
	}

	if len(resp.Segment.BlobItems) == 0 {
		//Not a directory
		return false
	}

	return true
}

// We use the list API instead of getProperties to limit permissions required by remove enumeration to just list
func (t *blobTraverser) detectRootUsingList() (props *azblob.BlobItemInternal, isBlob bool, isDirStub bool, err error) {
	blobURLParts := azblob.NewBlobURLParts(*t.rawURL)
	containerRawURL := copyHandlerUtil{}.getContainerUrl(blobURLParts)
	containerURL := azblob.NewContainerURL(containerRawURL, t.p)
	searchPrefix := strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)

	if searchPrefix == "" {
		// This is a container, which needs to be given a proper listing.
		return nil, false, false, nil
	}

	isVirtualDir := false
	for marker := (azblob.Marker{}); marker.NotDone(); {
		resp, err := containerURL.ListBlobsHierarchySegment(t.ctx, azblob.Marker{}, "/",
			azblob.ListBlobsSegmentOptions{
				Prefix: searchPrefix,
				Details: azblob.BlobListingDetails{
					Metadata:  true,
					Tags:      t.s2sPreserveSourceTags,
					Deleted:   t.includeDeleted,
					Snapshots: t.includeSnapshot,
					Versions:  t.includeVersion,
				}})
		if err != nil {
			return nil, false, false, err
		}
		for _, blob := range resp.Segment.BlobItems {
			// Root is a blob or a marker directory
			if blob.Name == searchPrefix || blob.Name == searchPrefix+common.AZCOPY_PATH_SEPARATOR_STRING {
				if gCopyUtil.doesBlobRepresentAFolder(blob.Metadata) {
					return &blob, false, true, nil
				} else {
					return &blob, true, false, nil
				}
			}
		}
		for _, virtualDir := range resp.Segment.BlobPrefixes {
			if virtualDir.Name == searchPrefix+common.AZCOPY_PATH_SEPARATOR_STRING {
				// Root is a virtual directory, don't return yet since it could be a marker directory
				isVirtualDir = true
			}
		}
		marker = resp.NextMarker
	}
	return nil, false, isVirtualDir, nil
}

func (t *blobTraverser) detectRootUsingGetProperties() (props *azblob.BlobGetPropertiesResponse, isBlob bool, isDirStub bool, err error) {
	// trim away the trailing slash before we check whether it's a single blob
	// so that we can detect the directory stub in case there is one
	blobUrlParts := azblob.NewBlobURLParts(*t.rawURL)
	blobUrlParts.BlobName = strings.TrimSuffix(blobUrlParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)

	if blobUrlParts.BlobName == "" {
		// This is a container, which needs to be given a proper listing.
		return nil, false, false, nil
	}

	// perform the check
	blobURL := azblob.NewBlobURL(blobUrlParts.URL(), t.p)
	clientProvidedKey := azblob.ClientProvidedKeyOptions{}
	if t.cpkOptions.IsSourceEncrypted {
		clientProvidedKey = common.GetClientProvidedKey(t.cpkOptions)
	}
	props, err = blobURL.GetProperties(t.ctx, azblob.BlobAccessConditions{}, clientProvidedKey)

	// if there was no problem getting the properties, it means that we are looking at a single blob
	if err == nil {
		if gCopyUtil.doesBlobRepresentAFolder(props.NewMetadata()) {
			return props, false, true, nil
		} else {
			return props, true, false, err
		}
	}

	return nil, false, false, err
}

func (t *blobTraverser) getBlobTags() (common.BlobTags, error) {
	blobUrlParts := azblob.NewBlobURLParts(*t.rawURL)
	blobUrlParts.BlobName = strings.TrimSuffix(blobUrlParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)

	// perform the check
	blobURL := azblob.NewBlobURL(blobUrlParts.URL(), t.p)
	blobTagsMap := make(common.BlobTags)
	blobGetTagsResp, err := blobURL.GetTags(t.ctx, nil)
	if err != nil {
		return blobTagsMap, err
	}

	for _, blobTag := range blobGetTagsResp.BlobTagSet {
		blobTagsMap[url.QueryEscape(blobTag.Key)] = url.QueryEscape(blobTag.Value)
	}
	return blobTagsMap, nil
}

func (t *blobTraverser) getRoot(preprocessor objectMorpher, parts azblob.BlobURLParts) (storedObject *StoredObject, isBlob bool, isDirStub bool, err error) {
	if azcopyScanningLogger != nil {
		msg := fmt.Sprintf("Detecting the root blob: %s", err)
		azcopyScanningLogger.Log(pipeline.LogDebug, msg)
	}
	// Try using GetProperties. If that fails due to permissions errors, try using List.
	var prop *azblob.BlobGetPropertiesResponse
	prop, isBlob, isDirStub, err = t.detectRootUsingGetProperties()
	retryWithList := false
	if stgErr, ok := err.(azblob.StorageError); ok {
		if stgErr.ServiceCode() == common.CPK_ERROR_SERVICE_CODE {
			if azcopyScanningLogger != nil {
				msg := fmt.Sprintf("Failed to detect root blob since it uses customer provided encryption keys (CPK) and CPK was not provided. Trying to detect root blob with list. If that fails too, retry by providing CPK. : %s", err)
				azcopyScanningLogger.Log(pipeline.LogDebug, msg)
			}
			retryWithList = true
		} else {
			resp := stgErr.Response()
			if resp != nil && resp.StatusCode == 403 {
				if azcopyScanningLogger != nil {
					msg := fmt.Sprintf("Failed to detect root blob due to lack of permissions. Trying to detect root blob with list. If that fails too, retry by providing read or list permissions. : %s", err)
					azcopyScanningLogger.Log(pipeline.LogDebug, msg)
				}
				retryWithList = true
			} else {
				return nil, false, false, fmt.Errorf("cannot list files due to reason %s", err)
			}
		}
	}
	if prop != nil {
		storedObject := newStoredObject(
			preprocessor,
			getObjectNameOnly(strings.TrimSuffix(parts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)),
			"",
			common.EntityType(common.IffUint8(isBlob, uint8(common.EEntityType.File()), uint8(common.EEntityType.Folder()))),
			prop.LastModified(),
			prop.ContentLength(),
			prop,
			blobPropertiesResponseAdapter{prop},
			common.FromAzBlobMetadataToCommonMetadata(prop.NewMetadata()), // .NewMetadata() seems odd to call, but it does actually retrieve the metadata from the blob properties.
			parts.ContainerName,
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
		return &storedObject, isBlob, isDirStub, nil
	}
	if retryWithList {
		var propItem *azblob.BlobItemInternal
		propItem, isBlob, isDirStub, err = t.detectRootUsingList()
		if stgErr, ok := err.(azblob.StorageError); ok {
			return nil, false, false, fmt.Errorf("cannot list files due to reason %s", stgErr)
		}
		if propItem != nil {
			storedObject := t.createStoredObjectForBlob(preprocessor, *propItem, "", parts.ContainerName)
			return &storedObject, isBlob, isDirStub, nil
		}
	}
	return nil, false, false, nil
}

func (t *blobTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	blobUrlParts := azblob.NewBlobURLParts(*t.rawURL)

	storedObject, isBlob, isDirStub, err := t.getRoot(preprocessor, blobUrlParts)
	// Enqueue the blob in two cases:
	// 	1. Targeting a single blob and the URL does not explicitly point to a virtual dir
	//	2. Scanning recursively with includeDirectoryStubs set to true, add the stub blob that represents the directory
	isSingleBlob := isBlob && !strings.HasSuffix(blobUrlParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)
	isDirectory := isDirStub && t.includeDirectoryStubs && t.recursive
	if isSingleBlob || isDirectory {
		if storedObject == nil {
			panic("isBlob should never be set if getting properties results in an error")
		}
		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(pipeline.LogDebug, "Detected the root as a blob.")
		}
		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EntityType(common.IffUint8(isBlob, uint8(common.EEntityType.File()), uint8(common.EEntityType.Folder()))))
		}

		err := processIfPassedFilters(filters, *storedObject, processor)
		_, err = getProcessingError(err)

		// short-circuit if we don't have anything else to scan and permanent delete is not on
		if !t.includeDeleted && (isBlob || err != nil) {
			return err
		}
	}

	// get the container URL so that we can list the blobs
	containerRawURL := copyHandlerUtil{}.getContainerUrl(blobUrlParts)
	containerURL := azblob.NewContainerURL(containerRawURL, t.p)

	// get the search prefix to aid in the listing
	// example: for a url like https://test.blob.core.windows.net/test/foo/bar/bla
	// the search prefix would be foo/bar/bla
	searchPrefix := blobUrlParts.BlobName

	// append a slash if it is not already present
	// example: foo/bar/bla becomes foo/bar/bla/ so that we only list children of the virtual directory
	if searchPrefix != "" && !strings.HasSuffix(searchPrefix, common.AZCOPY_PATH_SEPARATOR_STRING) && !t.includeSnapshot && !t.includeDeleted {
		searchPrefix += common.AZCOPY_PATH_SEPARATOR_STRING
	}

	// as a performance optimization, get an extra prefix to do pre-filtering. It's typically the start portion of a blob name.
	extraSearchPrefix := FilterSet(filters).GetEnumerationPreFilter(t.recursive)

	if t.parallelListing {
		return t.parallelList(containerURL, blobUrlParts.ContainerName, searchPrefix, extraSearchPrefix, preprocessor, processor, filters)
	}

	return t.serialList(containerURL, blobUrlParts.ContainerName, searchPrefix, extraSearchPrefix, preprocessor, processor, filters)
}

func (t *blobTraverser) parallelList(containerURL azblob.ContainerURL, containerName string, searchPrefix string,
	extraSearchPrefix string, preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error {
	// Define how to enumerate its contents
	// This func must be thread safe/goroutine safe
	enumerateOneDir := func(dir parallel.Directory, enqueueDir func(parallel.Directory), enqueueOutput func(parallel.DirectoryEntry, error)) error {
		currentDirPath := dir.(string)

		virtualDirs := map[string]bool{}

		for marker := (azblob.Marker{}); marker.NotDone(); {
			lResp, err := containerURL.ListBlobsHierarchySegment(t.ctx, marker, "/", azblob.ListBlobsSegmentOptions{Prefix: currentDirPath,
				Details: azblob.BlobListingDetails{Metadata: true, Tags: t.s2sPreserveSourceTags, Deleted: t.includeDeleted, Snapshots: t.includeSnapshot, Versions: t.includeVersion}})
			if err != nil {
				return fmt.Errorf("cannot list files due to reason %s", err)
			}

			// queue up the sub virtual directories if recursive is true
			if t.recursive {
				for _, virtualDir := range lResp.Segment.BlobPrefixes {
					enqueueDir(virtualDir.Name)
					if t.includeDirectoryStubs {
						if _, exists := virtualDirs[strings.TrimSuffix(virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING)]; !exists {
							virtualDirs[strings.TrimSuffix(virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING)] = true
						}
					}
				}
			}

			// process the blobs returned in this result segment
			for _, blobInfo := range lResp.Segment.BlobItems {
				// if the blob represents a folder, only include it if it is recursive and directory stubs should be included
				// otherwise skip it
				if t.doesBlobRepresentAFolder(blobInfo.Metadata) {
					continue
				}

				if gCopyUtil.doesBlobRepresentAFolder(blobInfo.Metadata) && t.includeDirectoryStubs {
					virtualDirs[strings.TrimSuffix(blobInfo.Name, common.AZCOPY_PATH_SEPARATOR_STRING)] = false
				}

				storedObject := t.createStoredObjectForBlob(preprocessor, blobInfo, strings.TrimPrefix(blobInfo.Name, searchPrefix), containerName)
				enqueueOutput(storedObject, nil)
			}

			// if debug mode is on, note down the result, this is not going to be fast
			if azcopyScanningLogger != nil && azcopyScanningLogger.ShouldLog(pipeline.LogDebug) {
				tokenValue := "NONE"
				if marker.Val != nil {
					tokenValue = *marker.Val
				}

				var vdirListBuilder strings.Builder
				for _, virtualDir := range lResp.Segment.BlobPrefixes {
					fmt.Fprintf(&vdirListBuilder, " %s,", virtualDir.Name)
				}
				var fileListBuilder strings.Builder
				for _, blobInfo := range lResp.Segment.BlobItems {
					fmt.Fprintf(&fileListBuilder, " %s,", blobInfo.Name)
				}
				msg := fmt.Sprintf("Enumerating %s with token %s. Sub-dirs:%s Files:%s", currentDirPath,
					tokenValue, vdirListBuilder.String(), fileListBuilder.String())
				azcopyScanningLogger.Log(pipeline.LogDebug, msg)
			}

			marker = lResp.NextMarker
		}

		// Datalake does not include directories in BlobItems but returns properties in BlobPrefixes.
		// Once ListBlob is updated to also fetch BlobPrefix Properties, we can get rid of this code and incorporate it above.
		if t.includeDirectoryStubs {
			for dirName, enqueued := range virtualDirs {
				if !enqueued {
					// try to get properties on the directory itself, since it's not listed in BlobItems
					fblobURL := containerURL.NewBlobURL(dirName)
					resp, err := fblobURL.GetProperties(t.ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
					folderRelativePath := strings.TrimPrefix(dirName, searchPrefix)
					if err == nil {
						storedObject := newStoredObject(
							preprocessor,
							getObjectNameOnly(dirName),
							folderRelativePath,
							common.EEntityType.Folder(),
							resp.LastModified(),
							resp.ContentLength(),
							resp,
							blobPropertiesResponseAdapter{resp},
							common.FromAzBlobMetadataToCommonMetadata(resp.NewMetadata()),
							containerName,
						)
						storedObject.archiveStatus = azblob.ArchiveStatusType(resp.ArchiveStatus())

						if t.s2sPreserveSourceTags {
							var BlobTags *azblob.BlobTags
							BlobTags, err = fblobURL.GetTags(t.ctx, nil)

							if err == nil {
								blobTagsMap := common.BlobTags{}
								for _, blobTag := range BlobTags.BlobTagSet {
									blobTagsMap[url.QueryEscape(blobTag.Key)] = url.QueryEscape(blobTag.Value)
								}
								storedObject.blobTags = blobTagsMap
							}
						}

						enqueueOutput(storedObject, err)
					}
				}
			}
		}

		return nil
	}

	// initiate parallel scanning, starting at the root path
	workerContext, cancelWorkers := context.WithCancel(t.ctx)
	cCrawled := parallel.Crawl(workerContext, searchPrefix+extraSearchPrefix, enumerateOneDir, EnumerationParallelism)

	for x := range cCrawled {
		item, workerError := x.Item()
		if workerError != nil {
			cancelWorkers()
			return workerError
		}

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.File())
		}

		object := item.(StoredObject)
		processErr := processIfPassedFilters(filters, object, processor)
		_, processErr = getProcessingError(processErr)
		if processErr != nil {
			cancelWorkers()
			return processErr
		}
	}

	return nil
}

func (t *blobTraverser) createStoredObjectForBlob(preprocessor objectMorpher, blobInfo azblob.BlobItemInternal, relativePath string, containerName string) StoredObject {
	adapter := blobPropertiesAdapter{blobInfo.Properties}

	_, isFolder := blobInfo.Metadata["hdi_isfolder"]
	object := newStoredObject(
		preprocessor,
		getObjectNameOnly(blobInfo.Name),
		relativePath,
		common.EntityType(common.IffUint8(isFolder, uint8(common.EEntityType.Folder()), uint8(common.EEntityType.File()))),
		blobInfo.Properties.LastModified,
		*blobInfo.Properties.ContentLength,
		adapter,
		adapter, // adapter satisfies both interfaces
		common.FromAzBlobMetadataToCommonMetadata(blobInfo.Metadata),
		containerName,
	)

	object.blobDeleted = blobInfo.Deleted
	if t.includeDeleted && t.includeSnapshot {
		object.blobSnapshotID = blobInfo.Snapshot
	} else if t.includeDeleted && t.includeVersion && blobInfo.VersionID != nil {
		object.blobVersionID = *blobInfo.VersionID
	}
	if t.s2sPreserveSourceTags && blobInfo.BlobTags != nil {
		blobTagsMap := common.BlobTags{}
		for _, blobTag := range blobInfo.BlobTags.BlobTagSet {
			blobTagsMap[url.QueryEscape(blobTag.Key)] = url.QueryEscape(blobTag.Value)
		}
		object.blobTags = blobTagsMap
	}
	return object
}

func (t *blobTraverser) doesBlobRepresentAFolder(metadata azblob.Metadata) bool {
	return gCopyUtil.doesBlobRepresentAFolder(metadata) && !(t.includeDirectoryStubs && t.recursive)
}

func (t *blobTraverser) serialList(containerURL azblob.ContainerURL, containerName string, searchPrefix string,
	extraSearchPrefix string, preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error {

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// see the TO DO in GetEnumerationPreFilter if/when we make this more directory-aware

		// look for all blobs that start with the prefix
		// Passing tags = true in the list call will save additional GetTags call
		// TODO optimize for the case where recursive is off
		listBlob, err := containerURL.ListBlobsFlatSegment(t.ctx, marker,
			azblob.ListBlobsSegmentOptions{Prefix: searchPrefix + extraSearchPrefix, Details: azblob.BlobListingDetails{Metadata: true, Tags: t.s2sPreserveSourceTags, Deleted: t.includeDeleted, Snapshots: t.includeSnapshot, Versions: t.includeVersion}})
		if err != nil {
			return fmt.Errorf("cannot list blobs. Failed with error %s", err.Error())
		}

		// process the blobs returned in this result segment
		for _, blobInfo := range listBlob.Segment.BlobItems {
			// if the blob represents a hdi folder, then skip it
			if t.doesBlobRepresentAFolder(blobInfo.Metadata) {
				continue
			}

			relativePath := strings.TrimPrefix(blobInfo.Name, searchPrefix)
			// if recursive
			if !t.recursive && strings.Contains(relativePath, common.AZCOPY_PATH_SEPARATOR_STRING) {
				continue
			}

			storedObject := t.createStoredObjectForBlob(preprocessor, blobInfo, relativePath, containerName)

			if t.incrementEnumerationCounter != nil {
				t.incrementEnumerationCounter(common.EEntityType.File())
			}

			processErr := processIfPassedFilters(filters, storedObject, processor)
			_, processErr = getProcessingError(processErr)
			if processErr != nil {
				return processErr
			}
		}

		marker = listBlob.NextMarker
	}

	return nil
}

func newBlobTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, recursive, includeDirectoryStubs bool, incrementEnumerationCounter enumerationCounterFunc, s2sPreserveSourceTags bool, cpkOptions common.CpkOptions, includeDeleted, includeSnapshot, includeVersion bool) (t *blobTraverser) {
	t = &blobTraverser{
		rawURL:                      rawURL,
		p:                           p,
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
