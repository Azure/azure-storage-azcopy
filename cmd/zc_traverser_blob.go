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
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aymanjarrousms/azure-storage-azcopy/v10/common/parallel"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"

	"github.com/aymanjarrousms/azure-storage-azcopy/v10/common"
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

	// Fields applicable only to sync operation.
	// isSync boolean tells whether its copy operation or sync operation.
	isSync bool

	// Error channel for scanning errors
	errorChannel chan ErrorFileInfo

	// Hierarchical map of files and folders seen on source side.
	indexerMap *folderIndexer

	// child-after-parent ordered communication channel between source and destination traverser.
	orderedTqueue parallel.OrderedTqueueInterface

	// Map for checking rename directories, so that complete sub-tree can be enumerated.
	possiblyRenamedMap *possiblyRenamedMap

	// For sync operation this flag tells whether this is source or target.
	isSource bool

	// see cookedSyncCmdArgs.maxObjectIndexerSizeInGB for details.
	maxObjectIndexerSizeInGB uint32

	// see cookedSyncCmdArgs.lastSyncTime for details.
	lastSyncTime time.Time

	// See cookedSyncCmdArgs.cfdMode for details.
	cfdMode common.CFDMode

	// see cookedSyncCmdArgs.metaDataOnlySync for details.
	metaDataOnlySync bool

	// scannerLogger to log scanning error.
	scannerLogger common.ILoggerResetable
}

func (t *blobTraverser) writeToErrorChannel(err ErrorFileInfo) {
	if t.scannerLogger != nil {
		t.scannerLogger.Log(pipeline.LogError, err.ErrorMsg.Error())
	} else {
		WarnStdoutAndScanningLog(err.ErrorMsg.Error())
	}
	if t.errorChannel != nil {
		t.errorChannel <- err
	}
}
func (t *blobTraverser) IsDirectory(isSource bool) bool {
	isDirDirect := copyHandlerUtil{}.urlIsContainerOrVirtualDirectory(t.rawURL)

	// Skip the single blob check if we're checking a destination.
	// This is an individual exception for blob because blob supports virtual directories and blobs sharing the same name.
	if isDirDirect || !isSource {
		return isDirDirect
	}

	_, isSingleBlob, _, err := t.getPropertiesIfSingleBlob()

	if stgErr, ok := err.(azblob.StorageError); ok {
		// We know for sure this is a single blob still, let it walk on through to the traverser.
		if stgErr.ServiceCode() == common.CPK_ERROR_SERVICE_CODE {
			return false
		}
	}

	return !isSingleBlob
}

func (t *blobTraverser) getPropertiesIfSingleBlob() (props *azblob.BlobGetPropertiesResponse, isBlob bool, isDirStub bool, err error) {
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
		}

		return props, true, false, err
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

func (t *blobTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	blobUrlParts := azblob.NewBlobURLParts(*t.rawURL)

	// check if the url points to a single blob
	blobProperties, isBlob, isDirStub, propErr := t.getPropertiesIfSingleBlob()

	if stgErr, ok := propErr.(azblob.StorageError); ok {
		writeToErrorChannel := func(err error) {
			if t.errorChannel != nil {
				ErrorFileInfo := ErrorFileInfo{
					FilePath: blobUrlParts.BlobName,
					ErrorMsg: err,
				}
				t.errorChannel <- ErrorFileInfo
			}
		}

		// Don't error out unless it's a CPK error just yet
		// If it's a CPK error, we know it's a single blob and that we can't get the properties on it anyway.
		if stgErr.ServiceCode() == common.CPK_ERROR_SERVICE_CODE {
			err := errors.New("this blob uses customer provided encryption keys (CPK). At the moment, AzCopy does not support CPK-encrypted blobs. " +
				"If you wish to make use of this blob, we recommend using one of the Azure Storage SDKs")
			writeToErrorChannel(err)
			return err
		}

		if resp := stgErr.Response(); resp == nil {
			err := fmt.Errorf("cannot list files due to reason %s", stgErr)
			writeToErrorChannel(err)
			return err
		} else {
			if resp.StatusCode == 403 { // Some nature of auth error-- Whatever the user is pointing at, they don't have access to, regardless of whether it's a file or a dir stub.
				err := fmt.Errorf("cannot list files due to reason %s", stgErr)
				writeToErrorChannel(err)
				return err
			}
		}
	}

	//
	// In case of sync, target enumeration driven by source. Source first make an entry to indexerMap then tell target
	// traverser to process that directory. Whereas this code breaks that logic. It cause 2 major issue to sync engine.
	// 1. It may happen target traverser try to process root entry, whereas source still need to add entry to indexerMap.
	// 2. Root entry will be processed twice which cause deletion of entry from indexerMap.
	//
	// Note: This code do the error checking and process root directory or file. It's ok to skip this step for sync
	//       as we do the processing of root directory or file in parallelList function. It's just delayed by one function
	//       call.
	//
	if !t.isSync {
		// schedule the blob in two cases:
		// 	1. either we are targeting a single blob and the URL wasn't explicitly pointed to a virtual dir
		//	2. either we are scanning recursively with includeDirectoryStubs set to true,
		//	   then we add the stub blob that represents the directory
		if (isBlob && !strings.HasSuffix(blobUrlParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)) ||
			(t.includeDirectoryStubs && isDirStub && t.recursive) {
			// sanity checking so highlighting doesn't highlight things we're not worried about.
			if blobProperties == nil {
				panic("isBlob should never be set if getting properties is an error")
			}

			err := fmt.Errorf("Detected the root[%v] as a blob.", t.rawURL)
			if t.scannerLogger != nil {
				t.scannerLogger.Log(pipeline.LogDebug, err.Error())
			} else if azcopyScanningLogger != nil {
				azcopyScanningLogger.Log(pipeline.LogDebug, "Detected the root as a blob.")
			}

			storedObject := newStoredObject(
				preprocessor,
				getObjectNameOnly(strings.TrimSuffix(blobUrlParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)),
				"",
				common.EntityType(common.IffUint8(isBlob, uint8(common.EEntityType.File()), uint8(common.EEntityType.Folder()))),
				blobProperties.LastModified(),
				blobProperties.ContentLength(),
				blobProperties,
				blobPropertiesResponseAdapter{blobProperties},
				common.FromAzBlobMetadataToCommonMetadata(blobProperties.NewMetadata()), // .NewMetadata() seems odd to call, but it does actually retrieve the metadata from the blob properties.
				blobUrlParts.ContainerName,
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
				t.incrementEnumerationCounter(common.EEntityType.File())
			}

			err = processIfPassedFilters(filters, storedObject, processor)
			_, err = getProcessingError(err)

			// short-circuit if we don't have anything else to scan and permanent delete is not on
			if !t.includeDeleted && (isBlob || err != nil) {
				return err
			}
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

//
// waitTillAllAncestorsAreProcessed will block the calling thread till all
// ancestors of 'currentDirPath' are processed and removed from ObjectIndexerMap.
// f.e., if 'currentDirPath' is "a/b/c/d", then it'll wait till there are any of the
// following directories present in the ObjectIndexerMap.
// "", "a", "a/b", "a/b/c"
//
// This will be called by TargetTraverser to ensure that it processes a
// directory from tqueue only after all its ancestors. The reason being that
// unless we process every ancestor of a directory we cannot safely conclude
// if any of the ancestors could be the result of a rename. Note that if a
// directory has any ancestor that is renamed, it means that the directory
// MUST be enumerated in order to copy its contents and hence we must know
// that when we process the directory from tqueue.
//
func (t *blobTraverser) waitTillAllAncestorsAreProcessed(currentDirPath string) {
	//
	// If possiblyRenamedMap is nil then we are not doing the special rename handling, hence
	// directories can be processed in any order (no need to wait for ancestors).
	//
	if t.possiblyRenamedMap == nil {
		return
	}

	if t.cfdMode == common.CFDModeFlags.TargetCompare() {
		panic("waitTillAllAncestorsAreProcessed() MUST NOT be called for TargetCompare mode!")
	}
	//
	// Empty currentDirPath represents root folder, root folder has no ancestors, so no need to wait.
	//
	if currentDirPath == "" {
		return
	}

	//
	// Wait for the parent to be processed by the Target Traverser.
	// As a directory is processed it'll be removed from t.indexerMap.folderMap[], so we know that the directory
	// is processed by waiting till it's no more present in t.indexerMap.folderMap[].
	// Since parent processing would have in turn waited for its parent and so on, we just need to wait for the
	// immediate parent to be sure that all ancestors of currentDirPath are processed by Target Traverser.
	//
	parent := filepath.Dir(currentDirPath)

	//
	// waitCount to track how much time spent waiting for directory to be processed.
	//
	waitCount := 0
	for {
		if t.indexerMap.directoryNotYetProcessed(parent) {
			time.Sleep(10 * time.Millisecond)
			waitCount++
			t.scannerLogger.Log(pipeline.LogInfo, fmt.Sprintf("currentDir(%s) parent(%s) still not processed after iteration[%v]", currentDirPath, parent, waitCount))
		} else {
			// parent directory processed by target traverser, now it can safely proceed with this directory.
			break
		}

		//
		// In most practical case it should be small wait, but to be safe lets use a fairly large wait.
		// If directory still not processed it means some bug.
		//
		if waitCount > 3600*100 {
			panic(fmt.Sprintf("currentDir(%s) parent(%s) still not processed after 3600 secs\n", currentDirPath, parent))
		}
	}

	// Sanity check, If parent of currentDirPath processed from ObjectIndexerMap, GrandParent entry should also be processed.
	if parent != "." {
		grandParent := filepath.Dir(parent)
		if t.indexerMap.directoryNotYetProcessed(grandParent) {
			panic(fmt.Sprintf("We should not never reach here, where for child(%s), parent(%s) entry processed, "+
				"but grandParent(%s) entry still present in ObjectIndexerMap", currentDirPath, parent, grandParent))
		}
	}
}

//
// hasAnAncestorThatIsPossiblyRenamed checks all ancestors of 'currentDirPath' starting
// from the immediate parent and walking upwards till the root and returns
// true if any of the ancestor is present in 'possiblyRenamedMap'.
// Directories are added to 'possiblyRenamedMap' in two places:
// 1. For directories that exist in the target, it's added to
//    'possiblyRenamedMap' if the target directory's inode is
//    different from source directory's inode.
// 2. For directories that don't exist in the target, all of them are added
//    to 'possiblyRenamedMap'. It could very well be a new directory but
//    that doesn't cause any additional overhead since new directories
//    are anyway attempted enumeration.
//
// Note: This must never be called for CFDmode==TargetCompare.
//
func (t *blobTraverser) hasAnAncestorThatIsPossiblyRenamed(currentDirPath string) bool {
	if t.cfdMode == common.CFDModeFlags.TargetCompare() {
		panic(fmt.Sprintf("hasAnAncestorThatIsPossiblyRenamed called for currentDirPath(%s) with TargetCompare", currentDirPath))
	}

	// possiblyRenamedMap can be nil. As of now we have possiblyRenamedMap to detect any potential rename.
	// In future, we may use different approach to detect rename as optimization.
	if t.possiblyRenamedMap == nil {
		return false
	}

	//
	// "" signifies root, root does not have any ancestor and hence cannot be renamed.
	//
	if currentDirPath == "" {
		return false
	}

	for ; currentDirPath != "."; currentDirPath = filepath.Dir(currentDirPath) {
		if t.possiblyRenamedMap.exists(currentDirPath) {
			return true
		}
	}
	return false
}

//
// Given a directory find out if it has changed since the last sync. A “changed” directory could mean one or more of the following:
// 1. One or more new files/subdirs created inside the directory.
// 2. One or more files/subdirs deleted.
// 3. Directory is renamed.
//
// It honours CFDMode to make the decision, f.e., if CFDMode allows ctime/mtime to be used for CFD it may not need to query attributes from target.
//
// If it returns True, TargetTraverser will enumerate the directory and compare each enumerated object with the source scanned objects in
// ObjectIndexer[] to find out if the object needs to be sync'ed.
//
// For CFDModes that allow ctime for CFD we can avoid enumerating target dir if we know directory has not changed. This increases sync efficiency.
//
// Note: If we discover that certain sources cannot be safely trusted for ctime update we can change this to return True for them, thus falling back
// on the more rigorous target<->source comparison. //
func (t *blobTraverser) hasDirectoryChangedSinceLastSync(currentDirPath string) bool {
	// Get the storedObject for currentDirPath to compare Ctime, Mtime for different mode.
	// Although we don't need for CFDMode == TargetCompare, but need for sanity check.
	so := t.indexerMap.getStoredObject(currentDirPath)

	if currentDirPath != so.relativePath {
		panic(fmt.Sprintf("curentDirPath[%s] not matched with storedObject relative path[%s]", currentDirPath, so.relativePath))
	}

	// Force enumeration for TargetCompare mode. For other CFDModes we enumerate a directory iff it has changed since last sync.
	if t.cfdMode == common.CFDModeFlags.TargetCompare() {
		return true
	}

	//
	// If CFDMode allows using ctime, compare directory ctime with LastSyncTime.
	// Note that directory ctime will change if a new object is created inside the
	// directory or an existing object is deleted or the directory is renamed.
	//
	if t.cfdMode == common.CFDModeFlags.CtimeMtime() {
		if so.lastChangeTime.After(t.lastSyncTime) {
			return true
		}
		return false
	} else if t.cfdMode == common.CFDModeFlags.Ctime() {
		if so.lastChangeTime.After(t.lastSyncTime) {
			return true
		} else if t.metaDataOnlySync && t.indexerMap.filesChangedInDirectory(so.relativePath, t.lastSyncTime) {
			//
			// If metaDataOnlySync is true and a file has "ctime > LastSyncTime" and CFDMode does not allow us to use mtime for checking if the
			// file's data+metadata or only metadata has changed, then we need to compare the file's source attributes with target attributes.
			// Since fetching attributes for individual target file may be expensive for some targets (f.e. Blob), so it would be better to enumerate
			// the target parent dir which will be cheaper due to ListDir returning many files and their attributes in a single call. In normal scenario,
			// there will be less than 2K files in a folder and all 2K files along with their attributes can be retrieved in a single ListDir call.
			// If not even one file changed in a directory we don't need to compare attributes for any file and hence we don't fetch the attributes.
			//
			return true
		} else {
			return false
		}
	} else {
		panic(fmt.Sprintf("Unsupported CFDMode: %d", t.cfdMode))
	}
}

func (t *blobTraverser) parallelList(containerURL azblob.ContainerURL, containerName string, searchPrefix string,
	extraSearchPrefix string, preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error {

	//
	// If we are scanning the target in sync mode (aka TargetTraverser) we need to do some special handling.
	// Set the following boolean to be used later in this function.
	//
	targetTraverser := t.isSync && !t.isSource

	//
	// Define how to enumerate its contents
	// This func must be thread safe/goroutine safe
	//
	// For the sync case, this is called for each directory added to tqueue by the source traverser. Note that since source traverser will add
	// directories present in the source, it may or may not be present in the target. If a directory is not present in the target, that's a case
	// of newly created directory at the source which must be created in the target. It should enumerate only the direct children of the directory
	// and create a storedObject for each and queue them for processing by processIfNecessary. Apart from queueing storedObject for each direct children,
	// it'll also queue a special storedObject for "finalize" processing. This will be queued *after* all the children.
	//
	enumerateOneDir := func(dir parallel.Directory, enqueueDir func(parallel.Directory), enqueueOutput func(parallel.DirectoryEntry, error)) error {
		if _, ok := dir.(string); !ok {
			panic("Directory entry is not string")
		}

		//
		// Flag to be passed in FinalizeDirectory to indicate to the receiver (processIfNecessary()) if it should copy all the files or call
		// HasFileChangedSinceLastSyncUsingLocalChecks() to find out files that need to be copied.
		//
		FinalizeAll := false
		currentDirPath := dir.(string)

		// This code for sync operation and when its target traverser.
		if targetTraverser {
			//
			// Don't proceed with processing this directory right away, wait till all of its ancestors are processed. We do this by periodically
			// checking ObjectIndexerMap to make sure there are no ancestors left in the ObjectIndexerMap.
			// This is important because as ancestor directories are processed from tqueue it may cause new directories to be added to
			// "possiblyRenamedMap" which we will need to correctly process this directory.
			// For CFDMode == TargetCompare, this should be a no-op.
			//
			t.waitTillAllAncestorsAreProcessed(currentDirPath)

			//
			// If source directory has not changed since last sync, then we don't really need to enumerate the target. SourceTraverser would have enumerated
			// this directory and added all the children in ObjectIndexer map. We just need to go over these objects and find out which of these need to be
			// sync'ed to target and sync them appropriately (data+metadata or only metadata).
			//
			// If directory has changed then there could be some files deleted and to find them we need to enumerate the target directory and compare. Also,
			// if directory is renamed then also it'll be considered changed. Note that a renamed directory needs to be fully enumerated at the target as even
			// files with same names as in the target could be entirely different files. This forces us to enumerate the target directory if the source
			// directory is seen to have changed, since we don’t know if it was renamed, in which case we must enumerate the target directory.
			//
			// We additionally check hasAnAncestorThatIsPossiblyRenamed() which returns True if any of the ancestors of ‘currentDirPath’ could have been possibly
			// renamed. For any “possibly renamed” directory we must enumerate all the children/grandchildren target directories to ensure that we copy all
			// their children correctly, even though the ctime/mtime of those directories won’t be more than LastSyncTime.
			//
			if !t.hasDirectoryChangedSinceLastSync(currentDirPath) && !t.hasAnAncestorThatIsPossiblyRenamed(currentDirPath) {
				goto FinalizeDirectory
			}

			// source traverser (local traverser) truncate all the forward slash in storedObject.
			// Without forward slash list blob not list blobs under this directory.
			if currentDirPath != "" {
				if currentDirPath[len(currentDirPath)-1] != '/' {
					currentDirPath = currentDirPath + "/"
				}
			}
		}

		//
		// Now that we are going to enumerate the target dir, we will process all files present in the target, and when FinalizeDirectory is called only those
		// files will be left in the ObjectIndexer map which are newly created in source. We need to blindly copy all of these to the target.
		// Set FinalizeAll to true to convey this to the receiver (processIfNecessary()).
		//
		FinalizeAll = true

		//
		// Enumerate direct children of currentDirPath (aka non-recursively for targetTraverser).
		// Each child object we will call enqueueOutput() which will cause it to be sent to processIfNecessary() for processing where we will perform the "do we need to sync it"
		// check. After currentDirPath is fully enumerated and all children queued for processing by processIfNecessary(), we also enqueue a special StoredObject to trigger the
		// "end of directory" processing by processIfNecessary(). This StoredObject is marked by isFolderEndMarker flag. This indicates that we are done processing target objects.
		// This could mean one of the following:
		//
		// 1. currentDirPath was found "not changed" and hence we didn't need to perform target enumeration. In this case we need to go over all StoredObjects
		//    belonging to currentDirPath in sourceFolderIndex and see if they are modified after last sync and transfer if needed.
		// 2. currentDirPath was found "changed" and hence we enumerated the target and we are done processing all enumerated children, so what's left now is
		//    new files/subdirs created in source after lastSyncTime and we need to transfer them all. This is marked by setting FinalizeAll to true.
		// 3. currentDirPath was not found in the target. In this case we need to create the directory in the target and transfer all its direct children. See Note below.
		//
		// Above is performed by the FinalizeTargetDirectory() method.
		//
		// Note: For the case of sync target traverser currentDirPath may not exist in the target (since it was picked from tqueue which means it exists at source but not necessarily
		//       at the target). In that case the following enumeration will not yield anything, note that it'll not fail but simply result in "nothing". In that case we will go to
		//       FinalizeDirectory and create a "end of folder" marker to be sent to processIfNecessary(). processIfNecessary() will perform the target directory creation.
		//
		for marker := (azblob.Marker{}); marker.NotDone(); {
			prefix := currentDirPath

			// If searchPrefix is present, it means user provided the sub-path. This is the place we need to look for files.
			// root.Url contains path till container, to search files under subPath we need to add it as prefix. Otherwise it tries to list
			// files under container.
			if targetTraverser && searchPrefix != "" {
				prefix = filepath.Join(searchPrefix, currentDirPath)

				// filepath.Join truncate last forward slash we need to add it again.
				prefix = prefix + "/"
			}

			lResp, err := containerURL.ListBlobsHierarchySegment(t.ctx, marker, "/", azblob.ListBlobsSegmentOptions{Prefix: prefix,
				Details: azblob.BlobListingDetails{Metadata: true, Tags: t.s2sPreserveSourceTags, Deleted: t.includeDeleted, Snapshots: t.includeSnapshot, Versions: t.includeVersion}})
			if err != nil {
				return fmt.Errorf("cannot list files due to reason %s", err)
			}

			// queue up the sub virtual directories if recursive is true
			if t.recursive {
				for _, virtualDir := range lResp.Segment.BlobPrefixes {
					if !targetTraverser {
						enqueueDir(virtualDir.Name)
					}

					if t.includeDirectoryStubs {
						// try to get properties on the directory itself, since it's not listed in BlobItems
						fblobURL := containerURL.NewBlobURL(strings.TrimSuffix(virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING))
						resp, err := fblobURL.GetProperties(t.ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
						folderRelativePath := strings.TrimSuffix(virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING)
						folderRelativePath = strings.TrimPrefix(folderRelativePath, searchPrefix)
						// TODO: Need to debug why its giving error here
						if err == nil {
							extendedProp, _ := common.ReadStatFromMetadata(resp.NewMetadata(), resp.ContentLength())
							storedObject := newStoredObject(
								preprocessor,
								getObjectNameOnly(strings.TrimSuffix(virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING)),
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
							storedObject.lastChangeTime = extendedProp.CTime()
							storedObject.lastModifiedTime = extendedProp.MTime()
							storedObject.inode = extendedProp.INode()
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

						if err != nil {
							t.writeToErrorChannel(ErrorFileInfo{FilePath: folderRelativePath, IsDir: true, ErrorMsg: err, IsSource: t.isSource})
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

				storedObject := t.createStoredObjectForBlob(preprocessor, blobInfo, strings.TrimPrefix(blobInfo.Name, searchPrefix), containerName)
				extendedProp, _ := common.ReadStatFromMetadata(blobInfo.Metadata, *blobInfo.Properties.ContentLength)
				storedObject.lastChangeTime = extendedProp.CTime()
				storedObject.lastModifiedTime = extendedProp.MTime()
				if t.s2sPreserveSourceTags && blobInfo.BlobTags != nil {
					blobTagsMap := common.BlobTags{}
					for _, blobTag := range blobInfo.BlobTags.BlobTagSet {
						blobTagsMap[url.QueryEscape(blobTag.Key)] = url.QueryEscape(blobTag.Value)
					}
					storedObject.blobTags = blobTagsMap
				}

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

	FinalizeDirectory:
		if targetTraverser {
			// This storedObject marks the end of folder enumeration. Comparator after recieving end marker
			// do the finalize operation on this directory.
			// Note: Kept the containerName for debugging, mainly for multiple job with same source and different container.
			storedObject := StoredObject{
				name:              getObjectNameOnly(strings.TrimSuffix(currentDirPath, common.AZCOPY_PATH_SEPARATOR_STRING)),
				relativePath:      strings.TrimSuffix(currentDirPath, common.AZCOPY_PATH_SEPARATOR_STRING),
				entityType:        common.EEntityType.Folder(),
				ContainerName:     containerName,
				isFolderEndMarker: true,
				isFinalizeAll:     FinalizeAll,
			}
			enqueueOutput(storedObject, nil)
		}
		return nil
	}

	// initiate parallel scanning, starting at the root path
	workerContext, cancelWorkers := context.WithCancel(t.ctx)
	channels := parallel.Crawl(workerContext, searchPrefix+extraSearchPrefix, "" /* relBase */, enumerateOneDir, EnumerationParallelism, func() int64 {
		if t.indexerMap != nil {
			return t.indexerMap.getObjectIndexerMapSize()
		}
		panic("ObjectIndexerMap is nil")
	}, t.orderedTqueue, t.isSource, t.isSync, t.maxObjectIndexerSizeInGB)

	errChan := make(chan error, len(channels))
	var wg sync.WaitGroup
	processFunc := func(index int) {
		defer wg.Done()
		for {
			select {
			case x, ok := <-channels[index]:
				if !ok {
					return
				}

				if x.EnqueueToTqueue() {
					// Do the sanity check, EnqueueToTqueue should be true in case of sync operation and traverser is source.
					if !t.isSync || !t.isSource {
						panic(fmt.Sprintf("Entry set for enqueue to tqueue for invalid operation, isSync[%v], isSource[%v]", t.isSync, t.isSource))
					}

					_, err := x.Item()
					if err != nil {
						panic(fmt.Sprintf("Error set for entry which needs to be inserted to tqueue: %v", err))
					}

					//
					// This is a special CrawlResult which signifies that we need to enqueue the given directory to tqueue for
					// target traverser to process. Tell orderedTqueue so that it can add it in a proper child-after-parent
					// order.
					//
					item, _ := x.Item()
					t.orderedTqueue.MarkProcessed(x.Idx(), item)
					continue
				}

				item, workerError := x.Item()
				if workerError != nil {
					errChan <- workerError
					t.writeToErrorChannel(ErrorFileInfo{ErrorMsg: workerError, IsSource: t.isSource})
					cancelWorkers()
					return
				}

				object := item.(StoredObject)

				if t.incrementEnumerationCounter != nil {
					t.incrementEnumerationCounter(object.entityType)
				}

				processErr := processIfPassedFilters(filters, object, processor)
				_, processErr = getProcessingError(processErr)
				if processErr != nil {
					fmt.Printf("Traverser failed with error: %v", processErr)
					errChan <- processErr
					t.writeToErrorChannel(ErrorFileInfo{FileName: object.name, FilePath: object.relativePath, FileLastModifiedTime: object.lastModifiedTime, IsDir: object.entityType == common.EEntityType.Folder(), ErrorMsg: processErr, IsSource: t.isSource})
					cancelWorkers()
					return
				}
			case err := <-errChan:
				fmt.Printf("Some other thread received error, so coming out.")
				// Requeue the error for other go routines to read.
				errChan <- err
				return
			}
		}
	}

	for i := 0; i < len(channels); i++ {
		wg.Add(1)
		go processFunc(i)
	}
	wg.Wait()

	fmt.Printf("Done processing of blob traverser channels")
	if len(errChan) > 0 {
		err := <-errChan
		return err
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
	return object
}

func (t *blobTraverser) doesBlobRepresentAFolder(metadata azblob.Metadata) bool {
	util := copyHandlerUtil{}
	if !t.includeDirectoryStubs || !t.recursive {
		err := fmt.Errorf("Either includeDirectoryStubs[%v] or recursive[%v] flag is false", t.includeDirectoryStubs, t.recursive)
		panic(err.Error())
	}
	return util.doesBlobRepresentAFolder(metadata)
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

			// Setting blob tags
			if t.s2sPreserveSourceTags && blobInfo.BlobTags != nil {
				blobTagsMap := common.BlobTags{}
				for _, blobTag := range blobInfo.BlobTags.BlobTagSet {
					blobTagsMap[url.QueryEscape(blobTag.Key)] = url.QueryEscape(blobTag.Value)
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

		marker = listBlob.NextMarker
	}

	return nil
}

func newBlobTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, recursive, includeDirectoryStubs bool, incrementEnumerationCounter enumerationCounterFunc, s2sPreserveSourceTags bool,
	cpkOptions common.CpkOptions, includeDeleted, includeSnapshot, includeVersion bool, errorChannel chan ErrorFileInfo, indexerMap *folderIndexer, possiblyRenamedMap *possiblyRenamedMap, orderedTqueue parallel.OrderedTqueueInterface, isSource bool, isSync bool, maxObjectIndexerSizeInGB uint32,
	lastSyncTime time.Time, cfdMode common.CFDMode, metaDataOnlySync bool, scannerLogger common.ILoggerResetable) (t *blobTraverser) {
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

		// Sync related fields.
		isSync:                   isSync,
		errorChannel:             errorChannel,
		indexerMap:               indexerMap,
		orderedTqueue:            orderedTqueue,
		possiblyRenamedMap:       possiblyRenamedMap,
		isSource:                 isSource,
		lastSyncTime:             lastSyncTime,
		cfdMode:                  cfdMode,
		maxObjectIndexerSizeInGB: maxObjectIndexerSizeInGB,
		metaDataOnlySync:         metaDataOnlySync,
		scannerLogger:            scannerLogger,
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
