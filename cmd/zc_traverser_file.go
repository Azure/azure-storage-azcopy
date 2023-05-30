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
	"strconv"
	"strings"
	"time"

	"github.com/aymanjarrousms/azure-storage-azcopy/v10/common/parallel"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/aymanjarrousms/azure-storage-file-go/azfile"

	"github.com/aymanjarrousms/azure-storage-azcopy/v10/common"
)

// allow us to iterate through a path pointing to the file endpoint
type fileTraverser struct {
	rawURL        *url.URL
	p             pipeline.Pipeline
	ctx           context.Context
	recursive     bool
	getProperties bool

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc

	// Fields applicable only to sync operation.
	// isSync boolean tells whether its copy operation or sync operation.
	isSync bool
}

func (t *fileTraverser) IsDirectory(bool) bool {
	return copyHandlerUtil{}.urlIsAzureFileDirectory(t.ctx, t.rawURL, t.p) // This handles all of the fanciness for us.
}

func (t *fileTraverser) getPropertiesIfSingleFile() (*azfile.FileGetPropertiesResponse, bool) {
	fileURL := azfile.NewFileURL(*t.rawURL, t.p)
	fileProps, filePropertiesErr := fileURL.GetProperties(t.ctx)

	// if there was no problem getting the properties, it means that we are looking at a single file
	if filePropertiesErr == nil {
		return fileProps, true
	}

	return nil, false
}

func (t *fileTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	targetURLParts := azfile.NewFileURLParts(*t.rawURL)

	// if not pointing to a share, check if we are pointing to a single file
	if targetURLParts.DirectoryOrFilePath != "" {
		// check if the url points to a single file
		fileProperties, isFile := t.getPropertiesIfSingleFile()
		if isFile {
			if azcopyScanningLogger != nil {
				azcopyScanningLogger.Log(pipeline.LogDebug, "Detected the root as a file.")
			}

			storedObject := newStoredObject(
				preprocessor,
				getObjectNameOnly(targetURLParts.DirectoryOrFilePath),
				"",
				common.EEntityType.File(),
				fileProperties.LastModified(),
				fileProperties.ContentLength(),
				fileProperties,
				noBlobProps,
				common.FromAzFileMetadataToCommonMetadata(fileProperties.NewMetadata()), // .NewMetadata() seems odd to call here, but it does actually obtain the metadata.
				targetURLParts.ShareName,
			)

			storedObject.inode, err = strconv.ParseUint(fileProperties.FileID(), 10, 64)

			if t.incrementEnumerationCounter != nil {
				t.incrementEnumerationCounter(common.EEntityType.File())
			}

			err := processIfPassedFilters(filters, storedObject, processor)
			_, err = getProcessingError(err)
			return err
		}
	}

	// else, its not just one file

	// This func must be threadsafe/goroutine safe
	convertToStoredObject := func(input parallel.InputObject) (parallel.OutputObject, error) {
		f := input.(azfileEntity)
		// compute the relative path of the file with respect to the target directory
		fileURLParts := azfile.NewFileURLParts(f.url)
		relativePath := strings.TrimPrefix(fileURLParts.DirectoryOrFilePath, targetURLParts.DirectoryOrFilePath)
		relativePath = strings.TrimPrefix(relativePath, common.AZCOPY_PATH_SEPARATOR_STRING)

		size := f.contentLength

		// We need to omit some properties if we don't get properties
		lmt := time.Time{}
		var fileId string
		var contentProps contentPropsProvider = noContentProps
		var meta common.Metadata = nil

		// Only get the properties if we're told to
		if t.getProperties {
			var fullProperties azfilePropertiesAdapter
			fullProperties, err = f.propertyGetter(t.ctx)
			if err != nil {
				return StoredObject{
					relativePath: relativePath,
				}, err
			}
			lmt = fullProperties.LastModified()
			if f.entityType == common.EEntityType.File() {
				contentProps = fullProperties.(*azfile.FileGetPropertiesResponse) // only files have content props. Folders don't.
				// Get an up-to-date size, because it's documented that the size returned by the listing might not be up-to-date,
				// if an SMB client has modified by not yet closed the file. (See https://docs.microsoft.com/en-us/rest/api/storageservices/list-directories-and-files)
				// Doing this here makes sure that our size is just as up-to-date as our LMT .
				// (If s2s-detect-source-changed is false, then this code won't run.  If if its false, we don't check for modifications anyway,
				// so it's fair to assume that the size will stay equal to that returned at by the listing operation)
				size = fullProperties.(*azfile.FileGetPropertiesResponse).ContentLength()
			}

			fileId = fullProperties.FileID()
			meta = common.FromAzFileMetadataToCommonMetadata(fullProperties.NewMetadata())
		}

		so := newStoredObject(
			preprocessor,
			getObjectNameOnly(f.name),
			relativePath,
			f.entityType,
			lmt,
			size,
			contentProps,
			noBlobProps,
			meta,
			targetURLParts.ShareName,
		)

		so.inode, err = strconv.ParseUint(fileId, 10, 64)
		so.isRootDirectory = f.rootDirectory
		so.isFolderEndMarker = f.isDirectoryEndMarker

		return so, err
	}

	processStoredObject := func(s StoredObject) error {
		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(s.entityType)
		}
		err := processIfPassedFilters(filters, s, processor)
		_, err = getProcessingError(err)
		return err
	}

	// get the directory URL so that we can list the files
	directoryURL := azfile.NewDirectoryURL(targetURLParts.URL(), t.p)

	// Our rule is that enumerators of folder-aware sources should include the root folder's properties.
	// So include the root dir/share in the enumeration results, if it exists or is just the share root.
	_, err = directoryURL.GetProperties(t.ctx)
	if err == nil || targetURLParts.DirectoryOrFilePath == "" {
		s, err := convertToStoredObject(newAzFileRootFolderEntity(directoryURL, "", true, false))
		if err != nil {
			return err
		}
		err = processStoredObject(s.(StoredObject))
		if err != nil {
			return err
		}
	}

	// Define how to enumerate its contents
	// This func must be threadsafe/goroutine safe
	enumerateOneDir := func(dir parallel.Directory, enqueueDir func(parallel.Directory), enqueueOutput func(parallel.DirectoryEntry, error)) error {
		currentDirURL := dir.(azfile.DirectoryURL)
		for marker := (azfile.Marker{}); marker.NotDone(); {
			lResp, err := currentDirURL.ListFilesAndDirectoriesSegment(t.ctx, marker, azfile.ListFilesAndDirectoriesOptions{})
			if err != nil {
				return fmt.Errorf("cannot list files due to reason %s", err)
			}
			for _, fileInfo := range lResp.FileItems {
				enqueueOutput(newAzFileFileEntity(currentDirURL, fileInfo), nil)
			}
			for _, dirInfo := range lResp.DirectoryItems {
				if !t.isSync {
					enqueueOutput(newAzFileChildFolderEntity(currentDirURL, dirInfo.Name, false), nil)
				}

				if t.recursive {
					// If recursive is turned on, add sub directories to be processed
					enqueueDir(currentDirURL.NewDirectoryURL(dirInfo.Name))
				}
			}

			// if debug mode is on, note down the result, this is not going to be fast
			if azcopyScanningLogger != nil && azcopyScanningLogger.ShouldLog(pipeline.LogDebug) {
				tokenValue := "NONE"
				if marker.Val != nil {
					tokenValue = *marker.Val
				}

				var dirListBuilder strings.Builder
				for _, dir := range lResp.DirectoryItems {
					fmt.Fprintf(&dirListBuilder, " %s,", dir.Name)
				}
				var fileListBuilder strings.Builder
				for _, fileInfo := range lResp.FileItems {
					fmt.Fprintf(&fileListBuilder, " %s,", fileInfo.Name)
				}

				dirName := azfile.NewFileURLParts(currentDirURL.URL()).DirectoryOrFilePath
				msg := fmt.Sprintf("Enumerating %s with token %s. Sub-dirs:%s Files:%s", dirName,
					tokenValue, dirListBuilder.String(), fileListBuilder.String())
				azcopyScanningLogger.Log(pipeline.LogDebug, msg)
			}

			marker = lResp.NextMarker
		}

		if t.isSync {
			// Enqueue the current directory with isFolderEndMarker = true, to trigger FinalizeDirectory
			enqueueOutput(
				newAzFileRootFolderEntity(
					currentDirURL,
					strings.TrimSuffix(currentDirURL.URL().Path,
						common.AZCOPY_PATH_SEPARATOR_STRING),
					false, /*isRoot*/
					true /*isFolderEndMarker*/),
				nil)
		}

		return nil
	}

	// run the actual enumeration.
	// First part is a parallel directory crawl
	// Second part is parallel conversion of the directories and files to stored objects. This is necessary because the conversion to stored object may hit the network and therefore be slow if not parallelized
	parallelism := EnumerationParallelism // for Azure Files we'll run two pools of this size, one for crawl and one for transform

	workerContext, cancelWorkers := context.WithCancel(t.ctx)

	cCrawled := parallel.Crawl(workerContext, directoryURL, "" /* relBase */, enumerateOneDir, parallelism, nil /* getObjectIndexerMapSize */, nil, /* tqueue */
		false /* isSource */, false /* isSync */, 0 /* maxObjectIndexrSizeInGB */)

	cTransformed := parallel.Transform(workerContext, cCrawled, convertToStoredObject, parallelism)

	for x := range cTransformed {
		item, workerError := x.Item()
		if workerError != nil {
			relativePath := ""
			if item != nil {
				relativePath = item.(StoredObject).relativePath
			}
			glcm.Info("Failed to scan directory/file " + relativePath + ". Logging errors in scanning logs.")
			if azcopyScanningLogger != nil {
				azcopyScanningLogger.Log(pipeline.LogWarning, workerError.Error())
			}
			continue
		}
		processErr := processStoredObject(item.(StoredObject))
		if processErr != nil {
			cancelWorkers()
			return processErr
		}
	}

	cancelWorkers()
	return
}

func newFileTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, recursive, getProperties bool, incrementEnumerationCounter enumerationCounterFunc, isSync bool) (t *fileTraverser) {
	t = &fileTraverser{rawURL: rawURL, p: p, ctx: ctx, recursive: recursive, getProperties: getProperties, incrementEnumerationCounter: incrementEnumerationCounter, isSync: isSync}
	return
}

//  allows polymorphic treatment of folders and files
type azfileEntity struct {
	name                 string
	contentLength        int64
	url                  url.URL
	propertyGetter       func(ctx context.Context) (azfilePropertiesAdapter, error)
	entityType           common.EntityType
	rootDirectory        bool
	isDirectoryEndMarker bool
}

func newAzFileFileEntity(containingDir azfile.DirectoryURL, fileInfo azfile.FileItem) azfileEntity {
	fu := containingDir.NewFileURL(fileInfo.Name)
	return azfileEntity{
		fileInfo.Name,
		fileInfo.Properties.ContentLength,
		fu.URL(),
		func(ctx context.Context) (azfilePropertiesAdapter, error) { return fu.GetProperties(ctx) },
		common.EEntityType.File(),
		false,
		false, /*isFolderEndMarker*/
	}
}

func newAzFileChildFolderEntity(containingDir azfile.DirectoryURL, dirName string, isDirectoryEndMarker bool) azfileEntity {
	du := containingDir.NewDirectoryURL(dirName)
	return newAzFileRootFolderEntity(du, dirName /*isRoot*/, false, isDirectoryEndMarker) // now that we have du, the logic is same as if it was the root
}

func newAzFileRootFolderEntity(rootDir azfile.DirectoryURL, name string, isRoot, isDirectoryEndMarker bool) azfileEntity {
	return azfileEntity{
		name,
		0,
		rootDir.URL(),
		func(ctx context.Context) (azfilePropertiesAdapter, error) { return rootDir.GetProperties(ctx) },
		common.EEntityType.Folder(),
		isRoot,
		isDirectoryEndMarker,
	}
}

// azureFilesMetadataAdapter allows polymorphic treatment of File and Folder properties, since both implement the method
type azfilePropertiesAdapter interface {
	NewMetadata() azfile.Metadata
	LastModified() time.Time
	FileID() string
}
