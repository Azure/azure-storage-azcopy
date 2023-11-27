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
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
	"runtime"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const trailingDotErrMsg = "File share contains file/directory %s with a trailing dot but the trailing dot parameter was set to Disable, meaning these files could be potentially treated in an unsafe manner."
const invalidNameErrorMsg = "Skipping File share path %s, as it is not a valid Blob or Windows name. Rename the object and retry the transfer"

// allow us to iterate through a path pointing to the file endpoint
type fileTraverser struct {
	rawURL        string
	serviceClient *service.Client
	ctx           context.Context
	recursive     bool
	getProperties bool

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc
	trailingDot common.TrailingDotOption
	destination *common.Location
}

func createShareClientFromServiceClient(fileURLParts file.URLParts, client *service.Client) (*share.Client, error) {
	shareClient := client.NewShareClient(fileURLParts.ShareName)
	if fileURLParts.ShareSnapshot != "" {
		return shareClient.WithSnapshot(fileURLParts.ShareSnapshot)
	}
	return shareClient, nil
}

func createDirectoryClientFromServiceClient(fileURLParts file.URLParts, client *service.Client) (*directory.Client, error) {
	shareClient, err := createShareClientFromServiceClient(fileURLParts, client)
	if err != nil {
		return nil, err
	}
	directoryClient := shareClient.NewDirectoryClient(fileURLParts.DirectoryOrFilePath)
	return directoryClient, err
}

func createFileClientFromServiceClient(fileURLParts file.URLParts, client *service.Client) (*file.Client, error) {
	shareClient, err := createShareClientFromServiceClient(fileURLParts, client)
	if err != nil {
		return nil, err
	}
	fileClient := shareClient.NewRootDirectoryClient().NewFileClient(fileURLParts.DirectoryOrFilePath)
	return fileClient, err
}

func (t *fileTraverser) IsDirectory(bool) (bool, error) {
	// Azure file share case
	if gCopyUtil.urlIsContainerOrVirtualDirectory(t.rawURL) {
		return true, nil
	}

	// Need make request to ensure if it's directory
	fileURLParts, err := file.ParseURL(t.rawURL)
	if err != nil {
		return false, err
	}
	directoryClient := t.serviceClient.NewShareClient(fileURLParts.ShareName).NewDirectoryClient(fileURLParts.DirectoryOrFilePath)
	_, err = directoryClient.GetProperties(t.ctx, nil)
	if err != nil {
		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(common.LogWarning, fmt.Sprintf("Failed to check if the destination is a folder or a file (Azure Files). Assuming the destination is a file: %s", err))
		}
		return false, nil
	}

	return true, nil
}

func (t *fileTraverser) getPropertiesIfSingleFile() (*file.GetPropertiesResponse, bool, error) {
	fileURLParts, err := file.ParseURL(t.rawURL)
	if err != nil {
		return nil, false, err
	}
	fileClient, err := createFileClientFromServiceClient(fileURLParts, t.serviceClient)
	if err != nil {
		return nil, false, err
	}
	fileProps, filePropertiesErr := fileClient.GetProperties(t.ctx, nil)

	// if there was no problem getting the properties, it means that we are looking at a single file
	if filePropertiesErr == nil {
		return &fileProps, true, nil
	}

	return nil, false, nil
}

func (t *fileTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	invalidBlobOrWindowsName := func(path string) bool {
		if t.destination != nil {
			if (t.destination.IsLocal() && runtime.GOOS == "windows") || *t.destination == common.ELocation.Blob() || *t.destination == common.ELocation.BlobFS() {
				/* Blob or Windows object name is invalid if it ends with period or
				   one of (virtual) directories in path ends with period.
				   This list is not exhaustive
				*/
				return strings.HasSuffix(path, ".") ||
					strings.Contains(path, "./")
			}
		}
		return false
	}
	targetURLParts, err := file.ParseURL(t.rawURL)
	if err != nil {
		return err
	}

	// if not pointing to a share, check if we are pointing to a single file
	if targetURLParts.DirectoryOrFilePath != "" {
		if invalidBlobOrWindowsName(targetURLParts.DirectoryOrFilePath) {
			WarnStdoutAndScanningLog(fmt.Sprintf(invalidNameErrorMsg, targetURLParts.DirectoryOrFilePath))
			return common.EAzError.InvalidBlobOrWindowsName()
		}
		if t.trailingDot != common.ETrailingDotOption.Enable() && strings.HasSuffix(targetURLParts.DirectoryOrFilePath, ".") {
			azcopyScanningLogger.Log(common.LogWarning, fmt.Sprintf(trailingDotErrMsg, getObjectNameOnly(targetURLParts.DirectoryOrFilePath)))
		}
		// check if the url points to a single file
		fileProperties, isFile, err := t.getPropertiesIfSingleFile()
		if err != nil {
			return err
		}
		if isFile {
			if azcopyScanningLogger != nil {
				azcopyScanningLogger.Log(common.LogDebug, "Detected the root as a file.")
			}

			storedObject := newStoredObject(
				preprocessor,
				getObjectNameOnly(targetURLParts.DirectoryOrFilePath),
				"",
				common.EEntityType.File(),
				*fileProperties.LastModified,
				*fileProperties.ContentLength,
				shareFilePropertiesAdapter{fileProperties},
				noBlobProps,
				fileProperties.Metadata,
				targetURLParts.ShareName,
			)

			storedObject.smbLastModifiedTime = *fileProperties.FileLastWriteTime

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
		fileURLParts, err := file.ParseURL(f.url)
		if err != nil {
			return nil, err
		}
		targetPath := strings.TrimSuffix(targetURLParts.DirectoryOrFilePath, common.AZCOPY_PATH_SEPARATOR_STRING)
		relativePath := strings.TrimPrefix(fileURLParts.DirectoryOrFilePath, targetPath)
		relativePath = strings.TrimPrefix(relativePath, common.AZCOPY_PATH_SEPARATOR_STRING)

		size := f.contentLength
		// We need to omit some properties if we don't get properties
		var lmt time.Time
		var smbLMT time.Time
		var contentProps contentPropsProvider = noContentProps
		var metadata common.Metadata

		// Only get the properties if we're told to
		if t.getProperties {
			var fullProperties filePropsProvider
			fullProperties, err = f.propertyGetter(t.ctx)
			if err != nil {
				return StoredObject{
					relativePath: relativePath,
				}, err
			}
			lmt = fullProperties.LastModified()
			smbLMT = fullProperties.FileLastWriteTime()
			contentProps = fullProperties
			// Get an up-to-date size, because it's documented that the size returned by the listing might not be up-to-date,
			// if an SMB client has modified by not yet closed the file. (See https://docs.microsoft.com/en-us/rest/api/storageservices/list-directories-and-files)
			// Doing this here makes sure that our size is just as up-to-date as our LMT .
			// (If s2s-detect-source-changed is false, then this code won't run.  If if its false, we don't check for modifications anyway,
			// so it's fair to assume that the size will stay equal to that returned at by the listing operation)
			size = fullProperties.ContentLength()
			metadata = fullProperties.Metadata()
		}
		obj := newStoredObject(
			preprocessor,
			getObjectNameOnly(f.name),
			relativePath,
			f.entityType,
			lmt,
			size,
			contentProps,
			noBlobProps,
			metadata,
			targetURLParts.ShareName,
		)

		obj.smbLastModifiedTime = smbLMT

		return obj, nil
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
	directoryClient, err := createDirectoryClientFromServiceClient(targetURLParts, t.serviceClient)
	if err != nil {
		return err
	}

	// Our rule is that enumerators of folder-aware sources should include the root folder's properties.
	// So include the root dir/share in the enumeration results, if it exists or is just the share root.
	_, err = directoryClient.GetProperties(t.ctx, nil)
	if err == nil || targetURLParts.DirectoryOrFilePath == "" {
		s, err := convertToStoredObject(newAzFileRootDirectoryEntity(directoryClient, ""))
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
		currentDirectoryClient := dir.(*directory.Client)
		pager := currentDirectoryClient.NewListFilesAndDirectoriesPager(nil)
		var marker *string
		for pager.More() {
			lResp, err := pager.NextPage(t.ctx)
			if err != nil {
				return fmt.Errorf("cannot list files due to reason %s", err)
			}
			for _, fileInfo := range lResp.Segment.Files {
				if invalidBlobOrWindowsName(*fileInfo.Name) {
					//Throw a warning on console and continue
					WarnStdoutAndScanningLog(fmt.Sprintf(invalidNameErrorMsg, *fileInfo.Name))
					continue
				} else {
					if t.trailingDot != common.ETrailingDotOption.Enable() && strings.HasSuffix(*fileInfo.Name, ".") {
						azcopyScanningLogger.Log(common.LogWarning, fmt.Sprintf(trailingDotErrMsg, *fileInfo.Name))
					}
				}
				enqueueOutput(newAzFileFileEntity(currentDirectoryClient, fileInfo), nil)

			}
			for _, dirInfo := range lResp.Segment.Directories {
				if invalidBlobOrWindowsName(*dirInfo.Name) {
					//Throw a warning on console and continue
					WarnStdoutAndScanningLog(fmt.Sprintf(invalidNameErrorMsg, *dirInfo.Name))
					continue
				} else {
					if t.trailingDot != common.ETrailingDotOption.Enable() && strings.HasSuffix(*dirInfo.Name, ".") {
						azcopyScanningLogger.Log(common.LogWarning, fmt.Sprintf(trailingDotErrMsg, *dirInfo.Name))
					}
				}
				enqueueOutput(newAzFileSubdirectoryEntity(currentDirectoryClient, *dirInfo.Name), nil)
				if t.recursive {
						// If recursive is turned on, add sub directories to be processed
					enqueueDir(currentDirectoryClient.NewSubdirectoryClient(*dirInfo.Name))
				}

			}

			// if debug mode is on, note down the result, this is not going to be fast
			if azcopyScanningLogger != nil && azcopyScanningLogger.ShouldLog(common.LogDebug) {
				tokenValue := "NONE"
				if marker != nil {
					tokenValue = *marker
				}

				var dirListBuilder strings.Builder
				for _, dir := range lResp.Segment.Directories {
					fmt.Fprintf(&dirListBuilder, " %s,", *dir.Name)
				}
				var fileListBuilder strings.Builder
				for _, fileInfo := range lResp.Segment.Files {
					fmt.Fprintf(&fileListBuilder, " %s,", *fileInfo.Name)
				}

				fileURLParts, err := file.ParseURL(currentDirectoryClient.URL())
				if err != nil {
					return err
				}
				directoryName := fileURLParts.DirectoryOrFilePath
				msg := fmt.Sprintf("Enumerating %s with token %s. Sub-dirs:%s Files:%s", directoryName,
					tokenValue, dirListBuilder.String(), fileListBuilder.String())
				azcopyScanningLogger.Log(common.LogDebug, msg)
			}

			marker = lResp.NextMarker
		}
		return nil
	}

	// run the actual enumeration.
	// First part is a parallel directory crawl
	// Second part is parallel conversion of the directories and files to stored objects. This is necessary because the conversion to stored object may hit the network and therefore be slow if not parallelized
	parallelism := EnumerationParallelism // for Azure Files we'll run two pools of this size, one for crawl and one for transform

	workerContext, cancelWorkers := context.WithCancel(t.ctx)

	cCrawled := parallel.Crawl(workerContext, directoryClient, enumerateOneDir, parallelism)

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
				azcopyScanningLogger.Log(common.LogWarning, workerError.Error())
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

func newFileTraverser(rawURL string, serviceClient *service.Client, ctx context.Context, recursive, getProperties bool, incrementEnumerationCounter enumerationCounterFunc, trailingDot common.TrailingDotOption, destination *common.Location) (t *fileTraverser) {
	t = &fileTraverser{
		rawURL: rawURL,
		serviceClient: serviceClient,
		ctx: ctx,
		recursive: recursive,
		getProperties: getProperties,
		incrementEnumerationCounter: incrementEnumerationCounter,
		trailingDot: trailingDot,
		destination: destination,
	}
	return
}

// allows polymorphic treatment of folders and files
type azfileEntity struct {
	name           string
	contentLength  int64
	url            string
	propertyGetter func(ctx context.Context) (filePropsProvider, error)
	entityType     common.EntityType
}

func newAzFileFileEntity(parentDirectoryClient *directory.Client, fileInfo *directory.File) azfileEntity {
	fileClient := parentDirectoryClient.NewFileClient(*fileInfo.Name)
	return azfileEntity{
		*fileInfo.Name,
		*fileInfo.Properties.ContentLength,
		fileClient.URL(),
		func(ctx context.Context) (filePropsProvider, error) {
			fileProperties, err := fileClient.GetProperties(ctx, nil)
			if err != nil {
				return nil, err
			}
			return shareFilePropertiesAdapter{&fileProperties}, nil
		},
		common.EEntityType.File(),
	}
}

func newAzFileSubdirectoryEntity(parentDirectoryClient *directory.Client, dirName string) azfileEntity {
	directoryClient := parentDirectoryClient.NewSubdirectoryClient(dirName)
	return newAzFileRootDirectoryEntity(directoryClient, dirName) // now that we have directoryClient, the logic is same as if it was the root
}

func newAzFileRootDirectoryEntity(directoryClient *directory.Client, name string) azfileEntity {
	return azfileEntity{
		name,
		0,
		directoryClient.URL(),
		func(ctx context.Context) (filePropsProvider, error) {
			directoryProperties, err := directoryClient.GetProperties(ctx, nil)
			if err != nil {
				return nil, err
			}
			return shareDirectoryPropertiesAdapter{&directoryProperties}, nil
		},
		common.EEntityType.Folder(),
	}
}
