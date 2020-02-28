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
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-storage-azcopy/common"
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
}

func (t *fileTraverser) isDirectory(bool) bool {
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

func (t *fileTraverser) traverse(preprocessor objectMorpher, processor objectProcessor, filters []objectFilter) (err error) {
	targetURLParts := azfile.NewFileURLParts(*t.rawURL)

	// if not pointing to a share, check if we are pointing to a single file
	if targetURLParts.DirectoryOrFilePath != "" {
		// check if the url points to a single file
		fileProperties, isFile := t.getPropertiesIfSingleFile()
		if isFile {
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

			if t.incrementEnumerationCounter != nil {
				t.incrementEnumerationCounter(common.EEntityType.File())
			}

			return processIfPassedFilters(filters, storedObject, processor)
		}
	}

	// else, its not just one file

	processEntity := func(f azfileEntity) error {
		// compute the relative path of the file with respect to the target directory
		fileURLParts := azfile.NewFileURLParts(f.url)
		relativePath := strings.TrimPrefix(fileURLParts.DirectoryOrFilePath, targetURLParts.DirectoryOrFilePath)
		relativePath = strings.TrimPrefix(relativePath, common.AZCOPY_PATH_SEPARATOR_STRING)

		// We need to omit some properties if we don't get properties
		lmt := time.Time{}
		var contentProps contentPropsProvider = noContentProps
		var meta common.Metadata = nil

		// Only get the properties if we're told to
		if t.getProperties {
			var fullProperties azfilePropertiesAdapter
			fullProperties, err = f.propertyGetter(t.ctx)
			if err != nil {
				return err
			}
			lmt = fullProperties.LastModified()
			if f.entityType == common.EEntityType.File() {
				contentProps = fullProperties.(*azfile.FileGetPropertiesResponse) // only files have content props. Folders don't.
			}
			meta = common.FromAzFileMetadataToCommonMetadata(fullProperties.NewMetadata())
		}
		storedObject := newStoredObject(
			preprocessor,
			getObjectNameOnly(f.name),
			relativePath,
			f.entityType,
			lmt,
			f.contentLength,
			contentProps,
			noBlobProps,
			meta,
			targetURLParts.ShareName,
		)

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(f.entityType)
		}
		return processIfPassedFilters(filters, storedObject, processor)
	}

	// get the directory URL so that we can list the files
	directoryURL := azfile.NewDirectoryURL(targetURLParts.URL(), t.p)

	// Our rule is that enumerators of folder-aware sources should include the root folder's properties.
	// So include the root dir in the enumeration results (If we can be sure it exists. If we can't see its properties, there's no point in putting it in the list, since we'd have no information about it)
	_, err = directoryURL.GetProperties(t.ctx)
	if err == nil {
		err = processEntity(newAzFileRootFolderEntity(&directoryURL, ""))
		if err != nil {
			return err
		}
	}

	// Enumerate its contents
	dirStack := &directoryStack{}
	dirStack.Push(directoryURL)
	for currentDirURL, ok := dirStack.Pop(); ok; currentDirURL, ok = dirStack.Pop() {
		// Perform list files and directories.
		for marker := (azfile.Marker{}); marker.NotDone(); {
			lResp, err := currentDirURL.ListFilesAndDirectoriesSegment(t.ctx, marker, azfile.ListFilesAndDirectoriesOptions{})
			if err != nil {
				return fmt.Errorf("cannot list files due to reason %s", err)
			}

			// Process the files and folders we listed
			fs := make([]azfileEntity, 0, len(lResp.FileItems)+len(lResp.DirectoryItems))
			for _, fileInfo := range lResp.FileItems {
				fs = append(fs, newAzFileFileEntity(currentDirURL, fileInfo))
			}
			for _, dirInfo := range lResp.DirectoryItems {
				fs = append(fs, newAzFileChildFolderEntity(currentDirURL, dirInfo.Name))
			}
			for _, f := range fs {
				processErr := processEntity(f)
				if processErr != nil {
					return processErr
				}
			}

			// If recursive is turned on, add sub directories.
			if t.recursive {
				for _, dirInfo := range lResp.DirectoryItems {
					d := currentDirURL.NewDirectoryURL(dirInfo.Name)
					dirStack.Push(d)
				}
			}
			marker = lResp.NextMarker
		}
	}

	return
}

func newFileTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, recursive, getProperties bool, incrementEnumerationCounter enumerationCounterFunc) (t *fileTraverser) {
	t = &fileTraverser{rawURL: rawURL, p: p, ctx: ctx, recursive: recursive, getProperties: getProperties, incrementEnumerationCounter: incrementEnumerationCounter}
	return
}

//  allows polymorphic treatment of folders and files
type azfileEntity struct {
	name           string
	contentLength  int64
	url            url.URL
	propertyGetter func(ctx context.Context) (azfilePropertiesAdapter, error)
	entityType     common.EntityType
}

func newAzFileFileEntity(containingDir *azfile.DirectoryURL, fileInfo azfile.FileItem) azfileEntity {
	fu := containingDir.NewFileURL(fileInfo.Name)
	return azfileEntity{
		fileInfo.Name,
		fileInfo.Properties.ContentLength,
		fu.URL(),
		func(ctx context.Context) (azfilePropertiesAdapter, error) { return fu.GetProperties(ctx) },
		common.EEntityType.File(),
	}
}

func newAzFileChildFolderEntity(containingDir *azfile.DirectoryURL, dirName string) azfileEntity {
	du := containingDir.NewDirectoryURL(dirName)
	return newAzFileRootFolderEntity(&du, dirName) // now that we have du, the logic is same as if it was the root
}

func newAzFileRootFolderEntity(rootDir *azfile.DirectoryURL, name string) azfileEntity {
	return azfileEntity{
		name,
		0,
		rootDir.URL(),
		func(ctx context.Context) (azfilePropertiesAdapter, error) { return rootDir.GetProperties(ctx) },
		common.EEntityType.Folder(),
	}
}

// azureFilesMetadataAdapter allows polymorphic treatment of File and Folder properties, since both implement the method
type azfilePropertiesAdapter interface {
	NewMetadata() azfile.Metadata
	LastModified() time.Time
}
