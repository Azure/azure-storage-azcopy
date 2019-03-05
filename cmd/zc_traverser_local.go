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
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
)

type localTraverser struct {
	fullPath  string
	recursive bool

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter func()
}

func (t *localTraverser) traverse(processor objectProcessor, filters []objectFilter) (err error) {
	singleFileInfo, isSingleFile, err := t.getInfoIfSingleFile()

	if err != nil {
		return fmt.Errorf("cannot scan the path %s, please verify that it is a valid", t.fullPath)
	}

	// if the path is a single file, then pass it through the filters and send to processor
	if isSingleFile {
		t.incrementEnumerationCounter()
		err = processIfPassedFilters(filters, newStoredObject(singleFileInfo.Name(),
			"", // relative path makes no sense when the full path already points to the file
			singleFileInfo.ModTime(), singleFileInfo.Size(), nil, blobTypeNA), processor)
		return

	} else {
		if t.recursive {
			err = filepath.Walk(t.fullPath, func(filePath string, fileInfo os.FileInfo, fileError error) error {
				if err != nil {
					glcm.Info(fmt.Sprintf("Accessing %s failed with error %s", filePath, err))
					return nil
				}

				// skip the subdirectories
				if fileInfo.IsDir() {
					return nil
				}

				t.incrementEnumerationCounter()

				// the relative path needs to be computed from the full path
				computedRelativePath := strings.TrimPrefix(cleanLocalPath(filePath), t.fullPath)

				// leading path separators are trimmed away
				computedRelativePath = strings.TrimPrefix(computedRelativePath, common.AZCOPY_PATH_SEPARATOR_STRING)

				return processIfPassedFilters(filters, newStoredObject(fileInfo.Name(), computedRelativePath,
					fileInfo.ModTime(), fileInfo.Size(), nil, blobTypeNA), processor)
			})

			return
		} else {
			// if recursive is off, we only need to scan the files immediately under the fullPath
			files, err := ioutil.ReadDir(t.fullPath)
			if err != nil {
				return err
			}

			// go through the files and return if any of them fail to process
			for _, singleFile := range files {
				if singleFile.IsDir() {
					continue
				}

				t.incrementEnumerationCounter()
				err = processIfPassedFilters(filters, newStoredObject(singleFile.Name(), singleFile.Name(), singleFile.ModTime(), singleFile.Size(), nil, blobTypeNA), processor)

				if err != nil {
					return err
				}
			}
		}
	}

	return
}

func replacePathSeparators(path string) string {
	if os.PathSeparator != common.AZCOPY_PATH_SEPARATOR_CHAR {
		return strings.Replace(path, string(os.PathSeparator), common.AZCOPY_PATH_SEPARATOR_STRING, -1)
	} else {
		return path
	}
}

func (t *localTraverser) getInfoIfSingleFile() (os.FileInfo, bool, error) {
	fileInfo, err := os.Stat(t.fullPath)

	if err != nil {
		return nil, false, err
	}

	if fileInfo.IsDir() {
		return nil, false, nil
	}

	return fileInfo, true, nil
}

func newLocalTraverser(fullPath string, recursive bool, incrementEnumerationCounter func()) *localTraverser {
	traverser := localTraverser{
		fullPath:                    cleanLocalPath(fullPath),
		recursive:                   recursive,
		incrementEnumerationCounter: incrementEnumerationCounter}
	return &traverser
}

func cleanLocalPath(localPath string) string {
	normalizedPath := path.Clean(replacePathSeparators(localPath))

	// detect if we are targeting a network share
	if strings.HasPrefix(localPath, "//") || strings.HasPrefix(localPath, `\\`) {
		// if yes, we have trimmed away one of the leading slashes, so add it back
		normalizedPath = common.AZCOPY_PATH_SEPARATOR_STRING + normalizedPath
	}

	return normalizedPath
}
