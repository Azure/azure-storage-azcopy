// Copyright Microsoft <wastore@microsoft.com>
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

package common

import (
	"net/url"
	"strings"
	"sync"
)

// folderCreationTracker is used to ensure than in an overwrite=false situation we
// only set folder properties on folders which were created by the current job. (To be consistent
// with the fact that when overwrite == false, we only set file properties on files created
// by the current job)
type FolderCreationTracker interface {
	RecordCreation(folder string)
	ShouldSetProperties(folder string, overwrite OverwriteOption, prompter prompter) bool
	StopTracking(folder string)
}

type prompter interface {
	ShouldOverwrite(objectPath string, objectType EntityType) bool
}

func NewFolderCreationTracker(fpo FolderPropertyOption) FolderCreationTracker {
	switch fpo {
	case EFolderPropertiesOption.AllFolders(),
		EFolderPropertiesOption.AllFoldersExceptRoot():
		return &simpleFolderTracker{
			mu:       &sync.Mutex{},
			contents: make(map[string]struct{}),
		}
	case EFolderPropertiesOption.NoFolders():
		// can't use simpleFolderTracker here, because when no folders are processed,
		// then StopTracking will never be called, so we'll just use more and more memory for the map
		return &nullFolderTracker{}
	default:
		panic("unknown folderPropertiesOption")
	}
}

type simpleFolderTracker struct {
	mu       *sync.Mutex
	contents map[string]struct{}
}

func (f *simpleFolderTracker) RecordCreation(folder string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.contents[folder] = struct{}{}
}

func (f *simpleFolderTracker) ShouldSetProperties(folder string, overwrite OverwriteOption, prompter prompter) bool {
	switch overwrite {
	case EOverwriteOption.True():
		return true
	case EOverwriteOption.Prompt(),
		EOverwriteOption.IfSourceNewer(), // TODO discuss if this case should be treated differently than false
		EOverwriteOption.False():

		f.mu.Lock()
		defer f.mu.Unlock()

		_, exists := f.contents[folder] // should only set properties if this job created the folder (i.e. it's in the map)

		// prompt only if we didn't create this folder
		if overwrite == EOverwriteOption.Prompt() && !exists {
			cleanedFolderPath := folder

			// if it's a local Windows path, skip since it doesn't have SAS and won't parse correctly as an URL
			if !strings.HasPrefix(folder, EXTENDED_PATH_PREFIX) {
				// get rid of SAS before prompting
				parsedURL, _ := url.Parse(folder)

				// really make sure that it's not a local path
				if parsedURL.Scheme != "" && parsedURL.Host != "" {
					parsedURL.RawQuery = ""
					cleanedFolderPath = parsedURL.String()
				}
			}
			return prompter.ShouldOverwrite(cleanedFolderPath, EEntityType.Folder())
		}

		return exists

	default:
		panic("unknown overwrite option")
	}
}

// stopTracking is useful to prevent too much memory usage in large jobs
func (f *simpleFolderTracker) StopTracking(folder string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.contents, folder)
}

type nullFolderTracker struct{}

func (f *nullFolderTracker) RecordCreation(folder string) {
	// no-op (the null tracker doesn't track anything)
}

func (f *nullFolderTracker) ShouldSetProperties(folder string, overwrite OverwriteOption, prompter prompter) bool {
	// There's no way this should ever be called, because we only create the nullTracker if we are
	// NOT transferring folder info.
	panic("wrong type of folder tracker has been instantiated. This type does not do any tracking")
}

func (f *nullFolderTracker) StopTracking(folder string) {
	// noop (because we don't track anything)
}
