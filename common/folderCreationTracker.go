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
	"sync"
)

// folderCreationTracker is used to ensure than in an overwrite=false situation we
// only set folder properties on folders which were created by the current job. (To be consistent
// with the fact that when overwrite == false, we only set file properties on files created
// by the current job)
type FolderCreationTracker interface {
	RecordCreation(folder string)
	ShouldSetProperties(folder string, overwrite OverwriteOption) bool
	StopTracking(folder string)
}

func NewFolderCreationTracker() FolderCreationTracker {
	return &simpleFolderTracker{
		mu:       &sync.Mutex{},
		contents: make(map[string]struct{}),
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

func (f *simpleFolderTracker) ShouldSetProperties(folder string, overwrite OverwriteOption) bool {
	switch overwrite {
	case EOverwriteOption.True():
		return true
	case EOverwriteOption.Prompt(), // "prompt" is treated as "false" because otherwise we'd have to display, and maintain state for, two different prompts - one for folders and one for files, since its too hard to find wording for ONE prompt to cover both cases. (And having two prompts would confuse users).
		EOverwriteOption.False():

		f.mu.Lock()
		defer f.mu.Unlock()

		_, exists := f.contents[folder] // should only set properties if this job created the folder (i.e. it's in the map)
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
