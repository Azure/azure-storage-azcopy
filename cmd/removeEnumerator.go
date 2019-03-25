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

import "github.com/Azure/azure-storage-file-go/azfile"

// provide an enumerator that lists a given blob resource (could be a blob or virtual dir)
// and schedule delete transfers to remove them
// TODO consider merging with newRemoveFileEnumerator
func newRemoveBlobEnumerator(cca *cookedCopyCmdArgs) (enumerator *copyEnumerator, err error) {
	sourceTraverser, err := newBlobTraverserForRemove(cca)
	if err != nil {
		return nil, err
	}

	// check if we are targeting a single blob
	_, isSingleBlob := sourceTraverser.getPropertiesIfSingleBlob()

	transferScheduler := newRemoveTransferProcessor(cca, NumOfFilesPerDispatchJobPart, isSingleBlob)
	includeFilters := buildIncludeFilters(cca.includePatterns)
	excludeFilters := buildExcludeFilters(cca.excludePatterns)

	// set up the filters in the right order
	filters := append(includeFilters, excludeFilters...)

	finalize := func() error {
		jobInitiated, err := transferScheduler.dispatchFinalPart()
		if err != nil {
			return err
		}

		if !jobInitiated {
			glcm.Error("Nothing to delete. Please verify that recursive flag is set properly if targeting a directory.")
		}

		return nil
	}

	return newCopyEnumerator(sourceTraverser, filters, transferScheduler.scheduleCopyTransfer, finalize), nil
}

// provide an enumerator that lists a given Azure File resource (could be a file or dir)
// and schedule delete transfers to remove them
// note that for a directory to be removed, it has to be emptied first
func newRemoveFileEnumerator(cca *cookedCopyCmdArgs) (enumerator *copyEnumerator, err error) {
	sourceTraverser, err := newFileTraverserForRemove(cca)
	if err != nil {
		return nil, err
	}

	// check if we are targeting a single blob
	_, isSingleFile := sourceTraverser.getPropertiesIfSingleFile()

	transferScheduler := newRemoveTransferProcessor(cca, NumOfFilesPerDispatchJobPart, isSingleFile)
	includeFilters := buildIncludeFilters(cca.includePatterns)
	excludeFilters := buildExcludeFilters(cca.excludePatterns)

	// set up the filters in the right order
	filters := append(includeFilters, excludeFilters...)

	finalize := func() error {
		jobInitiated, err := transferScheduler.dispatchFinalPart()
		if err != nil {
			return err
		}

		if !jobInitiated {
			glcm.Error("Nothing to delete. Please verify that recursive flag is set properly if targeting a directory.")
		}

		return nil
	}

	return newCopyEnumerator(sourceTraverser, filters, transferScheduler.scheduleCopyTransfer, finalize), nil
}

type directoryStack []azfile.DirectoryURL

func (s *directoryStack) Push(d azfile.DirectoryURL) {
	*s = append(*s, d)
}

func (s *directoryStack) Pop() (*azfile.DirectoryURL, bool) {
	l := len(*s)

	if l == 0 {
		return nil, false
	} else {
		e := (*s)[l-1]
		*s = (*s)[:l-1]
		return &e, true
	}
}
