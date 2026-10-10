// Copyright © 2019 Microsoft <wastore@microsoft.com>
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
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// a meta traverser that goes through a list of paths (potentially directory entities) and scans them one by one
// behaves like a single traverser (basically a "traverser of traverser")
type listTraverser struct {
	listReader              <-chan string
	recursive               bool
	childTraverserGenerator childTraverserGenerator

	// When set, entries that cannot be enumerated are sent here instead of
	// being skipped with a log line; see
	// InitResourceTraverserOptions.ReportListOfFilesEntryErrors.
	entryErrorChannel chan<- TraverserErrorItemInfo
	location          common.Location
	ctx               context.Context

	// directoriesSelfOnly: see
	// InitResourceTraverserOptions.ListOfFilesDirectoriesSelfOnly.
	directoriesSelfOnly bool

	// parallelism: see InitResourceTraverserOptions.ListOfFilesParallelism.
	parallelism int
}

// selfTraverser is implemented by traversers that can enumerate only the
// entity their root names, without listing what is under it.
type selfTraverser interface {
	TraverseSelf(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error
}

// errDirectoryNotSelfTraversable is reported for a directory entry, in
// self-only mode, on a source that cannot enumerate a directory alone.
var errDirectoryNotSelfTraversable = errors.New("list entry is a directory, and this source cannot transfer a directory without its contents")

type childTraverserGenerator func(childPath string) (ResourceTraverser, error)

// listEntryWarn logs list entries that matched nothing; replaced in tests.
var listEntryWarn = WarnStdoutAndScanningLog

// ListEntryErrorInfo reports a list-of-files entry that could not be
// enumerated. Err wraps the underlying error.
type ListEntryErrorInfo struct {
	EntryPath string
	Err       error
	Loc       common.Location
}

var _ TraverserErrorItemInfo = ListEntryErrorInfo{}

func (e ListEntryErrorInfo) FullPath() string            { return e.EntryPath }
func (e ListEntryErrorInfo) Name() string                { return e.EntryPath }
func (e ListEntryErrorInfo) Size() int64                 { return 0 }
func (e ListEntryErrorInfo) LastModifiedTime() time.Time { return time.Time{} }
func (e ListEntryErrorInfo) IsDir() bool                 { return false }
func (e ListEntryErrorInfo) ErrorMessage() error         { return e.Err }
func (e ListEntryErrorInfo) Location() common.Location   { return e.Loc }

// skipEntry handles an entry that could not be enumerated: reported on
// entryErrorChannel when set, otherwise skipped with logMsg. The send blocks
// so no failure is dropped; the consumer must drain the channel until
// Traverse returns. It gives up once ctx is canceled.
func (l *listTraverser) skipEntry(childPath string, err error, logMsg string) {
	if l.entryErrorChannel == nil {
		glcm.Info(logMsg)
		return
	}
	select {
	case l.entryErrorChannel <- ListEntryErrorInfo{EntryPath: childPath, Err: err, Loc: l.location}:
	case <-l.ctx.Done():
	}
}

// There is no impact to a list traverser returning false because a list traverser points directly to relative paths.
func (l *listTraverser) IsDirectory(bool) (bool, error) {
	return false, nil
}

// To kill the traverser, close() the channel under it, or cancel its context,
// which makes Traverse return the context's error.
// Behavior demonstrated: https://play.golang.org/p/OYdvLmNWgwO
func (l *listTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	var emptyEntries int64
	if l.parallelism > 1 {
		err = l.traverseParallel(preprocessor, processor, filters, &emptyEntries)
	} else {
		// read a channel until it closes to get a list of objects
		for childPath := range l.listReader {
			if err = l.traverseEntry(childPath, preprocessor, processor, filters, &emptyEntries); err != nil {
				break
			}
		}
	}
	if err != nil {
		return err
	}

	if emptyEntries > 0 {
		listEntryWarn(fmt.Sprintf("%d list entries matched nothing on the source and were skipped", emptyEntries))
	}
	return nil
}

// traverseParallel enumerates up to l.parallelism entries at once. Each
// entry costs round trips to the source (its properties, and tags when
// preserved), so a long list enumerated one entry at a time is bound by
// latency. Calls to processor are serialized, so the processor needs no
// synchronization; the order in which entries reach it is not the list's.
func (l *listTraverser) traverseParallel(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter, emptyEntries *int64) error {
	var processorMu sync.Mutex
	serialProcessor := func(object StoredObject) error {
		processorMu.Lock()
		defer processorMu.Unlock()
		return processor(object)
	}

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	for i := 0; i < l.parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for childPath := range l.listReader {
				if err := l.traverseEntry(childPath, preprocessor, serialProcessor, filters, emptyEntries); err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// traverseEntry enumerates one list entry. It returns an error only when
// the traversal must stop (the context is canceled); an entry that cannot be
// enumerated is reported or skipped (see skipEntry), and one that matched
// nothing is counted in emptyEntries.
func (l *listTraverser) traverseEntry(childPath string, preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter, emptyEntries *int64) error {
	// Stop on cancellation, rather than enumerating (and reporting the
	// cancellation errors of) every remaining entry.
	if ctxErr := l.ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	// fetch an appropriate traverser, and go through the child path, which could be
	//   1. a single entity
	//   2. a directory entity that needs to be scanned
	childTraverser, err := l.childTraverserGenerator(childPath)
	if err != nil {
		if ctxErr := l.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		l.skipEntry(childPath, err, fmt.Sprintf("Skipping %s due to error %s", childPath, err))
		return nil
	}
	// listTraverser will only ever execute on the source

	isDir, _ := childTraverser.IsDirectory(true)
	if !l.recursive && isDir {
		return nil // skip over directories
	}

	// In self-only mode a directory entry yields the directory itself,
	// never its contents.
	traverse := childTraverser.Traverse
	if isDir && l.directoriesSelfOnly {
		self, ok := childTraverser.(selfTraverser)
		if !ok {
			l.skipEntry(childPath, errDirectoryNotSelfTraversable,
				fmt.Sprintf("Skipping %s: %s", childPath, errDirectoryNotSelfTraversable))
			return nil
		}
		traverse = self.TraverseSelf
	}

	// when scanning a child path under the parent, we need to make sure that the relative paths of
	// the results are indeed starting right under the parent
	// ex: parent = /usr/foo
	// case 1: child1 is a file under the parent
	//         the relative path returned by the child traverser would be ""
	//         it should be "child1" instead
	// case 2: child2 is a directory, and it has items under it such as child2/grandchild1
	//         the relative path returned by the child traverser would be "grandchild1"
	//         it should be "child2/grandchild1" instead
	childPreProcessor := func(object *StoredObject) {
		object.relativePath = common.GenerateFullPath(childPath, object.relativePath)
	}
	preProcessorForThisChild := preprocessor.FollowedBy(childPreProcessor)

	var found int64
	countingProcessor := func(object StoredObject) error {
		atomic.AddInt64(&found, 1)
		return processor(object)
	}

	err = traverse(preProcessorForThisChild, countingProcessor, filters)
	if err != nil {
		if ctxErr := l.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		l.skipEntry(childPath, err, fmt.Sprintf("Skipping %s as it cannot be scanned due to error: %s", childPath, err))
	} else if l.entryErrorChannel != nil && atomic.LoadInt64(&found) == 0 && !(isDir && l.directoriesSelfOnly) {
		// Most likely deleted since the list was made, but a wrong path
		// (e.g. encoding) looks the same, so leave a trace. A directory
		// in self-only mode yields nothing by design when the transfer
		// does not carry folders (e.g. a flat-namespace directory stub
		// copied to a flat namespace, which a full traversal skips too).
		atomic.AddInt64(emptyEntries, 1)
		listEntryWarn(fmt.Sprintf("List entry %s matched nothing on the source (not found, or excluded by filters); skipping", childPath))
	}
	return nil
}
func newListTraverser(resource common.ResourceString, resourceLocation common.Location, ctx context.Context, options InitResourceTraverserOptions) ResourceTraverser {
	listChan := options.ListOfFiles
	recursive := options.Recursive

	if listChan == nil {
		panic("list of files channel must not be nil")
	}

	reportEntryErrors := options.ReportListOfFilesEntryErrors && options.ErrorChannel != nil
	if ctx == nil {
		ctx = context.Background()
	}

	traverserGenerator := func(relativeChildPath string) (ResourceTraverser, error) {
		source := resource.Clone()
		if resourceLocation != common.ELocation.Local() {
			// assume child path is not URL-encoded yet, this is consistent with the behavior of previous implementation
			childURL, _ := url.Parse(resource.Value)
			childURL.Path = common.GenerateFullPath(childURL.Path, relativeChildPath)
			source.Value = childURL.String()
		} else {
			// is local, only generate the full path
			source.Value = common.GenerateFullPath(resource.ValueLocal(), relativeChildPath)
		}

		// Construct a traverser that goes through the child
		traverser, err := InitResourceTraverser(source, resourceLocation, ctx, InitResourceTraverserOptions{
			DestResourceType: nil,

			Credential:           options.Credential,
			IncrementEnumeration: options.IncrementEnumeration,

			ListOfVersionIDs: nil,
			ErrorChannel:     nil,

			CpkOptions: options.CpkOptions,

			PreservePermissions: options.PreservePermissions,
			SymlinkHandling:     options.SymlinkHandling,
			SyncHashType:        options.SyncHashType,
			TrailingDotOption:   options.TrailingDotOption,

			Recursive:               options.Recursive,
			GetPropertiesInFrontend: options.GetPropertiesInFrontend,
			IncludeDirectoryStubs:   options.IncludeDirectoryStubs,
			PreserveBlobTags:        options.PreserveBlobTags,

			FailOnSingleBlobLookupError: reportEntryErrors,
		})
		if err != nil {
			return nil, err
		}
		return traverser, nil
	}

	t := &listTraverser{
		listReader:              listChan,
		recursive:               recursive,
		childTraverserGenerator: traverserGenerator,
		location:                resourceLocation,
		ctx:                     ctx,
		directoriesSelfOnly:     options.ListOfFilesDirectoriesSelfOnly,
		parallelism:             options.ListOfFilesParallelism,
	}
	if reportEntryErrors {
		t.entryErrorChannel = options.ErrorChannel
	}
	return t
}
