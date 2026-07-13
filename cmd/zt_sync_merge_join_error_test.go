//go:build smslidingwindow
// +build smslidingwindow

// Copyright © Microsoft <wastore@microsoft.com>
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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

// fakeMergeJoinTraverser is a minimal ResourceTraverser used to exercise the streaming
// merge-join error handling. It emits the given objects (in the order provided) to the
// processor callback and then returns finalErr, simulating a traversal that fails mid- or
// end-of-listing (e.g. token expiry or sustained throttling).
type fakeMergeJoinTraverser struct {
	objects  []StoredObject
	finalErr error
}

func (f *fakeMergeJoinTraverser) IsDirectory(bool) (bool, error) { return true, nil }

func (f *fakeMergeJoinTraverser) Traverse(_ objectMorpher, processor objectProcessor, _ []ObjectFilter) error {
	for _, o := range f.objects {
		if err := processor(o); err != nil {
			return err
		}
	}
	return f.finalErr
}

// mjTestFile builds a minimal file StoredObject with the given relative path.
func mjTestFile(relPath string) StoredObject {
	return StoredObject{
		name:             relPath,
		relativePath:     relPath,
		entityType:       common.EEntityType.File(),
		lastModifiedTime: time.Unix(1_600_000_000, 0),
		size:             1,
	}
}

// newMergeJoinTestEnumerator builds a minimal syncEnumerator wired with a recording
// destination cleaner (objectComparator). deletedPaths captures every object the merge-join
// would delete for a mirror sync, so a test can assert exactly which deletions happen.
func newMergeJoinTestEnumerator(deletedPaths *[]string, mu *sync.Mutex) *syncEnumerator {
	return &syncEnumerator{
		objectIndexer: newObjectIndexer(),
		objectComparator: func(o StoredObject) error {
			mu.Lock()
			*deletedPaths = append(*deletedPaths, o.relativePath)
			mu.Unlock()
			return nil
		},
		// Never invoked in these tests (there are no source-only or both-exist objects),
		// but the comparator construction binds ctp.scheduleCopyTransfer as a method value.
		ctp: &copyTransferProcessor{},
	}
}

// mirrorSyncArgs returns cooked args for a Blob->Blob mirror sync (delete-destination=true).
func mirrorSyncArgs() *cookedSyncCmdArgs {
	return &cookedSyncCmdArgs{
		fromTo:            common.EFromTo.BlobBlob(),
		deleteDestination: common.EDeleteDestination.True(),
	}
}

// TestMergeJoinSourceErrorDoesNotDeleteDestinationForMirror is the core regression for
// review Point 1: when the SOURCE listing fails, a mirror sync must NOT delete the
// destination objects that remain (they are not genuinely source-absent — the source
// stream was merely truncated by an error). The job must instead fail, attributed to source.
func TestMergeJoinSourceErrorDoesNotDeleteDestinationForMirror(t *testing.T) {
	a := assert.New(t)

	var deleted []string
	var mu sync.Mutex
	enum := newMergeJoinTestEnumerator(&deleted, &mu)
	cca := mirrorSyncArgs()

	// Source fails at the very start (0 objects, then an error) — simulating token expiry /
	// throttling before any object is listed.
	src := &fakeMergeJoinTraverser{finalErr: errors.New("source listing failed: token expired")}
	// Destination has objects that WOULD be deleted for a mirror if we mistook the truncated
	// source stream for "source has no such objects".
	dst := &fakeMergeJoinTraverser{objects: []StoredObject{
		mjTestFile("a.txt"), mjTestFile("b.txt"), mjTestFile("c.txt"),
	}}

	_, err := mergeJoinSyncDirChannelBased(context.Background(), enum, cca, "", src, dst, true)

	a.Error(err, "a source listing failure must surface as an error")
	var mjErr *mergeJoinTraversalError
	a.True(errors.As(err, &mjErr), "expected *mergeJoinTraversalError, got %T: %v", err, err)
	if mjErr != nil {
		a.Equal(cca.fromTo.From(), mjErr.location, "error must be attributed to the source side")
	}

	mu.Lock()
	a.Empty(deleted, "no destination objects may be deleted when the source listing failed")
	mu.Unlock()
}

// TestMergeJoinSourceCleanEmptyDeletesDestinationForMirror is the control for the test
// above: when the source legitimately lists zero objects WITHOUT error, a mirror sync must
// still delete all extra destination objects. This proves the short-circuit only triggers on
// a real error and does not suppress correct deletions.
func TestMergeJoinSourceCleanEmptyDeletesDestinationForMirror(t *testing.T) {
	a := assert.New(t)

	var deleted []string
	var mu sync.Mutex
	enum := newMergeJoinTestEnumerator(&deleted, &mu)
	cca := mirrorSyncArgs()

	src := &fakeMergeJoinTraverser{} // 0 objects, nil error — source is legitimately empty
	dst := &fakeMergeJoinTraverser{objects: []StoredObject{
		mjTestFile("a.txt"), mjTestFile("b.txt"), mjTestFile("c.txt"),
	}}

	_, err := mergeJoinSyncDirChannelBased(context.Background(), enum, cca, "", src, dst, true)

	a.NoError(err)
	mu.Lock()
	sort.Strings(deleted)
	a.Equal([]string{"a.txt", "b.txt", "c.txt"}, deleted,
		"a clean empty source must delete all extra destination objects for a mirror sync")
	mu.Unlock()
}

// TestMergeJoinPollErrs verifies the non-blocking error probe used by the merge/drain loops:
// it returns a side-tagged mergeJoinTraversalError only when an error is actually present.
func TestMergeJoinPollErrs(t *testing.T) {
	a := assert.New(t)
	cca := &cookedSyncCmdArgs{fromTo: common.EFromTo.S3Blob()}

	// both open & empty -> nil (no error, do not block)
	a.NoError(mergeJoinPollErrs(cca, make(chan error, 1), make(chan error, 1)))

	// source error present -> tagged as source
	srcErr := make(chan error, 1)
	srcErr <- errors.New("boom-src")
	err := mergeJoinPollErrs(cca, srcErr, make(chan error, 1))
	var mjErr *mergeJoinTraversalError
	a.True(errors.As(err, &mjErr))
	if mjErr != nil {
		a.Equal(cca.fromTo.From(), mjErr.location)
	}

	// dest error present -> tagged as destination
	dstErr := make(chan error, 1)
	dstErr <- errors.New("boom-dst")
	err = mergeJoinPollErrs(cca, make(chan error, 1), dstErr)
	a.True(errors.As(err, &mjErr))
	if mjErr != nil {
		a.Equal(cca.fromTo.To(), mjErr.location)
	}

	// both closed & empty -> nil (clean exhaustion, no error)
	sc := make(chan error)
	close(sc)
	dc := make(chan error)
	close(dc)
	a.NoError(mergeJoinPollErrs(cca, sc, dc))
}

// TestTraverserToChannelOrderViolationFails verifies review Point 2: an out-of-order listing
// is a HARD FAILURE, surfaced through the error channel (not just logged).
func TestTraverserToChannelOrderViolationFails(t *testing.T) {
	a := assert.New(t)

	// Emit descending keys ("b.txt" then "a.txt") to violate the sort invariant.
	tr := &fakeMergeJoinTraverser{objects: []StoredObject{
		mjTestFile("b.txt"), mjTestFile("a.txt"),
	}}
	objCh, errCh := traverserToChannel(context.Background(), tr, nil, "SRC[test]")
	for range objCh { // drain
	}
	err := <-errCh
	a.Error(err, "an out-of-order listing must surface as an error")
	if err != nil {
		a.Contains(err.Error(), "ORDER VIOLATION")
	}
}

// TestTraverserToChannelSortedSucceeds verifies the happy path: a correctly-sorted listing
// streams through in order with no error.
func TestTraverserToChannelSortedSucceeds(t *testing.T) {
	a := assert.New(t)

	tr := &fakeMergeJoinTraverser{objects: []StoredObject{
		mjTestFile("a.txt"), mjTestFile("b.txt"), mjTestFile("c.txt"),
	}}
	objCh, errCh := traverserToChannel(context.Background(), tr, nil, "SRC[test]")
	var got []string
	for o := range objCh {
		got = append(got, o.relativePath)
	}
	a.Equal([]string{"a.txt", "b.txt", "c.txt"}, got)
	a.NoError(<-errCh)
}
