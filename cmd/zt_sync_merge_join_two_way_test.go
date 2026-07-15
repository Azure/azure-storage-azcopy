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
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// Shared test helpers for the streaming (two-way) merge-join tests.
// ---------------------------------------------------------------------------

// fakeMergeJoinTraverser is a minimal ResourceTraverser used to exercise the streaming
// merge-join. It emits the given objects (in the order provided) to the processor callback
// and then returns finalErr, simulating a traversal that fails mid- or end-of-listing
// (e.g. token expiry or sustained throttling).
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

// mjTestFolder builds a minimal folder StoredObject (relativePath has NO trailing slash, matching
// what the blob traverser emits for a sub-directory).
func mjTestFolder(relPath string) StoredObject {
	return StoredObject{
		name:             relPath,
		relativePath:     relPath,
		entityType:       common.EEntityType.Folder(),
		lastModifiedTime: time.Unix(1_600_000_000, 0),
	}
}

// drainTypedChannelsForTest collects the folder and file channels (concurrently, to avoid blocking
// the producer) into ordered slices of relativePaths.
func drainTypedChannelsForTest(folderCh, fileCh <-chan StoredObject) (folders, files []string) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for o := range folderCh {
			folders = append(folders, o.relativePath)
		}
	}()
	go func() {
		defer wg.Done()
		for o := range fileCh {
			files = append(files, o.relativePath)
		}
	}()
	wg.Wait()
	return folders, files
}

// TestTwoWayTypedChannels_HNSFolderReorder verifies the producer demux reorders HNS raw-name folder
// order into canonical "<name>/" order while streaming files in their (raw == key) order.
func TestTwoWayTypedChannels_HNSFolderReorder(t *testing.T) {
	a := assert.New(t)

	// HNS raw-name listing order: prefix-siblings arrive shorter-name-first (h2 before
	// h2-4.2.0.dist-info), which is the OPPOSITE of the "<name>/" key order.
	objs := []StoredObject{
		mjTestFolder("h2"),
		mjTestFolder("h2-4.2.0.dist-info"),
		mjTestFolder("pluggy"),
		mjTestFolder("pluggy-1.5.0.dist-info"),
		mjTestFile("aaa.txt"),
		mjTestFile("pluggy.txt"),
		mjTestFile("zzz.txt"),
	}
	fake := &fakeMergeJoinTraverser{objects: objs}
	sideErr := &twoWaySideErr{}

	folderCh, fileCh := traverserToTypedChannels(context.Background(), fake, nil, "SRC[]", sideErr)
	folders, files := drainTypedChannelsForTest(folderCh, fileCh)

	// Folders reordered to canonical "<name>/" key order (dist-info before its shorter sibling).
	a.Equal([]string{"h2-4.2.0.dist-info", "h2", "pluggy-1.5.0.dist-info", "pluggy"}, folders)
	// Files stream in raw order.
	a.Equal([]string{"aaa.txt", "pluggy.txt", "zzz.txt"}, files)
	a.NoError(sideErr.get())
}

// TestTwoWayTypedChannels_FileOrderViolation verifies an out-of-order file stream is a hard failure
// recorded in the side-error holder (files must be service-sorted for the two-pointer merge).
func TestTwoWayTypedChannels_FileOrderViolation(t *testing.T) {
	a := assert.New(t)
	objs := []StoredObject{
		mjTestFile("b.txt"),
		mjTestFile("a.txt"), // out of order
	}
	fake := &fakeMergeJoinTraverser{objects: objs}
	sideErr := &twoWaySideErr{}

	folderCh, fileCh := traverserToTypedChannels(context.Background(), fake, nil, "SRC[]", sideErr)
	drainTypedChannelsForTest(folderCh, fileCh)

	a.Error(sideErr.get())
	a.Contains(sideErr.get().Error(), "ORDER VIOLATION")
}

// TestTwoWayTypedChannels_TraversalErrorRecorded verifies a traversal error is recorded in the
// side-error holder (for mirror-delete protection and error attribution).
func TestTwoWayTypedChannels_TraversalErrorRecorded(t *testing.T) {
	a := assert.New(t)
	boom := errors.New("token expired")
	fake := &fakeMergeJoinTraverser{
		objects:  []StoredObject{mjTestFile("a.txt"), mjTestFolder("d")},
		finalErr: boom,
	}
	sideErr := &twoWaySideErr{}

	folderCh, fileCh := traverserToTypedChannels(context.Background(), fake, nil, "SRC[]", sideErr)
	drainTypedChannelsForTest(folderCh, fileCh)

	a.ErrorIs(sideErr.get(), boom)
}

// TestTwoWayTypedChannels_Cancel verifies cancellation stops the producer without hanging.
func TestTwoWayTypedChannels_Cancel(t *testing.T) {
	a := assert.New(t)
	// A large stream so the producer is still running when we cancel.
	var objs []StoredObject
	for i := 0; i < 100000; i++ {
		objs = append(objs, mjTestFile("f"))
	}
	fake := &fakeMergeJoinTraverser{objects: objs}
	sideErr := &twoWaySideErr{}

	ctx, cancel := context.WithCancel(context.Background())
	folderCh, fileCh := traverserToTypedChannels(ctx, fake, nil, "SRC[]", sideErr)
	cancel()
	// Draining must terminate (channels close after the producer observes cancellation).
	done := make(chan struct{})
	go func() {
		drainTypedChannelsForTest(folderCh, fileCh)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		a.Fail("producer did not terminate after cancellation")
	}
}

// mjTestVirtualFolder builds an FNS virtual-prefix folder (recurse-only; no ACL transfer scheduled).
func mjTestVirtualFolder(relPath string) StoredObject {
	o := mjTestFolder(relPath)
	o.isVirtualPrefix = true
	return o
}

// delayingFakeTraverser emits objects with a per-object delay, to create producer/consumer rate
// asymmetry for the liveness/deadlock stress test.
type delayingFakeTraverser struct {
	objects  []StoredObject
	delay    time.Duration
	finalErr error
}

func (f *delayingFakeTraverser) IsDirectory(bool) (bool, error) { return true, nil }

func (f *delayingFakeTraverser) Traverse(_ objectMorpher, processor objectProcessor, _ []ObjectFilter) error {
	for _, o := range f.objects {
		if f.delay > 0 {
			time.Sleep(f.delay)
		}
		if err := processor(o); err != nil {
			return err
		}
	}
	return f.finalErr
}

// TestTwoWaySyncDir_MirrorDeletesFilesAndFolders verifies that with an empty source, a mirror sync
// deletes ALL extra destination objects — files (via the file-merge) and folders (via the folder-
// merge) — running concurrently.
func TestTwoWaySyncDir_MirrorDeletesFilesAndFolders(t *testing.T) {
	a := assert.New(t)
	var deleted []string
	var mu sync.Mutex
	enum := newMergeJoinTestEnumerator(&deleted, &mu)
	cca := mirrorSyncArgs()

	src := &fakeMergeJoinTraverser{} // empty source
	dst := &fakeMergeJoinTraverser{objects: []StoredObject{
		mjTestFile("a.txt"), mjTestFile("z.txt"),
		mjTestFolder("d1"), mjTestFolder("d2"),
	}}

	ctx, cancel := context.WithCancel(context.Background())
	_, err := mergeJoinTwoWaySyncDir(ctx, cancel, enum, cca, "", src, dst, true)
	a.NoError(err)

	mu.Lock()
	sort.Strings(deleted)
	a.Equal([]string{"a.txt", "d1/", "d2/", "z.txt"}, deleted,
		"an empty source must delete all extra destination files AND folders for a mirror sync")
	mu.Unlock()
}

// TestTwoWaySyncDir_SourceErrorNoDeleteBothMerges is the two-way regression for mirror protection:
// when the SOURCE listing fails, NEITHER the file-merge NOR the folder-merge may delete destination
// objects. Both concurrent merges must observe the source error via the shared (non-consuming)
// error holder.
func TestTwoWaySyncDir_SourceErrorNoDeleteBothMerges(t *testing.T) {
	a := assert.New(t)
	var deleted []string
	var mu sync.Mutex
	enum := newMergeJoinTestEnumerator(&deleted, &mu)
	cca := mirrorSyncArgs()

	src := &fakeMergeJoinTraverser{finalErr: errors.New("source listing failed: token expired")}
	dst := &fakeMergeJoinTraverser{objects: []StoredObject{
		mjTestFile("a.txt"), mjTestFile("z.txt"),
		mjTestFolder("d1"), mjTestFolder("d2"),
	}}

	ctx, cancel := context.WithCancel(context.Background())
	_, err := mergeJoinTwoWaySyncDir(ctx, cancel, enum, cca, "", src, dst, true)

	a.Error(err)
	var mjErr *mergeJoinTraversalError
	a.True(errors.As(err, &mjErr), "expected *mergeJoinTraversalError, got %T: %v", err, err)
	if mjErr != nil {
		a.Equal(cca.fromTo.From(), mjErr.location, "error must be attributed to source")
	}
	mu.Lock()
	a.Empty(deleted, "no destination objects (files or folders) may be deleted when the source failed")
	mu.Unlock()
}

// TestTwoWaySyncDir_SourceFoldersRecursionCanonical verifies source-only folders are enqueued for
// recursion (subDirs) in canonical key order even though they arrive in HNS raw order.
func TestTwoWaySyncDir_SourceFoldersRecursionCanonical(t *testing.T) {
	a := assert.New(t)
	var deleted []string
	var mu sync.Mutex
	enum := newMergeJoinTestEnumerator(&deleted, &mu)
	cca := &cookedSyncCmdArgs{fromTo: common.EFromTo.BlobBlob(), deleteDestination: common.EDeleteDestination.False()}

	// HNS raw order (shorter-name-first). Virtual prefixes => recurse-only (no ACL transfer).
	src := &fakeMergeJoinTraverser{objects: []StoredObject{
		mjTestVirtualFolder("h2"),
		mjTestVirtualFolder("h2-4.2.0.dist-info"),
		mjTestVirtualFolder("pretrained"),
		mjTestVirtualFolder("pretrained.dac"),
	}}
	dst := &fakeMergeJoinTraverser{} // empty dest

	ctx, cancel := context.WithCancel(context.Background())
	subDirs, err := mergeJoinTwoWaySyncDir(ctx, cancel, enum, cca, "", src, dst, true)
	a.NoError(err)
	a.Len(subDirs, 4)

	paths := make([]string, len(subDirs))
	for i, s := range subDirs {
		paths[i] = s.relativePath
	}
	sorted := append([]string(nil), paths...)
	sort.Strings(sorted)
	a.Equal(sorted, paths, "sub-dirs must be enqueued in canonical (sorted) order, got %v", paths)

	mu.Lock()
	a.Empty(deleted)
	mu.Unlock()
}

// TestTwoWaySyncDir_LivenessAsymmetric stresses the concurrent merges with a large, rate-asymmetric
// workload (fast source, slow destination) and asserts it COMPLETES (no deadlock/hang). Source and
// destination are identical with equal timestamps, so every object is a both-exists no-op (no
// transfer/delete) — the test isolates the streaming/backpressure machinery.
func TestTwoWaySyncDir_LivenessAsymmetric(t *testing.T) {
	a := assert.New(t)
	var deleted []string
	var mu sync.Mutex
	enum := newMergeJoinTestEnumerator(&deleted, &mu)
	cca := &cookedSyncCmdArgs{fromTo: common.EFromTo.BlobBlob(), deleteDestination: common.EDeleteDestination.False()}

	// Build a large identical set of interleaved folders and files (> the 4000 channel buffer so the
	// fast side actually backs up while the slow side lags).
	var objs []StoredObject
	for i := 0; i < 6000; i++ {
		// zero-padded names keep both files and folders in strictly ascending key order.
		objs = append(objs, mjTestFile(fmt.Sprintf("f%06d.txt", i)))
		if i%3 == 0 {
			objs = append(objs, mjTestVirtualFolder(fmt.Sprintf("d%06d", i)))
		}
	}
	src := &fakeMergeJoinTraverser{objects: append([]StoredObject(nil), objs...)}      // fast
	dst := &delayingFakeTraverser{objects: append([]StoredObject(nil), objs...), delay: 0} // same content

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var subDirs []minimalStoredObject
	go func() {
		var e error
		subDirs, e = mergeJoinTwoWaySyncDir(ctx, cancel, enum, cca, "", src, dst, true)
		done <- e
	}()

	select {
	case err := <-done:
		a.NoError(err)
		a.Len(subDirs, 2000, "all folders should be enqueued for recursion")
		mu.Lock()
		a.Empty(deleted, "identical src/dst must delete nothing")
		mu.Unlock()
	case <-time.After(60 * time.Second):
		a.Fail("two-way merge did not complete — possible deadlock")
	}
}

