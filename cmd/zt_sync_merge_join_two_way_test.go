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
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncOrchestratorContinuesAfterDirectoryErrors(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the sync orchestrator normalizes source URLs as filesystem paths on Windows")
	}
	for _, mergeJoin := range []bool{false, true} {
		t.Run(fmt.Sprintf("mergeJoin=%t", mergeJoin), func(t *testing.T) {
			const failedDirs = 1100
			var listing strings.Builder
			for i := 0; i < failedDirs; i++ {
				fmt.Fprintf(&listing, "<BlobPrefix><Name>failed%04d/</Name></BlobPrefix>", i)
			}
			listing.WriteString("<BlobPrefix><Name>healthy/</Name></BlobPrefix>")
			var healthyListed atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					if strings.HasPrefix(r.URL.Path, "/source/failed") {
						w.Header().Set("x-ms-error-code", "AuthorizationPermissionMismatch")
						w.WriteHeader(http.StatusForbidden)
					} else {
						w.Header().Set("x-ms-error-code", "BlobNotFound")
						w.WriteHeader(http.StatusNotFound)
					}
					return
				}
				if r.Method != http.MethodGet || r.URL.Query().Get("comp") != "list" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				var blobs string
				if strings.TrimSuffix(r.URL.Path, "/") == "/source" {
					switch r.URL.Query().Get("prefix") {
					case "":
						blobs = listing.String()
					case "healthy/":
						healthyListed.Store(true)
					default:
						t.Errorf("unexpected source prefix: %s", r.URL.Query().Get("prefix"))
					}
				}
				w.Header().Set("Content-Type", "application/xml")
				fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Blobs>%s</Blobs><NextMarker /></EnumerationResults>`, blobs)
			}))
			defer server.Close()

			oldOptions, oldParallelism, oldThrottling, oldUseOrchestrator := orchestratorOptions, crawlParallelism, enableThrottling, UseSyncOrchestrator
			t.Cleanup(func() {
				orchestratorOptions, crawlParallelism, enableThrottling, UseSyncOrchestrator = oldOptions, oldParallelism, oldThrottling, oldUseOrchestrator
			})
			orchestratorOptions = &SyncOrchestratorOptions{fromTo: common.EFromTo.BlobBlob()}
			crawlParallelism, enableThrottling, UseSyncOrchestrator = 64, false, true
			baseURL := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
			cca := &cookedSyncCmdArgs{
				fromTo:                common.EFromTo.BlobBlob(),
				source:                common.ResourceString{Value: baseURL + "/source"},
				destination:           common.ResourceString{Value: baseURL + "/target"},
				useStreamingMergeJoin: mergeJoin,
			}
			errorChannel := make(chan TraverserErrorItemInfo, 3*failedDirs)
			options := InitResourceTraverserOptions{
				Credential:   &common.CredentialInfo{CredentialType: enum.ECredentialType.Anonymous()},
				ErrorChannel: errorChannel,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			primaryTraverser, err := InitResourceTraverser(cca.source, cca.fromTo.From(), ctx, options)
			require.NoError(t, err)
			finalized := false
			enumerator := &syncEnumerator{
				primaryTraverser: primaryTraverser,
				primaryTraverserTemplate: ResourceTraverserTemplate{
					location: common.ELocation.Blob(),
					options:  options,
				},
				secondaryTraverserTemplate: ResourceTraverserTemplate{
					location: common.ELocation.Blob(),
					options:  options,
				},
				objectIndexer:       newObjectIndexer(),
				objectComparator:    func(StoredObject) error { return nil },
				orchestratorOptions: orchestratorOptions,
				ctp: &copyTransferProcessor{
					copyJobTemplate:        &common.CopyJobPartOrderRequest{FromTo: cca.fromTo},
					folderPropertiesOption: common.EFolderPropertiesOption.NoFolders(),
				},
				finalize: func() error {
					finalized = true
					return nil
				},
			}
			require.NoError(t, cca.runSyncOrchestrator(enumerator, ctx))
			require.NoError(t, ctx.Err(), "enumeration must finish without waiting for cancellation")
			assert.True(t, finalized)
			assert.True(t, healthyListed.Load(), "healthy directories must still be enumerated")
			close(errorChannel)
			failedPaths := make(map[string]bool)
			for err := range errorChannel {
				if err.IsDir() {
					assert.Contains(t, err.ErrorMessage().Error(), "AuthorizationPermissionMismatch")
					assert.False(t, failedPaths[err.FullPath()], "directory failure must only be reported once")
					failedPaths[err.FullPath()] = true
				}
			}
			assert.Len(t, failedPaths, failedDirs)
		})
	}
}

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

	folderCh, fileCh := traverserToTypedChannels(context.Background(), fake, nil, "SRC[]", sideErr, false)
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

	folderCh, fileCh := traverserToTypedChannels(context.Background(), fake, nil, "SRC[]", sideErr, false)
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

	folderCh, fileCh := traverserToTypedChannels(context.Background(), fake, nil, "SRC[]", sideErr, false)
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
	folderCh, fileCh := traverserToTypedChannels(ctx, fake, nil, "SRC[]", sideErr, false)
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
	src := &fakeMergeJoinTraverser{objects: append([]StoredObject(nil), objs...)}          // fast
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

// mjDestNotFoundErr builds the exact "destination lists empty" signal the blob traverser returns for
// a sync destination whose prefix has no children (flat-blob emptyPrefix, or DFS 404): the message
// ends in "Err BlobNotFound", which IsDestinationNotFoundDuringSync recognizes.
func mjDestNotFoundErr(dir string) error {
	return fmt.Errorf("blob https://acct.blob.core.windows.net/c/%s not found in destination. Err %s", dir, "BlobNotFound")
}

// newMergeJoinRecordingEnumerator builds a syncEnumerator wired with a REAL copyTransferProcessor
// whose scheduled transfers accumulate in its copyJobTemplate (part size set huge so nothing is
// dispatched to the STE), so a test can assert exactly which source objects were scheduled for copy.
func newMergeJoinRecordingEnumerator(deletedPaths *[]string, mu *sync.Mutex) *syncEnumerator {
	tmpl := &common.CopyJobPartOrderRequest{
		FromTo:              common.EFromTo.S3Blob(),
		Fpo:                 common.EFolderPropertiesOption.NoFolders(),
		SymlinkHandlingType: common.ESymlinkHandlingType.Skip(),
	}
	ctp := newCopyTransferProcessor(tmpl, 1_000_000, common.ResourceString{}, common.ResourceString{}, nil, nil, false, false)
	return &syncEnumerator{
		objectIndexer: newObjectIndexer(),
		objectComparator: func(o StoredObject) error {
			mu.Lock()
			*deletedPaths = append(*deletedPaths, o.relativePath)
			mu.Unlock()
			return nil
		},
		ctp: ctp,
	}
}

// scheduledTransferSources returns the Source of every transfer the merge-join scheduled for copy.
func scheduledTransferSources(enum *syncEnumerator) []string {
	out := make([]string, 0, len(enum.ctp.copyJobTemplate.Transfers.List))
	for _, tx := range enum.ctp.copyJobTemplate.Transfers.List {
		out = append(out, tx.Source)
	}
	for _, tx := range enum.ctp.shuffleBuffer {
		out = append(out, tx.Source)
	}
	return out
}

// TestTwoWaySyncDir_DestNotFoundIsBenignAndTransfersSource is the S3->Blob regression: when the
// destination prefix lists EMPTY (surfaced as "... not found in destination. Err BlobNotFound"), the
// old code treated it as a FATAL directory error — it returned an error (so the orchestrator never
// enqueued the sub-dirs => whole subtree silently skipped) and scheduled none of the new source
// files. The destination-absent case must be benign (same as isDestinationPresent=false): every
// source file is copied and every source sub-dir is enqueued for recursion.
func TestTwoWaySyncDir_DestNotFoundIsBenignAndTransfersSource(t *testing.T) {
	a := assert.New(t)
	var deleted []string
	var mu sync.Mutex
	enum := newMergeJoinRecordingEnumerator(&deleted, &mu)
	cca := &cookedSyncCmdArgs{fromTo: common.EFromTo.S3Blob(), deleteDestination: common.EDeleteDestination.False()}

	src := &fakeMergeJoinTraverser{objects: []StoredObject{
		mjTestFile("a.txt"), mjTestFile("z.txt"),
		mjTestVirtualFolder("dirA"), mjTestVirtualFolder("dirB"),
	}}
	// Destination lists empty -> traverser returns the not-found signal. isDestinationPresent=true
	// reproduces the real precondition (the parent enumeration believed the dir was present).
	dst := &fakeMergeJoinTraverser{finalErr: mjDestNotFoundErr("stg_millennium/working_view_A/")}

	ctx, cancel := context.WithCancel(context.Background())
	subDirs, err := mergeJoinTwoWaySyncDir(ctx, cancel, enum, cca, "", src, dst, true)

	a.NoError(err, "destination not-found (empty prefix) must be benign, not a fatal directory error")

	got := scheduledTransferSources(enum)
	sort.Strings(got)
	a.Equal([]string{"/a.txt", "/z.txt"}, got, "all new source files must be transferred when the destination is empty")

	subPaths := make([]string, len(subDirs))
	for i, s := range subDirs {
		subPaths[i] = s.relativePath
	}
	sort.Strings(subPaths)
	a.Equal([]string{"/dirA/", "/dirB/"}, subPaths, "source sub-dirs must be enqueued so the subtree is not skipped")
	for _, s := range subDirs {
		a.False(s.isPresentAtDestination, "sub-dirs of an absent destination must be marked not-present")
	}

	mu.Lock()
	a.Empty(deleted, "nothing to delete when the destination is empty")
	mu.Unlock()
}

// TestTwoWaySyncDir_DestRealErrorStillFatal guards against over-suppression: a genuine (non
// not-found) destination traversal error must still be fatal and attributed to the destination.
func TestTwoWaySyncDir_DestRealErrorStillFatal(t *testing.T) {
	a := assert.New(t)
	var deleted []string
	var mu sync.Mutex
	enum := newMergeJoinRecordingEnumerator(&deleted, &mu)
	cca := &cookedSyncCmdArgs{fromTo: common.EFromTo.S3Blob(), deleteDestination: common.EDeleteDestination.False()}

	src := &fakeMergeJoinTraverser{objects: []StoredObject{mjTestFile("a.txt")}}
	dst := &fakeMergeJoinTraverser{finalErr: errors.New("destination listing failed: 500 InternalServerError")}

	ctx, cancel := context.WithCancel(context.Background())
	_, err := mergeJoinTwoWaySyncDir(ctx, cancel, enum, cca, "", src, dst, true)

	a.Error(err, "a genuine destination traversal error must remain fatal")
	var mjErr *mergeJoinTraversalError
	a.True(errors.As(err, &mjErr), "expected *mergeJoinTraversalError, got %T: %v", err, err)
	if mjErr != nil {
		a.Equal(cca.fromTo.To(), mjErr.location, "error must be attributed to the destination")
	}
}

func TestFlatSyncTraverserConfiguration(t *testing.T) {
	for _, pair := range []common.FromTo{
		common.EFromTo.BlobBlob(), common.EFromTo.BlobBlobFS(), common.EFromTo.BlobFSBlob(),
		common.EFromTo.BlobFSBlobFS(), common.EFromTo.S3Blob(), common.EFromTo.LocalBlob(),
		common.EFromTo.FileFile(),
	} {
		for _, flat := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/flat=%t", pair, flat), func(t *testing.T) {
				src, dst := InitResourceTraverserOptions{}, InitResourceTraverserOptions{}
				configureFlatSyncTraversers(&cookedSyncCmdArgs{fromTo: pair, useFlatListing: flat}, &src, &dst)
				want := flat && pair == common.EFromTo.BlobBlob()
				for _, opts := range []InitResourceTraverserOptions{src, dst} {
					require.Equal(t, want, opts.FlatListing)
					require.Equal(t, want, opts.Recursive)
					if want {
						require.Equal(t, common.ELocation.Blob(), *opts.DestResourceType)
					}
				}
			})
		}
	}
}

func TestFlatSyncRespectsStreamingFlagAndRejectsOtherPairs(t *testing.T) {
	for _, pair := range []common.FromTo{
		common.EFromTo.BlobBlob(), common.EFromTo.BlobBlobFS(), common.EFromTo.BlobFSBlob(),
		common.EFromTo.BlobFSBlobFS(), common.EFromTo.S3Blob(), common.EFromTo.FileFile(),
	} {
		for _, streaming := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/streaming=%t", pair, streaming), func(t *testing.T) {
				raw := getDefaultSyncRawInput("https://source.blob.core.windows.net/c", "https://target.blob.core.windows.net/c")
				raw.fromTo = pair.String()
				raw.recursive = false
				raw.useFlatListing = true
				raw.useStreamingMergeJoin = streaming
				cooked, err := raw.toOptions()
				if pair == common.EFromTo.BlobBlob() {
					require.NoError(t, err)
					require.True(t, cooked.useFlatListing)
					require.True(t, cooked.useStreamingMergeJoin)
					require.True(t, useStreamingMergeJoin(&cooked))
					err = validate(&cooked, &SyncOrchestratorOptions{})
					require.NoError(t, err)
					cooked.useStreamingMergeJoin = false
					require.ErrorContains(t, validate(&cooked, &SyncOrchestratorOptions{}), "flat sync requires the Blob-to-Blob streaming merge-join")
				} else {
					require.ErrorContains(t, err, "flat sync requires")
				}
			})
		}
	}
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprintf("non-flat/streaming=%t", streaming), func(t *testing.T) {
			raw := getDefaultSyncRawInput("https://source.blob.core.windows.net/c", "https://target.blob.core.windows.net/c")
			raw.fromTo = common.EFromTo.BlobBlob().String()
			raw.recursive = false
			raw.useFlatListing = false
			raw.useStreamingMergeJoin = streaming
			cooked, err := raw.toOptions()
			require.NoError(t, err)
			require.False(t, cooked.useFlatListing)
			require.Equal(t, streaming, cooked.useStreamingMergeJoin)
			require.Equal(t, streaming, useStreamingMergeJoin(&cooked))
		})
	}
}

func TestFlatSyncMergeNestedFiles(t *testing.T) {
	for _, tc := range []struct{ mirror, destinationArrow bool }{
		{false, false}, {false, true}, {true, false}, {true, true},
	} {
		t.Run(fmt.Sprintf("mirror=%t/destinationArrow=%t", tc.mirror, tc.destinationArrow), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src, _ := newFlatTestListing(t, ctx, [][]string{{"a/new", "b/same"}, {"d/new"}}, "source-root/", nil)
			dst, _ := newFlatTestListing(t, ctx, [][]string{{"b/same"}, {"c/extra"}}, "destination-root/", nil, tc.destinationArrow)
			var deleted []string
			var mu sync.Mutex
			enumerator := newMergeJoinRecordingEnumerator(&deleted, &mu)
			enumerator.ctp.copyJobTemplate.FromTo = common.EFromTo.BlobBlob()
			cca := &cookedSyncCmdArgs{fromTo: common.EFromTo.BlobBlob(), useFlatListing: true}
			if tc.mirror {
				cca.deleteDestination = common.EDeleteDestination.True()
			} else {
				cca.deleteDestination = common.EDeleteDestination.False()
			}
			var unchanged int
			enumerator.primaryTraverserTemplate.options.IncrementNotTransferred = func(common.EntityType) { unchanged++ }
			dirs, err := mergeJoinTwoWaySyncDir(ctx, cancel, enumerator, cca, "", src, dst, true)
			require.NoError(t, err)
			require.Empty(t, dirs, "flat sync must not accumulate any child-directory frontier")
			require.Equal(t, []string{"/a/new", "/d/new"}, scheduledTransferSources(enumerator))
			require.Equal(t, 1, unchanged)
			if tc.mirror {
				require.Equal(t, []string{"c/extra"}, deleted)
			} else {
				require.Empty(t, deleted)
			}
			require.Empty(t, enumerator.objectIndexer.indexMap, "flat sync must not build a whole-tree index")
		})
	}
}

func TestFlatSyncEmptyOrMissingDestination(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%t", missing), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src, _ := newFlatTestListing(t, ctx, [][]string{{"a/b/c"}, {"x/y"}}, "", nil)
			dst, _ := newFlatTestListing(t, ctx, [][]string{{}}, "",
				func(_ int, w http.ResponseWriter, _ *http.Request) bool {
					if !missing {
						return true
					}
					w.Header().Set("x-ms-error-code", "BlobNotFound")
					w.WriteHeader(http.StatusNotFound)
					fmt.Fprint(w, `<Error><Code>BlobNotFound</Code><Message>missing</Message></Error>`)
					return false
				})
			var deleted []string
			var mu sync.Mutex
			enumerator := newMergeJoinRecordingEnumerator(&deleted, &mu)
			cca := &cookedSyncCmdArgs{fromTo: common.EFromTo.BlobBlob(), useFlatListing: true, deleteDestination: common.EDeleteDestination.False()}
			if missing {
				cca.deleteDestination = common.EDeleteDestination.True()
			}
			dirs, err := mergeJoinTwoWaySyncDir(ctx, cancel, enumerator, cca, "", src, dst, true)
			require.NoError(t, err)
			require.Empty(t, dirs)
			require.Equal(t, []string{"/a/b/c", "/x/y"}, scheduledTransferSources(enumerator))
			require.Empty(t, deleted)
		})
	}
}

func TestFlatSyncPreservesLastModifiedComparison(t *testing.T) {
	var deleted []string
	var mu sync.Mutex
	enumerator := newMergeJoinRecordingEnumerator(&deleted, &mu)
	var unchanged int
	enumerator.primaryTraverserTemplate.options.IncrementNotTransferred = func(common.EntityType) { unchanged++ }
	source := []StoredObject{mjTestFile("a/source-newer"), mjTestFile("b/same"), mjTestFile("c/target-newer")}
	target := append([]StoredObject(nil), source...)
	target[0].lastModifiedTime = target[0].lastModifiedTime.Add(-time.Hour)
	target[2].lastModifiedTime = target[2].lastModifiedTime.Add(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cca := mirrorSyncArgs()
	cca.useFlatListing = true
	dirs, err := mergeJoinTwoWaySyncDir(ctx, cancel, enumerator, cca, "",
		&fakeMergeJoinTraverser{objects: source}, &fakeMergeJoinTraverser{objects: target}, true)
	require.NoError(t, err)
	require.Empty(t, dirs)
	require.Equal(t, []string{"/a/source-newer"}, scheduledTransferSources(enumerator))
	require.Equal(t, 2, unchanged)
	require.Empty(t, deleted)
}

func TestFlatSyncSourceFailureDoesNotDeleteTarget(t *testing.T) {
	for _, failure := range []string{"Arrow", "AuthorizationPermissionMismatch"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src, _ := newFlatTestListing(t, ctx, [][]string{{"a/same"}, {}}, "",
				func(page int, w http.ResponseWriter, _ *http.Request) bool {
					if page != 1 {
						return true
					}
					if failure == "Arrow" {
						w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
						fmt.Fprint(w, "invalid Arrow stream")
					} else {
						w.Header().Set("x-ms-error-code", failure)
						w.WriteHeader(http.StatusForbidden)
						fmt.Fprint(w, `<Error><Code>AuthorizationPermissionMismatch</Code><Message>denied</Message></Error>`)
					}
					return false
				})
			dst, _ := newFlatTestListing(t, ctx, [][]string{{"a/same", "z/must-not-delete"}}, "", nil)
			var deleted []string
			var mu sync.Mutex
			enumerator := newMergeJoinRecordingEnumerator(&deleted, &mu)
			_, err := mergeJoinTwoWaySyncDir(ctx, cancel, enumerator, mirrorSyncArgs(), "", src, dst, true)
			require.ErrorContains(t, err, failure)
			require.Empty(t, deleted)
		})
	}
}

func TestFlatSyncStreamsBeforeListingCompletesAndCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pages := make([][]string, 3)
	for page := range pages {
		for i := 0; i < mergeJoinChannelBufferSize; i++ {
			pages[page] = append(pages[page], fmt.Sprintf("dir-%08d/file", page*mergeJoinChannelBufferSize+i))
		}
	}

	waitAtLastPage := func(page int, w http.ResponseWriter, r *http.Request) bool {
		if page == 2 {
			<-r.Context().Done()
			return false
		}
		return true
	}
	src, _ := newFlatTestListing(t, ctx, pages, "", waitAtLastPage)
	dst, _ := newFlatTestListing(t, ctx, pages, "", waitAtLastPage)
	var deleted []string
	var mu sync.Mutex
	enumerator := newMergeJoinRecordingEnumerator(&deleted, &mu)
	var compared atomic.Int64
	enumerator.primaryTraverserTemplate.options.IncrementNotTransferred = func(common.EntityType) { compared.Add(1) }
	done := make(chan error, 1)
	go func() {
		_, err := mergeJoinTwoWaySyncDir(ctx, cancel, enumerator, mirrorSyncArgs(), "", src, dst, true)
		done <- err
	}()
	require.Eventually(t, func() bool { return compared.Load() >= mergeJoinChannelBufferSize }, 10*time.Second, 10*time.Millisecond,
		"comparisons must advance while later listing pages are still pending")
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("flat sync did not drain after cancellation")
	}
}

func TestFlatSyncPrefetchAsymmetricSetOracle(t *testing.T) {
	splitPages := func(names []string, size int) [][]string {
		pages := [][]string{{}}
		for len(names) > 0 {
			n := size
			if n > len(names) {
				n = len(names)
			}
			pages = append(pages, names[:n], nil)
			names = names[n:]
		}
		return pages
	}
	var source, target, wantCopies, wantDeletes []string
	wantUnchanged := 0
	for i := 0; i < 12_000; i++ {
		name := fmt.Sprintf("dir-%06d/nested/file", i)
		inSource, inTarget := i%3 != 0, i%4 != 0
		if inSource {
			source = append(source, name)
		}
		if inTarget {
			target = append(target, name)
		}
		switch {
		case inSource && !inTarget:
			wantCopies = append(wantCopies, "/"+name)
		case inTarget && !inSource:
			wantDeletes = append(wantDeletes, name)
		case inSource && inTarget:
			wantUnchanged++
		}
	}
	for _, destinationArrow := range []bool{false, true} {
		t.Run(fmt.Sprintf("destinationArrow=%t", destinationArrow), func(t *testing.T) {
			captureCrawlLogs(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			src, srcCalls := newFlatTestListing(t, ctx, splitPages(source, 1499), "src/", nil)
			dst, dstCalls := newFlatTestListing(t, ctx, splitPages(target, 4093), "dst/", nil, destinationArrow)
			var deleted []string
			var mu sync.Mutex
			enumerator := newMergeJoinRecordingEnumerator(&deleted, &mu)
			enumerator.ctp.copyJobTemplate.FromTo = common.EFromTo.BlobBlob()
			unchanged := 0
			enumerator.primaryTraverserTemplate.options.IncrementNotTransferred = func(common.EntityType) { unchanged++ }
			cca := mirrorSyncArgs()
			cca.useFlatListing = true
			dirs, err := mergeJoinTwoWaySyncDir(ctx, cancel, enumerator, cca, "", src, dst, true)
			require.NoError(t, err)
			require.Empty(t, dirs)
			require.Equal(t, wantCopies, scheduledTransferSources(enumerator))
			require.Equal(t, wantDeletes, deleted)
			require.Equal(t, wantUnchanged, unchanged)
			require.EqualValues(t, len(splitPages(source, 1499)), srcCalls.Load())
			require.EqualValues(t, len(splitPages(target, 4093)), dstCalls.Load())
		})
	}
}

func TestFlatSyncPrefetchPreservesLeadingSlashKeys(t *testing.T) {
	captureCrawlLogs(t)
	root, err := validateAndGetRootObject("https://example.blob.core.windows.net/c", common.EFromTo.BlobBlob())
	require.NoError(t, err)
	require.Equal(t, "/", root.relativePath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src, _ := newFlatTestListing(t, ctx, [][]string{{"/a"}}, "", nil)
	dst, _ := newFlatTestListing(t, ctx, [][]string{{"a"}}, "", nil)
	var deleted []string
	var mu sync.Mutex
	enumerator := newMergeJoinRecordingEnumerator(&deleted, &mu)
	cca := mirrorSyncArgs()
	cca.useFlatListing = true
	_, err = mergeJoinTwoWaySyncDir(ctx, cancel, enumerator, cca, root.relativePath, src, dst, true)
	require.NoError(t, err)
	require.Equal(t, []string{"//a"}, scheduledTransferSources(enumerator))
	require.Equal(t, []string{"a"}, deleted)
}
