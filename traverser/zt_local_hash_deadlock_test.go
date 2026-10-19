package traverser

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

// Repros for IcM 21000002127226: sync --compare-hash hangs in the local scan once no hashing thread is taking
// files off the 1,000-slot hash queue.

const hashingScanReadableFiles = 1_500 // more than the hash queue holds

// hashingScanBadFiles is one bad file per hashing thread.
func hashingScanBadFiles() int { return max(1, runtime.NumCPU()/2) }

// A file that can't be read is indexed without a hash, and the hashing threads move on to the next file.
func TestLocalHashingScanDoesNotHangWhenReadsFail(t *testing.T) {
	var warnings atomic.Int32
	hooks := common.NewJobUIHooks()
	hooks.Warn = func(string) { warnings.Add(1) }
	prevHooks := common.GetLifecycleMgr()
	common.SetUIHooks(hooks)
	t.Cleanup(func() { common.SetUIHooks(prevHooks) })

	res := runHashingScan(t, createUnreadableFile, nil)
	require.NoError(t, res.err)
	require.EqualValues(t, 1, warnings.Load(), "one console warning covers every file that can't be hashed")

	files := 0
	for relPath, obj := range res.indexer.IndexMap {
		if obj.EntityType != common.EEntityType.File() {
			continue
		}
		files++
		if strings.HasPrefix(relPath, "bad-") {
			require.Nil(t, obj.Md5, "%s can't be read, so it has no hash", relPath)
		} else {
			require.Len(t, obj.Md5, md5.Size, relPath)
		}
	}
	require.Equal(t, hashingScanBadFiles()+hashingScanReadableFiles, files)
}

// An error that ends hashing ends the scan with that error, even when it hits every hashing thread.
func TestLocalHashingScanStopsOnProcessingError(t *testing.T) {
	errProcessing := errors.New("processing failed")
	res := runHashingScan(t, createReadableFile, errProcessing)
	require.ErrorIs(t, res.err, errProcessing)
}

// A file removed after the scan lists it is skipped, and the rest of the scan carries on.
func TestLocalHashingScanSkipsFilesRemovedBeforeHashing(t *testing.T) {
	root := tempDir(t)
	useTempHashStorage(t)
	for _, name := range []string{"gone-1.dat", "kept-1.dat", "gone-2.dat", "kept-2.dat"} {
		createReadableFile(t, filepath.Join(root, name))
	}

	trav, err := NewLocalTraverser(root, context.Background(), InitResourceTraverserOptions{
		Recursive:    true,
		SyncHashType: common.ESyncHashType.MD5(),
		FromTo:       common.EFromTo.LocalFile(),
	})
	require.NoError(t, err)
	trav.hashAdapter = removingHashAdapter{HashDataAdapter: trav.hashAdapter, root: root}

	indexer := NewObjectIndexer()
	require.NoError(t, trav.Traverse(NoPreProccessor, indexer.Store, nil))

	var files []string
	for relPath, obj := range indexer.IndexMap {
		if obj.EntityType == common.EEntityType.File() {
			files = append(files, relPath)
			require.Len(t, obj.Md5, md5.Size, relPath)
		}
	}
	require.ElementsMatch(t, []string{"kept-1.dat", "kept-2.dat"}, files)
}

// A file left open can't be deleted or renamed by anyone else on Windows.
func TestLocalHashingScanClosesHashedFiles(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("an open file only blocks deletion on Windows")
	}
	defer debug.SetGCPercent(debug.SetGCPercent(-1)) // keeps finalizers from closing leaked files before the check
	res := runHashingScan(t, createReadableFile, nil)
	require.NoError(t, res.err)

	dataDir := filepath.Join(res.root, "data")
	entries, err := os.ReadDir(dataDir)
	require.NoError(t, err)
	open := 0
	for _, e := range entries {
		if os.Remove(filepath.Join(dataDir, e.Name())) != nil {
			open++
		}
	}
	require.Zero(t, open, "files still open after Traverse returned")
}

type hashingScanResult struct {
	root    string
	indexer *ObjectIndexer
	err     error
}

// runHashingScan scans the bad files followed by the readable ones, failing the test if Traverse doesn't return.
// A non-nil badFileErr is returned by the processor for each bad file.
func runHashingScan(t *testing.T, createBadFile func(t *testing.T, path string), badFileErr error) hashingScanResult {
	bad, readable := hashingScanBadFiles(), hashingScanReadableFiles
	if common.GetLifecycleMgr() == nil {
		common.SetUIHooks(common.NewJobUIHooks()) // sync sets these before it scans; hashing warnings need them
	}

	root := tempDir(t)
	useTempHashStorage(t)

	// The root is listed before its subfolder, so the bad files reach the hash queue first.
	for i := 0; i < bad; i++ {
		createBadFile(t, filepath.Join(root, fmt.Sprintf("bad-%03d.dat", i)))
	}

	dataDir := filepath.Join(root, "data")
	require.NoError(t, os.Mkdir(dataDir, 0o755))
	for i := 0; i < readable; i++ {
		createReadableFile(t, filepath.Join(dataDir, fmt.Sprintf("file-%04d.dat", i)))
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var scanned atomic.Int64
	trav, err := NewLocalTraverser(root, ctx, InitResourceTraverserOptions{
		Recursive:    true,
		SyncHashType: common.ESyncHashType.MD5(),
		FromTo:       common.EFromTo.LocalFile(),
		IncrementEnumeration: func(entityType common.EntityType, _ common.SymlinkHandlingType, _ common.HardlinkHandlingType) {
			if entityType == common.EEntityType.File() {
				scanned.Add(1)
			}
		},
	})
	require.NoError(t, err)

	indexer := NewObjectIndexer()
	process := func(obj StoredObject) error {
		if badFileErr != nil && strings.HasPrefix(obj.RelativePath, "bad-") {
			return badFileErr
		}
		return indexer.Store(obj)
	}
	done := make(chan error, 1)
	go func() { done <- trav.Traverse(NoPreProccessor, process, nil) }()

	const timeout = 30 * time.Second
	select {
	case err := <-done:
		return hashingScanResult{root: root, indexer: indexer, err: err}
	case <-time.After(timeout):
	}
	t.Fatalf("Traverse did not return within %v. Files scanned froze at %d of %d.\n%s",
		timeout, scanned.Load(), bad+readable, hashingGoroutineReport())
	return hashingScanResult{}
}

func createReadableFile(t *testing.T, path string) {
	require.NoError(t, os.WriteFile(path, []byte(path), 0o644))
}

// useTempHashStorage keeps stored hashes out of the scanned folder and out of other tests.
func useTempHashStorage(t *testing.T) {
	prevMode, prevDir := common.LocalHashStorageMode, common.LocalHashDir
	common.LocalHashStorageMode, common.LocalHashDir = common.EHashStorageMode.HiddenFiles(), tempDir(t)
	t.Cleanup(func() { common.LocalHashStorageMode, common.LocalHashDir = prevMode, prevDir })
}

// removingHashAdapter removes gone-* files as the scan queues them for hashing, before a hashing thread reads them.
type removingHashAdapter struct {
	common.HashDataAdapter
	root string
}

func (a removingHashAdapter) GetHashData(relPath string) (*common.SyncHashData, error) {
	if strings.HasPrefix(relPath, "gone-") {
		if err := os.Remove(filepath.Join(a.root, relPath)); err != nil {
			return nil, err
		}
	}
	return a.HashDataAdapter.GetHashData(relPath)
}

// tempDir is t.TempDir without failing the test when files the hashing threads left open block removal.
func tempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "azcopy-hash-scan-")
	require.NoError(t, err)
	t.Cleanup(func() {
		for deadline := time.Now().Add(10 * time.Second); os.RemoveAll(dir) != nil && time.Now().Before(deadline); {
			runtime.GC() // finalizers close leaked file handles
			time.Sleep(100 * time.Millisecond)
		}
	})
	return dir
}

// hashingGoroutineReport shows what a dump of the hung AzCopy process would: this test's scan goroutine and its hashing threads.
// It must be called from the test goroutine.
func hashingGoroutineReport() string {
	self := make([]byte, 64)
	testID := strings.Fields(string(self[:runtime.Stack(self, false)]))[1]

	buf := make([]byte, 8<<20)
	goroutines := strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n")
	createdBy := func(fn, id string) (matches []string) {
		for _, g := range goroutines {
			if strings.Contains(g, "created by github.com/Azure/azure-storage-azcopy/v10/traverser."+fn+" in goroutine "+id+"\n") {
				matches = append(matches, g)
			}
		}
		return matches
	}

	scan := createdBy("runHashingScan", testID)
	if len(scan) == 0 {
		return "Scan goroutine not found"
	}
	workers := createdBy("(*localTraverser).prepareHashingThreads", strings.Fields(scan[0])[1])

	report := fmt.Sprintf("Live hashing threads: %d of %d started\n\nScan goroutine:\n%s", len(workers), runtime.NumCPU()/2, scan[0])
	if len(workers) > 0 {
		report += "\n\nFirst live hashing thread:\n" + workers[0]
	}
	return report
}
