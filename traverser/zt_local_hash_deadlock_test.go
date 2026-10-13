package traverser

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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

	res := runHashingScan(t, createUnreadableFile)
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

type hashingScanResult struct {
	root    string
	indexer *ObjectIndexer
	err     error
}

// runHashingScan scans the bad files followed by the readable ones, failing the test if Traverse doesn't return.
func runHashingScan(t *testing.T, createBadFile func(t *testing.T, path string)) hashingScanResult {
	bad, readable := hashingScanBadFiles(), hashingScanReadableFiles
	if common.GetLifecycleMgr() == nil {
		common.SetUIHooks(common.NewJobUIHooks()) // sync sets these before it scans; hashing warnings need them
	}

	root := tempDir(t)
	prevMode, prevDir := common.LocalHashStorageMode, common.LocalHashDir
	common.LocalHashStorageMode, common.LocalHashDir = common.EHashStorageMode.HiddenFiles(), tempDir(t)
	t.Cleanup(func() { common.LocalHashStorageMode, common.LocalHashDir = prevMode, prevDir })

	// The root is listed before its subfolder, so the bad files reach the hash queue first.
	for i := 0; i < bad; i++ {
		createBadFile(t, filepath.Join(root, fmt.Sprintf("bad-%03d.dat", i)))
	}

	dataDir := filepath.Join(root, "data")
	require.NoError(t, os.Mkdir(dataDir, 0o755))
	for i := 0; i < readable; i++ {
		p := filepath.Join(dataDir, fmt.Sprintf("file-%04d.dat", i))
		require.NoError(t, os.WriteFile(p, []byte(p), 0o644))
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
	done := make(chan error, 1)
	go func() { done <- trav.Traverse(NoPreProccessor, indexer.Store, nil) }()

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
