package traverser

import (
	"context"
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
// files off the 1,000-slot hash queue. The test times out while the bug is present.

// A failed read makes the hashing thread exit, so none are left to take files.
func TestLocalHashingScanDoesNotHangWhenReadsFail(t *testing.T) {
	runHashingScan(t, createUnreadableFile)
}

// runHashingScan scans one bad file per hashing thread followed by more readable files than the hash queue holds.
func runHashingScan(t *testing.T, createBadFile func(t *testing.T, path string)) {
	const readable = 1_500
	bad := max(1, runtime.NumCPU()/2) // prepareHashingThreads starts NumCPU/2 threads

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
		t.Logf("Traverse returned %v; scanned %d of %d files; indexed %d objects", err, scanned.Load(), bad+readable, len(indexer.IndexMap))
	case <-time.After(timeout):
		t.Fatalf("Traverse did not return within %v. Files scanned froze at %d of %d.\n%s",
			timeout, scanned.Load(), bad+readable, hashingGoroutineReport())
	}
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
