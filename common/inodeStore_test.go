package common

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// newTestInodeStore creates an InodeStore backed by a temp file.
// It returns the store and a cleanup function. The caller must defer cleanup().
func newTestInodeStore(t *testing.T) (*InodeStore, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	origFolder := AzcopyJobPlanFolder
	AzcopyJobPlanFolder = tmpDir

	jobID := NewJobID()
	store, err := NewInodeStore(jobID)
	assert.NoError(t, err)

	cleanup := func() {
		_ = store.file.Close()
		AzcopyJobPlanFolder = origFolder
	}
	return store, cleanup
}

// newTestInodeStoreWithFile creates an InodeStore at a known file path so that
// a second store can be opened against the same file (for rehydration tests).
func newTestInodeStoreWithFile(t *testing.T) (*InodeStore, JobID, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	origFolder := AzcopyJobPlanFolder
	AzcopyJobPlanFolder = tmpDir

	jobID := NewJobID()
	store, err := NewInodeStore(jobID)
	assert.NoError(t, err)

	cleanup := func() {
		_ = store.file.Close()
		AzcopyJobPlanFolder = origFolder
	}
	return store, jobID, cleanup
}

// ──────────────────────────────────────────────────────────
// NewInodeStore
// ──────────────────────────────────────────────────────────

func TestNewInodeStore_CreatesEmptyStore(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	assert.NotNil(t, store)
	assert.Empty(t, store.index)
	assert.Equal(t, int64(0), store.fileSize)
}

func TestNewInodeStore_InvalidFolder(t *testing.T) {
	origFolder := AzcopyJobPlanFolder
	AzcopyJobPlanFolder = "/nonexistent/path/that/does/not/exist"
	defer func() { AzcopyJobPlanFolder = origFolder }()

	_, err := NewInodeStore(NewJobID())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to open inode store file")
}

// ──────────────────────────────────────────────────────────
// GetOrAdd — new inode
// ──────────────────────────────────────────────────────────

func TestGetOrAdd_NewInode(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	existingPath, exists, err := store.GetOrAdd("111", "dir/file1.txt")
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Empty(t, existingPath)

	// Index should now have the inode
	assert.Contains(t, store.index, "111")
}

func TestGetOrAdd_NewInode_FileGrows(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	_, _, err := store.GetOrAdd("111", "a.txt")
	assert.NoError(t, err)

	before := store.fileSize
	assert.Greater(t, before, int64(0))

	_, _, err = store.GetOrAdd("222", "b.txt")
	assert.NoError(t, err)
	assert.Greater(t, store.fileSize, before)
}

// ──────────────────────────────────────────────────────────
// GetOrAdd — existing inode (second path)
// ──────────────────────────────────────────────────────────

func TestGetOrAdd_ExistingInode_ReturnsFirstPath(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	_, _, err := store.GetOrAdd("111", "first.txt")
	assert.NoError(t, err)

	existingPath, exists, err := store.GetOrAdd("111", "second.txt")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, "first.txt", existingPath)
}

func TestGetOrAdd_ExistingInode_UpdatesAnchorWhenSmaller(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	// First add: anchor = "z_file.txt"
	_, _, err := store.GetOrAdd("111", "z_file.txt")
	assert.NoError(t, err)

	// Second add with lexicographically smaller path: anchor should update
	_, _, err = store.GetOrAdd("111", "a_file.txt")
	assert.NoError(t, err)

	anchor, err := store.GetAnchor("111")
	assert.NoError(t, err)
	assert.Equal(t, "a_file.txt", anchor)
}

func TestGetOrAdd_ExistingInode_DoesNotUpdateAnchorWhenLarger(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	// First add: anchor = "a_file.txt"
	_, _, err := store.GetOrAdd("111", "a_file.txt")
	assert.NoError(t, err)

	// Second add with lexicographically larger path: anchor should NOT update
	_, _, err = store.GetOrAdd("111", "z_file.txt")
	assert.NoError(t, err)

	anchor, err := store.GetAnchor("111")
	assert.NoError(t, err)
	assert.Equal(t, "a_file.txt", anchor)
}

func TestGetOrAdd_ExistingInode_SamePathDoesNothing(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	_, _, err := store.GetOrAdd("111", "file.txt")
	assert.NoError(t, err)

	existingPath, exists, err := store.GetOrAdd("111", "file.txt")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, "file.txt", existingPath)
}

// ──────────────────────────────────────────────────────────
// GetOrAdd — multiple inodes
// ──────────────────────────────────────────────────────────

func TestGetOrAdd_MultipleInodes(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	_, _, err := store.GetOrAdd("111", "a.txt")
	assert.NoError(t, err)
	_, _, err = store.GetOrAdd("222", "b.txt")
	assert.NoError(t, err)
	_, _, err = store.GetOrAdd("333", "c.txt")
	assert.NoError(t, err)

	assert.Len(t, store.index, 3)

	a1, err := store.GetAnchor("111")
	assert.NoError(t, err)
	assert.Equal(t, "a.txt", a1)

	a2, err := store.GetAnchor("222")
	assert.NoError(t, err)
	assert.Equal(t, "b.txt", a2)

	a3, err := store.GetAnchor("333")
	assert.NoError(t, err)
	assert.Equal(t, "c.txt", a3)
}

// ──────────────────────────────────────────────────────────
// GetOrAdd — paths with spaces (tab-delimiter correctness)
// ──────────────────────────────────────────────────────────

func TestGetOrAdd_PathsWithSpaces(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	_, _, err := store.GetOrAdd("111", "dir with spaces/file name.txt")
	assert.NoError(t, err)

	existingPath, exists, err := store.GetOrAdd("111", "another path/link.txt")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, "dir with spaces/file name.txt", existingPath)

	anchor, err := store.GetAnchor("111")
	assert.NoError(t, err)
	// "another path/link.txt" < "dir with spaces/file name.txt" lexicographically
	assert.Equal(t, "another path/link.txt", anchor)
}

func TestGetOrAdd_PathsWithMultipleSpaces(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	path := "a   b   c/d  e  f.txt"
	_, _, err := store.GetOrAdd("111", path)
	assert.NoError(t, err)

	anchor, err := store.GetAnchor("111")
	assert.NoError(t, err)
	assert.Equal(t, path, anchor)
}

// ──────────────────────────────────────────────────────────
// GetAnchor
// ──────────────────────────────────────────────────────────

func TestGetAnchor_ExistingInode(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	_, _, err := store.GetOrAdd("111", "dir/anchor.txt")
	assert.NoError(t, err)

	anchor, err := store.GetAnchor("111")
	assert.NoError(t, err)
	assert.Equal(t, "dir/anchor.txt", anchor)
}

func TestGetAnchor_NonExistentInode(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	_, err := store.GetAnchor("999")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGetAnchor_AnchorUpdatesAfterSmallerPath(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	_, _, err := store.GetOrAdd("111", "z.txt")
	assert.NoError(t, err)
	_, _, err = store.GetOrAdd("111", "m.txt")
	assert.NoError(t, err)
	_, _, err = store.GetOrAdd("111", "a.txt")
	assert.NoError(t, err)

	anchor, err := store.GetAnchor("111")
	assert.NoError(t, err)
	assert.Equal(t, "a.txt", anchor, "anchor should be the lexicographically smallest path seen")
}

// ──────────────────────────────────────────────────────────
// overwriteRecord — relocation when content exceeds capacity
// ──────────────────────────────────────────────────────────

func TestOverwriteRecord_Relocation(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	// Add with a short path — sets the initial capacity
	_, _, err := store.GetOrAdd("111", "a.txt")
	assert.NoError(t, err)

	oldMeta := *store.index["111"] // copy

	// Now add a path that is lexicographically smaller but so long it exceeds capacity,
	// forcing record relocation to end of file.
	longPath := strings.Repeat("A", 200) // much longer than recordPadding
	// The anchor update only fires if newPath < currentAnchor, and "AAAA..." < "a.txt"
	_, _, err = store.GetOrAdd("111", longPath)
	assert.NoError(t, err)

	newMeta := store.index["111"]
	assert.Greater(t, newMeta.offset, oldMeta.offset, "record should have moved to a later offset")
	assert.Greater(t, newMeta.capacity, oldMeta.capacity, "capacity should have grown")

	// Verify the anchor was updated correctly despite relocation
	anchor, err := store.GetAnchor("111")
	assert.NoError(t, err)
	assert.Equal(t, longPath, anchor)
}

func TestOverwriteRecord_InPlace(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	// Add with a long-ish path first
	_, _, err := store.GetOrAdd("111", "zzzzz.txt")
	assert.NoError(t, err)

	oldMeta := *store.index["111"]

	// Update with shorter path (fits in existing capacity)
	_, _, err = store.GetOrAdd("111", "a.txt")
	assert.NoError(t, err)

	newMeta := store.index["111"]
	assert.Equal(t, oldMeta.offset, newMeta.offset, "record should stay in place")
	assert.Equal(t, oldMeta.capacity, newMeta.capacity, "capacity should not change")

	anchor, err := store.GetAnchor("111")
	assert.NoError(t, err)
	assert.Equal(t, "a.txt", anchor)
}

// ──────────────────────────────────────────────────────────
// Rehydration — resume from existing file
// ──────────────────────────────────────────────────────────

func TestRehydration_RestoresIndex(t *testing.T) {
	store, jobID, cleanup := newTestInodeStoreWithFile(t)
	defer cleanup()

	_, _, err := store.GetOrAdd("111", "file_a.txt")
	assert.NoError(t, err)
	_, _, err = store.GetOrAdd("222", "file_b.txt")
	assert.NoError(t, err)
	_, _, err = store.GetOrAdd("111", "file_c.txt") // updates anchor
	assert.NoError(t, err)
	_ = store.file.Close()

	// Re-open the same file — should rehydrate
	store2, err := NewInodeStore(jobID)
	assert.NoError(t, err)
	defer store2.file.Close()

	assert.Len(t, store2.index, 2)

	anchor1, err := store2.GetAnchor("111")
	assert.NoError(t, err)
	assert.Equal(t, "file_a.txt", anchor1, "anchor should still be firstPath=file_a.txt (file_c > file_a)")

	anchor2, err := store2.GetAnchor("222")
	assert.NoError(t, err)
	assert.Equal(t, "file_b.txt", anchor2)
}

func TestRehydration_FileSize(t *testing.T) {
	store, jobID, cleanup := newTestInodeStoreWithFile(t)
	defer cleanup()

	_, _, err := store.GetOrAdd("111", "a.txt")
	assert.NoError(t, err)
	_, _, err = store.GetOrAdd("222", "b.txt")
	assert.NoError(t, err)

	originalSize := store.fileSize
	_ = store.file.Close()

	store2, err := NewInodeStore(jobID)
	assert.NoError(t, err)
	defer store2.file.Close()

	assert.Equal(t, originalSize, store2.fileSize, "fileSize should match the on-disk file size")
}

func TestRehydration_ContinuesWritingAtCorrectOffset(t *testing.T) {
	store, jobID, cleanup := newTestInodeStoreWithFile(t)
	defer cleanup()

	_, _, err := store.GetOrAdd("111", "a.txt")
	assert.NoError(t, err)
	_ = store.file.Close()

	// Reopen and add more
	store2, err := NewInodeStore(jobID)
	assert.NoError(t, err)
	defer store2.file.Close()

	_, _, err = store2.GetOrAdd("222", "b.txt")
	assert.NoError(t, err)

	assert.Len(t, store2.index, 2)

	a1, err := store2.GetAnchor("111")
	assert.NoError(t, err)
	assert.Equal(t, "a.txt", a1)

	a2, err := store2.GetAnchor("222")
	assert.NoError(t, err)
	assert.Equal(t, "b.txt", a2)
}

func TestRehydration_RelocatedRecordLastWins(t *testing.T) {
	store, jobID, cleanup := newTestInodeStoreWithFile(t)
	defer cleanup()

	// Add inode, then force anchor update with a very long path to trigger relocation
	_, _, err := store.GetOrAdd("111", "z.txt")
	assert.NoError(t, err)

	longAnchor := strings.Repeat("A", 200) // "AAA..." < "z.txt", forces relocation
	_, _, err = store.GetOrAdd("111", longAnchor)
	assert.NoError(t, err)
	_ = store.file.Close()

	// Rehydrate — the last (relocated) record for inode "111" should win
	store2, err := NewInodeStore(jobID)
	assert.NoError(t, err)
	defer store2.file.Close()

	assert.Len(t, store2.index, 1)

	anchor, err := store2.GetAnchor("111")
	assert.NoError(t, err)
	assert.Equal(t, longAnchor, anchor)
}

func TestRehydration_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	origFolder := AzcopyJobPlanFolder
	AzcopyJobPlanFolder = tmpDir
	defer func() { AzcopyJobPlanFolder = origFolder }()

	jobID := NewJobID()
	store, err := NewInodeStore(jobID)
	assert.NoError(t, err)
	defer store.file.Close()

	assert.Empty(t, store.index)
	assert.Equal(t, int64(0), store.fileSize)
}

// ──────────────────────────────────────────────────────────
// Rehydration — paths with spaces survive round-trip
// ──────────────────────────────────────────────────────────

func TestRehydration_PathsWithSpacesSurvive(t *testing.T) {
	store, jobID, cleanup := newTestInodeStoreWithFile(t)
	defer cleanup()

	spacePath := "my dir/my file.txt"
	_, _, err := store.GetOrAdd("111", spacePath)
	assert.NoError(t, err)
	_ = store.file.Close()

	store2, err := NewInodeStore(jobID)
	assert.NoError(t, err)
	defer store2.file.Close()

	anchor, err := store2.GetAnchor("111")
	assert.NoError(t, err)
	assert.Equal(t, spacePath, anchor)
}

// ──────────────────────────────────────────────────────────
// writeRecord / readRecord round-trip
// ──────────────────────────────────────────────────────────

func TestWriteReadRecord_RoundTrip(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	offset, capacity, err := store.writeRecord("42", "first/path.txt", "anchor/path.txt")
	assert.NoError(t, err)

	meta := &inodeMeta{offset: offset, capacity: capacity}
	firstPath, anchor, err := store.readRecord(meta)
	assert.NoError(t, err)
	assert.Equal(t, "first/path.txt", firstPath)
	assert.Equal(t, "anchor/path.txt", anchor)
}

func TestWriteReadRecord_WithSpaces(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	offset, capacity, err := store.writeRecord("42", "dir one/file two.txt", "dir three/file four.txt")
	assert.NoError(t, err)

	meta := &inodeMeta{offset: offset, capacity: capacity}
	firstPath, anchor, err := store.readRecord(meta)
	assert.NoError(t, err)
	assert.Equal(t, "dir one/file two.txt", firstPath)
	assert.Equal(t, "dir three/file four.txt", anchor)
}

func TestWriteReadRecord_LongPaths(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	longPath := strings.Repeat("x", 4000) + "/file.txt"
	offset, capacity, err := store.writeRecord("99", longPath, longPath)
	assert.NoError(t, err)

	meta := &inodeMeta{offset: offset, capacity: capacity}
	firstPath, anchor, err := store.readRecord(meta)
	assert.NoError(t, err)
	assert.Equal(t, longPath, firstPath)
	assert.Equal(t, longPath, anchor)
}

func TestWriteReadRecord_MultipleRecords(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	o1, c1, err := store.writeRecord("1", "a.txt", "a.txt")
	assert.NoError(t, err)
	o2, c2, err := store.writeRecord("2", "b.txt", "b.txt")
	assert.NoError(t, err)
	o3, c3, err := store.writeRecord("3", "c.txt", "c.txt")
	assert.NoError(t, err)

	// Records should be at increasing offsets
	assert.Less(t, o1, o2)
	assert.Less(t, o2, o3)

	// Verify each is independently readable
	fp1, a1, err := store.readRecord(&inodeMeta{offset: o1, capacity: c1})
	assert.NoError(t, err)
	assert.Equal(t, "a.txt", fp1)
	assert.Equal(t, "a.txt", a1)

	fp2, a2, err := store.readRecord(&inodeMeta{offset: o2, capacity: c2})
	assert.NoError(t, err)
	assert.Equal(t, "b.txt", fp2)
	assert.Equal(t, "b.txt", a2)

	fp3, a3, err := store.readRecord(&inodeMeta{offset: o3, capacity: c3})
	assert.NoError(t, err)
	assert.Equal(t, "c.txt", fp3)
	assert.Equal(t, "c.txt", a3)
}

// ──────────────────────────────────────────────────────────
// overwriteRecord — direct tests
// ──────────────────────────────────────────────────────────

func TestOverwriteRecord_InPlace_Direct(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	offset, capacity, err := store.writeRecord("42", "first.txt", "z_anchor.txt")
	assert.NoError(t, err)

	meta := &inodeMeta{offset: offset, capacity: capacity}
	err = store.overwriteRecord(meta, "42", "first.txt", "a_anchor.txt")
	assert.NoError(t, err)

	// Offset and capacity should not change for in-place update
	assert.Equal(t, offset, meta.offset)
	assert.Equal(t, capacity, meta.capacity)

	firstPath, anchor, err := store.readRecord(meta)
	assert.NoError(t, err)
	assert.Equal(t, "first.txt", firstPath)
	assert.Equal(t, "a_anchor.txt", anchor)
}

func TestOverwriteRecord_Relocate_Direct(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	offset, capacity, err := store.writeRecord("42", "f.txt", "a.txt")
	assert.NoError(t, err)

	meta := &inodeMeta{offset: offset, capacity: capacity}
	longAnchor := strings.Repeat("x", 300)
	err = store.overwriteRecord(meta, "42", "f.txt", longAnchor)
	assert.NoError(t, err)

	// After relocation, offset should be beyond the original
	assert.Greater(t, meta.offset, offset)
	assert.Greater(t, meta.capacity, capacity)

	firstPath, anchor, err := store.readRecord(meta)
	assert.NoError(t, err)
	assert.Equal(t, "f.txt", firstPath)
	assert.Equal(t, longAnchor, anchor)
}

// ──────────────────────────────────────────────────────────
// rehydrateInodeStore — direct tests
// ──────────────────────────────────────────────────────────

func TestRehydrateInodeStore_ValidRecords(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test_store.txt")
	f, err := os.Create(tmpFile)
	assert.NoError(t, err)
	defer f.Close()

	// Manually write two tab-delimited, padded records
	writeTestRecord(t, f, "111", "path_a.txt", "path_a.txt")
	writeTestRecord(t, f, "222", "path_b.txt", "path_b.txt")

	fi, _ := f.Stat()
	index, err := rehydrateInodeStore(f, fi.Size())
	assert.NoError(t, err)
	assert.Len(t, index, 2)
	assert.Contains(t, index, "111")
	assert.Contains(t, index, "222")
}

func TestRehydrateInodeStore_LastOccurrenceWins(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test_store.txt")
	f, err := os.Create(tmpFile)
	assert.NoError(t, err)
	defer f.Close()

	// First record for inode "111"
	writeTestRecord(t, f, "111", "old.txt", "old.txt")
	firstRecordEnd, _ := f.Seek(0, io.SeekCurrent)

	// Second (relocated) record for inode "111"
	writeTestRecord(t, f, "111", "new.txt", "new.txt")

	fi, _ := f.Stat()
	index, err := rehydrateInodeStore(f, fi.Size())
	assert.NoError(t, err)
	assert.Len(t, index, 1)
	assert.Contains(t, index, "111")
	// The second record should win — its offset is beyond the first
	assert.GreaterOrEqual(t, index["111"].offset, firstRecordEnd)
}

func TestRehydrateInodeStore_SkipsCorruptRecords(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test_store.txt")
	f, err := os.Create(tmpFile)
	assert.NoError(t, err)
	defer f.Close()

	// Write a valid record
	writeTestRecord(t, f, "111", "good.txt", "good.txt")

	// Write a corrupt record (only two fields instead of three)
	_, _ = f.WriteString("corrupt_no_tabs_here                        \n")

	// Write another valid record
	writeTestRecord(t, f, "222", "also_good.txt", "also_good.txt")

	fi, _ := f.Stat()
	index, err := rehydrateInodeStore(f, fi.Size())
	assert.NoError(t, err)
	assert.Len(t, index, 2)
	assert.Contains(t, index, "111")
	assert.Contains(t, index, "222")
}

func TestRehydrateInodeStore_EmptyFile(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "empty_store.txt")
	f, err := os.Create(tmpFile)
	assert.NoError(t, err)
	defer f.Close()

	fi, _ := f.Stat()
	index, err := rehydrateInodeStore(f, fi.Size())
	assert.NoError(t, err)
	assert.Empty(t, index)
}

func TestRehydrateInodeStore_PathsWithSpaces(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test_store.txt")
	f, err := os.Create(tmpFile)
	assert.NoError(t, err)
	defer f.Close()

	writeTestRecord(t, f, "111", "my dir/my file.txt", "my dir/my file.txt")

	fi, _ := f.Stat()
	index, err := rehydrateInodeStore(f, fi.Size())
	assert.NoError(t, err)
	assert.Len(t, index, 1)
	assert.Contains(t, index, "111")

	// Verify the record can be read correctly via the store
	store := &InodeStore{
		index:    index,
		file:     f,
		fileSize: fi.Size(),
	}
	fp, anchor, err := store.readRecord(index["111"])
	assert.NoError(t, err)
	assert.Equal(t, "my dir/my file.txt", fp)
	assert.Equal(t, "my dir/my file.txt", anchor)
}

// ──────────────────────────────────────────────────────────
// Concurrent access
// ──────────────────────────────────────────────────────────

func TestConcurrentGetOrAdd(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			inode := fmt.Sprintf("%d", n%10) // 10 distinct inodes
			path := fmt.Sprintf("dir/file_%d.txt", n)
			_, _, err := store.GetOrAdd(inode, path)
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	// Should have exactly 10 inodes
	assert.Len(t, store.index, 10)

	// All anchors should be readable
	for i := 0; i < 10; i++ {
		inode := fmt.Sprintf("%d", i)
		_, err := store.GetAnchor(inode)
		assert.NoError(t, err)
	}
}

func TestConcurrentGetAnchor(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	// Pre-populate
	for i := 0; i < 10; i++ {
		_, _, err := store.GetOrAdd(fmt.Sprintf("%d", i), fmt.Sprintf("file_%d.txt", i))
		assert.NoError(t, err)
	}

	// Concurrent reads
	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			inode := fmt.Sprintf("%d", n%10)
			anchor, err := store.GetAnchor(inode)
			assert.NoError(t, err)
			assert.NotEmpty(t, anchor)
		}(i)
	}
	wg.Wait()
}

// ──────────────────────────────────────────────────────────
// Edge cases
// ──────────────────────────────────────────────────────────

func TestGetOrAdd_EmptyPath(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	_, _, err := store.GetOrAdd("111", "")
	assert.NoError(t, err)

	anchor, err := store.GetAnchor("111")
	assert.NoError(t, err)
	assert.Equal(t, "", anchor)
}

func TestGetOrAdd_SpecialCharactersInPath(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	// Various special characters that are valid in file paths
	specialPaths := []struct {
		inode string
		path  string
	}{
		{"1", "file with spaces.txt"},
		{"2", "file-with-dashes.txt"},
		{"3", "file_with_underscores.txt"},
		{"4", "file.multiple.dots.txt"},
		{"5", "dir/sub dir/file (1).txt"},
		{"6", "日本語/ファイル.txt"},
		{"7", "émojis/café.txt"},
		{"8", "path with  double  spaces/file.txt"},
	}

	for _, sp := range specialPaths {
		_, _, err := store.GetOrAdd(sp.inode, sp.path)
		assert.NoError(t, err, "failed for path: %s", sp.path)

		anchor, err := store.GetAnchor(sp.inode)
		assert.NoError(t, err, "failed to get anchor for path: %s", sp.path)
		assert.Equal(t, sp.path, anchor, "anchor mismatch for path: %s", sp.path)
	}
}

func TestGetOrAdd_LargeNumberOfInodes(t *testing.T) {
	store, cleanup := newTestInodeStore(t)
	defer cleanup()

	const count = 500
	for i := 0; i < count; i++ {
		_, _, err := store.GetOrAdd(fmt.Sprintf("%d", i), fmt.Sprintf("dir/%d/file.txt", i))
		assert.NoError(t, err)
	}

	assert.Len(t, store.index, count)

	// Spot check a few
	for _, i := range []int{0, 99, 250, 499} {
		anchor, err := store.GetAnchor(fmt.Sprintf("%d", i))
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("dir/%d/file.txt", i), anchor)
	}
}

// ──────────────────────────────────────────────────────────
// Integration: full lifecycle (write → close → rehydrate → write more → verify)
// ──────────────────────────────────────────────────────────

func TestFullLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	origFolder := AzcopyJobPlanFolder
	AzcopyJobPlanFolder = tmpDir
	defer func() { AzcopyJobPlanFolder = origFolder }()

	jobID := NewJobID()

	// Phase 1: initial job writes
	store, err := NewInodeStore(jobID)
	assert.NoError(t, err)

	_, _, err = store.GetOrAdd("100", "z.txt")
	assert.NoError(t, err)
	_, _, err = store.GetOrAdd("200", "y.txt")
	assert.NoError(t, err)
	_, _, err = store.GetOrAdd("100", "a.txt") // anchor update
	assert.NoError(t, err)
	_ = store.file.Close()

	// Phase 2: resume — rehydrate and add more
	store2, err := NewInodeStore(jobID)
	assert.NoError(t, err)

	assert.Len(t, store2.index, 2)
	anchor, _ := store2.GetAnchor("100")
	assert.Equal(t, "a.txt", anchor, "anchor from phase 1 should survive")

	_, _, err = store2.GetOrAdd("300", "dir/new.txt")
	assert.NoError(t, err)
	_, _, err = store2.GetOrAdd("200", "b.txt") // update anchor for 200
	assert.NoError(t, err)
	_ = store2.file.Close()

	// Phase 3: verify everything survives a second rehydration
	store3, err := NewInodeStore(jobID)
	assert.NoError(t, err)
	defer store3.file.Close()

	assert.Len(t, store3.index, 3)

	a100, _ := store3.GetAnchor("100")
	assert.Equal(t, "a.txt", a100)

	a200, _ := store3.GetAnchor("200")
	assert.Equal(t, "b.txt", a200)

	a300, _ := store3.GetAnchor("300")
	assert.Equal(t, "dir/new.txt", a300)
}

// ──────────────────────────────────────────────────────────
// helpers
// ──────────────────────────────────────────────────────────

// writeTestRecord writes a tab-delimited, padded record directly to a file
// (bypassing InodeStore) for use in rehydration tests.
func writeTestRecord(t *testing.T, f *os.File, inode, firstPath, anchor string) {
	t.Helper()
	content := fmt.Sprintf("%s\t%s\t%s", inode, firstPath, anchor)
	capacity := len(content) + recordPadding + 1
	padded := content + strings.Repeat(" ", capacity-len(content)-1) + "\n"
	_, err := f.Write([]byte(padded))
	assert.NoError(t, err)
}
