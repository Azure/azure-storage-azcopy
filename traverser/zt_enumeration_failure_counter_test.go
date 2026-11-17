package traverser

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

type failureRecorder struct {
	mu       sync.Mutex
	failures []common.EntityType
	scanned  []common.EntityType
	ch       chan TraverserErrorItemInfo
}

func (r *failureRecorder) fail(entityType common.EntityType) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, entityType)
}

func (r *failureRecorder) scan(entityType common.EntityType, _ common.SymlinkHandlingType, _ common.HardlinkHandlingType) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scanned = append(r.scanned, entityType)
}

func newRecordedLocalTraverser(t *testing.T, path string, rec *failureRecorder, adjust ...func(*InitResourceTraverserOptions)) *localTraverser {
	t.Helper()
	rec.ch = make(chan TraverserErrorItemInfo, 16)
	opts := InitResourceTraverserOptions{
		FromTo:                      common.EFromTo.LocalBlob(),
		ErrorChannel:                rec.ch,
		IncrementEnumeration:        rec.scan,
		IncrementEnumerationFailure: rec.fail,
	}
	for _, fn := range adjust {
		fn(&opts)
	}
	tr, err := NewLocalTraverser(path, context.Background(), opts)
	require.NoError(t, err)
	return tr
}

func TestLocalTraverserCountsProcessorFailureAsFileFailure(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0o600))

	rec := &failureRecorder{}
	err := newRecordedLocalTraverser(t, root, rec).Traverse(nil, func(StoredObject) error { return errors.New("boom") }, nil)

	require.Error(t, err)
	var rootErr *rootScanError
	require.False(t, errors.As(err, &rootErr))
	require.Equal(t, []common.EntityType{common.EEntityType.File()}, rec.failures)
	require.Empty(t, rec.scanned)
	require.Len(t, rec.ch, 1)
}

func TestLocalTraverserCountsAndSendsProcessorFailureOnEveryPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating symlinks requires privileges on Windows")
	}
	fail := func(entityType common.EntityType) ObjectProcessor {
		return func(object StoredObject) error {
			if object.EntityType == entityType {
				return errors.New("boom")
			}
			return nil
		}
	}

	t.Run("single file", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "file.txt")
		require.NoError(t, os.WriteFile(file, []byte("data"), 0o600))
		rec := &failureRecorder{}

		require.Error(t, newRecordedLocalTraverser(t, file, rec).Traverse(nil, fail(common.EEntityType.File()), nil))
		require.Equal(t, []common.EntityType{common.EEntityType.File()}, rec.failures)
		require.Empty(t, rec.scanned)
		require.Len(t, rec.ch, 1)
	})

	recursive := func(options *InitResourceTraverserOptions) { options.Recursive = true }
	t.Run("recursive walk", func(t *testing.T) {
		root := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0o600))
		rec := &failureRecorder{}

		// parallel.Walk drops the error that stops it, so only the counts and the channel are asserted here.
		_ = newRecordedLocalTraverser(t, root, rec, recursive).Traverse(nil, fail(common.EEntityType.File()), nil)
		require.Equal(t, []common.EntityType{common.EEntityType.File()}, rec.failures)
		require.NotContains(t, rec.scanned, common.EEntityType.File())
		require.Len(t, rec.ch, 1)
	})

	t.Run("recursive walk of a preserved symlink", func(t *testing.T) {
		root := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(root, "target.txt"), []byte("data"), 0o600))
		require.NoError(t, os.Symlink(filepath.Join(root, "target.txt"), filepath.Join(root, "link")))
		rec := &failureRecorder{}
		preserve := func(options *InitResourceTraverserOptions) {
			options.SymlinkHandling = common.ESymlinkHandlingType.Preserve()
		}

		_ = newRecordedLocalTraverser(t, root, rec, recursive, preserve).Traverse(nil, fail(common.EEntityType.Symlink()), nil)
		require.Equal(t, []common.EntityType{common.EEntityType.Symlink()}, rec.failures)
		require.NotContains(t, rec.scanned, common.EEntityType.Symlink())
		require.Len(t, rec.ch, 1)
	})
}

func TestLocalTraverserCountsUnreadableDirectoryAsFolderFailure(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs a directory the current user cannot read")
	}
	root := t.TempDir()
	require.NoError(t, os.Chmod(root, 0o000))
	t.Cleanup(func() { _ = os.Chmod(root, 0o700) })

	rec := &failureRecorder{}
	err := newRecordedLocalTraverser(t, root, rec).Traverse(nil, func(StoredObject) error { return nil }, nil)

	require.Error(t, err)
	require.Equal(t, []common.EntityType{common.EEntityType.Folder()}, rec.failures)
}

func TestLocalTraverserLeavesRootScanFailureToCaller(t *testing.T) {
	captureCrawlLogs(t)
	rec := &failureRecorder{}
	err := newRecordedLocalTraverser(t, filepath.Join(t.TempDir(), "missing"), rec).Traverse(nil, func(StoredObject) error { return nil }, nil)

	var rootErr *rootScanError
	require.ErrorAs(t, err, &rootErr)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Empty(t, rec.failures)
}

// TestFileTraverserCountsProcessorFailureOnce pins that a failed entity is counted as scanned by the failure callback only.
func TestFileTraverserCountsProcessorFailureOnce(t *testing.T) {
	rec := &failureRecorder{}
	traversal, _ := newFilesTestTraverser(t, filesTestListing(filesTestFile("a.txt")), InitResourceTraverserOptions{
		Recursive:                   true,
		ErrorChannel:                make(chan TraverserErrorItemInfo, 4),
		IncrementEnumeration:        rec.scan,
		IncrementEnumerationFailure: rec.fail,
	})

	err := traversal.Traverse(NoPreProccessor, func(object StoredObject) error {
		if object.EntityType == common.EEntityType.File() {
			return errors.New("boom")
		}
		return nil
	}, nil)

	require.Error(t, err)
	require.Equal(t, []common.EntityType{common.EEntityType.File()}, rec.failures)
	require.NotContains(t, rec.scanned, common.EEntityType.File())
}

func failingProcessor(entityType common.EntityType) ObjectProcessor {
	return func(object StoredObject) error {
		if object.EntityType == entityType {
			return errors.New("boom")
		}
		return nil
	}
}

func TestFileTraverserCountsRootProcessorFailureAsFolder(t *testing.T) {
	rec := &failureRecorder{}
	traversal, _ := newFilesTestTraverser(t, filesTestListing(), InitResourceTraverserOptions{
		Recursive:                   true,
		ErrorChannel:                make(chan TraverserErrorItemInfo, 4),
		IncrementEnumeration:        rec.scan,
		IncrementEnumerationFailure: rec.fail,
	})

	require.Error(t, traversal.Traverse(NoPreProccessor, failingProcessor(common.EEntityType.Folder()), nil))
	require.Equal(t, []common.EntityType{common.EEntityType.Folder()}, rec.failures)
	require.Empty(t, rec.scanned)
}

func TestBlobTraverserCountsRootBlobProcessorFailureOnce(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := w.Header()
		header.Set("Last-Modified", "Sun, 13 Sep 2020 12:26:40 GMT")
		header.Set("ETag", `"0x1"`)
		header.Set("Content-Length", "5")
		header.Set("Content-Type", "text/plain")
		header.Set("x-ms-blob-type", "BlockBlob")
	}))
	t.Cleanup(server.Close)
	endpoint := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	client, err := service.NewClientWithNoCredential(endpoint, nil)
	require.NoError(t, err)

	rec := &failureRecorder{}
	traversal := NewBlobTraverser(endpoint+"/c/blob.txt", client, context.Background(), InitResourceTraverserOptions{
		ErrorChannel:                make(chan TraverserErrorItemInfo, 4),
		IncrementEnumeration:        rec.scan,
		IncrementEnumerationFailure: rec.fail,
	})

	require.Error(t, traversal.Traverse(NoPreProccessor, failingProcessor(common.EEntityType.File()), nil))
	require.Equal(t, []common.EntityType{common.EEntityType.File()}, rec.failures)
	require.Empty(t, rec.scanned)
}

func TestBlobSerialListCountsProcessorAndListingFailures(t *testing.T) {
	t.Run("processor", func(t *testing.T) {
		listing, _ := newFlatTestListing(t, context.Background(), [][]string{{"a"}}, "", nil)
		rec := &failureRecorder{}
		listing.traverser.errorChannel = make(chan TraverserErrorItemInfo, 4)
		listing.traverser.incrementEnumerationCounter = rec.scan
		listing.traverser.incrementEnumerationFailure = rec.fail

		require.Error(t, listing.Traverse(NoPreProccessor, failingProcessor(common.EEntityType.File()), nil))
		require.Equal(t, []common.EntityType{common.EEntityType.File()}, rec.failures)
		require.Empty(t, rec.scanned)
	})

	t.Run("listing", func(t *testing.T) {
		listing, _ := newFlatTestListing(t, context.Background(), [][]string{{"a"}}, "",
			func(_ int, w http.ResponseWriter, _ *http.Request) bool {
				w.WriteHeader(http.StatusBadRequest)
				return false
			})
		rec := &failureRecorder{}
		listing.traverser.errorChannel = make(chan TraverserErrorItemInfo, 4)
		listing.traverser.incrementEnumerationFailure = rec.fail

		require.Error(t, listing.Traverse(NoPreProccessor, func(StoredObject) error { return nil }, nil))
		require.Equal(t, []common.EntityType{common.EEntityType.Folder()}, rec.failures)
	})
}
