package traverser

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

var errProcessing = errors.New("processing failed")

// setupWalkTest returns a folder holding three files.
func setupWalkTest(t *testing.T) string {
	t.Helper()
	prevHooks := common.GetLifecycleMgr()
	common.SetUIHooks(common.NewJobUIHooks()) // sync sets these before it scans; scan warnings need them
	t.Cleanup(func() { common.SetUIHooks(prevHooks) })

	dir := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0o644))
	}
	return dir
}

func TestWalkWithSymlinksReturnsCallbackError(t *testing.T) {
	cases := []struct {
		name   string
		failOn func(fi os.FileInfo) bool
	}{
		{"root", func(fi os.FileInfo) bool { return fi.IsDir() }},
		{"file", func(fi os.FileInfo) bool { return !fi.IsDir() }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := setupWalkTest(t)

			err := WalkWithSymlinks(context.Background(), dir, func(_ string, fi os.FileInfo, _ error) error {
				if tc.failOn(fi) {
					return errProcessing
				}
				return nil
			}, common.ESymlinkHandlingType.Skip(), nil, common.DefaultHardlinkHandlingType, nil, common.EFromTo.LocalBlob())

			require.ErrorIs(t, err, errProcessing)
		})
	}
}

type recordingTraverser struct {
	traversed bool
}

func (r *recordingTraverser) IsDirectory(bool) (bool, error) {
	return true, nil
}

func (r *recordingTraverser) Traverse(objectMorpher, ObjectProcessor, []ObjectFilter) error {
	r.traversed = true
	return nil
}

// For uploads the destination is compared against the source index, and anything missing from it
// is deleted, so a failed source scan has to stop sync before the destination is scanned.
func TestSyncEnumeratorStopsWhenLocalSourceScanFails(t *testing.T) {
	source, err := NewLocalTraverser(setupWalkTest(t), context.Background(), InitResourceTraverserOptions{
		Recursive: true,
		FromTo:    common.EFromTo.LocalBlob(),
	})
	require.NoError(t, err)

	failOnFile := func(obj StoredObject) error {
		if obj.EntityType == common.EEntityType.File() {
			return errProcessing
		}
		return nil
	}
	destination := &recordingTraverser{}
	enumerator := NewSyncEnumeratorWithObservers(source, destination, NewObjectIndexer(), nil,
		func(StoredObject) error { return nil }, func() error { return nil }, failOnFile, nil)

	require.ErrorIs(t, enumerator.Enumerate(), errProcessing)
	require.False(t, destination.traversed, "sync scanned the destination after the source scan failed")
}
