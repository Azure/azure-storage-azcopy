package traverser

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

type countedEntity struct {
	entityType common.EntityType
	symlinks   common.SymlinkHandlingType
}

// traverseLocalSymlinkDir traverses a directory holding one file and one symlink, and returns what was reported to the enumeration counter.
func traverseLocalSymlinkDir(t *testing.T, fromTo common.FromTo, recursive bool, handling common.SymlinkHandlingType) []countedEntity {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("creating symlinks requires privileges on Windows")
	}

	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "target.txt"), []byte("data"), 0o600))
	require.NoError(t, os.Symlink(filepath.Join(root, "target.txt"), filepath.Join(root, "link")))

	var mu sync.Mutex
	var counted []countedEntity
	tr, err := NewLocalTraverser(root, context.Background(), InitResourceTraverserOptions{
		Recursive:       recursive,
		SymlinkHandling: handling,
		FromTo:          fromTo,
		ErrorChannel:    make(chan TraverserErrorItemInfo, 16),
		IncrementEnumeration: func(entityType common.EntityType, symlinks common.SymlinkHandlingType, _ common.HardlinkHandlingType) {
			mu.Lock()
			defer mu.Unlock()
			counted = append(counted, countedEntity{entityType, symlinks})
		},
	})
	require.NoError(t, err)

	require.NoError(t, tr.Traverse(nil, func(StoredObject) error { return nil }, nil))
	return counted
}

func countSymlinks(counted []countedEntity, handling common.SymlinkHandlingType) int {
	n := 0
	for _, c := range counted {
		if c.entityType == common.EEntityType.Symlink() && c.symlinks == handling {
			n++
		}
	}
	return n
}

func TestLocalTraverserReportsSkippedSymlinkForAllFromTos(t *testing.T) {
	for _, fromTo := range []common.FromTo{common.EFromTo.LocalBlob(), common.EFromTo.LocalFile(), common.EFromTo.LocalBlobFS()} {
		for _, recursive := range []bool{true, false} {
			t.Run(fromTo.String()+map[bool]string{true: "/recursive", false: "/flat"}[recursive], func(t *testing.T) {
				counted := traverseLocalSymlinkDir(t, fromTo, recursive, common.ESymlinkHandlingType.Skip())
				require.Equal(t, 1, countSymlinks(counted, common.ESymlinkHandlingType.Skip()))
			})
		}
	}
}

func TestLocalTraverserDoesNotReportPreservedSymlinkAsSkipped(t *testing.T) {
	for _, recursive := range []bool{true, false} {
		t.Run(map[bool]string{true: "recursive", false: "flat"}[recursive], func(t *testing.T) {
			counted := traverseLocalSymlinkDir(t, common.EFromTo.LocalBlob(), recursive, common.ESymlinkHandlingType.Preserve())
			require.Zero(t, countSymlinks(counted, common.ESymlinkHandlingType.Skip()))
			require.Equal(t, 1, countSymlinks(counted, common.ESymlinkHandlingType.Preserve()))
		})
	}
}

func TestLocalTraverserDoesNotReportFollowedSymlinkAsSkipped(t *testing.T) {
	for _, recursive := range []bool{true, false} {
		t.Run(map[bool]string{true: "recursive", false: "flat"}[recursive], func(t *testing.T) {
			counted := traverseLocalSymlinkDir(t, common.EFromTo.LocalBlob(), recursive, common.ESymlinkHandlingType.Follow())
			for _, c := range counted {
				require.NotEqual(t, common.EEntityType.Symlink(), c.entityType, "a followed symlink is reported as its target")
			}
		})
	}
}
