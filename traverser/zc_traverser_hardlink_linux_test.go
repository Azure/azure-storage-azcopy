//go:build linux

package traverser

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func localHardlinkTestRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp(".", "hardlink-local-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(root)) })
	absoluteRoot, err := filepath.Abs(root)
	require.NoError(t, err)
	return absoluteRoot
}

func TestLocalNFSPreserveHardlinksKeepsFullRootAndMetadata(t *testing.T) {
	root := localHardlinkTestRoot(t)
	require.NoError(t, os.Mkdir(filepath.Join(root, "a"), 0755))
	require.NoError(t, os.Mkdir(filepath.Join(root, "z"), 0755))
	first := filepath.Join(root, "z", "source")
	require.NoError(t, os.WriteFile(first, []byte("content"), 0644))
	require.NoError(t, os.Link(first, filepath.Join(root, "a", "link\t\n ")))
	require.NoError(t, os.WriteFile(filepath.Join(root, "regular"), []byte("other"), 0644))
	store := common.NewInodeStoreFromBackend(&hardlinkStoreBuffer{})
	defer store.Close()
	traverser, err := NewLocalTraverser(root, context.Background(), InitResourceTraverserOptions{
		Recursive: true, FromTo: common.EFromTo.LocalFileNFS(),
		HardlinkHandling: common.EHardlinkHandlingType.Preserve(), InodeStore: store,
	})
	require.NoError(t, err)
	var mutex sync.Mutex
	objects := make(map[string]StoredObject)
	err = traverser.Traverse(nil, func(object StoredObject) error {
		mutex.Lock()
		objects[object.RelativePath] = object
		mutex.Unlock()
		return nil
	}, nil)
	require.NoError(t, err)
	a, z := objects["a/link\t\n "], objects["z/source"]
	require.Equal(t, common.EEntityType.Hardlink(), a.EntityType)
	require.NotEmpty(t, a.Inode)
	require.Equal(t, a.Inode, z.Inode)
	anchor, err := store.GetAnchor(a.Inode)
	require.NoError(t, err)
	require.Equal(t, "a/link\t\n ", anchor)
	require.Empty(t, objects["regular"].Inode)
	require.Empty(t, objects["regular"].TargetHardlinkFile)
	require.Equal(t, common.EEntityType.Folder(), objects["a"].EntityType)
}

func TestLocalNFSNonrecursiveSkipContinuesAfterHardlink(t *testing.T) {
	root := localHardlinkTestRoot(t)
	first := filepath.Join(root, "a")
	require.NoError(t, os.WriteFile(first, []byte("content"), 0644))
	require.NoError(t, os.Link(first, filepath.Join(root, "b")))
	require.NoError(t, os.WriteFile(filepath.Join(root, "z"), []byte("ordinary"), 0644))
	traverser, err := NewLocalTraverser(root, context.Background(), InitResourceTraverserOptions{
		FromTo: common.EFromTo.LocalFileNFS(), HardlinkHandling: common.EHardlinkHandlingType.Skip(),
	})
	require.NoError(t, err)
	var names []string
	require.NoError(t, traverser.Traverse(nil, func(object StoredObject) error {
		names = append(names, object.Name)
		return nil
	}, nil))
	require.Equal(t, []string{"z"}, names)
}
