package traverser

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestLocalTraverserLeavesStrippedRootOutOfFolderCount(t *testing.T) {
	for _, test := range []struct {
		stripTopDir bool
		folders     int
	}{
		{false, 2}, // root and sub
		{true, 1},  // sub only
	} {
		root := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(root, "sub"), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(root, "sub", "file.txt"), []byte("data"), 0o600))

		folders, files := 0, 0
		tr, err := NewLocalTraverser(root, context.Background(), InitResourceTraverserOptions{
			Recursive:    true,
			StripTopDir:  test.stripTopDir,
			FromTo:       common.EFromTo.LocalBlob(),
			ErrorChannel: make(chan TraverserErrorItemInfo, 16),
			IncrementEnumeration: func(entityType common.EntityType, _ common.SymlinkHandlingType, _ common.HardlinkHandlingType) {
				switch entityType {
				case common.EEntityType.Folder():
					folders++
				case common.EEntityType.File():
					files++
				}
			},
		})
		require.NoError(t, err)
		require.NoError(t, tr.Traverse(nil, func(StoredObject) error { return nil }, nil))

		require.Equal(t, test.folders, folders, "stripTopDir=%v", test.stripTopDir)
		require.Equal(t, 1, files)
	}
}

func TestFileTraverserReceivesStripTopDir(t *testing.T) {
	client, err := fileservice.NewClientWithNoCredential("https://account.file.core.windows.net", nil)
	require.NoError(t, err)
	for _, strip := range []bool{true, false} {
		tr := NewFileTraverser("https://account.file.core.windows.net/share", client, context.Background(), InitResourceTraverserOptions{StripTopDir: strip})
		require.Equal(t, strip, tr.stripTopDir)
	}
}
