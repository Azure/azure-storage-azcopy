package traverser

import (
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

type hardlinkStoreBuffer struct{ data []byte }

func (b *hardlinkStoreBuffer) ReadAt(data []byte, offset int64) (int, error) {
	if offset >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n := copy(data, b.data[offset:])
	if n != len(data) {
		return n, io.EOF
	}
	return n, nil
}

func (b *hardlinkStoreBuffer) WriteAt(data []byte, offset int64) (int, error) {
	end := int(offset) + len(data)
	if end > len(b.data) {
		b.data = append(b.data, make([]byte, end-len(b.data))...)
	}
	return copy(b.data[offset:], data), nil
}

func (*hardlinkStoreBuffer) Close() error { return nil }

func hardlinkTestObject(t *testing.T, store *common.InodeStore, namespace, name, relative string) StoredObject {
	t.Helper()
	metadata, err := registerHardlink(store, namespace, "same-file-id", relative)
	require.NoError(t, err)
	return NewStoredObject(nil, name, relative, common.EEntityType.Hardlink(), time.Time{}, 1,
		NoContentProps, NoBlobProps, NoMetadata, "", &metadata)
}

func TestHardlinkNamespaceSeparatesRootsAndSides(t *testing.T) {
	source := hardlinkNamespace("https://a.file.core.windows.net/share/root?sig=one", false, false)
	require.Equal(t, source, hardlinkNamespace("https://a.file.core.windows.net/share/root/?sig=other", false, false))
	require.NotEqual(t, source, hardlinkNamespace("https://a.file.core.windows.net/share/root", false, true))
	require.NotEqual(t, source, hardlinkNamespace("https://a.file.core.windows.net/share/other", false, false))
	require.NotEqual(t, source, hardlinkNamespace("https://a.file.core.windows.net/share/root?sharesnapshot=older", false, false))
	store := common.NewInodeStoreFromBackend(&hardlinkStoreBuffer{})
	defer store.Close()
	var received []StoredObject
	process := withHardlinkRegistration(store, func(object StoredObject) error {
		received = append(received, object)
		return nil
	})
	for _, role := range []bool{false, true} {
		ns := hardlinkNamespace("https://a.file.core.windows.net/share/root", false, role)
		require.NoError(t, process(hardlinkTestObject(t, store, ns, "file", "file")))
	}
	require.NotEqual(t, received[0].Inode, received[1].Inode)
	require.Empty(t, received[0].TargetHardlinkFile)
	require.Empty(t, received[1].TargetHardlinkFile)
}

type excludeHardlinkName string

func (excludeHardlinkName) DoesSupportThisOS() (string, bool) { return "", true }
func (excludeHardlinkName) AppliesOnlyToFiles() bool          { return true }
func (name excludeHardlinkName) DoesPass(object StoredObject) bool {
	return object.Name != string(name)
}

func TestHardlinkAnchorsExcludeFilteredPathsAndSelfLinks(t *testing.T) {
	store := common.NewInodeStoreFromBackend(&hardlinkStoreBuffer{})
	defer store.Close()
	var received []StoredObject
	process := withHardlinkRegistration(store, func(object StoredObject) error {
		received = append(received, object)
		return nil
	})
	filters := []ObjectFilter{excludeHardlinkName("excluded")}
	excluded := hardlinkTestObject(t, store, "root", "excluded", "a-excluded")
	require.ErrorIs(t, ProcessIfPassedFilters(filters, excluded, process), ErrIgnored)
	_, err := store.GetAnchor(excluded.Inode)
	require.Error(t, err)

	first := hardlinkTestObject(t, store, "root", "selected", "sub/selected")
	require.NoError(t, ProcessIfPassedFilters(filters, first, process))
	require.NoError(t, ProcessIfPassedFilters(filters, first, process))
	second := hardlinkTestObject(t, store, "root", "other", "other")
	require.NoError(t, ProcessIfPassedFilters(filters, second, process))
	require.Empty(t, received[0].TargetHardlinkFile)
	require.Empty(t, received[1].TargetHardlinkFile)
	require.Equal(t, "sub/selected", received[2].TargetHardlinkFile)
	anchor, err := store.GetAnchor(first.Inode)
	require.NoError(t, err)
	require.Equal(t, "other", anchor)
}

func TestHardlinkFilePathsRemainRelativeToFullRoot(t *testing.T) {
	store := common.NewInodeStoreFromBackend(&hardlinkStoreBuffer{})
	defer store.Close()
	fullRoot := "https://account.file.core.windows.net/share/root"
	traverser := &fileTraverser{
		basePath: fullRoot, inodeStore: store,
		inodeNamespace: hardlinkNamespace(fullRoot, false, false),
	}
	metadata, err := traverser.hardlinkMetadata(fullRoot+"/child/a%09b%0A%20", "file")
	require.NoError(t, err)
	require.Equal(t, "child/a\tb\n ", *metadata.inodePath)
	_, err = traverser.hardlinkMetadata("https://account.file.core.windows.net/share/root-other/file", "file")
	require.Error(t, err)
	require.NotEqual(t, hardlinkNamespace(filepath.Join("root", "a"), true, false),
		hardlinkNamespace(filepath.Join("root", "b"), true, false))
}

func TestHardlinkProcessorLegacyAndConfiguredHandling(t *testing.T) {
	count := 0
	inner := func(StoredObject) error { count++; return nil }
	legacy := NewFpoAwareProcessor(common.EFolderPropertiesOption.AllFolders(), inner)
	require.NoError(t, legacy(StoredObject{EntityType: common.EEntityType.Symlink()}))
	require.Equal(t, 0, count)
	configured := NewFpoAwareProcessorWithLinks(common.EFolderPropertiesOption.AllFolders(), inner,
		common.ESymlinkHandlingType.Preserve(), common.EHardlinkHandlingType.Preserve())
	require.NoError(t, configured(StoredObject{EntityType: common.EEntityType.Symlink()}))
	require.NoError(t, configured(StoredObject{EntityType: common.EEntityType.Hardlink()}))
	require.Equal(t, 2, count)
}

func TestHardlinkedSymlinkCountersUseFinalEntity(t *testing.T) {
	store := common.NewInodeStoreFromBackend(&hardlinkStoreBuffer{})
	defer store.Close()
	counts := make(map[common.EntityType]int)
	processor := hardlinkAwareProcessor(store, nil, func(StoredObject) error { return nil },
		func(entity common.EntityType, _ common.SymlinkHandlingType, _ common.HardlinkHandlingType) {
			counts[entity]++
		},
		common.ESymlinkHandlingType.Preserve(), common.EHardlinkHandlingType.Preserve())
	for _, name := range []string{"a", "b"} {
		object := hardlinkTestObject(t, store, "root", name, name)
		object.EntityType = common.EEntityType.Symlink()
		object.hardlinkedSymlink = true
		require.NoError(t, processor(object))
	}
	require.Equal(t, 1, counts[common.EEntityType.Symlink()])
	require.Equal(t, 1, counts[common.EEntityType.Hardlink()])
}

func TestHardlinkPreserveKeepsSingleFileProbeForCopy(t *testing.T) {
	t.Setenv("AZCOPY_AZFILES_STATS_POLL", "false")
	store := common.NewInodeStoreFromBackend(&hardlinkStoreBuffer{})
	defer store.Close()
	options := InitResourceTraverserOptions{
		FromTo: common.EFromTo.FileNFSFileNFS(), InodeStore: store,
		HardlinkHandling: common.EHardlinkHandlingType.Preserve(),
	}
	require.NoError(t, options.PerformChecks())
	traverser := NewFileTraverser("https://unit-test.invalid/share/file", nil, nil, options)
	require.False(t, traverser.skipRootProperties)
}
