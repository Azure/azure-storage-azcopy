package traverser

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func checkpoint5Store(t *testing.T, groups map[string][]string) (*common.InodeStore, map[string]StoredObject) {
	t.Helper()
	file, err := os.Create(filepath.Join(t.TempDir(), "inodes"))
	require.NoError(t, err)
	store := common.NewInodeStoreFromBackend(file)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	objects := make(map[string]StoredObject)
	for inode, paths := range groups {
		for _, path := range paths {
			target, _, err := store.GetOrAdd(inode, path)
			require.NoError(t, err)
			objects[path] = StoredObject{
				Name: path, RelativePath: path, EntityType: common.EEntityType.Hardlink(),
				Inode: inode, TargetHardlinkFile: target, Size: 10,
				LastModifiedTime: time.Unix(1700000000, 0),
			}
		}
	}
	return store, objects
}

func runCheckpoint5Comparison(t *testing.T, sourceFirst bool, store *common.InodeStore, source, destination map[string]StoredObject,
	restructure, clean ObjectProcessor, destinationIsLocal ...bool) (map[string]StoredObject, error) {
	t.Helper()
	scheduled := make(map[string]StoredObject)
	schedule := func(object StoredObject) error {
		scheduled[object.RelativePath] = object
		return nil
	}
	indexer := NewObjectIndexer()
	options := HardlinkSyncOptions{InodeStore: store, RestructureDeleter: restructure, DestinationCleaner: clean}
	if len(destinationIsLocal) > 0 {
		options.DestinationIsLocal = destinationIsLocal[0]
	}
	if sourceFirst {
		for key, value := range source {
			indexer.IndexMap[key] = value
		}
		comparator := NewSyncDestinationComparator(indexer, schedule, clean, common.ESyncHashType.None(),
			false, false, common.EDeleteDestination.False(), nil, nil, options)
		for _, object := range destination {
			if err := comparator.ProcessIfNecessary(object); err != nil {
				return scheduled, err
			}
		}
		if err := comparator.ProcessPendingHardlinks(); err != nil {
			return scheduled, err
		}
		return scheduled, indexer.Traverse(comparator.NormalizeAndSchedule(schedule), nil)
	}
	for key, value := range destination {
		indexer.IndexMap[key] = value
	}
	comparator := NewSyncSourceComparator(indexer, schedule, common.ESyncHashType.None(), false, false, nil, options)
	for _, object := range source {
		if err := comparator.ProcessIfNecessary(object); err != nil {
			return scheduled, err
		}
	}
	if err := comparator.ProcessPendingHardlinks(); err != nil {
		return scheduled, err
	}
	return scheduled, indexer.Traverse(clean, nil)
}

func TestCheckpoint5HardlinkComparison(t *testing.T) {
	for _, sourceFirst := range []bool{false, true} {
		for _, scenario := range []string{"new", "unchanged", "merge", "split", "split-with-extra", "extra-kept", "delete-error"} {
			name := scenario + map[bool]string{true: "/source-first", false: "/destination-first"}[sourceFirst]
			t.Run(name, func(t *testing.T) {
				store, source := checkpoint5Store(t, map[string][]string{"source-group": {"b", "a"}})
				destination := make(map[string]StoredObject)
				if scenario != "new" {
					for path, object := range source {
						inode := "destination-group"
						if scenario == "merge" {
							inode += "-" + path
						}
						target, _, err := store.GetOrAdd(inode, path)
						require.NoError(t, err)
						object.Inode, object.TargetHardlinkFile = inode, target
						destination[path] = object
					}
				}
				if scenario == "extra-kept" || scenario == "split-with-extra" {
					_, _, err := store.GetOrAdd("destination-group", "extra")
					require.NoError(t, err)
					extra := destination["a"]
					extra.Name, extra.RelativePath = "extra", "extra"
					destination["extra"] = extra
				}
				if scenario == "split" || scenario == "split-with-extra" {
					for path, object := range source {
						object.EntityType = common.EEntityType.File()
						object.Inode, object.TargetHardlinkFile = "", ""
						source[path] = object
					}
				}
				if scenario == "delete-error" {
					for path, object := range destination {
						object.EntityType = common.EEntityType.File()
						object.Inode, object.TargetHardlinkFile = "", ""
						destination[path] = object
					}
				}
				var restructured, extras []string
				deletionErr := errors.New("unlink failed")
				scheduled, err := runCheckpoint5Comparison(t, sourceFirst, store, source, destination,
					func(object StoredObject) error {
						restructured = append(restructured, object.RelativePath)
						if scenario == "delete-error" {
							return deletionErr
						}
						return nil
					},
					func(object StoredObject) error {
						extras = append(extras, object.RelativePath)
						return nil
					})
				if scenario == "delete-error" {
					require.ErrorIs(t, err, deletionErr)
					require.Empty(t, scheduled, "failed unlink must not schedule a write into the old inode")
					return
				}
				require.NoError(t, err)
				switch scenario {
				case "new", "merge":
					require.Len(t, scheduled, 2)
					require.Empty(t, scheduled["a"].TargetHardlinkFile)
					require.Equal(t, "a", scheduled["b"].TargetHardlinkFile)
				case "unchanged", "extra-kept":
					require.Empty(t, scheduled)
					require.Empty(t, restructured)
					if scenario == "extra-kept" {
						require.Equal(t, []string{"extra"}, extras, "extra files must use the flag-controlled cleaner, not restructuring")
					}
				case "split", "split-with-extra":
					require.NotEmpty(t, restructured)
					if scenario == "split-with-extra" {
						require.ElementsMatch(t, []string{"a", "b"}, restructured,
							"retaining an extra destination link must not leave source files attached to its inode")
						require.Equal(t, []string{"extra"}, extras)
					}
					for _, object := range scheduled {
						require.Equal(t, common.EEntityType.File(), object.EntityType)
						require.Empty(t, object.TargetHardlinkFile)
					}
				}
			})
		}
	}
}

func TestCheckpoint5AnchorErrorsStopComparison(t *testing.T) {
	store, source := checkpoint5Store(t, map[string][]string{"source-group": {"a", "b"}})
	object := source["a"]
	object.Inode = "missing"
	source["a"] = object
	dest := object
	dest.EntityType = common.EEntityType.File()
	for _, sourceFirst := range []bool{false, true} {
		scheduled, err := runCheckpoint5Comparison(t, sourceFirst, store, source, map[string]StoredObject{"a": dest},
			func(StoredObject) error { t.Fatal("anchor lookup failure must precede deletion"); return nil },
			func(StoredObject) error { return nil })
		require.Error(t, err)
		require.NotContains(t, scheduled, "a")
	}
}

func TestCheckpoint5AnchorChangesPreserveTopologyAndContent(t *testing.T) {
	for _, sourceFirst := range []bool{false, true} {
		for _, added := range []bool{false, true} {
			name := map[bool]string{true: "new-earlier-anchor", false: "renamed-newer-anchor"}[added] +
				map[bool]string{true: "/source-first", false: "/destination-first"}[sourceFirst]
			t.Run(name, func(t *testing.T) {
				sourcePaths, destinationPaths := []string{"c", "b"}, []string{"a", "b", "c"}
				if added {
					sourcePaths, destinationPaths = []string{"c", "b", "a"}, []string{"b", "c"}
				}
				store, source := checkpoint5Store(t, map[string][]string{"source": sourcePaths})
				destination := make(map[string]StoredObject)
				for _, path := range destinationPaths {
					target, _, err := store.GetOrAdd("destination", path)
					require.NoError(t, err)
					destination[path] = StoredObject{
						Name: path, RelativePath: path, EntityType: common.EEntityType.Hardlink(),
						Inode: "destination", TargetHardlinkFile: target, Size: 10,
						LastModifiedTime: time.Unix(1700000000, 0),
					}
				}
				if !added {
					for path, object := range source {
						object.LastModifiedTime = object.LastModifiedTime.Add(time.Hour)
						source[path] = object
					}
				}
				var removed []string
				scheduled, err := runCheckpoint5Comparison(t, sourceFirst, store, source, destination,
					func(object StoredObject) error { removed = append(removed, object.RelativePath); return nil },
					func(StoredObject) error { return nil })
				require.NoError(t, err)
				if added {
					require.Len(t, scheduled, 3)
					require.Empty(t, scheduled["a"].TargetHardlinkFile)
					require.Equal(t, "a", scheduled["b"].TargetHardlinkFile)
					require.Equal(t, "a", scheduled["c"].TargetHardlinkFile)
					require.ElementsMatch(t, []string{"b", "c"}, removed)
				} else {
					require.Len(t, scheduled, 1)
					require.Contains(t, scheduled, "b", "an anchor rename must not suppress newer same-size data")
					require.Empty(t, scheduled["b"].TargetHardlinkFile)
				}
			})
		}
	}
}

func TestCheckpoint5LocalAnchorReplacementRelinksSelectedAliases(t *testing.T) {
	store, source := checkpoint5Store(t, map[string][]string{"source": {"b", "a"}})
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "a"), []byte("old content"), 0600))
	require.NoError(t, os.Link(filepath.Join(root, "a"), filepath.Join(root, "b")))
	destination := make(map[string]StoredObject)
	for path, object := range source {
		target, _, err := store.GetOrAdd("destination", path)
		require.NoError(t, err)
		destinationObject := object
		destinationObject.Inode, destinationObject.TargetHardlinkFile = "destination", target
		destination[path] = destinationObject
		object.LastModifiedTime = object.LastModifiedTime.Add(time.Hour)
		source[path] = object
	}
	scheduled, err := runCheckpoint5Comparison(t, false, store, source, destination,
		func(object StoredObject) error { return os.Remove(filepath.Join(root, object.RelativePath)) },
		func(StoredObject) error { t.Fatal("no extra destination files exist"); return nil }, true)
	require.NoError(t, err)
	require.Len(t, scheduled, 2)
	require.Empty(t, scheduled["a"].TargetHardlinkFile)
	require.Equal(t, "a", scheduled["b"].TargetHardlinkFile)
	temporary := filepath.Join(root, "download.tmp")
	require.NoError(t, os.WriteFile(temporary, []byte("new content"), 0600))
	require.NoError(t, os.Rename(temporary, filepath.Join(root, "a")))
	require.NoError(t, os.Link(filepath.Join(root, scheduled["b"].TargetHardlinkFile), filepath.Join(root, "b")))
	anchor, err := os.Stat(filepath.Join(root, "a"))
	require.NoError(t, err)
	alias, err := os.Stat(filepath.Join(root, "b"))
	require.NoError(t, err)
	require.True(t, os.SameFile(anchor, alias))
	content, err := os.ReadFile(filepath.Join(root, "b"))
	require.NoError(t, err)
	require.Equal(t, "new content", string(content))
}

func TestCheckpoint5TrackedSymlinkAliasesRemainSkipped(t *testing.T) {
	for _, sourceFirst := range []bool{false, true} {
		t.Run(map[bool]string{true: "source-first", false: "destination-first"}[sourceFirst], func(t *testing.T) {
			store, source := checkpoint5Store(t, map[string][]string{"source": {"a", "b", "c"}})
			for path, object := range source {
				object.hardlinkedSymlink = true
				if path == "a" {
					object.EntityType = common.EEntityType.Symlink()
				}
				source[path] = object
			}
			destination := make(map[string]StoredObject)
			for _, path := range []string{"a", "b"} {
				target, _, err := store.GetOrAdd("destination", path)
				require.NoError(t, err)
				object := source[path]
				object.Inode, object.TargetHardlinkFile = "destination", target
				destination[path] = object
			}
			unexpected := func(object StoredObject) error {
				t.Fatalf("skipped source symlink %q must not be copied, deleted, or restructured", object.RelativePath)
				return nil
			}
			skippedAliases := 0
			options := HardlinkSyncOptions{
				InodeStore: store, RestructureDeleter: unexpected, DestinationCleaner: unexpected, SkipSourceSymlinks: true,
				IncrementSkippedSymlink: func() { skippedAliases++ },
			}
			index := NewObjectIndexer()
			if sourceFirst {
				index.IndexMap = source
				comparator := NewSyncDestinationComparator(index, unexpected, unexpected, common.ESyncHashType.None(),
					false, false, common.EDeleteDestination.True(), nil, nil, options)
				for _, object := range destination {
					require.NoError(t, comparator.ProcessIfNecessary(object))
				}
				require.NoError(t, comparator.ProcessPendingHardlinks())
				require.NoError(t, index.Traverse(comparator.NormalizeAndSchedule(unexpected), nil))
			} else {
				index.IndexMap = destination
				comparator := NewSyncSourceComparator(index, unexpected, common.ESyncHashType.None(), false, false, nil, options)
				for _, object := range source {
					require.NoError(t, comparator.ProcessIfNecessary(object))
				}
				require.NoError(t, comparator.ProcessPendingHardlinks())
				require.NoError(t, index.Traverse(unexpected, nil))
			}
			require.Equal(t, 2, skippedAliases, "count ignored aliases without counting the original symlink twice")
		})
	}
	var counted common.EntityType
	processor := hardlinkAwareProcessor(nil, nil, func(StoredObject) error { return nil },
		func(entity common.EntityType, _ common.SymlinkHandlingType, _ common.HardlinkHandlingType) {
			counted = entity
		},
		common.ESymlinkHandlingType.Preserve(), common.PreserveHardlinkHandlingType)
	require.NoError(t, processor(StoredObject{EntityType: common.EEntityType.Hardlink(), hardlinkedSymlink: true}))
	require.Equal(t, common.EEntityType.Hardlink(), counted, "generic traversal counters must retain final entity classification")
}
