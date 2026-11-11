package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestSyncSymlinkTrackingOptions(t *testing.T) {
	for _, fromTo := range []common.FromTo{
		common.EFromTo.LocalFileNFS(), common.EFromTo.FileNFSLocal(), common.EFromTo.FileNFSFileNFS(),
		common.EFromTo.LocalBlob(), common.EFromTo.BlobLocal(), common.EFromTo.BlobBlob(),
		common.EFromTo.LocalFile(),
	} {
		for _, deletion := range []common.DeleteDestination{
			common.EDeleteDestination.True(), common.EDeleteDestination.False(), common.EDeleteDestination.Prompt(),
		} {
			for _, handling := range []common.SymlinkHandlingType{
				common.ESymlinkHandlingType.Skip(), common.ESymlinkHandlingType.Follow(), common.ESymlinkHandlingType.Preserve(),
			} {
				t.Run(fmt.Sprintf("%s/%s/%d", fromTo, deletion, handling), func(t *testing.T) {
					src, dst := "https://account.file.core.windows.net/src", "https://account.file.core.windows.net/dst"
					if fromTo.From() == common.ELocation.Local() {
						src = t.TempDir()
					}
					if fromTo.To() == common.ELocation.Local() {
						dst = t.TempDir()
					}
					raw := getDefaultSyncRawInput(src, dst)
					raw.fromTo = fromTo.String()
					raw.deleteDestination = deletion.String()
					raw.followSymlinks = handling == common.ESymlinkHandlingType.Follow()
					raw.preserveSymlinks = handling == common.ESymlinkHandlingType.Preserve()
					cooked, err := raw.toOptions()
					require.NoError(t, err)
					require.Equal(t, handling, cooked.symlinkHandling)
					expected := handling
					if deletion != common.EDeleteDestination.False() &&
						(fromTo.From() == common.ELocation.FileNFS() || fromTo.To() == common.ELocation.FileNFS()) {
						expected = common.ESymlinkHandlingType.Preserve()
					}
					require.Equal(t, expected, cooked.destSymlinks)
					require.Equal(t, expected, cooked.srcSymLinkTracker)
				})
			}
		}
	}
}

func TestSyncSymlinkDeletion(t *testing.T) {
	for _, sourceFirst := range []bool{true, false} {
		for _, deleteDestination := range []string{"true", "false"} {
			for _, preserve := range []bool{true, false} {
				name := "destination-first"
				if sourceFirst {
					name = "source-first"
				}
				name += "/delete=" + deleteDestination
				if preserve {
					name += "/preserve"
				} else {
					name += "/skip"
				}
				t.Run(name, func(t *testing.T) {
					src, dst := t.TempDir(), t.TempDir()
					for _, dir := range []string{src, dst} {
						require.NoError(t, os.WriteFile(filepath.Join(dir, "target"), []byte("target contents"), 0600))
						trySymlink("target", filepath.Join(dir, "shared-link"), t)
					}
					trySymlink("target", filepath.Join(dst, "extra-link"), t)
					trySymlink("missing-target", filepath.Join(dst, "dangling-link"), t)
					trySymlink(".", filepath.Join(dst, "directory-link"), t)

					raw := getDefaultSyncRawInput(src, "https://account.file.core.windows.net/dst")
					raw.fromTo = common.EFromTo.LocalFileNFS().String()
					raw.deleteDestination = deleteDestination
					raw.preserveSymlinks = preserve
					cca, err := raw.toOptions()
					require.NoError(t, err)
					cca.destination = newLocalRes(dst)
					fpo := common.EFolderPropertiesOption.NoFolders()
					deleter := newSyncLocalDeleteProcessor(&cca, fpo)
					cleaner := newFpoAwareProcessor(fpo, deleter.removeImmediately,
						cca.destSymlinks, common.EHardlinkHandlingType.Follow())
					indexer := newObjectIndexer()
					scheduler := func(StoredObject) error { return nil }
					traverse := func(path string, handling common.SymlinkHandlingType, processor objectProcessor) {
						traverser, err := InitResourceTraverser(newLocalRes(path), common.ELocation.Local(),
							context.Background(), InitResourceTraverserOptions{
								Recursive:       true,
								SymlinkHandling: handling,
							})
						require.NoError(t, err)
						require.NoError(t, traverser.Traverse(nil, processor, nil))
					}
					if sourceFirst {
						traverse(src, cca.srcSymLinkTracker, indexer.store)
						comparator := newSyncDestinationComparator(indexer, scheduler, cleaner,
							common.ESyncHashType.None(), false, false, cca.deleteDestination, nil, nil)
						traverse(dst, cca.destSymlinks, comparator.processIfNecessary)
					} else {
						traverse(dst, cca.destSymlinks, indexer.store)
						comparator := newSyncSourceComparator(indexer, scheduler,
							common.ESyncHashType.None(), false, false, nil)
						traverse(src, cca.srcSymLinkTracker, comparator.processIfNecessary)
						require.NoError(t, indexer.traverse(cleaner, nil))
					}
					for _, link := range []string{"extra-link", "dangling-link", "directory-link"} {
						_, err := os.Lstat(filepath.Join(dst, link))
						if deleteDestination == "true" {
							require.ErrorIs(t, err, os.ErrNotExist)
						} else {
							require.NoError(t, err)
						}
					}
					target, err := os.Readlink(filepath.Join(dst, "shared-link"))
					require.NoError(t, err)
					require.Equal(t, "target", target)
					body, err := os.ReadFile(filepath.Join(dst, "target"))
					require.NoError(t, err)
					require.Equal(t, "target contents", string(body))
				})
			}
		}
	}
}
