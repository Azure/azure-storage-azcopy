//go:build smslidingwindow

package cmd

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func sortUnicodeTestObjects(objects []StoredObject) {
	sort.SliceStable(objects, func(i, j int) bool {
		return referenceBlobNameCompare(objects[i].relativePath, objects[j].relativePath) < 0
	})
}

func TestTwoWaySyncDirUnicodeAzurePairs(t *testing.T) {
	for _, pair := range []common.FromTo{
		common.EFromTo.BlobBlob(), common.EFromTo.BlobBlobFS(),
		common.EFromTo.BlobFSBlob(), common.EFromTo.BlobFSBlobFS(),
	} {
		for _, mirror := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/mirror=%t", pair, mirror), func(t *testing.T) {
				var source, destination []StoredObject
				for _, name := range []string{
					"ascii.txt", "leaf/x\ud7ffy.txt", "leaf/x\uff21y.txt",
					"leaf/x\ufeffy.txt", "leaf/x\U0001f600y.txt",
					"chars/#\ufeff/#\ufeff/f.txt", "chars/#\U0001f600/f.txt",
					"\u00e9.txt", "e\u0301.txt",
				} {
					source = append(source, mjTestFile(name))
					destination = append(destination, mjTestFile(name))
				}
				sourceOnly := "leaf/x\U00010000-new.txt"
				changed := "leaf/x\U0001f601-changed.txt"
				extra := "leaf/x\ufeff-extra.txt"
				source = append(source, mjTestFile(sourceOnly), mjTestFile(changed))
				old := mjTestFile(changed)
				old.lastModifiedTime = old.lastModifiedTime.Add(-time.Hour)
				destination = append(destination, old, mjTestFile(extra))
				folders := []string{"h2", "h2-4.2", "h2\ufeff", "h2\U0001f600", "h2\U0001f600-1", "h2\U0001f600\ufeff"}
				for _, name := range folders {
					source = append(source, mjTestVirtualFolder(name))
					destination = append(destination, mjTestVirtualFolder(name))
				}
				source = append(source, mjTestVirtualFolder("only\U0001f600"))
				destination = append(destination, mjTestVirtualFolder("only\ufeff"))
				sortUnicodeTestObjects(source)
				sortUnicodeTestObjects(destination)

				var deleted []string
				var mu sync.Mutex
				enumerator := newMergeJoinRecordingEnumerator(&deleted, &mu)
				enumerator.ctp.copyJobTemplate.FromTo = pair
				args := &cookedSyncCmdArgs{fromTo: pair, deleteDestination: common.EDeleteDestination.False()}
				if mirror {
					args.deleteDestination = common.EDeleteDestination.True()
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				subDirs, err := mergeJoinTwoWaySyncDir(ctx, cancel, enumerator, args, "",
					&fakeMergeJoinTraverser{objects: source}, &fakeMergeJoinTraverser{objects: destination}, true)
				require.NoError(t, err)
				var copied []string
				for _, path := range scheduledTransferSources(enumerator) {
					path, err = url.PathUnescape(path)
					require.NoError(t, err)
					copied = append(copied, path)
				}
				require.ElementsMatch(t, []string{"/" + sourceOnly, "/" + changed}, copied)
				if mirror {
					require.ElementsMatch(t, []string{extra, "only\ufeff/"}, deleted)
				} else {
					require.Empty(t, deleted)
				}
				require.Len(t, subDirs, len(folders)+1)
				for i, dir := range subDirs {
					require.Equal(t, dir.relativePath != "/only\U0001f600/", dir.isPresentAtDestination)
					if i > 0 {
						require.Less(t, referenceBlobNameCompare(subDirs[i-1].relativePath, dir.relativePath), 0)
					}
				}

				deleted = nil
				enumerator = newMergeJoinRecordingEnumerator(&deleted, &mu)
				enumerator.ctp.copyJobTemplate.FromTo = pair
				destination = append([]StoredObject(nil), source...)
				sortUnicodeTestObjects(destination)
				ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err = mergeJoinTwoWaySyncDir(ctx, cancel, enumerator, args, "",
					&fakeMergeJoinTraverser{objects: source}, &fakeMergeJoinTraverser{objects: destination}, true)
				require.NoError(t, err)
				require.Empty(t, scheduledTransferSources(enumerator), "rerunning a completed sync must be a no-op")
				require.Empty(t, deleted)
			})
		}
	}
}

// Preserve the legacy S3 byte-comparison path; this does not resolve its
// known mismatch with multi-item Azure listings in UTF-16 order.
func TestTwoWaySyncDirS3ComparisonUnchanged(t *testing.T) {
	var deleted []string
	var mu sync.Mutex
	enumerator := newMergeJoinRecordingEnumerator(&deleted, &mu)
	args := mirrorSyncArgs()
	args.fromTo = common.EFromTo.S3Blob()
	source := &fakeMergeJoinTraverser{objects: []StoredObject{
		mjTestFile("x\ufeff"), mjTestFile("x\U0001f600"),
		mjTestVirtualFolder("d\ufeff"), mjTestVirtualFolder("d\U0001f600"),
	}}
	destination := &fakeMergeJoinTraverser{objects: []StoredObject{
		mjTestFile("x\U0001f600"), mjTestVirtualFolder("d\U0001f600"),
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dirs, err := mergeJoinTwoWaySyncDir(ctx, cancel, enumerator, args, "", source, destination, true)
	require.NoError(t, err)
	require.Empty(t, deleted)
	require.Equal(t, []string{"/x%EF%BB%BF"}, scheduledTransferSources(enumerator))
	require.Len(t, dirs, 2)
	require.Equal(t, "/d\ufeff/", dirs[0].relativePath)
	require.False(t, dirs[0].isPresentAtDestination)
	require.Equal(t, "/d\U0001f600/", dirs[1].relativePath)
	require.True(t, dirs[1].isPresentAtDestination)
}

func TestStaticFlatRangeUnicodePageEdgesAndProducer(t *testing.T) {
	for _, pages := range [][][]string{
		{{"leaf/x\U0001f600y.txt", "leaf/x\ufeffy.txt"}},
		{{"leaf/x\U0001f600y.txt"}, {}, {"leaf/x\ufeffy.txt"}},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		listing, _ := newFlatTestListing(t, ctx, pages, "root/", nil)
		traverser := &flatRangeTraverser{blob: listing.traverser, bounds: flatListRange{"l", "m"}}
		sideErr := &twoWaySideErr{}
		folders, files := traverserToTypedChannels(ctx, traverser, nil, "SRC[]", sideErr, false, compareBlobNames)
		gotFolders, gotFiles := drainTypedChannelsForTest(folders, files)
		cancel()
		require.NoError(t, sideErr.get())
		require.Empty(t, gotFolders)
		require.Equal(t, []string{"leaf/x\U0001f600y.txt", "leaf/x\ufeffy.txt"}, gotFiles)
	}
}
