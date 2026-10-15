// Copyright © 2025 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package azcopy

import (
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"
)

const testBlobContainerURL = "https://account.blob.core.windows.net/container"

func TestSyncDecompressOptionValidation(t *testing.T) {
	a := assert.New(t)
	dst := t.TempDir()

	// Blob -> Local is allowed
	cooked, err := newCookedSyncOptions(testBlobContainerURL, dst, SyncOptions{AutoDecompress: true})
	a.NoError(err)
	a.True(cooked.autoDecompress)

	// Upload is rejected
	_, err = newCookedSyncOptions(dst, testBlobContainerURL, SyncOptions{AutoDecompress: true})
	a.ErrorContains(err, "only supported for downloads")

	// S2S is rejected
	_, err = newCookedSyncOptions(testBlobContainerURL, "https://account2.blob.core.windows.net/container", SyncOptions{AutoDecompress: true})
	a.ErrorContains(err, "only supported for downloads")

	// Hash-based comparison is rejected
	_, err = newCookedSyncOptions(testBlobContainerURL, dst, SyncOptions{AutoDecompress: true, CompareHash: common.ESyncHashType.MD5()})
	a.ErrorContains(err, "--compare-hash")

	for _, testCase := range []struct {
		name    string
		options SyncOptions
		flag    string
	}{
		{name: "include-pattern", options: SyncOptions{IncludePatterns: []string{"*.gz"}}, flag: "--include-pattern"},
		{name: "exclude-pattern", options: SyncOptions{ExcludePatterns: []string{"*.gz"}}, flag: "--exclude-pattern"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := newCookedSyncOptions(testBlobContainerURL, dst, SyncOptions{
				AutoDecompress:  true,
				IncludePatterns: testCase.options.IncludePatterns,
				ExcludePatterns: testCase.options.ExcludePatterns,
			})
			a.ErrorContains(err, testCase.flag)
		})
	}
}

type recordingProcessor struct {
	objects []traverser.StoredObject
}

func (r *recordingProcessor) process(o traverser.StoredObject) error {
	r.objects = append(r.objects, o)
	return nil
}

func TestSyncSourceComparatorAutoDecompress(t *testing.T) {
	a := assert.New(t)
	now := time.Now()

	newIndexer := func() *traverser.ObjectIndexer {
		indexer := traverser.NewObjectIndexer()
		for _, name := range []string{"a.json", "dir/b.json", "c.json.gz", "stale.json"} {
			_ = indexer.Store(traverser.StoredObject{
				Name:             name,
				RelativePath:     name,
				EntityType:       common.EEntityType.File(),
				LastModifiedTime: now,
			})
		}
		return indexer
	}

	srcObjects := []traverser.StoredObject{
		// compressed with a compression extension: matches decompressed destination, older => skip
		{RelativePath: "a.json.gz", ContentEncoding: "gzip", EntityType: common.EEntityType.File(), LastModifiedTime: now.Add(-time.Hour)},
		// compressed without extension: name is unchanged, newer => transfer
		{RelativePath: "dir/b.json", ContentEncoding: "gzip", EntityType: common.EEntityType.File(), LastModifiedTime: now.Add(time.Hour)},
		// not compressed: extension retained
		{RelativePath: "c.json.gz", EntityType: common.EEntityType.File(), LastModifiedTime: now.Add(-time.Hour)},
		// compressed, not present at the destination => transfer
		{RelativePath: "new.json.gzip", ContentEncoding: "GZIP", EntityType: common.EEntityType.File(), LastModifiedTime: now},
	}

	indexer := newIndexer()
	copier, cleaner := &recordingProcessor{}, &recordingProcessor{}
	comparator := NewSyncSourceComparator(indexer, copier.process, cleaner.process, nil, common.ESyncHashType.None(), false, false, nil)
	comparator.autoDecompress = true
	for _, o := range srcObjects {
		a.NoError(comparator.ProcessIfNecessary(o))
	}

	a.Len(copier.objects, 2)
	a.Equal("dir/b.json", copier.objects[0].RelativePath)
	a.Equal("new.json.gzip", copier.objects[1].RelativePath) // source path is untouched; the transfer strips the extension
	a.NoError(indexer.Traverse(cleaner.process, nil))
	a.Len(cleaner.objects, 1)
	a.Equal("stale.json", cleaner.objects[0].RelativePath)

	// Without decompression, "a.json.gz" does not match "a.json" and the decompressed file would be deleted
	indexer = newIndexer()
	copier, cleaner = &recordingProcessor{}, &recordingProcessor{}
	comparator = NewSyncSourceComparator(indexer, copier.process, cleaner.process, nil, common.ESyncHashType.None(), false, false, nil)
	for _, o := range srcObjects {
		a.NoError(comparator.ProcessIfNecessary(o))
	}
	a.Len(copier.objects, 3)
	a.NoError(indexer.Traverse(cleaner.process, nil))
	a.Len(cleaner.objects, 2)
}

func TestSyncTransferProcessorAutoDecompress(t *testing.T) {
	a := assert.New(t)
	for _, decompress := range []bool{true, false} {
		var captured []common.CopyTransfer
		template := &common.CopyJobPartOrderRequest{
			FromTo:         common.EFromTo.BlobLocal(),
			AutoDecompress: decompress,
			Fpo:            common.EFolderPropertiesOption.NoFolders(),
		}
		processor := NewCopyTransferProcessor(false, template, 10, common.ResourceString{Value: testBlobContainerURL}, common.ResourceString{Value: t.TempDir()},
			func(bool) {}, func() {}, false, true, func(req common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
				captured = append(captured, req.Transfers.List...)
				return common.CopyJobPartOrderResponse{JobStarted: true}
			})
		a.NoError(processor.ScheduleSyncRemoveSetPropertiesTransfer(traverser.StoredObject{
			RelativePath:    "logs/a.json.gz",
			ContentEncoding: "gzip",
			EntityType:      common.EEntityType.File(),
		}))
		_, err := processor.DispatchFinalPart()
		a.NoError(err)
		a.Len(captured, 1)
		a.Equal("/logs/a.json.gz", captured[0].Source)
		if decompress {
			a.Equal("/logs/a.json", captured[0].Destination)
		} else {
			a.Equal("/logs/a.json.gz", captured[0].Destination)
		}
	}
}
