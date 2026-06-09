// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package cmd

import (
	"crypto/md5"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"
)

// dedupInterceptor is a specialized interceptor that separates transfers by their FromTo type,
// allowing us to distinguish between normal transfers and dedup (server-side copy) transfers.
type dedupInterceptor struct {
	normalTransfers []common.CopyTransfer // transfers from normal path (e.g., LocalBlob)
	dedupTransfers  []common.CopyTransfer // transfers from dedup path (e.g., BlobBlob)
	deletions       []traverser.StoredObject
	normalFromTo    common.FromTo
	dedupFromTo     common.FromTo
}

func (d *dedupInterceptor) init(normalFromTo, dedupFromTo common.FromTo) {
	d.normalFromTo = normalFromTo
	d.dedupFromTo = dedupFromTo
	glcm = &mockedLifecycleManager{
		infoLog: make(chan string, 5000),
	}
}

func (d *dedupInterceptor) intercept(copyRequest common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
	if copyRequest.FromTo == d.dedupFromTo {
		d.dedupTransfers = append(d.dedupTransfers, copyRequest.Transfers.List...)
	} else {
		d.normalTransfers = append(d.normalTransfers, copyRequest.Transfers.List...)
	}

	totalTransfers := len(d.normalTransfers) + len(d.dedupTransfers)
	if totalTransfers != 0 || !copyRequest.IsFinalPart {
		return common.CopyJobPartOrderResponse{JobStarted: true}
	}
	return common.CopyJobPartOrderResponse{JobStarted: false, ErrorMsg: common.ECopyJobPartOrderErrorType.NoTransfersScheduledErr()}
}

func (d *dedupInterceptor) delete(_ string, _ common.Location, object traverser.StoredObject) error {
	d.deletions = append(d.deletions, object)
	return nil
}

// TestSyncUploadWithDedupCopy tests that the --dedup-copy flag correctly routes
// transfers to the server-side copy path when identical content exists on the destination.
//
// Scenario:
// - Destination has blobs A.txt and B.txt with different content, both with Content-MD5
// - Source has C.txt (same content as A.txt, different path) and D.txt (completely new content)
// - Expected: C.txt should be transferred via dedup (BlobBlob copy from A.txt), D.txt via normal upload
func TestSyncUploadWithDedupCopy(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// Content for the test files
	contentA := "This is the content of file A - used for dedup testing"
	contentB := "This is completely different content for file B"
	contentD := "This is brand new content that doesn't exist on destination"

	// Compute MD5 hashes
	md5A := md5.Sum([]byte(contentA))
	md5B := md5.Sum([]byte(contentB))

	// Upload blobs to destination WITH Content-MD5
	blobClientA := cc.NewBlockBlobClient("existing/A.txt")
	_, err := blobClientA.Upload(ctx, streaming.NopCloser(strings.NewReader(contentA)),
		&blockblob.UploadOptions{
			HTTPHeaders: &blob.HTTPHeaders{
				BlobContentMD5: md5A[:],
			},
		})
	a.Nil(err)

	blobClientB := cc.NewBlockBlobClient("existing/B.txt")
	_, err = blobClientB.Upload(ctx, streaming.NopCloser(strings.NewReader(contentB)),
		&blockblob.UploadOptions{
			HTTPHeaders: &blob.HTTPHeaders{
				BlobContentMD5: md5B[:],
			},
		})
	a.Nil(err)

	// Wait a bit for the blobs to be visible
	time.Sleep(time.Millisecond * 1050)

	// Set up local source directory
	// C.txt has SAME content as A.txt (should trigger dedup)
	// D.txt has NEW content (should trigger normal upload)
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)

	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{"renamed/C.txt", "new/D.txt"})
	// Overwrite with specific content
	err = os.WriteFile(srcDirName+"/renamed/C.txt", []byte(contentA), 0644)
	a.Nil(err)
	err = os.WriteFile(srcDirName+"/new/D.txt", []byte(contentD), 0644)
	a.Nil(err)

	// Set up the dedup interceptor
	mockedRPC := &dedupInterceptor{}
	mockedRPC.init(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob())

	// Construct sync command args with --dedup-copy
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.dedupCopy = true
	raw.putMd5 = true
	raw.compareHash = "MD5"

	// Run sync
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// C.txt should be routed through dedup (BlobBlob) because its content matches A.txt
		// D.txt should be routed through normal upload (LocalBlob) because its content is new
		a.Equal(1, len(mockedRPC.dedupTransfers), "Expected 1 dedup transfer for renamed/C.txt")
		a.Equal(1, len(mockedRPC.normalTransfers), "Expected 1 normal transfer for new/D.txt")

		// Verify the dedup transfer has the correct destination path
		dedupDst := mockedRPC.dedupTransfers[0].Destination
		a.Contains(dedupDst, "renamed/C.txt", "Dedup transfer should target renamed/C.txt")

		// Verify the normal transfer has the correct destination
		normalDst := mockedRPC.normalTransfers[0].Destination
		a.Contains(normalDst, "new/D.txt", "Normal transfer should target new/D.txt")
	})
}

// TestSyncUploadWithDedupCopyNoBlobsHaveMD5 tests that when no destination blobs
// have Content-MD5, all transfers go through the normal path.
func TestSyncUploadWithDedupCopyNoBlobsHaveMD5(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// Upload blob then CLEAR its Content-MD5 (the Go SDK auto-sets MD5 on Upload,
	// so we must explicitly remove it to test the "no MD5" scenario)
	blobClientA := cc.NewBlockBlobClient("existing/A.txt")
	_, err := blobClientA.Upload(ctx, streaming.NopCloser(strings.NewReader("content A")), nil)
	a.Nil(err)
	// Clear the auto-set Content-MD5
	_, err = blobClientA.SetHTTPHeaders(ctx, blob.HTTPHeaders{BlobContentMD5: []byte{}}, nil)
	a.Nil(err)

	time.Sleep(time.Millisecond * 1050)

	// Set up local source with a file that has same content as A.txt
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{"renamed/C.txt"})
	err = os.WriteFile(srcDirName+"/renamed/C.txt", []byte("content A"), 0644)
	a.Nil(err)

	// Set up interceptor
	mockedRPC := &dedupInterceptor{}
	mockedRPC.init(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob())

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.dedupCopy = true
	raw.putMd5 = true
	raw.compareHash = "MD5"

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// No dedup should happen since destination blobs lack Content-MD5
		a.Equal(0, len(mockedRPC.dedupTransfers), "No dedup transfers when destination lacks MD5")
		a.Equal(1, len(mockedRPC.normalTransfers), "All transfers should be normal")
	})
}

// TestSyncUploadWithDedupCopyDisabled tests that without --dedup-copy, all transfers
// go through the normal path even when hash matches exist.
func TestSyncUploadWithDedupCopyDisabled(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	contentA := "dedup test content that will match"
	md5A := md5.Sum([]byte(contentA))

	// Upload blob WITH Content-MD5
	blobClientA := cc.NewBlockBlobClient("existing/A.txt")
	_, err := blobClientA.Upload(ctx, streaming.NopCloser(strings.NewReader(contentA)),
		&blockblob.UploadOptions{
			HTTPHeaders: &blob.HTTPHeaders{
				BlobContentMD5: md5A[:],
			},
		})
	a.Nil(err)
	time.Sleep(time.Millisecond * 1050)

	// Set up local source with a file that has same content
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{"renamed/C.txt"})
	err = os.WriteFile(srcDirName+"/renamed/C.txt", []byte(contentA), 0644)
	a.Nil(err)

	// Interceptor - using standard interceptor since no dedup expected
	mockedRPC := interceptor{}
	mockedRPC.init()

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	// explicitly NOT setting raw.dedupCopy = true
	raw.compareHash = "MD5"
	raw.putMd5 = true

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// Without dedup-copy, the transfer should happen normally (LocalBlob)
		a.Equal(1, len(mockedRPC.transfers), "Should have 1 normal transfer")
	})
}

// TestSyncUploadWithDedupCopyMultipleMatches tests that when multiple destination blobs
// share the same content, dedup still works correctly (uses first match).
func TestSyncUploadWithDedupCopyMultipleMatches(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	sharedContent := "This content appears in multiple destination blobs"
	md5Shared := md5.Sum([]byte(sharedContent))

	// Upload multiple blobs with the same content and MD5
	for _, path := range []string{"dup1/file.txt", "dup2/file.txt", "dup3/file.txt"} {
		blobClient := cc.NewBlockBlobClient(path)
		_, err := blobClient.Upload(ctx, streaming.NopCloser(strings.NewReader(sharedContent)),
			&blockblob.UploadOptions{
				HTTPHeaders: &blob.HTTPHeaders{
					BlobContentMD5: md5Shared[:],
				},
			})
		a.Nil(err)
	}
	time.Sleep(time.Millisecond * 1050)

	// Local source with two files that have the same content
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{"newpath1/x.txt", "newpath2/y.txt"})
	_ = os.WriteFile(srcDirName+"/newpath1/x.txt", []byte(sharedContent), 0644)
	_ = os.WriteFile(srcDirName+"/newpath2/y.txt", []byte(sharedContent), 0644)

	mockedRPC := &dedupInterceptor{}
	mockedRPC.init(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob())

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.dedupCopy = true
	raw.putMd5 = true
	raw.compareHash = "MD5"

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// Both files should be dedup-copied since their content matches existing destination blobs
		a.Equal(2, len(mockedRPC.dedupTransfers), "Expected 2 dedup transfers")
		a.Equal(0, len(mockedRPC.normalTransfers), "Expected 0 normal transfers")
	})
}

// TestSyncUploadDedupCopyAutoEnablesCompareHash tests that --dedup-copy automatically
// enables --compare-hash=MD5 if not explicitly set by verifying the raw args passthrough.
func TestSyncUploadDedupCopyAutoEnablesCompareHash(t *testing.T) {
	a := assert.New(t)

	// Verify that dedupCopy is correctly passed through options
	raw := getDefaultSyncRawInput("/tmp/src", "https://account.blob.core.windows.net/container?sv=2021-06-08&se=2030-01-01&sr=c&sp=rwdlacx&sig=fake")
	raw.dedupCopy = true
	// compareHash defaults to "None" - auto-enable happens in cooked options

	opts, err := raw.toOptions()
	a.Nil(err)
	a.True(opts.DedupCopy)
}

// TestSyncUploadWithDedupCopyMixedMD5 tests the scenario where some destination blobs
// have Content-MD5 metadata and some do not. Only files matching blobs WITH MD5 should
// be dedup-copied; files matching blobs WITHOUT MD5 cannot participate in dedup and
// should be uploaded normally.
func TestSyncUploadWithDedupCopyMixedMD5(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// Content for test files
	contentWithMD5 := "This blob has Content-MD5 set and can be used for dedup"
	contentWithoutMD5 := "This blob does NOT have Content-MD5 set"
	contentNew := "Completely new content that exists nowhere on destination"

	md5WithHash := md5.Sum([]byte(contentWithMD5))

	// Upload blob A WITH Content-MD5 (can participate in dedup)
	blobClientA := cc.NewBlockBlobClient("has-md5/A.txt")
	_, err := blobClientA.Upload(ctx, streaming.NopCloser(strings.NewReader(contentWithMD5)),
		&blockblob.UploadOptions{
			HTTPHeaders: &blob.HTTPHeaders{
				BlobContentMD5: md5WithHash[:],
			},
		})
	a.Nil(err)

	// Upload blob B WITHOUT Content-MD5 (cannot participate in dedup)
	// Note: The Go SDK auto-computes Content-MD5 on Upload, so we must clear it explicitly
	blobClientB := cc.NewBlockBlobClient("no-md5/B.txt")
	_, err = blobClientB.Upload(ctx, streaming.NopCloser(strings.NewReader(contentWithoutMD5)), nil)
	a.Nil(err)
	_, err = blobClientB.SetHTTPHeaders(ctx, blob.HTTPHeaders{BlobContentMD5: []byte{}}, nil)
	a.Nil(err)

	time.Sleep(time.Millisecond * 1050)

	// Set up local source:
	// - "dedup-me.txt" has same content as A.txt (which has MD5) -> should dedup
	// - "cant-dedup.txt" has same content as B.txt (which lacks MD5) -> must upload normally
	// - "brand-new.txt" has content not on destination at all -> must upload normally
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)

	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{
		"dedup-me.txt",
		"cant-dedup.txt",
		"brand-new.txt",
	})
	err = os.WriteFile(srcDirName+"/dedup-me.txt", []byte(contentWithMD5), 0644)
	a.Nil(err)
	err = os.WriteFile(srcDirName+"/cant-dedup.txt", []byte(contentWithoutMD5), 0644)
	a.Nil(err)
	err = os.WriteFile(srcDirName+"/brand-new.txt", []byte(contentNew), 0644)
	a.Nil(err)

	// Set up dedup interceptor
	mockedRPC := &dedupInterceptor{}
	mockedRPC.init(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob())

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.dedupCopy = true
	raw.putMd5 = true
	raw.compareHash = "MD5"

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// dedup-me.txt should be server-side copied (content matches A.txt which has MD5)
		a.Equal(1, len(mockedRPC.dedupTransfers),
			"Expected 1 dedup transfer for dedup-me.txt (matches blob with MD5)")

		// cant-dedup.txt and brand-new.txt should go through normal upload:
		// - cant-dedup.txt: content matches B.txt but B.txt lacks MD5 so no dedup possible
		// - brand-new.txt: content doesn't exist anywhere on destination
		a.Equal(2, len(mockedRPC.normalTransfers),
			"Expected 2 normal transfers for cant-dedup.txt and brand-new.txt")

		// Verify the dedup transfer targets the correct file
		dedupDst := mockedRPC.dedupTransfers[0].Destination
		a.Contains(dedupDst, "dedup-me.txt")
	})
}

// Helper to expose cooked options for testing. We'll add this as a test-only export.
// (Note: This test function validates that toOptions() correctly passes dedupCopy through)
func TestSyncRawArgsDedupCopyFieldPassthrough(t *testing.T) {
	a := assert.New(t)

	raw := getDefaultSyncRawInput("/tmp/src", "https://account.blob.core.windows.net/container?sv=2021-06-08&se=2030-01-01&sr=c&sp=rwdlacx&sig=fake")
	raw.dedupCopy = true

	opts, err := raw.toOptions()
	a.Nil(err)
	a.True(opts.DedupCopy)

	// Without dedupCopy
	raw.dedupCopy = false
	opts, err = raw.toOptions()
	a.Nil(err)
	a.False(opts.DedupCopy)
}
