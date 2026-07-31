// Copyright © Microsoft <wastore@microsoft.com>
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

package ste

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

type dedupeHashWireTransport struct {
	request *http.Request
}

func (t *dedupeHashWireTransport) Do(req *http.Request) (*http.Response, error) {
	t.request = req.Clone(req.Context())
	t.request.Header = req.Header.Clone()
	first := sha256.Sum256([]byte("first"))
	second := sha256.Sum256([]byte("second"))
	body := `<RangeHashList>` +
		`<RangeHash><Offset>0</Offset><Length>3</Length><Sha256>` +
		base64.StdEncoding.EncodeToString(first[:]) +
		`</Sha256></RangeHash>` +
		`<RangeHash><Offset>10</Offset><Length>7</Length><Sha256>` +
		base64.StdEncoding.EncodeToString(second[:]) +
		`</Sha256></RangeHash>` +
		`</RangeHashList>`
	headers := http.Header{}
	headers.Set("Content-Type", "application/xml")
	headers.Set("x-ms-request-id", "hash-request")
	headers.Set("x-ms-test-dedupe-sha256-cpu-time-us", "17")
	return &http.Response{
		Request:    req,
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     headers,
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func headerValueFold(header http.Header, name string) string {
	for headerName, values := range header {
		if strings.EqualFold(headerName, name) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

type fakeDedupeRangeHasher struct {
	mu sync.Mutex

	sourceCalls int
	targetCalls int

	sourceRanges []blockblob.BlobHashRange
	targetRanges map[string][]blockblob.BlobHashRange
	sourceHashes map[srcBlockKey][32]byte
	targetHashes map[string]map[srcBlockKey][32]byte
	sourceErr    error
	targetErr    map[string]error
}

func (h *fakeDedupeRangeHasher) HashSource(
	_ context.Context,
	_ azcore.ETag,
	ranges []blockblob.BlobHashRange,
) (blockblob.GetBlobHashResponse, int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sourceCalls++
	h.sourceRanges = append([]blockblob.BlobHashRange(nil), ranges...)
	if h.sourceErr != nil {
		return blockblob.GetBlobHashResponse{}, 1, h.sourceErr
	}
	results := make([]blockblob.BlobHashResult, 0, len(ranges))
	for i := len(ranges) - 1; i >= 0; i-- {
		rnge := ranges[i]
		hash := h.sourceHashes[srcBlockKey{offset: rnge.Offset, size: rnge.Count}]
		results = append(results, blockblob.BlobHashResult{
			Offset: rnge.Offset,
			Count:  rnge.Count,
			SHA256: append([]byte(nil), hash[:]...),
		})
	}
	return blockblob.GetBlobHashResponse{RangeHashes: results}, 1, nil
}

func (h *fakeDedupeRangeHasher) HashTarget(
	_ context.Context,
	targetURI string,
	_ azcore.ETag,
	ranges []blockblob.BlobHashRange,
) (blockblob.GetBlobHashResponse, int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.targetCalls++
	if h.targetRanges == nil {
		h.targetRanges = make(map[string][]blockblob.BlobHashRange)
	}
	h.targetRanges[targetURI] = append([]blockblob.BlobHashRange(nil), ranges...)
	if err := h.targetErr[targetURI]; err != nil {
		return blockblob.GetBlobHashResponse{}, 1, err
	}
	results := make([]blockblob.BlobHashResult, 0, len(ranges))
	for i := len(ranges) - 1; i >= 0; i-- {
		rnge := ranges[i]
		hash := h.targetHashes[targetURI][srcBlockKey{offset: rnge.Offset, size: rnge.Count}]
		results = append(results, blockblob.BlobHashResult{
			Offset: rnge.Offset,
			Count:  rnge.Count,
			SHA256: append([]byte(nil), hash[:]...),
		})
	}
	return blockblob.GetBlobHashResponse{RangeHashes: results}, 1, nil
}

func crcOnlyBlock(offset, size int64, crc uint64) PlannedBlock {
	return PlannedBlock{
		Offset:    offset,
		Size:      size,
		CRC64:     crc,
		HasCRC64:  true,
		HasSHA256: false,
	}
}

func TestSDKDedupeRangeHasherWireContractAndCPUCallback(t *testing.T) {
	transport := &dedupeHashWireTransport{}
	client, err := blockblob.NewClientWithNoCredential(
		"https://acct.blob.core.windows.net/c/source?sig=secret",
		&blockblob.ClientOptions{ClientOptions: policy.ClientOptions{Transport: transport}},
	)
	assert.NoError(t, err)

	var callbackRole string
	var callbackResponse blockblob.GetBlobHashResponse
	hasher := &sdkDedupeRangeHasher{
		jptm: &testJobPartTransferManager{},
		onResponse: func(role string, response blockblob.GetBlobHashResponse) {
			callbackRole = role
			callbackResponse = response
		},
	}
	ranges := []blockblob.BlobHashRange{{Offset: 0, Count: 3}, {Offset: 10, Count: 7}}
	response, batches, err := hasher.hashRanges(
		context.Background(),
		"source",
		client,
		azcore.ETag(`"source-etag"`),
		ranges,
		nil,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, batches)
	assert.Len(t, response.RangeHashes, 2)
	assert.Equal(t, "source", callbackRole)
	assert.Equal(t, int64(17), *callbackResponse.SHA256CPUTimeUS)
	assert.Equal(t, "hash-request", *callbackResponse.RequestID)

	assert.NotNil(t, transport.request)
	assert.Equal(t, http.MethodGet, transport.request.Method)
	assert.Equal(t, "hash", transport.request.URL.Query().Get("comp"))
	assert.Equal(t, "secret", transport.request.URL.Query().Get("sig"))
	assert.Equal(t, "Sha256", headerValueFold(transport.request.Header, "x-ms-hash-algorithm"))
	assert.Equal(t, "bytes=0-2,10-16", headerValueFold(transport.request.Header, "x-ms-multi-range"))
	assert.Equal(t, `"source-etag"`, headerValueFold(transport.request.Header, "If-Match"))
	assert.Empty(t, headerValueFold(transport.request.Header, "x-ms-encryption-key"))
	assert.True(t, transport.request.Body == nil || transport.request.Body == http.NoBody)
	assert.Zero(t, transport.request.ContentLength)

	algorithm := blob.EncryptionAlgorithmTypeAES256
	encryptionKey := "destination-key"
	encryptionKeySHA256 := "destination-key-sha256"
	_, _, err = hasher.hashRanges(
		context.Background(),
		"target",
		client,
		azcore.ETag(`"target-etag"`),
		ranges,
		&blob.CPKInfo{
			EncryptionAlgorithm: &algorithm,
			EncryptionKey:       &encryptionKey,
			EncryptionKeySHA256: &encryptionKeySHA256,
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, encryptionKey, headerValueFold(transport.request.Header, "x-ms-encryption-key"))
	assert.Equal(t, encryptionKeySHA256, headerValueFold(transport.request.Header, "x-ms-encryption-key-sha256"))
}

func TestResolveDedupeCandidateHashesNoCRCHitAvoidsSHA(t *testing.T) {
	state := &dedupeJobState{committed: common.NewDedupeHashTable()}
	state.committed.Insert(common.BlockEntry{
		CRC64:        1,
		TargetURI:    "https://acct.blob.core.windows.net/c/wrong-size",
		TargetOffset: 0,
		TargetLength: 4,
		ETag:         azcore.ETag(`"target"`),
	})
	hasher := &fakeDedupeRangeHasher{}
	plan := &SourceGridPlan{Blocks: []PlannedBlock{
		crcOnlyBlock(0, 3, 1),
		crcOnlyBlock(3, 7, 2),
		crcOnlyBlock(10, 2, 3),
	}}

	index, stats, err := resolveDedupeCandidateHashes(
		context.Background(),
		state,
		plan,
		azcore.ETag(`"source"`),
		"https://acct.blob.core.windows.net/c/current",
		hasher,
	)

	assert.NoError(t, err)
	assert.Empty(t, index)
	assert.Zero(t, stats.candidateBlocks)
	assert.Zero(t, hasher.sourceCalls)
	assert.Zero(t, hasher.targetCalls)
}

func TestResolveDedupeCandidateHashesMatchesByRangeNotResultOrder(t *testing.T) {
	shared := sha256.Sum256([]byte("shared"))
	other := sha256.Sum256([]byte("other"))
	targetURI := "https://acct.blob.core.windows.net/c/previous?sig=secret"
	state := &dedupeJobState{committed: common.NewDedupeHashTable()}
	state.committed.Insert(common.BlockEntry{
		CRC64:        10,
		TargetURI:    targetURI,
		TargetOffset: 100,
		TargetLength: 3,
		ETag:         azcore.ETag(`"target"`),
	})
	state.committed.Insert(common.BlockEntry{
		CRC64:        20,
		TargetURI:    targetURI,
		TargetOffset: 200,
		TargetLength: 7,
		ETag:         azcore.ETag(`"target"`),
	})
	plan := &SourceGridPlan{Blocks: []PlannedBlock{
		crcOnlyBlock(0, 3, 10),
		crcOnlyBlock(3, 7, 20),
	}}
	plan.Blocks[0].SrcBlockName = "duplicate-block-id"
	plan.Blocks[1].SrcBlockName = "duplicate-block-id"
	hasher := &fakeDedupeRangeHasher{
		sourceHashes: map[srcBlockKey][32]byte{
			{offset: 0, size: 3}: shared,
			{offset: 3, size: 7}: other,
		},
		targetHashes: map[string]map[srcBlockKey][32]byte{
			targetURI: {
				{offset: 100, size: 3}: shared,
				{offset: 200, size: 7}: sha256.Sum256([]byte("mismatch")),
			},
		},
		targetErr: make(map[string]error),
	}

	index, stats, err := resolveDedupeCandidateHashes(
		context.Background(),
		state,
		plan,
		azcore.ETag(`"source"`),
		"https://acct.blob.core.windows.net/c/current",
		hasher,
	)

	assert.NoError(t, err)
	assert.Equal(t, 2, stats.candidateBlocks)
	assert.Equal(t, 2, stats.candidateOccurrences)
	assert.Zero(t, stats.targetHashCacheHits)
	assert.Equal(t, 2, stats.targetHashCacheMisses)
	assert.Equal(t, 1, hasher.sourceCalls)
	assert.Equal(t, 1, hasher.targetCalls)
	assert.Equal(t, []blockblob.BlobHashRange{{Offset: 0, Count: 3}, {Offset: 3, Count: 7}}, hasher.sourceRanges)
	assert.Equal(t, []blockblob.BlobHashRange{{Offset: 100, Count: 3}, {Offset: 200, Count: 7}}, hasher.targetRanges[targetURI])

	target, hit := decideStaging(index, state.committed, 0, 3, "https://acct.blob.core.windows.net/c/current")
	assert.True(t, hit)
	assert.Equal(t, targetURI, target.TargetURI)
	_, hit = decideStaging(index, state.committed, 3, 7, "https://acct.blob.core.windows.net/c/current")
	assert.False(t, hit)
}

func TestResolveDedupeCandidateHashesReusesCachedTargetSHA(t *testing.T) {
	hash := sha256.Sum256([]byte("cached"))
	targetURI := "https://acct.blob.core.windows.net/c/previous"
	state := &dedupeJobState{committed: common.NewDedupeHashTable()}
	state.committed.Insert(common.BlockEntry{
		CRC64:        10,
		SHA256:       hash,
		HasSHA256:    true,
		TargetURI:    targetURI,
		TargetOffset: 100,
		TargetLength: 3,
		ETag:         azcore.ETag(`"target"`),
	})
	hasher := &fakeDedupeRangeHasher{
		sourceHashes: map[srcBlockKey][32]byte{{offset: 0, size: 3}: hash},
		targetHashes: make(map[string]map[srcBlockKey][32]byte),
		targetErr:    make(map[string]error),
	}

	index, stats, err := resolveDedupeCandidateHashes(
		context.Background(),
		state,
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(0, 3, 10)}},
		azcore.ETag(`"source"`),
		"https://acct.blob.core.windows.net/c/current",
		hasher,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, hasher.sourceCalls)
	assert.Zero(t, hasher.targetCalls)
	assert.Equal(t, 1, stats.targetHashCacheHits)
	assert.Zero(t, stats.targetHashCacheMisses)
	_, hit := decideStaging(index, state.committed, 0, 3, "https://acct.blob.core.windows.net/c/current")
	assert.True(t, hit)
}

func TestResolveDedupeCandidateHashesCoalescesConcurrentTargetHashing(t *testing.T) {
	hash := sha256.Sum256([]byte("shared"))
	targetURI := "https://acct.blob.core.windows.net/c/previous"
	state := &dedupeJobState{committed: common.NewDedupeHashTable()}
	state.committed.Insert(common.BlockEntry{
		CRC64:        10,
		TargetURI:    targetURI,
		TargetOffset: 100,
		TargetLength: 3,
		ETag:         azcore.ETag(`"target"`),
	})
	hasher := &fakeDedupeRangeHasher{
		sourceHashes: map[srcBlockKey][32]byte{{offset: 0, size: 3}: hash},
		targetHashes: map[string]map[srcBlockKey][32]byte{
			targetURI: {{offset: 100, size: 3}: hash},
		},
		targetErr: make(map[string]error),
	}
	plan := &SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(0, 3, 10)}}

	const workers = 16
	results := make(chan map[srcBlockKey]srcBlockHashes, workers)
	errorsCh := make(chan error, workers)
	var waitGroup sync.WaitGroup
	for i := 0; i < workers; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			index, _, err := resolveDedupeCandidateHashes(
				context.Background(),
				state,
				plan,
				azcore.ETag(`"source"`),
				"https://acct.blob.core.windows.net/c/current",
				hasher,
			)
			results <- index
			errorsCh <- err
		}()
	}
	waitGroup.Wait()
	close(results)
	close(errorsCh)

	for err := range errorsCh {
		assert.NoError(t, err)
	}
	for index := range results {
		_, hit := decideStaging(
			index,
			state.committed,
			0,
			3,
			"https://acct.blob.core.windows.net/c/current",
		)
		assert.True(t, hit)
	}
	assert.Equal(t, workers, hasher.sourceCalls)
	assert.Equal(t, 1, hasher.targetCalls)
}

func TestResolveDedupeCandidateHashesEvictsTargetEpochOn412(t *testing.T) {
	targetURI := "https://acct.blob.core.windows.net/c/previous"
	state := &dedupeJobState{committed: common.NewDedupeHashTable()}
	state.committed.Insert(common.BlockEntry{
		CRC64:        10,
		TargetURI:    targetURI,
		TargetOffset: 100,
		TargetLength: 3,
		ETag:         azcore.ETag(`"target"`),
	})
	hash := sha256.Sum256([]byte("source"))
	hasher := &fakeDedupeRangeHasher{
		sourceHashes: map[srcBlockKey][32]byte{{offset: 0, size: 3}: hash},
		targetHashes: make(map[string]map[srcBlockKey][32]byte),
		targetErr: map[string]error{
			targetURI: &azcore.ResponseError{StatusCode: http.StatusPreconditionFailed},
		},
	}

	index, stats, err := resolveDedupeCandidateHashes(
		context.Background(),
		state,
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(0, 3, 10)}},
		azcore.ETag(`"source"`),
		"https://acct.blob.core.windows.net/c/current",
		hasher,
	)

	assert.NoError(t, err)
	assert.Len(t, index, 1)
	assert.Equal(t, 1, stats.targetEpochInvalidations)
	assert.Empty(t, state.committed.LookupByCRC64(10))
}

func TestResolveDedupeCandidateHashesTargetFailureFallsBackWithoutCaching(t *testing.T) {
	targetURI := "https://acct.blob.core.windows.net/c/previous"
	state := &dedupeJobState{committed: common.NewDedupeHashTable()}
	state.committed.Insert(common.BlockEntry{
		CRC64:        10,
		TargetURI:    targetURI,
		TargetOffset: 100,
		TargetLength: 3,
		ETag:         azcore.ETag(`"target"`),
	})
	hash := sha256.Sum256([]byte("source"))
	hasher := &fakeDedupeRangeHasher{
		sourceHashes: map[srcBlockKey][32]byte{{offset: 0, size: 3}: hash},
		targetHashes: make(map[string]map[srcBlockKey][32]byte),
		targetErr:    map[string]error{targetURI: context.DeadlineExceeded},
	}

	index, stats, err := resolveDedupeCandidateHashes(
		context.Background(),
		state,
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(0, 3, 10)}},
		azcore.ETag(`"source"`),
		"https://acct.blob.core.windows.net/c/current",
		hasher,
	)

	assert.NoError(t, err)
	assert.Len(t, index, 1)
	assert.Equal(t, 1, stats.targetHashFailures)
	candidates := state.committed.LookupByCRC64AndLength(10, 3)
	if assert.Len(t, candidates, 1) {
		assert.False(t, candidates[0].HasSHA256)
	}
	_, hit := decideStaging(index, state.committed, 0, 3, "https://acct.blob.core.windows.net/c/current")
	assert.False(t, hit)
	assert.False(t, hasDedupeSHAMismatch(
		index,
		state.committed,
		0,
		3,
		"https://acct.blob.core.windows.net/c/current",
	))
}

func TestResolveDedupeCandidateHashesReportsSource412(t *testing.T) {
	state := &dedupeJobState{committed: common.NewDedupeHashTable()}
	state.committed.Insert(common.BlockEntry{
		CRC64:        10,
		TargetURI:    "https://acct.blob.core.windows.net/c/previous",
		TargetOffset: 100,
		TargetLength: 3,
		ETag:         azcore.ETag(`"target"`),
	})
	hasher := &fakeDedupeRangeHasher{
		sourceErr: &azcore.ResponseError{StatusCode: http.StatusPreconditionFailed},
	}

	index, stats, err := resolveDedupeCandidateHashes(
		context.Background(),
		state,
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(0, 3, 10)}},
		azcore.ETag(`"source"`),
		"https://acct.blob.core.windows.net/c/current",
		hasher,
	)

	assert.Error(t, err)
	assert.Empty(t, index)
	assert.Equal(t, 1, stats.sourceEpochInvalidations)
	assert.Equal(t, 1, state.committed.Len())
}

func TestBatchDedupeBlobHashRanges(t *testing.T) {
	t.Run("range count", func(t *testing.T) {
		ranges := make([]blockblob.BlobHashRange, 257)
		for i := range ranges {
			ranges[i] = blockblob.BlobHashRange{Offset: int64(i * 2), Count: 1}
		}
		batches, err := batchDedupeBlobHashRanges(ranges)
		assert.NoError(t, err)
		assert.Len(t, batches, 2)
		assert.Len(t, batches[0], 256)
		assert.Len(t, batches[1], 1)
	})

	t.Run("aggregate bytes", func(t *testing.T) {
		ranges := []blockblob.BlobHashRange{
			{Offset: 0, Count: dedupeGetBlobHashMaxBytes},
			{Offset: dedupeGetBlobHashMaxBytes, Count: 1},
		}
		batches, err := batchDedupeBlobHashRanges(ranges)
		assert.NoError(t, err)
		assert.Len(t, batches, 2)
	})

	t.Run("header bytes", func(t *testing.T) {
		ranges := make([]blockblob.BlobHashRange, 205)
		for i := range ranges {
			ranges[i] = blockblob.BlobHashRange{
				Offset: 1000000000000000000 + int64(i*2),
				Count:  1,
			}
		}
		batches, err := batchDedupeBlobHashRanges(ranges)
		assert.NoError(t, err)
		assert.Len(t, batches, 2)
		for _, batch := range batches {
			headerBytes := len("bytes=")
			for i, rnge := range batch {
				if i > 0 {
					headerBytes++
				}
				end := rnge.Offset + rnge.Count - 1
				headerBytes += len(strconv.FormatInt(rnge.Offset, 10)) +
					1 +
					len(strconv.FormatInt(end, 10))
			}
			assert.LessOrEqual(t, headerBytes, dedupeGetBlobHashMaxHeaderBytes)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := batchDedupeBlobHashRanges([]blockblob.BlobHashRange{{Offset: -1, Count: 1}})
		assert.Error(t, err)
	})

	t.Run("unsorted or overlapping", func(t *testing.T) {
		_, err := batchDedupeBlobHashRanges([]blockblob.BlobHashRange{
			{Offset: 10, Count: 2},
			{Offset: 9, Count: 1},
		})
		assert.Error(t, err)

		_, err = batchDedupeBlobHashRanges([]blockblob.BlobHashRange{
			{Offset: 0, Count: 10},
			{Offset: 9, Count: 2},
		})
		assert.Error(t, err)
	})
}

func TestApplyResolvedSourceHashesPersistsOnlyCalculatedSHA(t *testing.T) {
	first := sha256.Sum256([]byte("first"))
	plan := &SourceGridPlan{Blocks: []PlannedBlock{
		crcOnlyBlock(0, 3, 10),
		crcOnlyBlock(3, 7, 20),
	}}
	applyResolvedSourceHashes(plan, map[srcBlockKey]srcBlockHashes{
		{offset: 0, size: 3}: {crc64: 10, sha256: first},
	})

	assert.True(t, plan.Blocks[0].HasCRC64)
	assert.True(t, plan.Blocks[0].HasSHA256)
	assert.True(t, plan.Blocks[0].HasHashes)
	assert.Equal(t, first, plan.Blocks[0].SHA256)
	assert.True(t, plan.Blocks[1].HasCRC64)
	assert.False(t, plan.Blocks[1].HasSHA256)
	assert.False(t, plan.Blocks[1].HasHashes)
}
