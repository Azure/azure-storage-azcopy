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
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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

type dedupeHashPartialTransport struct {
	mu            sync.Mutex
	requests      []*http.Request
	successRanges []blockblob.BlobHashRange
}

func (t *dedupeHashPartialTransport) Do(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	cloned := req.Clone(req.Context())
	cloned.Header = req.Header.Clone()
	t.requests = append(t.requests, cloned)
	call := len(t.requests)
	t.mu.Unlock()

	if call == 2 {
		return nil, context.DeadlineExceeded
	}

	var body strings.Builder
	body.WriteString("<RangeHashList>")
	for _, rnge := range t.successRanges {
		hash := sha256.Sum256([]byte(fmt.Sprintf("%d:%d", rnge.Offset, rnge.Count)))
		fmt.Fprintf(
			&body,
			"<RangeHash><Offset>%d</Offset><Length>%d</Length><Sha256>%s</Sha256></RangeHash>",
			rnge.Offset,
			rnge.Count,
			base64.StdEncoding.EncodeToString(hash[:]),
		)
	}
	body.WriteString("</RangeHashList>")

	headers := http.Header{}
	headers.Set("Content-Type", "application/xml")
	headers.Set("x-ms-request-id", "partial-hash-request")
	return &http.Response{
		Request:    req,
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     headers,
		Body:       io.NopCloser(strings.NewReader(body.String())),
	}, nil
}

func (t *dedupeHashPartialTransport) snapshotRequests() []*http.Request {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*http.Request(nil), t.requests...)
}

type dedupeStageWireTransport struct {
	mu       sync.Mutex
	requests []*http.Request
}

func (t *dedupeStageWireTransport) Do(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	cloned := req.Clone(req.Context())
	cloned.Header = req.Header.Clone()
	t.requests = append(t.requests, cloned)
	t.mu.Unlock()

	headers := http.Header{}
	headers.Set("x-ms-request-id", "stage-request")
	return &http.Response{
		Request:    req,
		StatusCode: http.StatusCreated,
		Status:     "201 Created",
		Header:     headers,
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

func (t *dedupeStageWireTransport) snapshotRequests() []*http.Request {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*http.Request(nil), t.requests...)
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

	sourceRanges     []blockblob.BlobHashRange
	targetRanges     map[string][]blockblob.BlobHashRange
	targetCallCounts map[string]int
	sourceHashes     map[srcBlockKey][32]byte
	targetHashes     map[string]map[srcBlockKey][32]byte
	sourceErr        error
	targetErr        map[string]error

	sourceResponse  *blockblob.GetBlobHashResponse
	targetResponses map[string]blockblob.GetBlobHashResponse
	sourceStarted   chan<- struct{}
	sourceRelease   <-chan struct{}
}

type dedupeResolutionTestTransferManager struct {
	*testJobPartTransferManager
	ctx           context.Context
	contextCalled chan<- struct{}
}

func (m *dedupeResolutionTestTransferManager) LogAtLevelForCurrentTransfer(common.LogLevel, string) {
}

func (m *dedupeResolutionTestTransferManager) Context() context.Context {
	if m.contextCalled != nil {
		m.contextCalled <- struct{}{}
	}
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func (h *fakeDedupeRangeHasher) HashSource(
	_ context.Context,
	_ azcore.ETag,
	ranges []blockblob.BlobHashRange,
) (dedupeRangeHashResult, error) {
	h.mu.Lock()
	h.sourceCalls++
	h.sourceRanges = append([]blockblob.BlobHashRange(nil), ranges...)
	response := h.sourceResponse
	err := h.sourceErr
	started := h.sourceStarted
	release := h.sourceRelease
	sourceHashes := h.sourceHashes
	h.mu.Unlock()

	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		<-release
	}
	if response != nil {
		return dedupeRangeHashResult{
			response:        *response,
			attemptedRanges: append([]blockblob.BlobHashRange(nil), ranges...),
			batches:         1,
		}, err
	}
	if err != nil {
		return dedupeRangeHashResult{
			attemptedRanges: append([]blockblob.BlobHashRange(nil), ranges...),
			batches:         1,
		}, err
	}
	results := make([]blockblob.BlobHashResult, 0, len(ranges))
	for i := len(ranges) - 1; i >= 0; i-- {
		rnge := ranges[i]
		hash := sourceHashes[srcBlockKey{offset: rnge.Offset, size: rnge.Count}]
		results = append(results, blockblob.BlobHashResult{
			Offset: rnge.Offset,
			Count:  rnge.Count,
			SHA256: append([]byte(nil), hash[:]...),
		})
	}
	return dedupeRangeHashResult{
		response:        blockblob.GetBlobHashResponse{RangeHashes: results},
		attemptedRanges: append([]blockblob.BlobHashRange(nil), ranges...),
		batches:         1,
	}, nil
}

func (h *fakeDedupeRangeHasher) HashTarget(
	ctx context.Context,
	targetURI string,
	_ azcore.ETag,
	ranges []blockblob.BlobHashRange,
) (dedupeRangeHashResult, error) {
	contextErr := ctx.Err()
	h.mu.Lock()
	h.targetCalls++
	if h.targetRanges == nil {
		h.targetRanges = make(map[string][]blockblob.BlobHashRange)
	}
	if h.targetCallCounts == nil {
		h.targetCallCounts = make(map[string]int)
	}
	h.targetRanges[targetURI] = append([]blockblob.BlobHashRange(nil), ranges...)
	h.targetCallCounts[targetURI]++
	response, hasResponse := h.targetResponses[targetURI]
	err := h.targetErr[targetURI]
	targetHashes := h.targetHashes[targetURI]
	h.mu.Unlock()

	if contextErr != nil {
		return dedupeRangeHashResult{}, contextErr
	}
	if hasResponse {
		return dedupeRangeHashResult{
			response:        response,
			attemptedRanges: append([]blockblob.BlobHashRange(nil), ranges...),
			batches:         1,
		}, err
	}
	if err != nil {
		return dedupeRangeHashResult{
			attemptedRanges: append([]blockblob.BlobHashRange(nil), ranges...),
			batches:         1,
		}, err
	}
	results := make([]blockblob.BlobHashResult, 0, len(ranges))
	for i := len(ranges) - 1; i >= 0; i-- {
		rnge := ranges[i]
		hash := targetHashes[srcBlockKey{offset: rnge.Offset, size: rnge.Count}]
		results = append(results, blockblob.BlobHashResult{
			Offset: rnge.Offset,
			Count:  rnge.Count,
			SHA256: append([]byte(nil), hash[:]...),
		})
	}
	return dedupeRangeHashResult{
		response:        blockblob.GetBlobHashResponse{RangeHashes: results},
		attemptedRanges: append([]blockblob.BlobHashRange(nil), ranges...),
		batches:         1,
	}, nil
}

func (h *fakeDedupeRangeHasher) targetCallCount(targetURI string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.targetCallCounts[targetURI]
}

type cancelAfterTargetHasher struct {
	*fakeDedupeRangeHasher
	targetURI string
	cancel    context.CancelFunc
	once      sync.Once
}

func (h *cancelAfterTargetHasher) HashTarget(
	ctx context.Context,
	targetURI string,
	etag azcore.ETag,
	ranges []blockblob.BlobHashRange,
) (dedupeRangeHashResult, error) {
	result, err := h.fakeDedupeRangeHasher.HashTarget(ctx, targetURI, etag, ranges)
	if targetURI == h.targetURI {
		h.once.Do(h.cancel)
	}
	return result, err
}

type cancelSecondBatchDedupeRangeHasher struct {
	mu sync.Mutex

	cancelRole string
	cancel     context.CancelFunc

	sourceCalls    int
	targetCalls    int
	sourceRequests [][]blockblob.BlobHashRange
	targetRequests map[string][][]blockblob.BlobHashRange
	sourceHashes   map[srcBlockKey][32]byte
	targetHashes   map[string]map[srcBlockKey][32]byte
}

func (h *cancelSecondBatchDedupeRangeHasher) HashSource(
	ctx context.Context,
	_ azcore.ETag,
	ranges []blockblob.BlobHashRange,
) (dedupeRangeHashResult, error) {
	h.mu.Lock()
	h.sourceCalls++
	call := h.sourceCalls
	h.sourceRequests = append(
		h.sourceRequests,
		append([]blockblob.BlobHashRange(nil), ranges...),
	)
	h.mu.Unlock()
	return h.hash(ctx, "source", call, ranges, h.sourceHashes)
}

func (h *cancelSecondBatchDedupeRangeHasher) HashTarget(
	ctx context.Context,
	targetURI string,
	_ azcore.ETag,
	ranges []blockblob.BlobHashRange,
) (dedupeRangeHashResult, error) {
	h.mu.Lock()
	h.targetCalls++
	call := h.targetCalls
	if h.targetRequests == nil {
		h.targetRequests = make(map[string][][]blockblob.BlobHashRange)
	}
	h.targetRequests[targetURI] = append(
		h.targetRequests[targetURI],
		append([]blockblob.BlobHashRange(nil), ranges...),
	)
	hashes := h.targetHashes[targetURI]
	h.mu.Unlock()
	return h.hash(ctx, "target", call, ranges, hashes)
}

func (h *cancelSecondBatchDedupeRangeHasher) hash(
	ctx context.Context,
	role string,
	call int,
	ranges []blockblob.BlobHashRange,
	hashes map[srcBlockKey][32]byte,
) (dedupeRangeHashResult, error) {
	batches, err := batchDedupeBlobHashRanges(ranges)
	if err != nil {
		return dedupeRangeHashResult{}, err
	}
	if role == h.cancelRole && call == 1 {
		if len(batches) < 3 {
			return dedupeRangeHashResult{}, fmt.Errorf(
				"cancel-second-batch test requires at least three batches",
			)
		}
		attempted := append([]blockblob.BlobHashRange(nil), batches[0]...)
		attempted = append(attempted, batches[1]...)
		h.cancel()
		return dedupeRangeHashResult{
			response:        dedupeHashResponseForRanges(batches[0], hashes),
			attemptedRanges: attempted,
			batches:         2,
		}, ctx.Err()
	}
	return dedupeRangeHashResult{
		response:        dedupeHashResponseForRanges(ranges, hashes),
		attemptedRanges: append([]blockblob.BlobHashRange(nil), ranges...),
		batches:         len(batches),
	}, nil
}

func dedupeHashResponseForRanges(
	ranges []blockblob.BlobHashRange,
	hashes map[srcBlockKey][32]byte,
) blockblob.GetBlobHashResponse {
	results := make([]blockblob.BlobHashResult, 0, len(ranges))
	for _, rnge := range ranges {
		hash := hashes[srcBlockKey{offset: rnge.Offset, size: rnge.Count}]
		results = append(results, blockblob.BlobHashResult{
			Offset: rnge.Offset,
			Count:  rnge.Count,
			SHA256: append([]byte(nil), hash[:]...),
		})
	}
	return blockblob.GetBlobHashResponse{RangeHashes: results}
}

func (h *cancelSecondBatchDedupeRangeHasher) sourceRequest(call int) []blockblob.BlobHashRange {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]blockblob.BlobHashRange(nil), h.sourceRequests[call]...)
}

func (h *cancelSecondBatchDedupeRangeHasher) sourceRequestCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.sourceRequests)
}

func (h *cancelSecondBatchDedupeRangeHasher) targetRequest(
	targetURI string,
	call int,
) []blockblob.BlobHashRange {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]blockblob.BlobHashRange(nil), h.targetRequests[targetURI][call]...)
}

func (h *cancelSecondBatchDedupeRangeHasher) targetRequestCount(targetURI string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.targetRequests[targetURI])
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

func newDedupeBatchCancellationFixture(
	indexTargetSHA bool,
) (
	*SourceGridPlan,
	*SourceGridPlan,
	map[srcBlockKey][32]byte,
	map[srcBlockKey][32]byte,
) {
	count := 2*dedupeGetBlobHashOperationalMaxRanges + 1
	sourceBlocks := make([]PlannedBlock, count)
	targetBlocks := make([]PlannedBlock, count)
	sourceHashes := make(map[srcBlockKey][32]byte, count)
	targetHashes := make(map[srcBlockKey][32]byte, count)
	for i := 0; i < count; i++ {
		crc64 := uint64(i + 1)
		sourceOffset := int64(i * 2)
		targetOffset := int64(1000 + i*2)
		hash := sha256.Sum256([]byte(fmt.Sprintf("batch-range-%d", i)))
		sourceBlocks[i] = crcOnlyBlock(sourceOffset, 1, crc64)
		targetBlocks[i] = crcOnlyBlock(targetOffset, 1, crc64)
		if indexTargetSHA {
			targetBlocks[i].SHA256 = hash
			targetBlocks[i].HasSHA256 = true
			targetBlocks[i].HasHashes = true
		}
		sourceHashes[srcBlockKey{offset: sourceOffset, size: 1}] = hash
		targetHashes[srcBlockKey{offset: targetOffset, size: 1}] = hash
	}
	return &SourceGridPlan{Blocks: sourceBlocks},
		&SourceGridPlan{Blocks: targetBlocks},
		sourceHashes,
		targetHashes
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
	result, err := hasher.hashRanges(
		context.Background(),
		"source",
		client,
		azcore.ETag(`"source-etag"`),
		ranges,
		nil,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, result.batches)
	assert.Equal(t, ranges, result.attemptedRanges)
	assert.Len(t, result.response.RangeHashes, 2)
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
	_, err = hasher.hashRanges(
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

func TestSDKDedupeRangeHasherPreservesSuccessfulBatchesOnLaterTimeout(t *testing.T) {
	ranges := make([]blockblob.BlobHashRange, 2*dedupeGetBlobHashOperationalMaxRanges+1)
	for i := range ranges {
		ranges[i] = blockblob.BlobHashRange{Offset: int64(i * 2), Count: 1}
	}
	transport := &dedupeHashPartialTransport{successRanges: ranges[:dedupeGetBlobHashOperationalMaxRanges]}
	client, err := blockblob.NewClientWithNoCredential(
		"https://acct.blob.core.windows.net/c/source",
		&blockblob.ClientOptions{ClientOptions: policy.ClientOptions{
			Transport: transport,
			Retry:     policy.RetryOptions{MaxRetries: -1},
		}},
	)
	assert.NoError(t, err)

	hasher := &sdkDedupeRangeHasher{}
	result, err := hasher.hashRanges(
		context.Background(),
		"source",
		client,
		azcore.ETag(`"source-etag"`),
		ranges,
		nil,
	)

	assert.Error(t, err)
	assert.Equal(t, 2, result.batches)
	assert.Equal(t, ranges[:2*dedupeGetBlobHashOperationalMaxRanges], result.attemptedRanges)
	assert.Len(t, result.response.RangeHashes, dedupeGetBlobHashOperationalMaxRanges)

	requests := transport.snapshotRequests()
	if assert.Len(t, requests, 2) {
		assert.Equal(t, `"source-etag"`, headerValueFold(requests[0].Header, "If-Match"))
		assert.Equal(t, `"source-etag"`, headerValueFold(requests[1].Header, "If-Match"))
		assert.Equal(t, "bytes=0-0,2-2,4-4,6-6,8-8,10-10,12-12,14-14",
			headerValueFold(requests[0].Header, "x-ms-multi-range"))
		assert.Equal(t, "bytes=16-16,18-18,20-20,22-22,24-24,26-26,28-28,30-30",
			headerValueFold(requests[1].Header, "x-ms-multi-range"))
	}
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
	assert.Equal(t, 2, stats.newCandidateBlocks)
	assert.Equal(t, 2, stats.newCandidateOccurrences)
	assert.Zero(t, stats.targetSHAIndexHits)
	assert.Equal(t, 2, stats.targetSHAIndexMisses)
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

func TestResolveDedupeCandidateHashesReusesIndexedTargetSHA(t *testing.T) {
	hash := sha256.Sum256([]byte("indexed"))
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
	assert.Equal(t, 1, stats.targetSHAIndexHits)
	assert.Zero(t, stats.targetSHAIndexMisses)
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
		targetResponses: map[string]blockblob.GetBlobHashResponse{
			targetURI: {RangeHashes: []blockblob.BlobHashResult{{
				Offset: 100,
				Count:  3,
				SHA256: append([]byte(nil), hash[:]...),
			}}},
		},
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

	_, _, err = resolveDedupeCandidateHashes(
		context.Background(),
		state,
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(0, 3, 10)}},
		azcore.ETag(`"source"`),
		"https://acct.blob.core.windows.net/c/current",
		hasher,
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, hasher.targetCalls)
}

func TestResolveDedupeCandidateHashesPreservesPartialSourceResults(t *testing.T) {
	firstHash := sha256.Sum256([]byte("first"))
	secondHash := sha256.Sum256([]byte("second"))
	targetURI := "https://acct.blob.core.windows.net/c/previous"
	state := &dedupeJobState{committed: common.NewDedupeHashTable()}
	for _, entry := range []common.BlockEntry{
		{
			CRC64:        10,
			TargetURI:    targetURI,
			TargetOffset: 100,
			TargetLength: 3,
			ETag:         azcore.ETag(`"target"`),
		},
		{
			CRC64:        20,
			TargetURI:    targetURI,
			TargetOffset: 200,
			TargetLength: 7,
			ETag:         azcore.ETag(`"target"`),
		},
	} {
		state.committed.Insert(entry)
	}
	hasher := &fakeDedupeRangeHasher{
		sourceResponse: &blockblob.GetBlobHashResponse{RangeHashes: []blockblob.BlobHashResult{{
			Offset: 0,
			Count:  3,
			SHA256: append([]byte(nil), firstHash[:]...),
		}}},
		sourceErr: context.DeadlineExceeded,
		targetHashes: map[string]map[srcBlockKey][32]byte{
			targetURI: {
				{offset: 100, size: 3}: firstHash,
				{offset: 200, size: 7}: secondHash,
			},
		},
		targetErr: make(map[string]error),
	}
	plan := &SourceGridPlan{Blocks: []PlannedBlock{
		crcOnlyBlock(0, 3, 10),
		crcOnlyBlock(3, 7, 20),
	}}

	sourceState, stats, err := resolveDedupeCandidateHashesIncremental(
		context.Background(),
		state,
		plan,
		azcore.ETag(`"source"`),
		"https://acct.blob.core.windows.net/c/current",
		hasher,
		dedupeSourceHashResolutionState{},
	)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Len(t, sourceState.hashes, 1)
	assert.Equal(t, 2, stats.sourceHashRanges)
	assert.Equal(t, []blockblob.BlobHashRange{{Offset: 100, Count: 3}}, hasher.targetRanges[targetURI])
	_, firstHit := decideStaging(sourceState.hashes, state.committed, 0, 3, "https://acct.blob.core.windows.net/c/current")
	_, secondHit := decideStaging(sourceState.hashes, state.committed, 3, 7, "https://acct.blob.core.windows.net/c/current")
	assert.True(t, firstHit)
	assert.False(t, secondHit)

	sourceState, _, err = resolveDedupeCandidateHashesIncremental(
		context.Background(),
		state,
		plan,
		azcore.ETag(`"source"`),
		"https://acct.blob.core.windows.net/c/current",
		hasher,
		sourceState,
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, hasher.sourceCalls)
	assert.Equal(t, 1, hasher.targetCalls)
}

func TestResolveDedupeCandidateHashesPreservesPartialTargetResults(t *testing.T) {
	firstHash := sha256.Sum256([]byte("first"))
	secondHash := sha256.Sum256([]byte("second"))
	targetURI := "https://acct.blob.core.windows.net/c/previous"
	state := &dedupeJobState{committed: common.NewDedupeHashTable()}
	for _, entry := range []common.BlockEntry{
		{
			CRC64:        10,
			TargetURI:    targetURI,
			TargetOffset: 100,
			TargetLength: 3,
			ETag:         azcore.ETag(`"target"`),
		},
		{
			CRC64:        20,
			TargetURI:    targetURI,
			TargetOffset: 200,
			TargetLength: 7,
			ETag:         azcore.ETag(`"target"`),
		},
	} {
		state.committed.Insert(entry)
	}
	hasher := &fakeDedupeRangeHasher{
		sourceHashes: map[srcBlockKey][32]byte{
			{offset: 0, size: 3}: firstHash,
			{offset: 3, size: 7}: secondHash,
		},
		targetResponses: map[string]blockblob.GetBlobHashResponse{
			targetURI: {RangeHashes: []blockblob.BlobHashResult{{
				Offset: 100,
				Count:  3,
				SHA256: append([]byte(nil), firstHash[:]...),
			}}},
		},
		targetErr: map[string]error{targetURI: context.DeadlineExceeded},
	}
	plan := &SourceGridPlan{Blocks: []PlannedBlock{
		crcOnlyBlock(0, 3, 10),
		crcOnlyBlock(3, 7, 20),
	}}

	sourceState, stats, err := resolveDedupeCandidateHashesIncremental(
		context.Background(),
		state,
		plan,
		azcore.ETag(`"source"`),
		"https://acct.blob.core.windows.net/c/current",
		hasher,
		dedupeSourceHashResolutionState{},
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, stats.targetHashFailures)
	first := state.committed.LookupByCRC64AndLength(10, 3)
	second := state.committed.LookupByCRC64AndLength(20, 7)
	if assert.Len(t, first, 1) {
		assert.True(t, first[0].HasSHA256)
		assert.Equal(t, firstHash, first[0].SHA256)
	}
	if assert.Len(t, second, 1) {
		assert.False(t, second[0].HasSHA256)
	}
	_, firstHit := decideStaging(sourceState.hashes, state.committed, 0, 3, "https://acct.blob.core.windows.net/c/current")
	_, secondHit := decideStaging(sourceState.hashes, state.committed, 3, 7, "https://acct.blob.core.windows.net/c/current")
	assert.True(t, firstHit)
	assert.False(t, secondHit)

	sourceState, _, err = resolveDedupeCandidateHashesIncremental(
		context.Background(),
		state,
		plan,
		azcore.ETag(`"source"`),
		"https://acct.blob.core.windows.net/c/current",
		hasher,
		sourceState,
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, hasher.sourceCalls)
	assert.Equal(t, 1, hasher.targetCalls)
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
	hash := sha256.Sum256([]byte("source"))
	hasher := &fakeDedupeRangeHasher{
		sourceResponse: &blockblob.GetBlobHashResponse{RangeHashes: []blockblob.BlobHashResult{{
			Offset: 0,
			Count:  3,
			SHA256: append([]byte(nil), hash[:]...),
		}}},
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
	assert.Equal(t, 1, hasher.sourceCalls)
	assert.Zero(t, hasher.targetCalls)
}

func TestBatchDedupeBlobHashRanges(t *testing.T) {
	t.Run("range count", func(t *testing.T) {
		assert.Less(t, dedupeGetBlobHashOperationalMaxRanges, dedupeGetBlobHashMaxRanges)
		ranges := make([]blockblob.BlobHashRange, 2*dedupeGetBlobHashOperationalMaxRanges+1)
		for i := range ranges {
			ranges[i] = blockblob.BlobHashRange{Offset: int64(i * 2), Count: 1}
		}
		batches, err := batchDedupeBlobHashRanges(ranges)
		assert.NoError(t, err)
		assert.Len(t, batches, 3)
		assert.Len(t, batches[0], dedupeGetBlobHashOperationalMaxRanges)
		assert.Len(t, batches[1], dedupeGetBlobHashOperationalMaxRanges)
		assert.Len(t, batches[2], 1)
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
		assert.Greater(t, len(batches), 2)
		for _, batch := range batches {
			assert.LessOrEqual(t, len(batch), dedupeGetBlobHashOperationalMaxRanges)
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

		_, err = batchDedupeBlobHashRanges([]blockblob.BlobHashRange{{
			Offset: 0,
			Count:  dedupeGetBlobHashMaxBytes + 1,
		}})
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

func newDedupeResolutionTestCopier(
	jobID common.JobID,
	plan *SourceGridPlan,
	hasher dedupeRangeHasher,
) *urlToBlockBlobCopier {
	manager := &dedupeResolutionTestTransferManager{testJobPartTransferManager: &testJobPartTransferManager{info: &TransferInfo{
		JobID:       jobID,
		Source:      "https://source.blob.core.windows.net/c/current",
		Destination: "https://dest.blob.core.windows.net/c/current",
		SrcFilePath: "current",
		DstFilePath: "current",
	}}}
	return &urlToBlockBlobCopier{blockBlobSenderBase: blockBlobSenderBase{
		jptm:         manager,
		dedupeMode:   dedupeActEnforce,
		dedupePlan:   plan,
		dedupeETag:   azcore.ETag(`"source"`),
		dedupeHasher: hasher,
	}}
}

type concurrentDedupeStageFixture struct {
	state       *dedupeJobState
	copier      *urlToBlockBlobCopier
	hasher      *fakeDedupeRangeHasher
	transport   *dedupeStageWireTransport
	sourcePlan  *SourceGridPlan
	targetURI   string
	hashStarted chan struct{}
	hashRelease chan struct{}
	contextUsed chan struct{}
}

func newConcurrentDedupeStageFixture(
	t *testing.T,
	mode dedupeActMode,
	blockCount int,
) concurrentDedupeStageFixture {
	t.Helper()

	jobID := common.NewJobID()
	t.Cleanup(func() {
		clearDedupeStateForJob(jobID)
	})

	const blockSize = int64(3)
	targetURI := "https://dest.blob.core.windows.net/c/target"
	sourcePlan := &SourceGridPlan{Blocks: make([]PlannedBlock, blockCount)}
	targetPlan := &SourceGridPlan{Blocks: make([]PlannedBlock, blockCount)}
	sourceHashes := make(map[srcBlockKey][32]byte, blockCount)
	targetHashes := make(map[srcBlockKey][32]byte, blockCount)
	for i := 0; i < blockCount; i++ {
		sourceOffset := int64(i) * blockSize
		targetOffset := int64(1000+i) * blockSize
		crc := uint64(100 + i)
		hash := sha256.Sum256([]byte(fmt.Sprintf("concurrent-block-%d", i)))
		sourcePlan.Blocks[i] = crcOnlyBlock(sourceOffset, blockSize, crc)
		targetPlan.Blocks[i] = crcOnlyBlock(targetOffset, blockSize, crc)
		sourceHashes[srcBlockKey{offset: sourceOffset, size: blockSize}] = hash
		targetHashes[srcBlockKey{offset: targetOffset, size: blockSize}] = hash
	}

	hashStarted := make(chan struct{}, 1)
	hashRelease := make(chan struct{})
	hasher := &fakeDedupeRangeHasher{
		sourceHashes:  sourceHashes,
		targetHashes:  map[string]map[srcBlockKey][32]byte{targetURI: targetHashes},
		targetErr:     make(map[string]error),
		sourceStarted: hashStarted,
		sourceRelease: hashRelease,
	}
	copier := newDedupeResolutionTestCopier(jobID, sourcePlan, hasher)
	copier.dedupeMode = mode
	contextUsed := make(chan struct{}, blockCount*4+4)
	copier.jptm.(*dedupeResolutionTestTransferManager).contextCalled = contextUsed
	transport := &dedupeStageWireTransport{}
	client, err := blockblob.NewClientWithNoCredential(
		"https://dest.blob.core.windows.net/c/current",
		&blockblob.ClientOptions{ClientOptions: policy.ClientOptions{
			Transport: transport,
			Retry:     policy.RetryOptions{MaxRetries: -1},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	copier.destBlockBlobClient = client

	if recorded := recordCommittedBlocks(
		jobID,
		targetURI,
		"",
		azcore.ETag(`"target"`),
		targetPlan,
	); recorded != blockCount {
		t.Fatalf("recorded %d committed blocks, want %d", recorded, blockCount)
	}

	return concurrentDedupeStageFixture{
		state:       dedupeStateForJob(jobID),
		copier:      copier,
		hasher:      hasher,
		transport:   transport,
		sourcePlan:  sourcePlan,
		targetURI:   targetURI,
		hashStarted: hashStarted,
		hashRelease: hashRelease,
		contextUsed: contextUsed,
	}
}

type dedupeStageResult struct {
	blockIndex int
	handled    bool
}

func startDedupeStage(
	copier *urlToBlockBlobCopier,
	block PlannedBlock,
	blockIndex int,
	results chan<- dedupeStageResult,
) {
	encodedBlockID := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("block-%d", blockIndex)))
	handled := copier.tryDedupeStage(
		common.NewChunkID("current", block.Offset, block.Size),
		int32(blockIndex),
		encodedBlockID,
		block.Size,
	)
	results <- dedupeStageResult{blockIndex: blockIndex, handled: handled}
}

func waitForDedupeStageResults(
	t *testing.T,
	results <-chan dedupeStageResult,
	count int,
) []dedupeStageResult {
	t.Helper()
	collected := make([]dedupeStageResult, 0, count)
	for len(collected) < count {
		select {
		case result := <-results:
			collected = append(collected, result)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for dedupe stage result %d/%d", len(collected), count)
		}
	}
	return collected
}

func waitForDedupeContextCalls(t *testing.T, calls <-chan struct{}, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-calls:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for dedupe context call %d/%d", i, count)
		}
	}
}

func TestTryDedupeStageEnforceWaitersReusePublishedHashes(t *testing.T) {
	const blockCount = 17
	fixture := newConcurrentDedupeStageFixture(t, dedupeActEnforce, blockCount)
	results := make(chan dedupeStageResult, blockCount)

	go startDedupeStage(fixture.copier, fixture.sourcePlan.Blocks[0], 0, results)
	select {
	case <-fixture.hashStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for hash resolution to start")
	}
	waitForDedupeContextCalls(t, fixture.contextUsed, 1)
	for i := 1; i < blockCount; i++ {
		go startDedupeStage(fixture.copier, fixture.sourcePlan.Blocks[i], i, results)
	}
	waitForDedupeContextCalls(t, fixture.contextUsed, blockCount-1)
	close(fixture.hashRelease)

	seen := make([]bool, blockCount)
	for _, result := range waitForDedupeStageResults(t, results, blockCount) {
		assert.True(t, result.handled, "block %d should reuse the published target hash", result.blockIndex)
		seen[result.blockIndex] = true
	}
	assert.NotContains(t, seen, false)
	assert.Equal(t, 1, fixture.hasher.sourceCalls)
	assert.Equal(t, 1, fixture.hasher.targetCalls)
	assert.Len(t, fixture.hasher.sourceRanges, blockCount)
	assert.Len(t, fixture.hasher.targetRanges[fixture.targetURI], blockCount)
	requests := fixture.transport.snapshotRequests()
	assert.Len(t, requests, blockCount)
	expectedRanges := make(map[string]int, blockCount)
	for i := range fixture.sourcePlan.Blocks {
		targetOffset := int64(1000+i) * fixture.sourcePlan.Blocks[i].Size
		expectedRanges[fmt.Sprintf("bytes=%d-%d", targetOffset, targetOffset+fixture.sourcePlan.Blocks[i].Size-1)]++
	}
	for _, request := range requests {
		assert.Equal(t, fixture.targetURI, headerValueFold(request.Header, "x-ms-copy-source"))
		sourceRange := headerValueFold(request.Header, "x-ms-source-range")
		assert.Positive(t, expectedRanges[sourceRange], "unexpected or duplicate source range %q", sourceRange)
		expectedRanges[sourceRange]--
	}
	for sourceRange, remaining := range expectedRanges {
		assert.Zero(t, remaining, "source range %q was not reused", sourceRange)
	}
	assert.EqualValues(t, blockCount, fixture.state.progressSnapshot().referencedBlocks)
}

func TestTryDedupeStageWaiterCancellationDoesNotDeadlock(t *testing.T) {
	fixture := newConcurrentDedupeStageFixture(t, dedupeActEnforce, 2)
	ctx, cancel := context.WithCancel(context.Background())
	fixture.copier.jptm.(*dedupeResolutionTestTransferManager).ctx = ctx
	results := make(chan dedupeStageResult, 2)

	go startDedupeStage(fixture.copier, fixture.sourcePlan.Blocks[0], 0, results)
	select {
	case <-fixture.hashStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for hash resolution to start")
	}
	waitForDedupeContextCalls(t, fixture.contextUsed, 1)
	go startDedupeStage(fixture.copier, fixture.sourcePlan.Blocks[1], 1, results)
	waitForDedupeContextCalls(t, fixture.contextUsed, 1)
	cancel()

	select {
	case result := <-results:
		assert.Equal(t, 1, result.blockIndex)
		assert.False(t, result.handled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled dedupe waiter did not unblock")
	}
	assert.Empty(t, fixture.transport.snapshotRequests())

	close(fixture.hashRelease)
	owner := waitForDedupeStageResults(t, results, 1)[0]
	assert.Equal(t, 0, owner.blockIndex)
	assert.False(t, owner.handled)
}

func TestTryDedupeStageResolutionTimeoutFallsBackToSource(t *testing.T) {
	fixture := newConcurrentDedupeStageFixture(t, dedupeActEnforce, 1)
	fixture.hasher.sourceRelease = nil
	fixture.hasher.sourceErr = context.DeadlineExceeded

	results := make(chan dedupeStageResult, 1)
	go startDedupeStage(fixture.copier, fixture.sourcePlan.Blocks[0], 0, results)
	result := waitForDedupeStageResults(t, results, 1)[0]

	assert.False(t, result.handled)
	assert.Equal(t, 1, fixture.hasher.sourceCalls)
	assert.Zero(t, fixture.hasher.targetCalls)
	assert.Empty(t, fixture.transport.snapshotRequests())
	_, resolveErr := fixture.copier.dedupeResolutionSnapshot()
	assert.ErrorIs(t, resolveErr, context.DeadlineExceeded)
}

func TestTryDedupeStageShadowWaitersStillObservePublishedHashes(t *testing.T) {
	const blockCount = 17
	fixture := newConcurrentDedupeStageFixture(t, dedupeActShadow, blockCount)
	results := make(chan dedupeStageResult, blockCount)

	go startDedupeStage(fixture.copier, fixture.sourcePlan.Blocks[0], 0, results)
	select {
	case <-fixture.hashStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for hash resolution to start")
	}
	waitForDedupeContextCalls(t, fixture.contextUsed, 1)
	for i := 1; i < blockCount; i++ {
		go startDedupeStage(fixture.copier, fixture.sourcePlan.Blocks[i], i, results)
	}
	waitForDedupeContextCalls(t, fixture.contextUsed, blockCount-1)
	close(fixture.hashRelease)

	for _, result := range waitForDedupeStageResults(t, results, blockCount) {
		assert.False(t, result.handled)
	}
	assert.Equal(t, 1, fixture.hasher.sourceCalls)
	assert.Equal(t, 1, fixture.hasher.targetCalls)
	assert.Empty(t, fixture.transport.snapshotRequests())
	snapshot := fixture.state.progressSnapshot()
	assert.EqualValues(t, blockCount, snapshot.wouldReferenceBlocks)
	assert.Zero(t, snapshot.referencedBlocks)
}

func TestDedupeResolutionReconsidersNewCommittedCandidates(t *testing.T) {
	jobID := common.NewJobID()
	defer clearDedupeStateForJob(jobID)
	state := dedupeStateForJob(jobID)
	hash := sha256.Sum256([]byte("shared"))
	firstTarget := "https://dest.blob.core.windows.net/c/first"
	secondTarget := "https://dest.blob.core.windows.net/c/second"
	hasher := &fakeDedupeRangeHasher{
		sourceHashes: map[srcBlockKey][32]byte{{offset: 0, size: 3}: hash},
		targetHashes: map[string]map[srcBlockKey][32]byte{
			firstTarget:  {{offset: 100, size: 3}: hash},
			secondTarget: {{offset: 200, size: 3}: hash},
		},
		targetErr: make(map[string]error),
	}
	plan := &SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(0, 3, 10)}}
	copier := newDedupeResolutionTestCopier(jobID, plan, hasher)

	copier.ensureDedupeHashesResolved(state)
	assert.Zero(t, hasher.sourceCalls)
	assert.Zero(t, hasher.targetCalls)

	assert.Equal(t, 1, recordCommittedBlocks(
		jobID,
		firstTarget,
		"",
		azcore.ETag(`"first"`),
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(100, 3, 10)}},
	))
	copier.ensureDedupeHashesResolved(state)
	index, err := copier.dedupeResolutionSnapshot()
	assert.NoError(t, err)
	_, hit := decideStaging(index, state.committed, 0, 3, "https://dest.blob.core.windows.net/c/current")
	assert.True(t, hit)
	assert.Equal(t, 1, hasher.sourceCalls)
	assert.Equal(t, 1, hasher.targetCalls)
	firstSnapshot := state.progressSnapshot()
	assert.EqualValues(t, 1, firstSnapshot.crcCandidateBlocks)
	assert.EqualValues(t, 1, firstSnapshot.crcCandidateOccurrences)

	assert.Equal(t, 1, recordCommittedBlocks(
		jobID,
		secondTarget,
		"",
		azcore.ETag(`"second"`),
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(200, 3, 10)}},
	))
	copier.ensureDedupeHashesResolved(state)
	assert.Equal(t, 1, hasher.sourceCalls)
	assert.Equal(t, 2, hasher.targetCalls)
	copier.dedupeResolveMu.Lock()
	secondStats := copier.dedupeResolveStats
	copier.dedupeResolveMu.Unlock()
	assert.Equal(t, 1, secondStats.candidateBlocks)
	assert.Equal(t, 2, secondStats.candidateOccurrences)
	assert.Zero(t, secondStats.newCandidateBlocks)
	assert.Equal(t, 1, secondStats.newCandidateOccurrences)
	secondSnapshot := state.progressSnapshot()
	assert.EqualValues(t, 1, secondSnapshot.crcCandidateBlocks)
	assert.EqualValues(t, 2, secondSnapshot.crcCandidateOccurrences)
}

func TestDedupeResolutionCancellationLeavesLaterTargetEpochEligible(t *testing.T) {
	jobID := common.NewJobID()
	defer clearDedupeStateForJob(jobID)
	state := dedupeStateForJob(jobID)
	firstHash := sha256.Sum256([]byte("first"))
	secondHash := sha256.Sum256([]byte("second"))
	firstTarget := "https://dest.blob.core.windows.net/c/a"
	secondTarget := "https://dest.blob.core.windows.net/c/b"
	fakeHasher := &fakeDedupeRangeHasher{
		sourceHashes: map[srcBlockKey][32]byte{
			{offset: 0, size: 3}: firstHash,
			{offset: 3, size: 3}: secondHash,
		},
		targetHashes: map[string]map[srcBlockKey][32]byte{
			firstTarget:  {{offset: 100, size: 3}: firstHash},
			secondTarget: {{offset: 200, size: 3}: secondHash},
		},
		targetErr: make(map[string]error),
	}
	resolutionContext, cancelResolution := context.WithCancel(context.Background())
	defer cancelResolution()
	hasher := &cancelAfterTargetHasher{
		fakeDedupeRangeHasher: fakeHasher,
		targetURI:             firstTarget,
		cancel:                cancelResolution,
	}
	plan := &SourceGridPlan{Blocks: []PlannedBlock{
		crcOnlyBlock(0, 3, 10),
		crcOnlyBlock(3, 3, 20),
	}}
	copier := newDedupeResolutionTestCopier(jobID, plan, hasher)
	manager := copier.jptm.(*dedupeResolutionTestTransferManager)
	manager.ctx = resolutionContext

	assert.Equal(t, 1, recordCommittedBlocks(
		jobID,
		firstTarget,
		"",
		azcore.ETag(`"first"`),
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(100, 3, 10)}},
	))
	assert.Equal(t, 1, recordCommittedBlocks(
		jobID,
		secondTarget,
		"",
		azcore.ETag(`"second"`),
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(200, 3, 20)}},
	))

	assert.True(t, copier.ensureDedupeHashesResolved(state))
	assert.Equal(t, 1, fakeHasher.targetCallCount(firstTarget))
	assert.Zero(t, fakeHasher.targetCallCount(secondTarget))
	assert.False(t, state.targetHashRangeFailed(
		dedupeTargetEpoch{targetURI: secondTarget, etag: azcore.ETag(`"second"`)},
		srcBlockKey{offset: 200, size: 3},
	))

	index, err := copier.dedupeResolutionSnapshot()
	assert.NoError(t, err)
	_, firstHit := decideStaging(index, state.committed, 0, 3, "https://dest.blob.core.windows.net/c/current")
	_, secondHit := decideStaging(index, state.committed, 3, 3, "https://dest.blob.core.windows.net/c/current")
	assert.True(t, firstHit)
	assert.False(t, secondHit)

	manager.ctx = context.Background()
	assert.Equal(t, 1, recordCommittedBlocks(
		jobID,
		"https://dest.blob.core.windows.net/c/new-generation",
		"",
		azcore.ETag(`"new-generation"`),
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(300, 3, 30)}},
	))
	assert.True(t, copier.ensureDedupeHashesResolved(state))
	assert.Equal(t, 1, fakeHasher.targetCallCount(firstTarget))
	assert.Equal(t, 1, fakeHasher.targetCallCount(secondTarget))

	index, err = copier.dedupeResolutionSnapshot()
	assert.NoError(t, err)
	_, firstHit = decideStaging(index, state.committed, 0, 3, "https://dest.blob.core.windows.net/c/current")
	_, secondHit = decideStaging(index, state.committed, 3, 3, "https://dest.blob.core.windows.net/c/current")
	assert.True(t, firstHit)
	assert.True(t, secondHit)
}

func TestDedupeResolutionCancellationLeavesUnstartedTargetBatchEligible(t *testing.T) {
	jobID := common.NewJobID()
	defer clearDedupeStateForJob(jobID)
	state := dedupeStateForJob(jobID)
	sourcePlan, targetPlan, sourceHashes, targetHashes := newDedupeBatchCancellationFixture(false)
	targetURI := "https://dest.blob.core.windows.net/c/target"
	resolutionContext, cancelResolution := context.WithCancel(context.Background())
	defer cancelResolution()
	hasher := &cancelSecondBatchDedupeRangeHasher{
		cancelRole:   "target",
		cancel:       cancelResolution,
		sourceHashes: sourceHashes,
		targetHashes: map[string]map[srcBlockKey][32]byte{targetURI: targetHashes},
	}
	copier := newDedupeResolutionTestCopier(jobID, sourcePlan, hasher)
	manager := copier.jptm.(*dedupeResolutionTestTransferManager)
	manager.ctx = resolutionContext

	assert.Equal(t, len(targetPlan.Blocks), recordCommittedBlocks(
		jobID,
		targetURI,
		"",
		azcore.ETag(`"target"`),
		targetPlan,
	))
	assert.True(t, copier.ensureDedupeHashesResolved(state))
	assert.Equal(t, 1, hasher.targetRequestCount(targetURI))
	assert.Len(t, hasher.targetRequest(targetURI, 0), len(targetPlan.Blocks))

	copier.dedupeResolveMu.Lock()
	firstStats := copier.dedupeResolveStats
	copier.dedupeResolveMu.Unlock()
	assert.Equal(t, 2*dedupeGetBlobHashOperationalMaxRanges, firstStats.targetHashRanges)
	assert.Equal(t, 2, firstStats.targetHashBatches)
	assert.Equal(t, 1, firstStats.targetHashFailures)

	epoch := dedupeTargetEpoch{targetURI: targetURI, etag: azcore.ETag(`"target"`)}
	for i, block := range targetPlan.Blocks {
		candidates := state.committed.LookupByCRC64AndLength(block.CRC64, block.Size)
		if assert.Len(t, candidates, 1) {
			assert.Equal(t, i < dedupeGetBlobHashOperationalMaxRanges, candidates[0].HasSHA256)
		}
		key := srcBlockKey{offset: block.Offset, size: block.Size}
		assert.Equal(
			t,
			i >= dedupeGetBlobHashOperationalMaxRanges &&
				i < 2*dedupeGetBlobHashOperationalMaxRanges,
			state.targetHashRangeFailed(epoch, key),
		)
	}

	index, err := copier.dedupeResolutionSnapshot()
	assert.NoError(t, err)
	_, firstHit := decideStaging(
		index,
		state.committed,
		sourcePlan.Blocks[0].Offset,
		sourcePlan.Blocks[0].Size,
		"https://dest.blob.core.windows.net/c/current",
	)
	_, lastHit := decideStaging(
		index,
		state.committed,
		sourcePlan.Blocks[len(sourcePlan.Blocks)-1].Offset,
		sourcePlan.Blocks[len(sourcePlan.Blocks)-1].Size,
		"https://dest.blob.core.windows.net/c/current",
	)
	assert.True(t, firstHit)
	assert.False(t, lastHit)

	manager.ctx = context.Background()
	assert.Equal(t, 1, recordCommittedBlocks(
		jobID,
		"https://dest.blob.core.windows.net/c/new-generation",
		"",
		azcore.ETag(`"new-generation"`),
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(2000, 1, 1000)}},
	))
	assert.True(t, copier.ensureDedupeHashesResolved(state))
	assert.Equal(t, 2, hasher.targetRequestCount(targetURI))
	lastTarget := targetPlan.Blocks[len(targetPlan.Blocks)-1]
	assert.Equal(t, []blockblob.BlobHashRange{{
		Offset: lastTarget.Offset,
		Count:  lastTarget.Size,
	}}, hasher.targetRequest(targetURI, 1))

	index, err = copier.dedupeResolutionSnapshot()
	assert.NoError(t, err)
	_, lastHit = decideStaging(
		index,
		state.committed,
		sourcePlan.Blocks[len(sourcePlan.Blocks)-1].Offset,
		sourcePlan.Blocks[len(sourcePlan.Blocks)-1].Size,
		"https://dest.blob.core.windows.net/c/current",
	)
	assert.True(t, lastHit)
	for i := dedupeGetBlobHashOperationalMaxRanges; i < 2*dedupeGetBlobHashOperationalMaxRanges; i++ {
		block := targetPlan.Blocks[i]
		assert.True(t, state.targetHashRangeFailed(
			epoch,
			srcBlockKey{offset: block.Offset, size: block.Size},
		))
	}
	snapshot := state.progressSnapshot()
	assert.EqualValues(t, len(targetPlan.Blocks), snapshot.targetHashRanges)
	assert.EqualValues(t, 3, snapshot.targetHashBatches)
	assert.EqualValues(t, len(targetPlan.Blocks), snapshot.crcCandidateBlocks)
	assert.EqualValues(t, len(targetPlan.Blocks), snapshot.crcCandidateOccurrences)
}

func TestDedupeResolutionCancellationLeavesUnstartedSourceBatchEligible(t *testing.T) {
	jobID := common.NewJobID()
	defer clearDedupeStateForJob(jobID)
	state := dedupeStateForJob(jobID)
	sourcePlan, targetPlan, sourceHashes, targetHashes := newDedupeBatchCancellationFixture(true)
	targetURI := "https://dest.blob.core.windows.net/c/target"
	resolutionContext, cancelResolution := context.WithCancel(context.Background())
	defer cancelResolution()
	hasher := &cancelSecondBatchDedupeRangeHasher{
		cancelRole:   "source",
		cancel:       cancelResolution,
		sourceHashes: sourceHashes,
		targetHashes: map[string]map[srcBlockKey][32]byte{targetURI: targetHashes},
	}
	copier := newDedupeResolutionTestCopier(jobID, sourcePlan, hasher)
	manager := copier.jptm.(*dedupeResolutionTestTransferManager)
	manager.ctx = resolutionContext

	assert.Equal(t, len(targetPlan.Blocks), recordCommittedBlocks(
		jobID,
		targetURI,
		"",
		azcore.ETag(`"target"`),
		targetPlan,
	))
	assert.True(t, copier.ensureDedupeHashesResolved(state))
	assert.Equal(t, 1, hasher.sourceRequestCount())
	assert.Len(t, hasher.sourceRequest(0), len(sourcePlan.Blocks))
	assert.Zero(t, hasher.targetRequestCount(targetURI))

	copier.dedupeResolveMu.Lock()
	firstStats := copier.dedupeResolveStats
	copier.dedupeResolveMu.Unlock()
	assert.Equal(t, 2*dedupeGetBlobHashOperationalMaxRanges, firstStats.sourceHashRanges)
	assert.Equal(t, 2, firstStats.sourceHashBatches)

	index, err := copier.dedupeResolutionSnapshot()
	assert.ErrorIs(t, err, context.Canceled)
	assert.Len(t, index, dedupeGetBlobHashOperationalMaxRanges)
	_, firstHit := decideStaging(
		index,
		state.committed,
		sourcePlan.Blocks[0].Offset,
		sourcePlan.Blocks[0].Size,
		"https://dest.blob.core.windows.net/c/current",
	)
	_, lastHit := decideStaging(
		index,
		state.committed,
		sourcePlan.Blocks[len(sourcePlan.Blocks)-1].Offset,
		sourcePlan.Blocks[len(sourcePlan.Blocks)-1].Size,
		"https://dest.blob.core.windows.net/c/current",
	)
	assert.True(t, firstHit)
	assert.False(t, lastHit)

	manager.ctx = context.Background()
	assert.Equal(t, 1, recordCommittedBlocks(
		jobID,
		"https://dest.blob.core.windows.net/c/new-generation",
		"",
		azcore.ETag(`"new-generation"`),
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(2000, 1, 1000)}},
	))
	assert.True(t, copier.ensureDedupeHashesResolved(state))
	assert.Equal(t, 2, hasher.sourceRequestCount())
	lastSource := sourcePlan.Blocks[len(sourcePlan.Blocks)-1]
	assert.Equal(t, []blockblob.BlobHashRange{{
		Offset: lastSource.Offset,
		Count:  lastSource.Size,
	}}, hasher.sourceRequest(1))
	assert.Zero(t, hasher.targetRequestCount(targetURI))

	index, err = copier.dedupeResolutionSnapshot()
	assert.NoError(t, err)
	assert.Len(t, index, dedupeGetBlobHashOperationalMaxRanges+1)
	_, lastHit = decideStaging(
		index,
		state.committed,
		lastSource.Offset,
		lastSource.Size,
		"https://dest.blob.core.windows.net/c/current",
	)
	assert.True(t, lastHit)
	middleSource := sourcePlan.Blocks[dedupeGetBlobHashOperationalMaxRanges]
	_, middleHit := decideStaging(
		index,
		state.committed,
		middleSource.Offset,
		middleSource.Size,
		"https://dest.blob.core.windows.net/c/current",
	)
	assert.False(t, middleHit)
	snapshot := state.progressSnapshot()
	assert.EqualValues(t, len(sourcePlan.Blocks), snapshot.sourceHashRanges)
	assert.EqualValues(t, 3, snapshot.sourceHashBatches)
	assert.EqualValues(t, len(sourcePlan.Blocks), snapshot.crcCandidateBlocks)
	assert.EqualValues(t, len(sourcePlan.Blocks), snapshot.crcCandidateOccurrences)
}

func TestDedupeResolutionCountsDistinctSourceTargetOccurrences(t *testing.T) {
	jobID := common.NewJobID()
	defer clearDedupeStateForJob(jobID)
	state := dedupeStateForJob(jobID)
	hash := sha256.Sum256([]byte("shared"))
	targetURI := "https://dest.blob.core.windows.net/c/target"
	hasher := &fakeDedupeRangeHasher{
		sourceHashes: map[srcBlockKey][32]byte{
			{offset: 0, size: 3}: hash,
			{offset: 3, size: 3}: hash,
		},
		targetHashes: map[string]map[srcBlockKey][32]byte{
			targetURI: {{offset: 100, size: 3}: hash},
		},
		targetErr: make(map[string]error),
	}
	plan := &SourceGridPlan{Blocks: []PlannedBlock{
		crcOnlyBlock(0, 3, 10),
		crcOnlyBlock(3, 3, 10),
	}}
	copier := newDedupeResolutionTestCopier(jobID, plan, hasher)

	assert.Equal(t, 1, recordCommittedBlocks(
		jobID,
		targetURI,
		"",
		azcore.ETag(`"target"`),
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(100, 3, 10)}},
	))
	assert.True(t, copier.ensureDedupeHashesResolved(state))

	copier.dedupeResolveMu.Lock()
	stats := copier.dedupeResolveStats
	copier.dedupeResolveMu.Unlock()
	assert.Equal(t, 2, stats.candidateBlocks)
	assert.Equal(t, 2, stats.candidateOccurrences)
	assert.Equal(t, 2, stats.newCandidateBlocks)
	assert.Equal(t, 2, stats.newCandidateOccurrences)
	assert.Equal(t, 1, hasher.targetCallCount(targetURI))
	snapshot := state.progressSnapshot()
	assert.EqualValues(t, 2, snapshot.crcCandidateBlocks)
	assert.EqualValues(t, 2, snapshot.crcCandidateOccurrences)

	assert.Equal(t, 1, recordCommittedBlocks(
		jobID,
		"https://dest.blob.core.windows.net/c/new-generation",
		"",
		azcore.ETag(`"new-generation"`),
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(200, 3, 20)}},
	))
	assert.True(t, copier.ensureDedupeHashesResolved(state))
	copier.dedupeResolveMu.Lock()
	stats = copier.dedupeResolveStats
	copier.dedupeResolveMu.Unlock()
	assert.Zero(t, stats.newCandidateBlocks)
	assert.Zero(t, stats.newCandidateOccurrences)
	snapshot = state.progressSnapshot()
	assert.EqualValues(t, 2, snapshot.crcCandidateBlocks)
	assert.EqualValues(t, 2, snapshot.crcCandidateOccurrences)
}

func TestDedupeResolutionCoalescesConcurrentReresolution(t *testing.T) {
	jobID := common.NewJobID()
	defer clearDedupeStateForJob(jobID)
	state := dedupeStateForJob(jobID)
	hash := sha256.Sum256([]byte("shared"))
	targetURI := "https://dest.blob.core.windows.net/c/target"
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	hasher := &fakeDedupeRangeHasher{
		sourceHashes:  map[srcBlockKey][32]byte{{offset: 0, size: 3}: hash},
		targetHashes:  map[string]map[srcBlockKey][32]byte{targetURI: {{offset: 100, size: 3}: hash}},
		targetErr:     make(map[string]error),
		sourceStarted: started,
		sourceRelease: release,
	}
	plan := &SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(0, 3, 10)}}
	copier := newDedupeResolutionTestCopier(jobID, plan, hasher)
	copier.ensureDedupeHashesResolved(state)
	assert.Equal(t, 1, recordCommittedBlocks(
		jobID,
		targetURI,
		"",
		azcore.ETag(`"target"`),
		&SourceGridPlan{Blocks: []PlannedBlock{crcOnlyBlock(100, 3, 10)}},
	))

	const workers = 16
	var waitGroup sync.WaitGroup
	for i := 0; i < workers; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			copier.ensureDedupeHashesResolved(state)
		}()
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for source hash resolution")
	}
	waitStarted := time.Now()
	assert.False(t, copier.ensureDedupeHashesResolved(state))
	assert.Less(t, time.Since(waitStarted), 100*time.Millisecond)
	close(release)

	done := make(chan struct{})
	go func() {
		waitGroup.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for concurrent resolvers")
	}

	assert.Equal(t, 1, hasher.sourceCalls)
	assert.Equal(t, 1, hasher.targetCalls)
	index, err := copier.dedupeResolutionSnapshot()
	assert.NoError(t, err)
	_, hit := decideStaging(index, state.committed, 0, 3, "https://dest.blob.core.windows.net/c/current")
	assert.True(t, hit)
	snapshot := state.progressSnapshot()
	assert.EqualValues(t, 1, snapshot.crcCandidateBlocks)
	assert.EqualValues(t, 1, snapshot.crcCandidateOccurrences)
}

func TestDedupeCandidateOccurrenceKeyExcludesCredentials(t *testing.T) {
	source := srcBlockKey{offset: 0, size: 3}
	entry := common.BlockEntry{
		CRC64:        10,
		TargetURI:    "https://dest.blob.core.windows.net/c/target?sv=2026&sig=first-secret",
		TargetOffset: 100,
		TargetLength: 3,
		ETag:         azcore.ETag(`"target"`),
	}
	sameOccurrence := entry
	sameOccurrence.TargetURI = "https://dest.blob.core.windows.net/c/target?sv=2027&sig=second-secret"

	key := newDedupeCandidateOccurrenceKey(source, entry)
	assert.Equal(t, key, newDedupeCandidateOccurrenceKey(source, sameOccurrence))
	assert.Equal(t, "https://dest.blob.core.windows.net/c/target", key.targetURI)
	assert.NotContains(t, key.targetURI, "secret")
	assert.NotEqual(t, key, newDedupeCandidateOccurrenceKey(srcBlockKey{offset: 3, size: 3}, entry))

	differentEpoch := entry
	differentEpoch.ETag = azcore.ETag(`"new-target"`)
	assert.NotEqual(t, key, newDedupeCandidateOccurrenceKey(source, differentEpoch))
}

func TestApplyResolvedSourceHashesPersistsOnlyCalculatedSHA(t *testing.T) {
	first := sha256.Sum256([]byte("first"))
	stale := sha256.Sum256([]byte("stale"))
	plan := &SourceGridPlan{Blocks: []PlannedBlock{
		crcOnlyBlock(0, 3, 10),
		crcOnlyBlock(3, 7, 20),
	}}
	plan.Blocks[1].SHA256 = stale
	plan.Blocks[1].HasSHA256 = true
	plan.Blocks[1].HasHashes = true
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
	assert.Equal(t, [32]byte{}, plan.Blocks[1].SHA256)
}
