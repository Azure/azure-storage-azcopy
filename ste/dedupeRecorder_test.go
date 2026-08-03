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
	"crypto/sha256"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

// hashedBlock builds a PlannedBlock whose hashes are derived from content, so identical content
// produces identical CRC64+SHA256 (and therefore a dedupe hit).
func hashedBlock(content string, offset, size int64) PlannedBlock {
	sum := sha256.Sum256([]byte(content))
	// Derive a stable, content-dependent CRC64 surrogate from the first 8 bytes of the SHA256.
	var crc uint64
	for i := 0; i < 8; i++ {
		crc = crc<<8 | uint64(sum[i])
	}
	return PlannedBlock{
		Offset:       offset,
		Size:         size,
		SrcBlockName: content,
		CRC64:        crc,
		HasCRC64:     true,
		SHA256:       sum,
		HasSHA256:    true,
		HasHashes:    true,
	}
}

func hashCPUBlock(size *int64, crc64Length, sha256Length int) *blockblob.Block {
	return &blockblob.Block{
		Size:   size,
		Crc64:  make([]byte, crc64Length),
		Sha256: make([]byte, sha256Length),
	}
}

type hashCPUTestTransferManager struct {
	*testJobPartTransferManager

	logsMu sync.Mutex
	logs   []string
}

func (m *hashCPUTestTransferManager) LogAtLevelForCurrentTransfer(_ common.LogLevel, message string) {
	m.logsMu.Lock()
	defer m.logsMu.Unlock()
	m.logs = append(m.logs, message)
}

func (m *hashCPUTestTransferManager) logMessages() []string {
	m.logsMu.Lock()
	defer m.logsMu.Unlock()
	return append([]string(nil), m.logs...)
}

func assertNoHashCPUMetricFields(t *testing.T, message string) {
	t.Helper()
	for _, field := range []string{
		"crc64CpuTimeUs",
		"sha256CpuTimeUs",
		"hashCpuTimeUs",
		"hashedBlocks",
		"hashedBytes",
		"hashCpuTimeResponses",
		"crc64CpuTimeMissingResponses",
		"sha256CpuTimeMissingResponses",
		"requestIdMissingResponses",
		"crc64CpuTimeInvalidResponses",
		"sha256CpuTimeInvalidResponses",
		"crc64CpuTimeOverflowed",
		"sha256CpuTimeOverflowed",
		"hashCpuTimeOverflowed",
		"hashedBlocksOverflowed",
		"hashedBytesOverflowed",
		"hashMetricsOverflowed",
	} {
		assert.NotContains(t, message, field)
	}
}

func TestHashCPUResponseDeltaCPUValues(t *testing.T) {
	requestID := "  request-42  "
	tests := []struct {
		name               string
		crc64              *int64
		sha256             *int64
		requestID          *string
		wantCRC64          int64
		wantSHA256         int64
		wantCRC64Missing   bool
		wantSHA256Missing  bool
		wantCRC64Invalid   bool
		wantSHA256Invalid  bool
		wantTrimmedRequest string
	}{
		{
			name:               "positive values and trimmed request ID",
			crc64:              ptrTo(int64(7)),
			sha256:             ptrTo(int64(11)),
			requestID:          &requestID,
			wantCRC64:          7,
			wantSHA256:         11,
			wantTrimmedRequest: "request-42",
		},
		{
			name:   "explicit zero",
			crc64:  ptrTo(int64(0)),
			sha256: ptrTo(int64(0)),
		},
		{
			name:             "crc64 nil",
			sha256:           ptrTo(int64(5)),
			wantSHA256:       5,
			wantCRC64Missing: true,
		},
		{
			name:              "sha256 nil",
			crc64:             ptrTo(int64(3)),
			wantCRC64:         3,
			wantSHA256Missing: true,
		},
		{
			name:              "both nil",
			wantCRC64Missing:  true,
			wantSHA256Missing: true,
		},
		{
			name:              "synthetic negative pointers",
			crc64:             ptrTo(int64(-1)),
			sha256:            ptrTo(int64(-2)),
			wantCRC64Invalid:  true,
			wantSHA256Invalid: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			delta := hashCPUResponseDelta(blockblob.GetBlockListResponse{
				CRC64CPUTimeUS:  test.crc64,
				SHA256CPUTimeUS: test.sha256,
				RequestID:       test.requestID,
			})

			assert.Equal(t, test.wantTrimmedRequest, delta.requestID)
			assert.Equal(t, test.wantCRC64, delta.crc64CPUTimeUS)
			assert.Equal(t, test.wantSHA256, delta.sha256CPUTimeUS)
			assert.Equal(t, test.wantCRC64+test.wantSHA256, delta.hashCPUTimeUS)
			assert.Equal(t, test.wantCRC64Missing, delta.crc64CPUTimeMissing)
			assert.Equal(t, test.wantSHA256Missing, delta.sha256CPUTimeMissing)
			assert.Equal(t, test.wantCRC64Invalid, delta.crc64CPUTimeInvalid)
			assert.Equal(t, test.wantSHA256Invalid, delta.sha256CPUTimeInvalid)
			assert.False(t, delta.hashCPUTimeOverflowed)
		})
	}
}

func TestGetBlockListCRCResponseDeltaIgnoresSHA(t *testing.T) {
	response := blockblob.GetBlockListResponse{
		CRC64CPUTimeUS:  ptrTo(int64(7)),
		SHA256CPUTimeUS: ptrTo(int64(999)),
		RequestID:       ptrTo("crc-only-request"),
		BlockList: blockblob.BlockList{CommittedBlocks: []*blockblob.Block{
			{Size: ptrTo(int64(37)), Crc64: make([]byte, 8), Sha256: make([]byte, 32)},
		}},
	}

	delta := getBlockListCRCResponseDelta(response)

	assert.EqualValues(t, 7, delta.crc64CPUTimeUS)
	assert.Zero(t, delta.sha256CPUTimeUS)
	assert.EqualValues(t, 7, delta.hashCPUTimeUS)
	assert.False(t, delta.sha256CPUTimeMissing)
	assert.EqualValues(t, 1, delta.hashedBlocks)
	assert.EqualValues(t, 37, delta.hashedBytes)
}

func TestGetBlobHashSHAResponseDeltaDiagnostics(t *testing.T) {
	missing := getBlobHashSHAResponseDelta(blockblob.GetBlobHashResponse{})
	assert.True(t, missing.sha256CPUTimeMissing)
	assert.False(t, missing.crc64CPUTimeMissing)

	invalid := getBlobHashSHAResponseDelta(blockblob.GetBlobHashResponse{
		SHA256CPUTimeUS: ptrTo(int64(-1)),
	})
	assert.True(t, invalid.sha256CPUTimeInvalid)
	assert.False(t, invalid.sha256CPUTimeMissing)
	assert.Zero(t, invalid.sha256CPUTimeUS)
}

func TestHashCPUResponseDeltaCountsOnlyCompleteHashes(t *testing.T) {
	response := blockblob.GetBlockListResponse{}
	response.CommittedBlocks = []*blockblob.Block{
		hashCPUBlock(ptrTo(int64(10)), 8, 32),
		hashCPUBlock(ptrTo(int64(20)), 8, 32),
		hashCPUBlock(nil, 8, 32),
		hashCPUBlock(ptrTo(int64(0)), 8, 32),
		hashCPUBlock(ptrTo(int64(-5)), 8, 32),
		hashCPUBlock(ptrTo(int64(100)), 7, 32),
		hashCPUBlock(ptrTo(int64(100)), 8, 31),
		hashCPUBlock(ptrTo(int64(100)), 9, 32),
		hashCPUBlock(ptrTo(int64(100)), 8, 33),
		nil,
	}

	delta := hashCPUResponseDelta(response)

	assert.EqualValues(t, 5, delta.hashedBlocks)
	assert.EqualValues(t, 30, delta.hashedBytes)
	assert.False(t, delta.hashedBlocksOverflowed)
	assert.False(t, delta.hashedBytesOverflowed)
}

func TestHashCPUResponseDeltaSaturatesCombinedCPUAndHashedBytes(t *testing.T) {
	response := blockblob.GetBlockListResponse{
		CRC64CPUTimeUS:  ptrTo(int64(math.MaxInt64)),
		SHA256CPUTimeUS: ptrTo(int64(1)),
	}
	response.CommittedBlocks = []*blockblob.Block{
		hashCPUBlock(ptrTo(int64(math.MaxInt64)), 8, 32),
		hashCPUBlock(ptrTo(int64(1)), 8, 32),
	}

	delta := hashCPUResponseDelta(response)

	assert.EqualValues(t, math.MaxInt64, delta.hashCPUTimeUS)
	assert.True(t, delta.hashCPUTimeOverflowed)
	assert.EqualValues(t, 2, delta.hashedBlocks)
	assert.EqualValues(t, math.MaxInt64, delta.hashedBytes)
	assert.True(t, delta.hashedBytesOverflowed)
}

func TestHashCPURecordAccumulatesTotalsAndDiagnostics(t *testing.T) {
	state := &dedupeHashCPUState{}
	responses := []blockblob.GetBlockListResponse{
		{
			CRC64CPUTimeUS: ptrTo(int64(10)),
			RequestID:      ptrTo("request-1"),
			BlockList: blockblob.BlockList{CommittedBlocks: []*blockblob.Block{
				hashCPUBlock(ptrTo(int64(100)), 8, 32),
			}},
		},
		{
			SHA256CPUTimeUS: ptrTo(int64(20)),
			RequestID:       ptrTo("request-2"),
			BlockList: blockblob.BlockList{CommittedBlocks: []*blockblob.Block{
				hashCPUBlock(ptrTo(int64(50)), 8, 32),
			}},
		},
		{
			CRC64CPUTimeUS:  ptrTo(int64(-1)),
			SHA256CPUTimeUS: ptrTo(int64(-2)),
			RequestID:       ptrTo("   "),
		},
	}

	for _, response := range responses {
		_, accepted := state.record(hashCPUResponseDelta(response))
		assert.True(t, accepted)
	}

	assert.Equal(t, dedupeHashCPUSnapshot{
		crc64CPUTimeUS:                10,
		sha256CPUTimeUS:               20,
		hashCPUTimeUS:                 30,
		hashedBlocks:                  2,
		hashedBytes:                   150,
		hashCPUTimeResponses:          3,
		crc64CPUTimeMissingResponses:  1,
		sha256CPUTimeMissingResponses: 1,
		requestIDMissingResponses:     1,
		crc64CPUTimeInvalidResponses:  1,
		sha256CPUTimeInvalidResponses: 1,
	}, state.snapshot())
}

func TestHashCPURecordRejectsDuplicateRequestID(t *testing.T) {
	state := &dedupeHashCPUState{}
	delta := dedupeHashCPUResponseDelta{
		requestID:       "request-duplicate",
		crc64CPUTimeUS:  2,
		sha256CPUTimeUS: 3,
		hashCPUTimeUS:   5,
		hashedBlocks:    1,
		hashedBytes:     64,
	}

	first, accepted := state.record(delta)
	assert.True(t, accepted)
	assert.EqualValues(t, 1, first.cumulative.hashCPUTimeResponses)

	duplicate, accepted := state.record(delta)
	assert.False(t, accepted)
	assert.Equal(t, dedupeHashCPUEventSnapshot{}, duplicate)

	snapshot := state.snapshot()
	assert.EqualValues(t, 2, snapshot.crc64CPUTimeUS)
	assert.EqualValues(t, 3, snapshot.sha256CPUTimeUS)
	assert.EqualValues(t, 1, snapshot.hashedBlocks)
	assert.EqualValues(t, 64, snapshot.hashedBytes)
	assert.EqualValues(t, 1, snapshot.hashCPUTimeResponses)
}

func TestHashCPURecordConcurrentDuplicateRequestID(t *testing.T) {
	const workers = 64
	state := &dedupeHashCPUState{}
	delta := dedupeHashCPUResponseDelta{
		requestID:       "request-concurrent",
		crc64CPUTimeUS:  13,
		sha256CPUTimeUS: 17,
		hashCPUTimeUS:   30,
		hashedBlocks:    1,
		hashedBytes:     128,
	}
	start := make(chan struct{})
	results := make(chan bool, workers)
	var waitGroup sync.WaitGroup

	for i := 0; i < workers; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			_, accepted := state.record(delta)
			results <- accepted
		}()
	}
	close(start)
	waitGroup.Wait()
	close(results)

	accepted := 0
	for result := range results {
		if result {
			accepted++
		}
	}
	assert.Equal(t, 1, accepted)
	assert.Equal(t, dedupeHashCPUSnapshot{
		crc64CPUTimeUS:       13,
		sha256CPUTimeUS:      17,
		hashCPUTimeUS:        30,
		hashedBlocks:         1,
		hashedBytes:          128,
		hashCPUTimeResponses: 1,
	}, state.snapshot())
}

func TestHashCPURecordAcceptsEveryMissingRequestID(t *testing.T) {
	state := &dedupeHashCPUState{}
	requestIDs := []*string{nil, ptrTo(""), ptrTo(" \t ")}

	for _, requestID := range requestIDs {
		_, accepted := state.record(hashCPUResponseDelta(blockblob.GetBlockListResponse{
			CRC64CPUTimeUS:  ptrTo(int64(0)),
			SHA256CPUTimeUS: ptrTo(int64(0)),
			RequestID:       requestID,
		}))
		assert.True(t, accepted)
	}

	snapshot := state.snapshot()
	assert.EqualValues(t, 3, snapshot.hashCPUTimeResponses)
	assert.EqualValues(t, 3, snapshot.requestIDMissingResponses)
	assert.Zero(t, snapshot.crc64CPUTimeMissingResponses)
	assert.Zero(t, snapshot.sha256CPUTimeMissingResponses)
}

func TestHashCPURecordSaturationAndStickyOverflow(t *testing.T) {
	t.Run("components", func(t *testing.T) {
		state := &dedupeHashCPUState{
			crc64CPUTimeUS:  math.MaxInt64,
			sha256CPUTimeUS: math.MaxInt64,
		}
		_, accepted := state.record(dedupeHashCPUResponseDelta{
			requestID:       "component-overflow",
			crc64CPUTimeUS:  1,
			sha256CPUTimeUS: 1,
			hashCPUTimeUS:   2,
		})
		assert.True(t, accepted)

		snapshot := state.snapshot()
		assert.EqualValues(t, math.MaxInt64, snapshot.crc64CPUTimeUS)
		assert.EqualValues(t, math.MaxInt64, snapshot.sha256CPUTimeUS)
		assert.EqualValues(t, math.MaxInt64, snapshot.hashCPUTimeUS)
		assert.True(t, snapshot.crc64CPUTimeOverflowed)
		assert.True(t, snapshot.sha256CPUTimeOverflowed)
		assert.True(t, snapshot.hashCPUTimeOverflowed)
		assert.True(t, snapshot.hashMetricsOverflowed)

		_, accepted = state.record(dedupeHashCPUResponseDelta{requestID: "after-component-overflow"})
		assert.True(t, accepted)
		snapshot = state.snapshot()
		assert.True(t, snapshot.crc64CPUTimeOverflowed)
		assert.True(t, snapshot.sha256CPUTimeOverflowed)
		assert.True(t, snapshot.hashCPUTimeOverflowed)
		assert.True(t, snapshot.hashMetricsOverflowed)
	})

	t.Run("combined", func(t *testing.T) {
		state := &dedupeHashCPUState{
			crc64CPUTimeUS:  math.MaxInt64 - 1,
			sha256CPUTimeUS: 1,
		}
		_, accepted := state.record(dedupeHashCPUResponseDelta{
			requestID:       "combined-overflow",
			sha256CPUTimeUS: 1,
			hashCPUTimeUS:   1,
		})
		assert.True(t, accepted)

		snapshot := state.snapshot()
		assert.False(t, snapshot.crc64CPUTimeOverflowed)
		assert.False(t, snapshot.sha256CPUTimeOverflowed)
		assert.True(t, snapshot.hashCPUTimeOverflowed)
		assert.EqualValues(t, math.MaxInt64, snapshot.hashCPUTimeUS)
		assert.True(t, snapshot.hashMetricsOverflowed)
	})

	t.Run("block and byte totals", func(t *testing.T) {
		state := &dedupeHashCPUState{
			hashedBlocks: math.MaxInt64,
			hashedBytes:  math.MaxInt64,
		}
		_, accepted := state.record(dedupeHashCPUResponseDelta{
			requestID:    "hash-total-overflow",
			hashedBlocks: 1,
			hashedBytes:  1,
		})
		assert.True(t, accepted)

		snapshot := state.snapshot()
		assert.EqualValues(t, math.MaxInt64, snapshot.hashedBlocks)
		assert.EqualValues(t, math.MaxInt64, snapshot.hashedBytes)
		assert.True(t, snapshot.hashedBlocksOverflowed)
		assert.True(t, snapshot.hashedBytesOverflowed)
		assert.True(t, snapshot.hashMetricsOverflowed)

		_, accepted = state.record(dedupeHashCPUResponseDelta{requestID: "after-hash-total-overflow"})
		assert.True(t, accepted)
		snapshot = state.snapshot()
		assert.True(t, snapshot.hashedBlocksOverflowed)
		assert.True(t, snapshot.hashedBytesOverflowed)
		assert.True(t, snapshot.hashMetricsOverflowed)
	})

	t.Run("diagnostic counters", func(t *testing.T) {
		state := &dedupeHashCPUState{
			hashCPUTimeResponses:          math.MaxInt64,
			crc64CPUTimeMissingResponses:  math.MaxInt64,
			sha256CPUTimeMissingResponses: math.MaxInt64,
			requestIDMissingResponses:     math.MaxInt64,
			crc64CPUTimeInvalidResponses:  math.MaxInt64,
			sha256CPUTimeInvalidResponses: math.MaxInt64,
		}
		_, accepted := state.record(dedupeHashCPUResponseDelta{
			crc64CPUTimeMissing:  true,
			sha256CPUTimeMissing: true,
			crc64CPUTimeInvalid:  true,
			sha256CPUTimeInvalid: true,
		})
		assert.True(t, accepted)

		snapshot := state.snapshot()
		assert.EqualValues(t, math.MaxInt64, snapshot.hashCPUTimeResponses)
		assert.EqualValues(t, math.MaxInt64, snapshot.crc64CPUTimeMissingResponses)
		assert.EqualValues(t, math.MaxInt64, snapshot.sha256CPUTimeMissingResponses)
		assert.EqualValues(t, math.MaxInt64, snapshot.requestIDMissingResponses)
		assert.EqualValues(t, math.MaxInt64, snapshot.crc64CPUTimeInvalidResponses)
		assert.EqualValues(t, math.MaxInt64, snapshot.sha256CPUTimeInvalidResponses)
		assert.True(t, snapshot.hashMetricsOverflowed)

		_, accepted = state.record(dedupeHashCPUResponseDelta{requestID: "after-diagnostic-overflow"})
		assert.True(t, accepted)
		assert.True(t, state.snapshot().hashMetricsOverflowed)
	})
}

func TestHashCPUTimeMessageContainsExactDeltaAndCumulativeFields(t *testing.T) {
	response := blockblob.GetBlockListResponse{
		CRC64CPUTimeUS:  ptrTo(int64(4)),
		SHA256CPUTimeUS: ptrTo(int64(6)),
		RequestID:       ptrTo("  request-event  "),
		BlockList: blockblob.BlockList{CommittedBlocks: []*blockblob.Block{
			hashCPUBlock(ptrTo(int64(9)), 8, 32),
			hashCPUBlock(nil, 8, 32),
		}},
	}
	state := &dedupeHashCPUState{}
	snapshot, accepted := state.record(hashCPUResponseDelta(response))
	assert.True(t, accepted)

	jobID := common.NewJobID()
	info := &TransferInfo{
		JobID:       jobID,
		DstFilePath: "dir/file.bin",
		Destination: "https://acct.blob.core.windows.net/c/file.bin?sig=secret",
	}
	expectedFields := "requestId=\"request-event\" crc64CpuTimeUsDelta=4 " +
		"sha256CpuTimeUsDelta=6 hashCpuTimeUsDelta=10 hashedBlocksDelta=2 " +
		"hashedBytesDelta=9 crc64CpuTimeUs=4 sha256CpuTimeUs=6 hashCpuTimeUs=10 " +
		"hashedBlocks=2 hashedBytes=9 hashCpuTimeResponses=1 " +
		"crc64CpuTimeMissingResponses=0 sha256CpuTimeMissingResponses=0 " +
		"requestIdMissingResponses=0 crc64CpuTimeInvalidResponses=0 " +
		"sha256CpuTimeInvalidResponses=0 crc64CpuTimeOverflowed=false " +
		"sha256CpuTimeOverflowed=false hashCpuTimeOverflowed=false " +
		"hashedBlocksOverflowed=false hashedBytesOverflowed=false hashMetricsOverflowed=false"
	expected := fmt.Sprintf(
		"%s event=hash_cpu_time mode=enforce jobId=%s file=%q destination=%q %s",
		dedupeProgressPrefix,
		jobID.String(),
		info.DstFilePath,
		"https://acct.blob.core.windows.net/c/file.bin",
		expectedFields,
	)

	assert.Equal(t, expectedFields, snapshot.fields())
	assert.Equal(t, expected, dedupeHashCPUTimeMessage(dedupeActEnforce, info, snapshot))
}

func TestHashCPUEmitterScopesModesAndAcceptedResponses(t *testing.T) {
	oldQueue := dedupeProgressQueue
	queue := make(chan dedupeProgressEntry, 8)
	dedupeProgressQueue = queue
	t.Cleanup(func() {
		dedupeProgressQueue = oldQueue
	})

	jobID := common.NewJobID()
	t.Cleanup(func() {
		clearDedupeStateForJob(jobID)
	})
	manager := &hashCPUTestTransferManager{
		testJobPartTransferManager: &testJobPartTransferManager{
			info: &TransferInfo{
				JobID:       jobID,
				DstFilePath: "empty-block-list.bin",
				Destination: "https://acct.blob.core.windows.net/c/empty-block-list.bin",
			},
		},
	}
	state := dedupeStateForJob(jobID)
	response := blockblob.GetBlockListResponse{
		CRC64CPUTimeUS:  ptrTo(int64(7)),
		SHA256CPUTimeUS: ptrTo(int64(11)),
		RequestID:       ptrTo("request-empty-list"),
	}

	emitHashCPUTime(manager, dedupeActOff, response)
	emitHashCPUTime(manager, dedupeActShadow, response)
	assert.Equal(t, dedupeHashCPUSnapshot{}, state.hashCPU.snapshot())
	assert.Empty(t, queue)
	assert.Empty(t, manager.logMessages())

	emitHashCPUTime(manager, dedupeActEnforce, response)
	snapshot := state.hashCPU.snapshot()
	assert.EqualValues(t, 7, snapshot.crc64CPUTimeUS)
	assert.Zero(t, snapshot.sha256CPUTimeUS)
	assert.EqualValues(t, 7, snapshot.hashCPUTimeUS)
	assert.Zero(t, snapshot.hashedBlocks)
	assert.Zero(t, snapshot.hashedBytes)
	assert.EqualValues(t, 1, snapshot.hashCPUTimeResponses)
	assert.Len(t, queue, 1)
	assert.Len(t, manager.logMessages(), 1)
	first := <-queue
	assert.Contains(t, first.message, "event=hash_cpu_time")
	assert.Contains(t, first.message, `operation="get_block_list" role="source"`)
	assert.Contains(t, first.message, `requestId="request-empty-list"`)

	emitHashCPUTime(manager, dedupeActEnforce, response)
	assert.Empty(t, queue)
	assert.Len(t, manager.logMessages(), 1)
	assert.EqualValues(t, 1, state.hashCPU.snapshot().hashCPUTimeResponses)

	secondResponse := response
	secondResponse.RequestID = ptrTo("request-empty-list-2")
	emitHashCPUTime(manager, dedupeActEnforce, secondResponse)
	assert.Len(t, queue, 1)
	assert.Len(t, manager.logMessages(), 2)
	assert.EqualValues(t, 2, state.hashCPU.snapshot().hashCPUTimeResponses)
}

func TestGetBlobHashCPUEmitterRecordsSHAOnly(t *testing.T) {
	oldQueue := dedupeProgressQueue
	queue := make(chan dedupeProgressEntry, 2)
	dedupeProgressQueue = queue
	t.Cleanup(func() {
		dedupeProgressQueue = oldQueue
	})

	jobID := common.NewJobID()
	t.Cleanup(func() {
		clearDedupeStateForJob(jobID)
	})
	manager := &hashCPUTestTransferManager{
		testJobPartTransferManager: &testJobPartTransferManager{
			info: &TransferInfo{
				JobID:       jobID,
				DstFilePath: "candidate.bin",
				Destination: "https://acct.blob.core.windows.net/c/candidate.bin",
			},
		},
	}
	response := blockblob.GetBlobHashResponse{
		SHA256CPUTimeUS: ptrTo(int64(23)),
		RequestID:       ptrTo("request-get-blob-hash"),
		RangeHashes: []blockblob.BlobHashResult{
			{Offset: 0, Count: 3, SHA256: make([]byte, 32)},
			{Offset: 10, Count: 7, SHA256: make([]byte, 32)},
		},
	}

	emitGetBlobHashCPUTime(manager, dedupeActEnforce, "target", response)

	snapshot := dedupeStateForJob(jobID).hashCPU.snapshot()
	assert.Zero(t, snapshot.crc64CPUTimeUS)
	assert.EqualValues(t, 23, snapshot.sha256CPUTimeUS)
	assert.EqualValues(t, 23, snapshot.hashCPUTimeUS)
	assert.EqualValues(t, 2, snapshot.hashedBlocks)
	assert.EqualValues(t, 10, snapshot.hashedBytes)
	assert.Zero(t, snapshot.crc64CPUTimeMissingResponses)
	assert.Zero(t, snapshot.sha256CPUTimeMissingResponses)
	assert.Len(t, queue, 1)
	assert.Len(t, manager.logMessages(), 1)
	message := (<-queue).message
	assert.Contains(t, message, `operation="get_blob_hash" role="target"`)
	assert.Contains(t, message, `sha256CpuTimeUsDelta=23`)
	assert.Contains(t, message, `hashedBlocksDelta=2`)
	assert.Contains(t, message, `hashedBytesDelta=10`)
}

func TestDedupeHashResolutionProgressCounters(t *testing.T) {
	oldQueue := dedupeProgressQueue
	queue := make(chan dedupeProgressEntry, 4)
	dedupeProgressQueue = queue
	t.Cleanup(func() {
		dedupeProgressQueue = oldQueue
	})

	jobID := common.NewJobID()
	t.Cleanup(func() {
		clearDedupeStateForJob(jobID)
	})
	manager := &hashCPUTestTransferManager{
		testJobPartTransferManager: &testJobPartTransferManager{
			info: &TransferInfo{
				JobID:       jobID,
				DstFilePath: "candidate.bin",
				Destination: "https://acct.blob.core.windows.net/c/candidate.bin?sig=secret",
			},
		},
	}
	stats := dedupeHashResolutionStats{
		candidateBlocks:          2,
		candidateOccurrences:     3,
		newCandidateBlocks:       2,
		newCandidateOccurrences:  3,
		sourceHashRanges:         2,
		sourceHashBatches:        1,
		targetHashRanges:         3,
		targetHashBatches:        2,
		targetSHAIndexHits:       4,
		targetSHAIndexMisses:     3,
		targetHashFailures:       1,
		sourceEpochInvalidations: 1,
		targetEpochInvalidations: 2,
	}

	emitDedupeHashResolutionProgress(manager, dedupeActEnforce, stats, nil)

	snapshot := dedupeStateForJob(jobID).progressSnapshot()
	assert.EqualValues(t, 2, snapshot.crcCandidateBlocks)
	assert.EqualValues(t, 3, snapshot.crcCandidateOccurrences)
	assert.EqualValues(t, 2, snapshot.sourceHashRanges)
	assert.EqualValues(t, 1, snapshot.sourceHashBatches)
	assert.EqualValues(t, 3, snapshot.targetHashRanges)
	assert.EqualValues(t, 2, snapshot.targetHashBatches)
	assert.EqualValues(t, 4, snapshot.targetSHAIndexHits)
	assert.EqualValues(t, 3, snapshot.targetSHAIndexMisses)
	assert.EqualValues(t, 1, snapshot.targetHashFailures)
	assert.EqualValues(t, 3, snapshot.epochInvalidations)

	assert.Len(t, queue, 4)
	messages := []string{(<-queue).message, (<-queue).message, (<-queue).message, (<-queue).message}
	combined := strings.Join(messages, "\n")
	assert.Contains(t, combined, "event=crc_candidate_hit")
	assert.Contains(t, combined, `event=epoch_invalidated_412`)
	assert.Contains(t, combined, `role="source_hash"`)
	assert.Contains(t, combined, `role="target_hash"`)
	assert.Contains(t, combined, "event=get_blob_hash_complete")
	assert.NotContains(t, combined, "sig=secret")
}

func TestDedupeHashResolutionProgressNoCandidate(t *testing.T) {
	oldQueue := dedupeProgressQueue
	queue := make(chan dedupeProgressEntry, 1)
	dedupeProgressQueue = queue
	t.Cleanup(func() {
		dedupeProgressQueue = oldQueue
	})

	jobID := common.NewJobID()
	t.Cleanup(func() {
		clearDedupeStateForJob(jobID)
	})
	manager := &hashCPUTestTransferManager{
		testJobPartTransferManager: &testJobPartTransferManager{
			info: &TransferInfo{
				JobID:       jobID,
				DstFilePath: "miss.bin",
				Destination: "https://acct.blob.core.windows.net/c/miss.bin",
			},
		},
	}

	emitDedupeHashResolutionProgress(
		manager,
		dedupeActEnforce,
		dedupeHashResolutionStats{},
		nil,
	)

	assert.Len(t, queue, 1)
	assert.Contains(t, (<-queue).message, "event=crc_candidate_miss")
}

func TestHashCPUFieldsAreIsolatedFromGenericProgressEvents(t *testing.T) {
	info := &TransferInfo{
		JobID:       common.NewJobID(),
		DstFilePath: "file.bin",
		Destination: "https://acct.blob.core.windows.net/c/file.bin",
	}
	snapshot := dedupeProgressSnapshot{
		referencedBlocks:   1,
		referencedBytes:    64,
		sourceStagedBlocks: 2,
		sourceStagedBytes:  128,
	}

	for _, event := range []string{"target_reuse", "small_file_transfer_complete"} {
		message := dedupeProgressMessage(
			event,
			dedupeActEnforce,
			info,
			`operation="unrelated" requestId="service-request"`,
			snapshot,
		)
		assert.Contains(t, message, "event="+event)
		assertNoHashCPUMetricFields(t, message)
	}
}

func TestDedupeProgressSnapshotEnforce(t *testing.T) {
	a := assert.New(t)
	st := &dedupeJobState{}
	st.addFileStarted()
	st.addFileStarted()
	st.addFileCommitted()
	st.addReferenced(100)
	st.addReferenced(50)
	st.addSourceStaged(100)
	st.addFallback()
	st.addSmallFileStarted(1024)
	st.addSmallFileResult(common.ETransferStatus.Success(), 1024)

	a.Equal(
		"filesStarted=2 filesCommitted=1 targetURIBlocks=2 targetURIBytes=150 "+
			"sourceURIBlocks=1 sourceURIBytes=100 fallbackBlocks=1 transferredBlocks=3 "+
			"transferredBytes=250 wanSavingsPercent=60.0 crcDiscoveredBlocks=0 "+
			"crcDiscoveredBytes=0 crcCandidateBlocks=0 crcCandidateOccurrences=0 "+
			"sourceHashRanges=0 sourceHashBatches=0 targetHashRanges=0 "+
			"targetHashBatches=0 targetSHAIndexHits=0 targetSHAIndexMisses=0 "+
			"targetHashFailures=0 epochInvalidations=0 "+
			"shaConfirmedBlocks=0 shaMismatchBlocks=0 smallFilesStarted=1 "+
			"smallFilesCompleted=1 smallFilesFailed=0 smallFilesSkipped=0 "+
			"smallFilesCanceled=0 smallFilesInProgress=0 smallFileBytesStarted=1024 "+
			"smallFileBytesCompleted=1024 smallFileBytesFailed=0 smallFileBytesSkipped=0 "+
			"smallFileBytesCanceled=0 smallFileBytesInProgress=0",
		st.progressSnapshot().fields(dedupeActEnforce))
}

func TestDedupeProgressSmallFileThreshold(t *testing.T) {
	jptm := &testJobPartTransferManager{
		info: &TransferInfo{
			SourceSize: smallFileProgressThresholdBytes - 1,
			EntityType: common.EEntityType.File(),
		},
		fromTo: common.EFromTo.BlobBlob(),
	}
	assert.True(t, isSmallFileProgressTransfer(jptm))

	jptm.info.SourceSize = 0
	assert.True(t, isSmallFileProgressTransfer(jptm))

	jptm.info.SourceSize = smallFileProgressThresholdBytes
	assert.False(t, isSmallFileProgressTransfer(jptm))

	jptm.info.SourceSize = smallFileProgressThresholdBytes - 1
	jptm.fromTo = common.EFromTo.LocalBlob()
	assert.False(t, isSmallFileProgressTransfer(jptm))

	jptm.fromTo = common.EFromTo.BlobBlob()
	jptm.info.EntityType = common.EEntityType.Folder()
	assert.False(t, isSmallFileProgressTransfer(jptm))
}

func TestDedupeProgressSmallFileResultCounters(t *testing.T) {
	st := &dedupeJobState{}
	st.addSmallFileStarted(100)
	st.addSmallFileStarted(200)
	st.addSmallFileStarted(300)
	st.addSmallFileStarted(400)
	st.addSmallFileStarted(500)

	assert.Equal(
		t,
		"small_file_transfer_complete",
		st.addSmallFileResult(common.ETransferStatus.Success(), 100),
	)
	assert.Equal(
		t,
		"small_file_transfer_failed",
		st.addSmallFileResult(common.ETransferStatus.Failed(), 200),
	)
	assert.Equal(
		t,
		"small_file_transfer_skipped",
		st.addSmallFileResult(common.ETransferStatus.SkippedEntityAlreadyExists(), 300),
	)
	assert.Equal(
		t,
		"small_file_transfer_canceled",
		st.addSmallFileResult(common.ETransferStatus.Cancelled(), 400),
	)

	snapshot := st.progressSnapshot()
	assert.EqualValues(t, 5, snapshot.smallFilesStarted)
	assert.EqualValues(t, 1, snapshot.smallFilesCompleted)
	assert.EqualValues(t, 1, snapshot.smallFilesFailed)
	assert.EqualValues(t, 1, snapshot.smallFilesSkipped)
	assert.EqualValues(t, 1, snapshot.smallFilesCanceled)
	assert.EqualValues(t, 1500, snapshot.smallFileBytesStarted)
	assert.EqualValues(t, 100, snapshot.smallFileBytesCompleted)
	assert.EqualValues(t, 200, snapshot.smallFileBytesFailed)
	assert.EqualValues(t, 300, snapshot.smallFileBytesSkipped)
	assert.EqualValues(t, 400, snapshot.smallFileBytesCanceled)
	assert.Contains(t, snapshot.fields(dedupeActEnforce), "smallFilesInProgress=1")
	assert.Contains(t, snapshot.fields(dedupeActEnforce), "smallFileBytesInProgress=500")
}

func TestDedupeProgressSmallFileStartMarkerIsExactlyOnce(t *testing.T) {
	jptm := &jobPartTransferMgr{}

	assert.False(t, jptm.smallFileProgressStarted())
	assert.True(t, jptm.markSmallFileProgressStarted())
	assert.True(t, jptm.smallFileProgressStarted())
	assert.False(t, jptm.markSmallFileProgressStarted())
}

func TestDedupeProgressSmallFileCancellationNormalizesStatus(t *testing.T) {
	assert.Equal(
		t,
		common.ETransferStatus.Cancelled(),
		normalizeSmallFileResultStatus(common.ETransferStatus.Started(), true),
	)
	assert.Equal(
		t,
		common.ETransferStatus.Started(),
		normalizeSmallFileResultStatus(common.ETransferStatus.Started(), false),
	)
	assert.Equal(
		t,
		common.ETransferStatus.Success(),
		normalizeSmallFileResultStatus(common.ETransferStatus.Success(), true),
	)
}

func TestDedupeJobPartCompletionTracker(t *testing.T) {
	tracker := make(dedupeJobPartCompletionTracker)
	part0 := PartNumber(0)
	part1 := PartNumber(1)

	assert.False(t, tracker.record(nil))
	assert.True(t, tracker.record(&part0))
	assert.False(t, tracker.record(&part0))
	assert.False(t, tracker.allKnownPartsReported(2))
	assert.True(t, tracker.record(&part1))
	assert.True(t, tracker.allKnownPartsReported(2))

	tracker.reset()
	assert.False(t, tracker.allKnownPartsReported(2))
}

func TestDedupePausedRunResetsOnlyAfterAllPartsReport(t *testing.T) {
	assert.True(
		t,
		shouldResetDedupeRunProgress(common.EJobStatus.Paused(), true),
	)
	assert.False(
		t,
		shouldResetDedupeRunProgress(common.EJobStatus.Paused(), false),
	)
	assert.False(
		t,
		shouldResetDedupeRunProgress(common.EJobStatus.Completed(), true),
	)
}

func TestDedupeProgressMessageSanitizesSASAndIsExtractable(t *testing.T) {
	a := assert.New(t)
	jobID := common.NewJobID()
	info := &TransferInfo{
		JobID:       jobID,
		DstFilePath: "folder/file.bin",
		Destination: "https://acct.blob.core.windows.net/c/file.bin?sv=2026&sig=CURRENT_SECRET",
	}
	targetURI := sanitizedDestForDedupe(
		"https://acct.blob.core.windows.net/c/previous.bin?sv=2026&sig=TARGET_SECRET")
	message := dedupeProgressMessage(
		"target_reuse",
		dedupeActEnforce,
		info,
		"blockIndex=3 sourceOffset=4096 blockBytes=1024 targetURI="+fmt.Sprintf("%q", targetURI)+
			" targetOffset=8192 targetLength=1024",
		dedupeProgressSnapshot{
			referencedBlocks:   1,
			referencedBytes:    1024,
			sourceStagedBlocks: 1,
			sourceStagedBytes:  1024,
			filesStarted:       2,
			filesCommitted:     1,
		})

	a.True(strings.HasPrefix(message, dedupeProgressPrefix+" "))
	a.Contains(message, "event=target_reuse")
	a.Contains(message, "targetURI=\"https://acct.blob.core.windows.net/c/previous.bin\"")
	a.Contains(message, "wanSavingsPercent=50.0")
	a.NotContains(message, "CURRENT_SECRET")
	a.NotContains(message, "TARGET_SECRET")
	a.NotContains(message, "sig=")

	root := t.TempDir()
	sink := newDedupeProgressFileSink(func() string { return root })
	a.NoError(sink.write(jobID, message))

	logPath, err := sink.logPath(jobID)
	a.NoError(err)
	a.Equal(filepath.Join(root, dedupeLogDirectoryName, jobID.String()+".log"), logPath)
	logBytes, err := os.ReadFile(logPath)
	a.NoError(err)
	a.Equal(message+"\n", string(logBytes))

	a.NoError(sink.write(jobID, dedupeProgressPrefix+" event=test sig=SHOULD_NOT_LEAK"))
	logBytes, err = os.ReadFile(logPath)
	a.NoError(err)
	a.NotContains(string(logBytes), "SHOULD_NOT_LEAK")
	a.Contains(string(logBytes), "sig=-REDACTED-")
	a.NoError(sink.close(jobID))

	queue := make(chan dedupeProgressEntry, 1)
	a.True(tryEnqueueDedupeProgress(queue, dedupeProgressEntry{jobID: jobID, message: "first"}))
	a.False(tryEnqueueDedupeProgress(queue, dedupeProgressEntry{jobID: jobID, message: "second"}))
}

func TestBlockHasHashes(t *testing.T) {
	a := assert.New(t)

	a.True(blockHasHashes(hashedBlock("alpha", 0, 10)))
	a.True(blockHasHashes(PlannedBlock{HasHashes: true})) // valid hashes may contain all zero bytes
	a.False(blockHasHashes(PlannedBlock{CRC64: 1}))
	a.False(blockHasHashes(PlannedBlock{SHA256: [32]byte{0: 1}}))
	a.False(blockHasHashes(PlannedBlock{Offset: 0, Size: 100})) // no hashes -> not eligible
}

func TestMeasureAndRecord_SkipsBlocksWithoutHashes(t *testing.T) {
	a := assert.New(t)
	tbl := common.NewDedupeHashTable()

	blocks := []PlannedBlock{
		{Offset: 0, Size: 100},   // no hashes
		{Offset: 100, Size: 200}, // no hashes
	}
	hashed, hits, bytes := measureAndRecord(tbl, common.NewJobID(), "https://acct.blob.core.windows.net/c/b", blocks)

	a.EqualValues(0, hashed)
	a.EqualValues(0, hits)
	a.EqualValues(0, bytes)
	a.Equal(0, tbl.Len())
}

func TestMeasureAndRecord_DistinctBlocksProduceNoHits(t *testing.T) {
	a := assert.New(t)
	tbl := common.NewDedupeHashTable()

	blocks := []PlannedBlock{
		hashedBlock("a", 0, 10),
		hashedBlock("b", 10, 20),
		hashedBlock("c", 30, 30),
	}
	hashed, hits, bytes := measureAndRecord(tbl, common.NewJobID(), "uri", blocks)

	a.EqualValues(3, hashed)
	a.EqualValues(0, hits)
	a.EqualValues(0, bytes)
	a.Equal(3, tbl.Len())
}

func TestMeasureAndRecord_IntraBlobDuplicate(t *testing.T) {
	a := assert.New(t)
	tbl := common.NewDedupeHashTable()

	// The third block repeats the content of the first -> one would-be hit of size 10.
	blocks := []PlannedBlock{
		hashedBlock("dup", 0, 10),
		hashedBlock("other", 10, 20),
		hashedBlock("dup", 30, 10),
	}
	hashed, hits, bytes := measureAndRecord(tbl, common.NewJobID(), "uri", blocks)

	a.EqualValues(3, hashed)
	a.EqualValues(1, hits)
	a.EqualValues(10, bytes)
	a.Equal(3, tbl.Len()) // every target occurrence is preserved, including the duplicate range
}

func TestMeasureAndRecord_CrossBlobAccumulation(t *testing.T) {
	a := assert.New(t)
	tbl := common.NewDedupeHashTable()
	job := common.NewJobID()

	// First blob: two distinct blocks, no hits.
	h1, hit1, b1 := measureAndRecord(tbl, job, "uri-blob1", []PlannedBlock{
		hashedBlock("shared", 0, 50),
		hashedBlock("unique1", 50, 25),
	})
	a.EqualValues(2, h1)
	a.EqualValues(0, hit1)
	a.EqualValues(0, b1)

	// Second blob: re-uses "shared" content from the first blob -> one cross-blob would-be hit.
	h2, hit2, b2 := measureAndRecord(tbl, job, "uri-blob2", []PlannedBlock{
		hashedBlock("shared", 0, 50),
		hashedBlock("unique2", 50, 25),
	})
	a.EqualValues(2, h2)
	a.EqualValues(1, hit2)
	a.EqualValues(50, b2)

	a.Equal(4, tbl.Len()) // shared is preserved at both target occurrences
}

func TestMeasureAndRecord_PopulatesTargetRange(t *testing.T) {
	a := assert.New(t)
	tbl := common.NewDedupeHashTable()

	// A recorded block must carry the target sub-range so Phase 2 can later stage it
	// from [TargetOffset, TargetOffset+TargetLength) of the already-migrated dest blob.
	b := hashedBlock("x", 4096, 1000)
	measureAndRecord(tbl, common.NewJobID(), "https://acct.blob.core.windows.net/c/b", []PlannedBlock{b})

	got, ok := tbl.Lookup(b.CRC64, b.SHA256)
	a.True(ok)
	a.Equal("https://acct.blob.core.windows.net/c/b", got.TargetURI)
	a.EqualValues(4096, got.TargetOffset)
	a.EqualValues(1000, got.TargetLength)
}

func TestDestinationWithSASForDedupe_MergesDestinationSAS(t *testing.T) {
	a := assert.New(t)

	got := destinationWithSASForDedupe("https://acct.blob.core.windows.net/c/b.bin", "?sv=2021&sig=SECRET")
	u, err := url.Parse(got)
	a.NoError(err)
	a.Equal("2021", u.Query().Get("sv"))
	a.Equal("SECRET", u.Query().Get("sig"))

	got = destinationWithSASForDedupe("https://acct.blob.core.windows.net/c/b.bin?existing=true", "sv=2021&sig=SECRET")
	u, err = url.Parse(got)
	a.NoError(err)
	a.Equal("true", u.Query().Get("existing"))
	a.Equal("2021", u.Query().Get("sv"))
	a.Equal("SECRET", u.Query().Get("sig"))

	alreadyAuthenticated := "https://acct.blob.core.windows.net/c/b.bin?sv=2021&sig=SECRET"
	u, err = url.Parse(destinationWithSASForDedupe(alreadyAuthenticated, "sv=2021&sig=SECRET"))
	a.NoError(err)
	a.Len(u.Query()["sig"], 1)

	u, err = url.Parse(destinationWithSASForDedupe(
		"https://acct.blob.core.windows.net/c/b.bin?sv=old&sig=OLD",
		"sv=2021&sig=NEW"))
	a.NoError(err)
	a.Equal("2021", u.Query().Get("sv"))
	a.Equal("NEW", u.Query().Get("sig"))
	a.Len(u.Query()["sig"], 1)

	plain := "https://acct.blob.core.windows.net/c/b.bin"
	a.Equal(plain, destinationWithSASForDedupe(plain, ""))
}

func TestSanitizedDestForDedupe_StripsSAS(t *testing.T) {
	a := assert.New(t)

	got := sanitizedDestForDedupe("https://acct.blob.core.windows.net/c/b.bin?sv=2021&sig=SECRET")
	a.Equal("https://acct.blob.core.windows.net/c/b.bin", got)

	// No query string is left unchanged.
	plain := "https://acct.blob.core.windows.net/c/b.bin"
	a.Equal(plain, sanitizedDestForDedupe(plain))

	// Even malformed URLs must not leak their query or fragment to progress output.
	a.Equal("://malformed", sanitizedDestForDedupe("://malformed?sig=SECRET#fragment"))
}

func TestDedupePercent(t *testing.T) {
	a := assert.New(t)

	a.EqualValues(0, dedupePercent(0, 0))
	a.EqualValues(0, dedupePercent(0, 10))
	a.EqualValues(50, dedupePercent(5, 10))
	a.EqualValues(100, dedupePercent(10, 10))
	a.EqualValues(100, dedupePercent(15, 10))
}

func TestDedupeStateForJob_IsolatesAndClears(t *testing.T) {
	a := assert.New(t)

	jobA := common.NewJobID()
	jobB := common.NewJobID()

	stA := dedupeStateForJob(jobA)
	stB := dedupeStateForJob(jobB)
	a.NotSame(stA, stB)                  // distinct jobs get distinct tables
	a.Same(stA, dedupeStateForJob(jobA)) // same job returns the same state

	measureAndRecord(stA.table, jobA, "uri", []PlannedBlock{hashedBlock("x", 0, 10)})
	a.Equal(1, stA.table.Len())

	clearDedupeStateForJob(jobA)
	clearDedupeStateForJob(jobB)

	// A fresh state is created after clearing, with an empty table.
	a.Equal(0, dedupeStateForJob(jobA).table.Len())
	clearDedupeStateForJob(jobA)
}

func TestBuildSourceBlockHashIndex(t *testing.T) {
	a := assert.New(t)

	plan := &SourceGridPlan{Blocks: []PlannedBlock{
		hashedBlock("a", 0, 100),
		hashedBlock("b", 100, 200),
		{Offset: 300, Size: 50}, // no hashes -> excluded from the index
	}}
	idx := buildSourceBlockHashIndex(plan)

	a.Len(idx, 2)
	_, ok := idx[srcBlockKey{offset: 0, size: 100}]
	a.True(ok)
	_, ok = idx[srcBlockKey{offset: 100, size: 200}]
	a.True(ok)
	_, ok = idx[srcBlockKey{offset: 300, size: 50}]
	a.False(ok)
}

func TestDecideStaging_NoHashForChunk(t *testing.T) {
	a := assert.New(t)

	idx := buildSourceBlockHashIndex(&SourceGridPlan{Blocks: []PlannedBlock{hashedBlock("a", 0, 100)}})
	committed := common.NewDedupeHashTable()

	// A chunk whose (offset,size) matches no source block has no known hash -> stage from source.
	_, reference := decideStaging(idx, committed, 0, 64, "https://acct.blob.core.windows.net/c/current")
	a.False(reference)
}

func TestDecideStaging_HashButNotYetCommitted(t *testing.T) {
	a := assert.New(t)

	b := hashedBlock("a", 0, 100)
	idx := buildSourceBlockHashIndex(&SourceGridPlan{Blocks: []PlannedBlock{b}})
	committed := common.NewDedupeHashTable() // nothing migrated yet

	_, reference := decideStaging(idx, committed, 0, 100, "https://acct.blob.core.windows.net/c/current")
	a.False(reference) // hash known, but content not present at the destination -> stage from source
}

func TestDecideStaging_Hit(t *testing.T) {
	a := assert.New(t)

	b := hashedBlock("a", 0, 100)
	idx := buildSourceBlockHashIndex(&SourceGridPlan{Blocks: []PlannedBlock{b}})

	committed := common.NewDedupeHashTable()
	committed.Insert(common.BlockEntry{
		CRC64:        b.CRC64,
		SHA256:       b.SHA256,
		TargetURI:    "https://acct.blob.core.windows.net/c/already-migrated",
		TargetOffset: 0,
		TargetLength: 100,
		ETag:         azcore.ETag("etag-1"),
	})

	target, reference := decideStaging(idx, committed, 0, 100, "https://acct.blob.core.windows.net/c/current")
	a.True(reference)
	a.Equal("https://acct.blob.core.windows.net/c/already-migrated", target.TargetURI)
	a.EqualValues(0, target.TargetOffset)
	a.EqualValues(100, target.TargetLength)
	a.Equal(azcore.ETag("etag-1"), target.ETag)
}

func TestDecideStaging_RejectsUnsafeTarget(t *testing.T) {
	a := assert.New(t)

	b := hashedBlock("a", 0, 100)
	idx := buildSourceBlockHashIndex(&SourceGridPlan{Blocks: []PlannedBlock{b}})

	for _, entry := range []common.BlockEntry{
		{
			CRC64: b.CRC64, SHA256: b.SHA256,
			TargetURI:    "https://acct.blob.core.windows.net/c/missing-etag",
			TargetLength: 100,
		},
		{
			CRC64: b.CRC64, SHA256: b.SHA256,
			TargetURI:    "https://acct.blob.core.windows.net/c/wrong-size",
			TargetLength: 50, ETag: azcore.ETag("etag-1"),
		},
	} {
		committed := common.NewDedupeHashTable()
		committed.Insert(entry)
		_, reference := decideStaging(idx, committed, 0, 100, "https://acct.blob.core.windows.net/c/current")
		a.False(reference)
	}
}

func TestDecideStaging_SameTargetIsNotReusable(t *testing.T) {
	a := assert.New(t)

	b := hashedBlock("a", 0, 100)
	idx := buildSourceBlockHashIndex(&SourceGridPlan{Blocks: []PlannedBlock{b}})

	committed := common.NewDedupeHashTable()
	committed.Insert(common.BlockEntry{
		CRC64:        b.CRC64,
		SHA256:       b.SHA256,
		TargetURI:    "https://acct.blob.core.windows.net/c/current",
		TargetOffset: 0,
		TargetLength: 100,
		ETag:         azcore.ETag("etag-1"),
	})

	_, reference := decideStaging(idx, committed, 0, 100, "https://acct.blob.core.windows.net/c/current?sig=secret")
	a.False(reference)
}

func TestDecideStaging_SkipsSameTargetAndUsesAnotherOccurrence(t *testing.T) {
	a := assert.New(t)

	b := hashedBlock("a", 0, 100)
	idx := buildSourceBlockHashIndex(&SourceGridPlan{Blocks: []PlannedBlock{b}})
	committed := common.NewDedupeHashTable()
	committed.Insert(common.BlockEntry{
		CRC64:        b.CRC64,
		SHA256:       b.SHA256,
		HasSHA256:    true,
		TargetURI:    "https://acct.blob.core.windows.net/c/current",
		TargetOffset: 0,
		TargetLength: 100,
		ETag:         azcore.ETag("etag-current"),
	})
	committed.Insert(common.BlockEntry{
		CRC64:        b.CRC64,
		SHA256:       b.SHA256,
		HasSHA256:    true,
		TargetURI:    "https://acct.blob.core.windows.net/c/previous",
		TargetOffset: 200,
		TargetLength: 100,
		ETag:         azcore.ETag("etag-previous"),
	})

	target, reference := decideStaging(
		idx,
		committed,
		0,
		100,
		"https://acct.blob.core.windows.net/c/current?sig=secret",
	)
	a.True(reference)
	a.Equal("https://acct.blob.core.windows.net/c/previous", target.TargetURI)
	a.EqualValues(200, target.TargetOffset)
}

func TestHasDedupeSHAMismatchRequiresComparableTargetSHA(t *testing.T) {
	source := hashedBlock("source", 0, 100)
	idx := buildSourceBlockHashIndex(&SourceGridPlan{Blocks: []PlannedBlock{source}})
	committed := common.NewDedupeHashTable()
	targetURI := "https://acct.blob.core.windows.net/c/previous"
	entry := common.BlockEntry{
		CRC64:        source.CRC64,
		TargetURI:    targetURI,
		TargetOffset: 0,
		TargetLength: 100,
		ETag:         azcore.ETag("etag"),
	}
	committed.Insert(entry)

	assert.False(t, hasDedupeSHAMismatch(
		idx,
		committed,
		0,
		100,
		"https://acct.blob.core.windows.net/c/current",
	))

	mismatch := sha256.Sum256([]byte("different"))
	assert.True(t, committed.SetSHA256ForCRC64(
		source.CRC64,
		targetURI,
		0,
		100,
		azcore.ETag("etag"),
		mismatch,
	))
	assert.True(t, hasDedupeSHAMismatch(
		idx,
		committed,
		0,
		100,
		"https://acct.blob.core.windows.net/c/current",
	))

	committed.RemoveTargetEpoch(targetURI, azcore.ETag("etag"))
	assert.False(t, hasDedupeSHAMismatch(
		idx,
		committed,
		0,
		100,
		"https://acct.blob.core.windows.net/c/current",
	))
}
