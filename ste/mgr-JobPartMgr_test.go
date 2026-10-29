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

package ste

import (
	"math"
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"time"
)

func TestInferContentType(t *testing.T) {
	a := assert.New(t)
	// Arrange
	partMgr := jobPartMgr{}

	// the goal is to make sure content type detection at least works for static websites
	testCases := map[string]string{
		"/usr/foo/bla.txt":             "text/plain",
		"/usr/foo/bla.html":            "text/html",
		"/usr/foo/bla.css":             "text/css",
		"/usr/foo/bla.js":              "application/javascript",
		"/usr/foo/bla.json":            "application/json",
		"/usr/foo/bla.jpeg":            "image/jpeg",
		"/usr/foo/bla.png":             "image/png",
		"/usr/foo/bla.multiple.dot.js": "application/javascript",
		"/usr/foo/no/extension":        "application/octet-stream",
		"/usr/foo/bla.HTML":            "text/html",
	}

	// Action & Assert
	for testPath, expectedType := range testCases {
		contentType := partMgr.inferContentType(testPath, make([]byte, 5))

		// make sure the inferred type is correct
		// we use Contains to check because charset is also in contentType
		a.True(strings.Contains(contentType, expectedType))
	}
}

func runSummaryUpdates(t *testing.T, initial common.ListJobSummaryResponse, parts []JobPartCreatedMsg, transfers []xferDoneMsg) common.ListJobSummaryResponse {
	t.Helper()
	jm := &jobMgr{
		jobID: common.NewJobID(),
		jstm: &jobStatusManager{
			respChan:        make(chan common.ListJobSummaryResponse),
			listReq:         make(chan struct{}),
			partCreated:     make(chan JobPartCreatedMsg),
			xferDone:        make(chan xferDoneMsg),
			xferDoneDrained: make(chan struct{}),
			statusMgrDone:   make(chan struct{}),
		},
	}
	jm.ResurrectSummary(initial)
	go jm.handleStatusUpdateMessage()

	for _, part := range parts {
		jm.jstm.partCreated <- part
	}
	for _, transfer := range transfers {
		jm.SendXferDoneMsg(transfer)
	}
	close(jm.jstm.xferDone)
	select {
	case <-jm.jstm.xferDoneDrained:
	case <-time.After(5 * time.Second):
		t.Fatal("status manager did not drain transfer updates")
	}
	result := jm.ListJobSummary()
	select {
	case <-jm.jstm.statusMgrDone:
	case <-time.After(5 * time.Second):
		t.Fatal("status manager did not close")
	}
	return result
}

func TestJobSummaryPartCountsBeyondUint32(t *testing.T) {
	const initialCount = uint64(math.MaxUint32 - 1)
	initial := common.ListJobSummaryResponse{
		TotalTransfers:          initialCount,
		FileTransfers:           initialCount,
		FolderPropertyTransfers: initialCount,
		FilePropertyTransfers:   initialCount,
	}
	part := JobPartCreatedMsg{
		TotalTransfers:        4,
		FileTransfers:         2,
		FolderTransfer:        1,
		FilePropertyTransfers: 1,
		TotalBytesEnumerated:  100,
	}
	finalPart := part
	finalPart.IsFinalPart = true
	summary := runSummaryUpdates(t, initial, []JobPartCreatedMsg{part, finalPart}, nil)
	require.Equal(t, initialCount+8, summary.TotalTransfers)
	require.Equal(t, initialCount+4, summary.FileTransfers)
	require.Equal(t, initialCount+2, summary.FolderPropertyTransfers)
	require.Equal(t, initialCount+2, summary.FilePropertyTransfers)
	require.Equal(t, uint64(200), summary.TotalBytesEnumerated)
	require.Equal(t, uint64(200), summary.TotalBytesExpected)
	require.True(t, summary.CompleteJobOrdered)
}

func TestJobSummaryCompletionCountsBeyondUint32(t *testing.T) {
	for _, initialCount := range []uint64{0, math.MaxUint32 - 1, math.MaxUint32, math.MaxUint32 + 1} {
		t.Run(strconv.FormatUint(initialCount, 10), func(t *testing.T) {
			initial := common.ListJobSummaryResponse{
				TransfersCompleted: initialCount,
				FoldersCompleted:   initialCount,
				TransfersFailed:    initialCount,
				FoldersFailed:      initialCount,
				TransfersSkipped:   initialCount,
				FoldersSkipped:     initialCount,
			}
			var transfers []xferDoneMsg
			for _, status := range []common.TransferStatus{
				common.ETransferStatus.Success(),
				common.ETransferStatus.Failed(),
				common.ETransferStatus.TierAvailabilityCheckFailure(),
				common.ETransferStatus.BlobTierFailure(),
				common.ETransferStatus.SkippedEntityAlreadyExists(),
				common.ETransferStatus.SkippedBlobHasSnapshots(),
			} {
				transfers = append(transfers,
					xferDoneMsg{TransferStatus: status, TransferSize: 100},
					xferDoneMsg{TransferStatus: status, IsFolderProperties: true})
			}
			summary := runSummaryUpdates(t, initial, nil, transfers)
			require.Equal(t, initialCount+2, summary.TransfersCompleted)
			require.Equal(t, initialCount+1, summary.FoldersCompleted)
			require.Equal(t, initialCount+6, summary.TransfersFailed)
			require.Equal(t, initialCount+3, summary.FoldersFailed)
			require.Equal(t, initialCount+4, summary.TransfersSkipped)
			require.Equal(t, initialCount+2, summary.FoldersSkipped)
			require.Equal(t, uint64(100), summary.TotalBytesTransferred)
			require.Len(t, summary.FailedTransfers, 6)
			require.Len(t, summary.SkippedTransfers, 4)
		})
	}
}
