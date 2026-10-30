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
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestJobStatusManagerCountsFailedErrorCodesAcrossSummaries(t *testing.T) {
	a := assert.New(t)
	// Unbuffered xferDone makes each send wait until the status manager has received the message.
	jm := &jobMgr{jobID: common.NewJobID(), jstm: &jobStatusManager{
		respChan:        make(chan common.ListJobSummaryResponse),
		listReq:         make(chan struct{}),
		xferDone:        make(chan xferDoneMsg),
		xferDoneDrained: make(chan struct{}),
		statusMgrDone:   make(chan struct{}),
	}}
	go jm.handleStatusUpdateMessage()
	failed := func(code int32) xferDoneMsg {
		return xferDoneMsg{TransferStatus: common.ETransferStatus.Failed(), ErrorCode: code}
	}

	jm.SendXferDoneMsg(failed(403))
	first := jm.ListJobSummary()
	a.Equal(map[int32]uint32{403: 1}, first.FailedTransferErrorCodeCounts)
	first.FailedTransferErrorCodeCounts[403] = 99 // a caller's copy must not alias the live counts

	jm.SendXferDoneMsg(failed(404))
	jm.SendXferDoneMsg(failed(0))
	close(jm.jstm.xferDone)
	jm.waitToDrainXferDone()
	final := jm.ListJobSummary()
	a.EqualValues(3, final.TransfersFailed)
	a.Len(final.FailedTransfers, 2, "the list only holds failures since the previous summary")
	a.Equal(map[int32]uint32{403: 1, 404: 1, 0: 1}, final.FailedTransferErrorCodeCounts)

	// Resuming starts a new attempt, so its failures are counted from zero, like TransfersFailed.
	<-jm.jstm.statusMgrDone
	jm.ResetFailedTransfersCount()
	a.Nil(jm.ListJobSummary().FailedTransferErrorCodeCounts)
}
