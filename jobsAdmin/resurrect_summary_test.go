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

package jobsAdmin

import (
	"fmt"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/stretchr/testify/assert"
)

type planOnlyPartMgr struct {
	ste.IJobPartMgr
	plan *ste.JobPartPlanHeader
}

func (p planOnlyPartMgr) Plan() *ste.JobPartPlanHeader { return p.plan }

// planOnlyJobMgr serves what resurrectJobSummary reads from a job loaded only from plan files.
type planOnlyJobMgr struct {
	ste.IJobMgr
	jobID common.JobID
	part  planOnlyPartMgr
}

func (j planOnlyJobMgr) JobID() common.JobID { return j.jobID }
func (j planOnlyJobMgr) JobPartMgr(ste.PartNumber) (ste.IJobPartMgr, bool) {
	return j.part, true
}
func (j planOnlyJobMgr) IterateJobParts(_ bool, f func(common.PartNumber, ste.IJobPartMgr)) {
	f(0, j.part)
}
func (j planOnlyJobMgr) SuccessfulBytesInActiveFiles() uint64 { return 0 }
func (j planOnlyJobMgr) AllTransfersScheduled() bool          { return true }
func (j planOnlyJobMgr) ActiveConnections() int64             { return 0 }
func (j planOnlyJobMgr) GetPerfInfo() ([]string, common.PerfConstraint) {
	return nil, common.EPerfConstraint.Unknown()
}
func (j planOnlyJobMgr) PipelineNetworkStats() *ste.PipelineNetworkStats { return nil }
func (j planOnlyJobMgr) TransferDirection() common.TransferDirection {
	return common.ETransferDirection.Upload()
}

type plannedTransfer struct {
	status     common.TransferStatus
	errorCode  int32
	entityType common.EntityType
}

func resurrectPlannedSummary(t *testing.T, transfers []plannedTransfer) common.ListJobSummaryResponse {
	t.Helper()
	previousPlanFolder := common.AzcopyJobPlanFolder
	common.AzcopyJobPlanFolder = t.TempDir()
	t.Cleanup(func() { common.AzcopyJobPlanFolder = previousPlanFolder })

	pacer := ste.NewTokenBucketPacer(0, 0)
	previousAdmin := JobsAdmin
	JobsAdmin = &jobsAdmin{pacer: pacer}
	t.Cleanup(func() {
		JobsAdmin = previousAdmin
		_ = pacer.Close()
	})

	jobID := common.NewJobID()
	order := common.CopyJobPartOrderRequest{
		JobID:           jobID,
		IsFinalPart:     true,
		FromTo:          common.EFromTo.LocalBlob(),
		Fpo:             common.EFolderPropertiesOption.AllFolders(),
		SourceRoot:      common.ResourceString{Value: "/source"},
		DestinationRoot: common.ResourceString{Value: "https://account.blob.core.windows.net/container"},
	}
	for i, transfer := range transfers {
		name := fmt.Sprintf("/file%d.txt", i)
		order.Transfers.List = append(order.Transfers.List,
			common.CopyTransfer{Source: name, Destination: name, EntityType: transfer.entityType, SourceSize: 10})
	}
	planFile := ste.JobPartPlanFileName(fmt.Sprintf(ste.JobPartPlanFileNameFormat, jobID.String(), 0, ste.DataSchemaVersion))
	planFile.Create(order)
	mmf := planFile.Map()
	t.Cleanup(mmf.Unmap)
	plan := mmf.Plan()
	for i, transfer := range transfers {
		plan.Transfer(uint32(i)).SetTransferStatus(transfer.status, true)
		plan.Transfer(uint32(i)).SetErrorCode(transfer.errorCode, true)
	}

	return resurrectJobSummary(planOnlyJobMgr{jobID: jobID, part: planOnlyPartMgr{plan: plan}})
}

func TestResurrectJobSummaryCountsFailedTransferErrorCodes(t *testing.T) {
	summary := resurrectPlannedSummary(t, []plannedTransfer{
		{status: common.ETransferStatus.Success()},
		{status: common.ETransferStatus.Failed(), errorCode: 404},
		{status: common.ETransferStatus.Failed(), errorCode: 404},
		{status: common.ETransferStatus.Failed(), errorCode: 403},
		{status: common.ETransferStatus.BlobTierFailure()},
		{status: common.ETransferStatus.SkippedEntityAlreadyExists()},
	})

	assert.EqualValues(t, 4, summary.TransfersFailed)
	assert.Equal(t, map[int32]uint32{0: 1, 403: 1, 404: 2}, summary.FailedTransferErrorCodeCounts)
}

func TestResurrectJobSummaryWithoutFailuresHasNoErrorCodes(t *testing.T) {
	summary := resurrectPlannedSummary(t, []plannedTransfer{
		{status: common.ETransferStatus.Success()},
		{status: common.ETransferStatus.SkippedEntityAlreadyExists()},
	})

	assert.Zero(t, summary.TransfersFailed)
	assert.Nil(t, summary.FailedTransferErrorCodeCounts)
}

func TestResurrectJobSummaryCountsFolderOutcomes(t *testing.T) {
	folder := common.EEntityType.Folder()
	summary := resurrectPlannedSummary(t, []plannedTransfer{
		{status: common.ETransferStatus.Success()},
		{status: common.ETransferStatus.Success(), entityType: folder},
		{status: common.ETransferStatus.Failed(), entityType: folder},
		{status: common.ETransferStatus.SkippedEntityAlreadyExists(), entityType: folder},
	})

	assert.EqualValues(t, 4, summary.TotalTransfers)
	assert.EqualValues(t, 1, summary.FileTransfers)
	assert.EqualValues(t, 3, summary.FolderPropertyTransfers)
	assert.EqualValues(t, 2, summary.TransfersCompleted)
	assert.EqualValues(t, 1, summary.TransfersFailed)
	assert.EqualValues(t, 1, summary.TransfersSkipped)
	assert.EqualValues(t, 1, summary.FoldersCompleted)
	assert.EqualValues(t, 1, summary.FoldersFailed)
	assert.EqualValues(t, 1, summary.FoldersSkipped)
}
