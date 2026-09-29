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

type resurrectedPartMgr struct {
	ste.IJobPartMgr
	plan *ste.JobPartPlanHeader
}

func (p resurrectedPartMgr) Plan() *ste.JobPartPlanHeader { return p.plan }

// resurrectedJobMgr provides only what resurrectJobSummary reads from a job loaded from plan files.
type resurrectedJobMgr struct {
	ste.IJobMgr
	jobID common.JobID
	part  resurrectedPartMgr
}

func (j resurrectedJobMgr) JobID() common.JobID { return j.jobID }
func (j resurrectedJobMgr) JobPartMgr(ste.PartNumber) (ste.IJobPartMgr, bool) {
	return j.part, true
}
func (j resurrectedJobMgr) IterateJobParts(_ bool, f func(common.PartNumber, ste.IJobPartMgr)) {
	f(0, j.part)
}
func (j resurrectedJobMgr) SuccessfulBytesInActiveFiles() uint64 { return 0 }
func (j resurrectedJobMgr) AllTransfersScheduled() bool          { return true }
func (j resurrectedJobMgr) ActiveConnections() int64             { return 0 }
func (j resurrectedJobMgr) GetPerfInfo() ([]string, common.PerfConstraint) {
	return nil, common.EPerfConstraint.Unknown()
}
func (j resurrectedJobMgr) PipelineNetworkStats() *ste.PipelineNetworkStats { return nil }
func (j resurrectedJobMgr) TransferDirection() common.TransferDirection {
	return common.ETransferDirection.Upload()
}

func TestResurrectJobSummaryCountsFolderProperties(t *testing.T) {
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
	planFile := ste.JobPartPlanFileName(fmt.Sprintf(ste.JobPartPlanFileNameFormat, jobID.String(), 0, ste.DataSchemaVersion))
	planFile.Create(common.CopyJobPartOrderRequest{
		JobID:           jobID,
		IsFinalPart:     true,
		FromTo:          common.EFromTo.LocalBlob(),
		Fpo:             common.EFolderPropertiesOption.AllFolders(),
		SourceRoot:      common.ResourceString{Value: "/source"},
		DestinationRoot: common.ResourceString{Value: "https://account.blob.core.windows.net/container"},
		Transfers: common.Transfers{List: []common.CopyTransfer{
			{Source: "/file.txt", Destination: "/file.txt", EntityType: common.EEntityType.File(), SourceSize: 10},
			{Source: "/completed", Destination: "/completed", EntityType: common.EEntityType.Folder()},
			{Source: "/failed", Destination: "/failed", EntityType: common.EEntityType.Folder()},
			{Source: "/skipped", Destination: "/skipped", EntityType: common.EEntityType.Folder()},
		}},
	})
	mmf := planFile.Map()
	t.Cleanup(mmf.Unmap)
	plan := mmf.Plan()
	plan.Transfer(0).SetTransferStatus(common.ETransferStatus.Success(), true)
	plan.Transfer(1).SetTransferStatus(common.ETransferStatus.Success(), true)
	plan.Transfer(2).SetTransferStatus(common.ETransferStatus.Failed(), true)
	plan.Transfer(3).SetTransferStatus(common.ETransferStatus.SkippedEntityAlreadyExists(), true)

	summary := resurrectJobSummary(resurrectedJobMgr{jobID: jobID, part: resurrectedPartMgr{plan: plan}})

	assert.EqualValues(t, 4, summary.TotalTransfers)
	assert.EqualValues(t, 1, summary.FileTransfers)
	assert.EqualValues(t, 3, summary.FolderPropertyTransfers)
	assert.EqualValues(t, 2, summary.TransfersCompleted)
	assert.EqualValues(t, 1, summary.TransfersFailed)
	assert.EqualValues(t, 1, summary.TransfersSkipped)
	assert.EqualValues(t, 1, summary.FoldersCompleted, "completed folder properties")
	assert.EqualValues(t, 1, summary.FoldersFailed, "failed folder properties")
	assert.EqualValues(t, 1, summary.FoldersSkipped, "skipped folder properties")
}
