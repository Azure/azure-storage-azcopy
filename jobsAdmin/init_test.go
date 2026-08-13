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

package jobsAdmin

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

type executeJobPartOrderTestAdmin struct {
	*jobsAdmin
	jobMgr *executeJobPartOrderTestJobMgr
}

func (a *executeJobPartOrderTestAdmin) NewJobPartPlanFileName(
	jobID common.JobID,
	partNumber common.PartNumber,
) ste.JobPartPlanFileName {
	return ste.JobPartPlanFileName(fmt.Sprintf(
		ste.JobPartPlanFileNameFormat,
		jobID.String(),
		partNumber,
		ste.DataSchemaVersion,
	))
}

func (a *executeJobPartOrderTestAdmin) JobMgrEnsureExists(
	common.JobID,
	common.LogLevel,
	string,
) ste.IJobMgr {
	return a.jobMgr
}

func (*executeJobPartOrderTestAdmin) RegisterStatsMonitorIfNotDone() {}

type executeJobPartOrderTestJobMgr struct {
	ste.IJobMgr
	totalFiles int64
	jobPart    *executeJobPartOrderTestJobPartMgr
}

func (*executeJobPartOrderTestJobMgr) SetInMemoryTransitJobState(ste.InMemoryTransitJobState) {}

func (jm *executeJobPartOrderTestJobMgr) AddTotalNumFilesProcessed(numFiles int64) {
	jm.totalFiles += numFiles
}

func (jm *executeJobPartOrderTestJobMgr) GetTotalNumFilesProcessed() int64 {
	return jm.totalFiles
}

func (jm *executeJobPartOrderTestJobMgr) AddJobPart(args *ste.AddJobPartArgs) ste.IJobPartMgr {
	jm.jobPart = &executeJobPartOrderTestJobPartMgr{
		sourceSAS:      args.SourceSAS,
		destinationSAS: args.DestinationSAS,
	}
	return jm.jobPart
}

func (*executeJobPartOrderTestJobMgr) SendJobPartCreatedMsg(ste.JobPartCreatedMsg) {}

type executeJobPartOrderTestJobPartMgr struct {
	ste.IJobPartMgr
	sourceSAS      string
	destinationSAS string
}

func (jpm *executeJobPartOrderTestJobPartMgr) SAS() (string, string) {
	return jpm.sourceSAS, jpm.destinationSAS
}

func TestExecuteNewCopyJobPartOrderPreservesRuntimeSASWithoutPersistingIt(t *testing.T) {
	const (
		sourceSAS      = "sp=rl&sig=source-runtime-sentinel"
		destinationSAS = "sp=rcwdl&sig=destination-runtime-sentinel"
	)

	originalJobsAdmin := JobsAdmin
	originalPlanFolder := common.AzcopyJobPlanFolder
	common.AzcopyJobPlanFolder = t.TempDir()
	t.Cleanup(func() {
		JobsAdmin = originalJobsAdmin
		common.AzcopyJobPlanFolder = originalPlanFolder
	})

	jobMgr := &executeJobPartOrderTestJobMgr{}
	testAdmin := &executeJobPartOrderTestAdmin{jobMgr: jobMgr}
	JobsAdmin = testAdmin

	order := common.CopyJobPartOrderRequest{
		JobID:   common.NewJobID(),
		PartNum: 0,
		FromTo:  common.EFromTo.BlobBlob(),
		Fpo:     common.EFolderPropertiesOption.NoFolders(),
		SourceRoot: common.ResourceString{
			Value: "https://source.blob.core.windows.net/container",
			SAS:   sourceSAS,
		},
		DestinationRoot: common.ResourceString{
			Value: "https://destination.blob.core.windows.net/container",
			SAS:   destinationSAS,
		},
	}

	response := ExecuteNewCopyJobPartOrder(order)
	if !response.JobStarted {
		t.Fatal("expected job part order to start")
	}
	if jobMgr.jobPart == nil {
		t.Fatal("expected AddJobPart to receive the job part")
	}

	actualSourceSAS, actualDestinationSAS := jobMgr.jobPart.SAS()
	if actualSourceSAS != sourceSAS {
		t.Fatalf("source SAS mismatch: got %q", actualSourceSAS)
	}
	if actualDestinationSAS != destinationSAS {
		t.Fatalf("destination SAS mismatch: got %q", actualDestinationSAS)
	}

	planFile := testAdmin.NewJobPartPlanFileName(order.JobID, order.PartNum)
	planBytes, err := os.ReadFile(filepath.Join(common.AzcopyJobPlanFolder, string(planFile)))
	if err != nil {
		t.Fatalf("reading generated plan file: %v", err)
	}
	for _, sentinel := range []string{sourceSAS, destinationSAS} {
		if bytes.Contains(planBytes, []byte(sentinel)) {
			t.Fatalf("generated plan file contains runtime SAS sentinel %q", sentinel)
		}
	}

	nonSASJobMgr := &executeJobPartOrderTestJobMgr{}
	testAdmin.jobMgr = nonSASJobMgr
	nonSASOrder := order
	nonSASOrder.JobID = common.NewJobID()
	nonSASOrder.SourceRoot.SAS = ""
	nonSASOrder.DestinationRoot.SAS = ""

	response = ExecuteNewCopyJobPartOrder(nonSASOrder)
	if !response.JobStarted {
		t.Fatal("expected non-SAS job part order to start")
	}
	actualSourceSAS, actualDestinationSAS = nonSASJobMgr.jobPart.SAS()
	if actualSourceSAS != "" || actualDestinationSAS != "" {
		t.Fatalf(
			"expected non-SAS job part to remain empty, got source %q destination %q",
			actualSourceSAS,
			actualDestinationSAS,
		)
	}
}
