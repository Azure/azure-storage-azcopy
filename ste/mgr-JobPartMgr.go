package ste

import (
	"context"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"net/http"
	"strings"
	"sync/atomic"
)

var _ IJobPartMgr = &jobPartMgr{}

type IJobPartMgr interface {
	Plan() *JobPartPlanHeader
	ScheduleTransfers(jobCtx context.Context)

	ReportTransferDone() (lastTransfer bool, transfersCompleted uint32)
	RunPrologue(jptm IJobPartTransferMgr, pacer *pacer)
	//CancelJob()
	Close()
	common.ILogger
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// jobPartMgr represents the runtime information for a Job's Part
type jobPartMgr struct {
	// These fields represent the part's existence
	jobMgr   IJobMgr // Refers to this part's Job (for logging, cancelling, etc.)
	filename JobPartPlanFileName

	// When the part is schedule to run (inprogress), the below fields are used
	planMMF   JobPartPlanMMF        // This Job part plan's MMF

	// Additional data shared by all of this Job Part's transfers; initialized when this jobPartMgr is created
	blobHTTPHeaders          azblob.BlobHTTPHeaders
	blobMetadata             azblob.Metadata
	preserveLastModifiedTime bool

	// numberOfTransfersDone_doNotUse represents the number of transfer of JobPartOrder
	// which are either completed or failed
	// numberOfTransfersDone_doNotUse determines the final cancellation of JobPartOrder
	atomicTransfersDone uint32
}

func (jpm *jobPartMgr) Plan() *JobPartPlanHeader { return jpm.planMMF.Plan() }

// ScheduleTransfers schedules this job part's transfers. It is called when a new job part is ordered & is also called to resume a paused Job
func (jpm *jobPartMgr) ScheduleTransfers(jobCtx context.Context) {
	jpm.atomicTransfersDone = 0      // Reset the # of transfers done back to 0
	jpm.planMMF = jpm.filename.Map() // Open the job part plan file & memory-map it in
	plan := jpm.planMMF.Plan()

	// *** Open the job part: process any job part plan-setting used by all transfers ***
	dstData := plan.DstBlobData

	jpm.blobHTTPHeaders = azblob.BlobHTTPHeaders{
		ContentType:     string(dstData.ContentType[:dstData.MetadataLength]),
		ContentEncoding: string(dstData.ContentType[:dstData.ContentTypeLength]),
	}

	// For this job part, split the metadata string apart and create an azblob.Metadata out of it
	metadataString := string(dstData.Metadata[:dstData.MetadataLength])
	jpm.blobMetadata = azblob.Metadata{}
	for _, keyAndValue := range strings.Split(metadataString, ";") { // key/value pairs are separated by ';'
		kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
		jpm.blobMetadata[kv[0]] = kv[1]
	}
	jpm.preserveLastModifiedTime = plan.DstLocalData.PreserveLastModifiedTime

	//TODO : calculate transfer factory func.
	// *** Schedule this job part's transfers ***
	for t := uint32(0); t < plan.NumTransfers; t++ {
		jppt := plan.Transfer(t)
		if ts := jppt.TransferStatus(); ts == common.ETransferStatus.Success() || ts == common.ETransferStatus.Failed() {
			jpm.ReportTransferDone() // Don't schedule an already-completed/failed transfer
			continue
		}

		// Each transfer gets its own context (so any chunk can cancel the whole transfer) based off the job's context
		transferCtx, transferCancel := context.WithCancel(jobCtx)
		jptm := &jobPartTransferMgr{
			newJobXfer:computeJobXfer(plan.FromTo),
			priority: plan.Priority,
			//TODO: pacer: ja.pacer,
			jobPartMgr:          jpm,
			jobPartPlanTransfer: jppt,
			transferIndex:       t,
			ctx:                 transferCtx,
			cancel:              transferCancel,
			//TODO: insert the factory func interface in jptm.
			// numChunks will be set by the transfer's prologue method
		}
		if jpm.ShouldLog(pipeline.LogInfo) {
			jpm.Log(pipeline.LogInfo, fmt.Sprintf("scheduling JobID=%v, Part#=%d, Transfer#=%d, priority=%v", plan.JobID, plan.PartNum, t, plan.Priority))
		}
		JobsAdmin.(*jobsAdmin).ScheduleTransfer(jptm)
	}
}

func (jpm *jobPartMgr) blobDstData(dataFileToXfer common.MMF) (headers azblob.BlobHTTPHeaders, metadata azblob.Metadata) {
	if jpm.planMMF.Plan().DstBlobData.NoGuessMimeType {
		return jpm.blobHTTPHeaders, jpm.blobMetadata
	}
	return azblob.BlobHTTPHeaders{ContentType: http.DetectContentType(dataFileToXfer)}, jpm.blobMetadata
}

func (jpm *jobPartMgr) RunPrologue(jptm IJobPartTransferMgr, pacer *pacer){
	jpm.jobMgr.RunPrologue(jptm, pacer)
}

func (jpm *jobPartMgr) localDstData() (preserveLastModifiedTime bool) {
	dstData := &jpm.Plan().DstLocalData
	return dstData.PreserveLastModifiedTime
}

// Call Done when a transfer has completed its epilog; this method returns the number of transfers completed so far
// TODO: Return true for last transfer?
// TODO : Should lastTransfer and transferDone be deleted.
func (jpm *jobPartMgr) ReportTransferDone() (lastTransfer bool, transfersDone uint32) {
	transfersDone = atomic.AddUint32(&jpm.atomicTransfersDone, 1)
	if jpm.ShouldLog(pipeline.LogInfo) {
		plan := jpm.Plan()
		jpm.Log(pipeline.LogInfo, fmt.Sprintf("JobID=%v, Part#=%d, TransfersDone=%d of %d", plan.JobID, plan.PartNum, transfersDone, plan.NumTransfers))
	}

	// TODO: If last transfer, notify Part?
	return transfersDone == jpm.planMMF.Plan().NumTransfers, transfersDone
}
//func (jpm *jobPartMgr) CancelJob() { jpm.jobMgr.Cancel() }
func (jpm *jobPartMgr) Close() {
	jpm.planMMF.Unmap()
	// Clear other fields to all for GC
	jpm.blobHTTPHeaders = azblob.BlobHTTPHeaders{}
	jpm.blobMetadata = azblob.Metadata{}
	jpm.preserveLastModifiedTime = false
	// TODO: Delete file?
	/*if err := os.Remove(jpm.planFile.Name()); err != nil {
		jpm.Panic(fmt.Errorf("error removing Job Part Plan file %s. Error=%v", jpm.planFile.Name(), err))
	}*/
}

func (jpm *jobPartMgr) ShouldLog(level pipeline.LogLevel) bool  { return jpm.jobMgr.ShouldLog(level) }
func (jpm *jobPartMgr) Log(level pipeline.LogLevel, msg string) { jpm.jobMgr.Log(level, msg) }
func (jpm *jobPartMgr) Panic(err error)                         { jpm.jobMgr.Panic(err) }

// TODO: Can we delete this method?
// numberOfTransfersDone returns the numberOfTransfersDone_doNotUse of JobPartPlanInfo
// instance in thread safe manner
//func (jpm *jobPartMgr) numberOfTransfersDone() uint32 {	return atomic.LoadUint32(&jpm.numberOfTransfersDone_doNotUse)}

// setNumberOfTransfersDone sets the number of transfers done to a specific value
// in a thread safe manner
//func (jppi *jobPartPlanInfo) setNumberOfTransfersDone(val uint32) {
//	atomic.StoreUint32(&jPartPlanInfo.numberOfTransfersDone_doNotUse, val)
//}
