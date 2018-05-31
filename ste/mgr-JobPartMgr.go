package ste

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

var _ IJobPartMgr = &jobPartMgr{}


type IJobPartMgr interface {
	Plan() *JobPartPlanHeader
	ScheduleTransfers(jobCtx context.Context, includeTransfer map[string]int, excludeTransfer map[string]int)
	StartJobXfer(jptm IJobPartTransferMgr)
	ReportTransferDone() uint32
	IsForceWriteTrue() bool
	ScheduleChunks(chunkFunc chunkFunc)
	AddToBytesDone(value int64) int64
	AddToBytesToTransfer(value int64) int64
	BytesDone() int64
	BytesToTransfer() int64
	RescheduleTransfer(jptm IJobPartTransferMgr)
	BlobTiers() (blockBlobTier common.BlockBlobTier, pageBlobTier common.PageBlobTier)
	//CancelJob()
	Close()
	common.ILogger
}

type serviceAPIVersionOverride struct {}

// ServiceAPIVersionOverride is a global variable in package ste which is a key to Service Api Version Value set in the every Job's context.
var ServiceAPIVersionOverride = serviceAPIVersionOverride{}

// defaultServiceApiVersion is the default value of service api version that is set as value to the ServiceAPIVersionOverride in every Job's context.
const defaultServiceApiVersion = "2017-04-17"

// NewVersionPolicy creates a factory that can override the service version
// set in the request header.
// If the context has key overwrite-current-version set to false, then x-ms-version in
// request is not overwritten else it will set x-ms-version to 207-04-17
func NewVersionPolicyFactory() pipeline.Factory {
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		return func(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
			// get the service api version value using the ServiceAPIVersionOverride set in the context.
			if value := ctx.Value(ServiceAPIVersionOverride); value != nil {
				request.Header.Set("x-ms-version", value.(string))
			}
			resp, err := next.Do(ctx, request)
			return resp, err
		}
	})
}

// NewBlobPipeline creates a Pipeline using the specified credentials and options.
func NewBlobPipeline(c azblob.Credential, o azblob.PipelineOptions, r XferRetryOptions, p *pacer) pipeline.Pipeline {
	if c == nil {
		panic("c can't be nil")
	}
	// Closest to API goes first; closest to the wire goes last
	f := []pipeline.Factory{
		azblob.NewTelemetryPolicyFactory(o.Telemetry),
		azblob.NewUniqueRequestIDPolicyFactory(),
		NewXferRetryPolicyFactory(r),
		c,
		pipeline.MethodFactoryMarker(), // indicates at what stage in the pipeline the method factory is invoked
		NewPacerPolicyFactory(p),
		NewVersionPolicyFactory(),
		azblob.NewRequestLogPolicyFactory(o.RequestLog),
	}

	return pipeline.NewPipeline(f, pipeline.Options{HTTPSender: nil, Log: o.Log})
}

// newFilePipeline creates a Pipeline using the specified credentials and options.
func newFilePipeline(c azfile.Credential, o azfile.PipelineOptions, r azfile.RetryOptions, p *pacer) pipeline.Pipeline {
	if c == nil {
		panic("c can't be nil")
	}
	// Closest to API goes first; closest to the wire goes last
	f := []pipeline.Factory{
		azfile.NewTelemetryPolicyFactory(o.Telemetry),
		azfile.NewUniqueRequestIDPolicyFactory(),
		azfile.NewRetryPolicyFactory(r),
		c,
		pipeline.MethodFactoryMarker(), // indicates at what stage in the pipeline the method factory is invoked
		NewPacerPolicyFactory(p),
		NewVersionPolicyFactory(),
		azfile.NewRequestLogPolicyFactory(o.RequestLog),
	}

	return pipeline.NewPipeline(f, pipeline.Options{HTTPSender: nil, Log: o.Log})
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// jobPartMgr represents the runtime information for a Job's Part
type jobPartMgr struct {
	// These fields represent the part's existence
	jobMgr   IJobMgr // Refers to this part's Job (for logging, cancelling, etc.)
	filename JobPartPlanFileName

	// When the part is schedule to run (inprogress), the below fields are used
	planMMF JobPartPlanMMF // This Job part plan's MMF

	// Additional data shared by all of this Job Part's transfers; initialized when this jobPartMgr is created
	blobHTTPHeaders azblob.BlobHTTPHeaders
	fileHTTPHeaders azfile.FileHTTPHeaders

	// Additional data shared by all of this Job Part's transfers; initialized when this jobPartMgr is created
	blockBlobTier common.BlockBlobTier

	// Additional data shared by all of this Job Part's transfers; initialized when this jobPartMgr is created
	pageBlobTier common.PageBlobTier

	blobMetadata azblob.Metadata
	fileMetadata azfile.Metadata

	preserveLastModifiedTime bool

	newJobXfer newJobXfer // Method used to start the transfer

	priority common.JobPriority

	pacer *pacer // Pacer used by chunks when uploading data

	pipeline pipeline.Pipeline // ordered list of Factory objects and an object implementing the HTTPSender interface

	// numberOfTransfersDone_doNotUse represents the number of transfer of JobPartOrder
	// which are either completed or failed
	// numberOfTransfersDone_doNotUse determines the final cancellation of JobPartOrder
	atomicTransfersDone uint32

	// bytes transferred defines the number of bytes of a job part that are uploaded / downloaded successfully or failed.
	// bytesDone is used to represent the progress of Job more precisely.
	bytesDone int64

	// totalBytesToTransfer defines the total number of bytes of JobPart that needs to uploaded or downloaded.
	// It is the sum of size of all the transfer of a job part.
	totalBytesToTransfer int64
}

func (jpm *jobPartMgr) Plan() *JobPartPlanHeader { return jpm.planMMF.Plan() }

// ScheduleTransfers schedules this job part's transfers. It is called when a new job part is ordered & is also called to resume a paused Job
func (jpm *jobPartMgr) ScheduleTransfers(jobCtx context.Context, includeTransfer map[string]int, excludeTransfer map[string]int) {
	jpm.atomicTransfersDone = 0      // Reset the # of transfers done back to 0
	jpm.planMMF = jpm.filename.Map() // Open the job part plan file & memory-map it in
	plan := jpm.planMMF.Plan()

	// *** Open the job part: process any job part plan-setting used by all transfers ***
	dstData := plan.DstBlobData

	jpm.blobHTTPHeaders = azblob.BlobHTTPHeaders{
		ContentType:     string(dstData.ContentType[:dstData.ContentTypeLength]),
		ContentEncoding: string(dstData.ContentEncoding[:dstData.ContentEncodingLength]),
	}

	jpm.blockBlobTier = dstData.BlockBlobTier
	jpm.pageBlobTier = dstData.PageBlobTier
	jpm.fileHTTPHeaders = azfile.FileHTTPHeaders{
		ContentType:     string(dstData.ContentType[:dstData.ContentTypeLength]),
		ContentEncoding: string(dstData.ContentEncoding[:dstData.ContentEncodingLength]),
	}
	// For this job part, split the metadata string apart and create an azblob.Metadata out of it
	metadataString := string(dstData.Metadata[:dstData.MetadataLength])
	jpm.blobMetadata = azblob.Metadata{}
	if len(metadataString) > 0 {
		for _, keyAndValue := range strings.Split(metadataString, ";") { // key/value pairs are separated by ';'
			kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
			jpm.blobMetadata[kv[0]] = kv[1]
		}
	}

	jpm.fileMetadata = azfile.Metadata{}
	if len(metadataString) > 0 {
		for _, keyAndValue := range strings.Split(metadataString, ";") { // key/value pairs are separated by ';'
			kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
			jpm.fileMetadata[kv[0]] = kv[1]
		}
	}

	jpm.preserveLastModifiedTime = plan.DstLocalData.PreserveLastModifiedTime

	jpm.newJobXfer = computeJobXfer(plan.FromTo)

	jpm.priority = plan.Priority

	// *** Schedule this job part's transfers ***
	for t := uint32(0); t < plan.NumTransfers; t++ {
		jppt := plan.Transfer(t)
		jpm.AddToBytesToTransfer(jppt.SourceSize)
		ts := jppt.TransferStatus()
		if ts == common.ETransferStatus.Success() {
			jpm.ReportTransferDone()                   // Don't schedule an already-completed/failed transfer
			jpm.AddToBytesDone(jppt.SourceSize) // Since transfer is not scheduled, hence increasing the
			continue
		}

		// If the list of transfer to be included is passed
		// then check current transfer exists in the list of included transfer
		// If it doesn't exists, skip the transfer
		if len(includeTransfer) > 0 {
			// Get the source string from the part plan header
			src, _ := plan.TransferSrcDstStrings(t)
			// If source doesn't exists, skip the transfer
			_, ok := includeTransfer[src]
			if !ok {
				jpm.ReportTransferDone()                   // Don't schedule transfer which is not mentioned to be included
				jpm.AddToBytesDone(jppt.SourceSize) // Since transfer is not scheduled, hence increasing the number of bytes done
				continue
			}
		}
		// If the list of transfer to be excluded is passed
		// then check the current transfer in the list of excluded transfer
		// If it exists, then skip the transfer
		if len(excludeTransfer) > 0 {
			// Get the source string from the part plan header
			src, _ := plan.TransferSrcDstStrings(t)
			// If the source exists in the list of excluded transfer
			// skip the transfer
			_, ok := excludeTransfer[src]
			if ok {
				jpm.ReportTransferDone()                   // Don't schedule transfer which is mentioned to be excluded
				jpm.AddToBytesDone(jppt.SourceSize) // Since transfer is not scheduled, hence increasing the number of bytes done
				continue
			}
		}

		// If the transfer was failed, then while rescheduling the transfer marking it Started.
		if ts == common.ETransferStatus.Failed() {
			jppt.SetTransferStatus(common.ETransferStatus.Started(), true)
		}

		// Each transfer gets its own context (so any chunk can cancel the whole transfer) based off the job's context
		transferCtx, transferCancel := context.WithCancel(jobCtx)
		jptm := &jobPartTransferMgr{
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
		JobsAdmin.(*jobsAdmin).ScheduleTransfer(jpm.priority, jptm)
	}
}

func (jpm *jobPartMgr) ScheduleChunks(chunkFunc chunkFunc) {
	JobsAdmin.ScheduleChunk(jpm.priority, chunkFunc)
}

func (jpm *jobPartMgr) RescheduleTransfer(jptm IJobPartTransferMgr) {
	JobsAdmin.(*jobsAdmin).ScheduleTransfer(jpm.priority, jptm)
}

func (jpm *jobPartMgr) createPipeline() {
	if jpm.pipeline == nil {
		fromTo := jpm.planMMF.Plan().FromTo

		switch fromTo {
		case common.EFromTo.BlobTrash():
			fallthrough
		case common.EFromTo.BlobLocal(): // download from Azure Blob to local file system
			fallthrough
		case common.EFromTo.LocalBlob(): // upload from local file system to Azure blob
			jpm.pipeline = NewBlobPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
				Log:       jpm.jobMgr.PipelineLogInfo(),
				Telemetry: azblob.TelemetryOptions{Value: "azcopy-V2"},
			},
				XferRetryOptions{
					Policy:        0,
					MaxTries:      UploadMaxTries,
					TryTimeout:    UploadTryTimeout,
					RetryDelay:    UploadRetryDelay,
					MaxRetryDelay: UploadMaxRetryDelay},
				jpm.pacer)
		case common.EFromTo.FileTrash():
			fallthrough
		case common.EFromTo.FileLocal(): // download from Azure File to local file system
			fallthrough
		case common.EFromTo.LocalFile(): // upload from local file system to Azure File
			jpm.pipeline = newFilePipeline(
				azfile.NewAnonymousCredential(),
				azfile.PipelineOptions{
					Log:       jpm.jobMgr.PipelineLogInfo(),
					Telemetry: azfile.TelemetryOptions{Value: "azcopy-V2"},
				},
				azfile.RetryOptions{
					Policy:        azfile.RetryPolicyExponential,
					MaxTries:      UploadMaxTries,
					TryTimeout:    UploadTryTimeout,
					RetryDelay:    UploadRetryDelay,
					MaxRetryDelay: UploadMaxRetryDelay,
				},
				jpm.pacer)
		default:
			panic(fmt.Errorf("Unrecognized FromTo: %q", fromTo.String()))
		}
	}
}

func (jpm *jobPartMgr) StartJobXfer(jptm IJobPartTransferMgr) {
	jpm.createPipeline()
	jpm.newJobXfer(jptm, jpm.pipeline, jpm.pacer)
}

func (jpm *jobPartMgr) AddToBytesDone(value int64) int64 {
	return atomic.AddInt64(&jpm.bytesDone, value)
}

func (jpm *jobPartMgr) AddToBytesToTransfer(value int64) int64 {
	return atomic.AddInt64(&jpm.totalBytesToTransfer, value)
}

func (jpm *jobPartMgr) BytesDone() int64 {
	return atomic.LoadInt64(&jpm.bytesDone)
}

func (jpm *jobPartMgr) BytesToTransfer() int64 {
	return atomic.LoadInt64(&jpm.totalBytesToTransfer)
}

func (jpm *jobPartMgr) IsForceWriteTrue() bool {
	return jpm.Plan().ForceWrite
}

func (jpm *jobPartMgr) blobDstData(dataFileToXfer common.MMF) (headers azblob.BlobHTTPHeaders, metadata azblob.Metadata) {
	if jpm.planMMF.Plan().DstBlobData.NoGuessMimeType {
		return jpm.blobHTTPHeaders, jpm.blobMetadata
	}
	return azblob.BlobHTTPHeaders{ContentType: http.DetectContentType(dataFileToXfer)}, jpm.blobMetadata
}

func (jpm *jobPartMgr) fileDstData(dataFileToXfer common.MMF) (headers azfile.FileHTTPHeaders, metadata azfile.Metadata) {
	if jpm.planMMF.Plan().DstBlobData.NoGuessMimeType {
		return jpm.fileHTTPHeaders, jpm.fileMetadata
	}
	return azfile.FileHTTPHeaders{ContentType: http.DetectContentType(dataFileToXfer)}, jpm.fileMetadata
}

func (jpm *jobPartMgr) BlobTiers() (blockBlobTier common.BlockBlobTier, pageBlobTier common.PageBlobTier) {
	return jpm.blockBlobTier, jpm.pageBlobTier
}

func (jpm *jobPartMgr) localDstData() (preserveLastModifiedTime bool) {
	dstData := &jpm.Plan().DstLocalData
	return dstData.PreserveLastModifiedTime
}

// Call Done when a transfer has completed its epilog; this method returns the number of transfers completed so far
func (jpm *jobPartMgr) ReportTransferDone() (transfersDone uint32) {
	transfersDone = atomic.AddUint32(&jpm.atomicTransfersDone, 1)
	if jpm.ShouldLog(pipeline.LogInfo) {
		plan := jpm.Plan()
		jpm.Log(pipeline.LogInfo, fmt.Sprintf("JobID=%v, Part#=%d, TransfersDone=%d of %d", plan.JobID, plan.PartNum, transfersDone, plan.NumTransfers))
	}
	if transfersDone == jpm.planMMF.Plan().NumTransfers {
		jpm.jobMgr.ReportJobPartDone()
	}
	return transfersDone
}

//func (jpm *jobPartMgr) CancelJob() { jpm.jobMgr.Cancel() }
func (jpm *jobPartMgr) Close() {
	jpm.planMMF.Unmap()
	// Clear other fields to all for GC
	jpm.blobHTTPHeaders = azblob.BlobHTTPHeaders{}
	jpm.blobMetadata = azblob.Metadata{}
	jpm.fileHTTPHeaders = azfile.FileHTTPHeaders{}
	jpm.fileMetadata = azfile.Metadata{}
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
