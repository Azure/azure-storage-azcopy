package ste

import (
	"context"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	azruntime "github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var _ IJobPartMgr = &jobPartMgr{}

// debug knob
var DebugSkipFiles = make(map[string]bool)

type IJobPartMgr interface {
	Plan() *JobPartPlanHeader
	ScheduleTransfers(jobCtx context.Context)
	StartJobXfer(jptm IJobPartTransferMgr)
	ReportTransferDone(status common.TransferStatus) uint32
	GetOverwriteOption() common.OverwriteOption
	GetForceIfReadOnly() bool
	AutoDecompress() bool
	ScheduleChunks(chunkFunc chunkFunc)
	RescheduleTransfer(jptm IJobPartTransferMgr)
	BlobTypeOverride() common.BlobType
	BlobTiers() (blockBlobTier common.BlockBlobTier, pageBlobTier common.PageBlobTier)
	ShouldPutMd5() bool
	DeleteDestinationFileIfNecessary() bool
	SAS() (string, string)
	// CancelJob()
	Close()
	// TODO: added for debugging purpose. remove later
	OccupyAConnection()
	// TODO: added for debugging purpose. remove later
	ReleaseAConnection()
	SlicePool() common.ByteSlicePooler
	CacheLimiter() common.CacheLimiter
	FileCountLimiter() common.CacheLimiter
	ExclusiveDestinationMap() *common.ExclusiveStringMap
	ChunkStatusLogger() common.ChunkStatusLogger
	common.ILogger

	// These functions return Container/fileshare clients.
	// They must be type asserted before use. In cases where they dont
	// make sense (say SrcServiceClient for upload) they are il
	SrcServiceClient() *common.ServiceClient
	DstServiceClient() *common.ServiceClient
	SourceIsOAuth() bool

	getOverwritePrompter() *overwritePrompter
	getFolderCreationTracker() FolderCreationTracker
	SecurityInfoPersistenceManager() *securityInfoPersistenceManager
	FolderDeletionManager() common.FolderDeletionManager
	CpkInfo() *blob.CPKInfo
	CpkScopeInfo() *blob.CPKScopeInfo
	IsSourceEncrypted() bool
	/* Status Manager Updates */
	SendXferDoneMsg(msg xferDoneMsg)
	PropertiesToTransfer() common.SetPropertiesFlags
	ResetFailedTransfersCount() // Resets number of failed transfers after a job is resumed
}

// NewAzcopyHTTPClient creates a new HTTP client.
// We must minimize use of this, and instead maximize reuse of the returned client object.
// Why? Because that makes our connection pooling more efficient, and prevents us exhausting the
// number of available network sockets on resource-constrained Linux systems. (E.g. when
// 'ulimit -Hn' is low).
func NewAzcopyHTTPClient(maxIdleConns int) *http.Client {
	const concurrentDialsPerCpu = 10 // exact value doesn't matter too much, but too low will be too slow, and too high will reduce the beneficial effect on thread count
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                  common.GlobalProxyLookup,
			MaxConnsPerHost:        concurrentDialsPerCpu * runtime.NumCPU(),
			MaxIdleConns:           0, // No limit
			MaxIdleConnsPerHost:    maxIdleConns,
			IdleConnTimeout:        180 * time.Second,
			TLSHandshakeTimeout:    10 * time.Second,
			ExpectContinueTimeout:  1 * time.Second,
			DisableKeepAlives:      false,
			DisableCompression:     true, // must disable the auto-decompression of gzipped files, and just download the gzipped version. See https://github.com/Azure/azure-storage-azcopy/issues/374
			MaxResponseHeaderBytes: 0,
			// ResponseHeaderTimeout:  time.Duration{},
			// ExpectContinueTimeout:  time.Duration{},
		},
	}
}

func NewClientOptions(retry policy.RetryOptions, telemetry policy.TelemetryOptions, transport policy.Transporter, log LogOptions, srcCred *common.ScopedToken, dstCred *common.ScopedAuthenticator) azcore.ClientOptions {
	// Pipeline will look like
	// [includeResponsePolicy, newAPIVersionPolicy (ignored), NewTelemetryPolicy, perCall, NewRetryPolicy, perRetry, NewLogPolicy, httpHeaderPolicy, bodyDownloadPolicy]
	perCallPolicies := []policy.Policy{azruntime.NewRequestIDPolicy(), NewVersionPolicy(), newFileUploadRangeFromURLFixPolicy()}
	// TODO : Default logging policy is not equivalent to old one. tracing HTTP request
	perRetryPolicies := []policy.Policy{newRetryNotificationPolicy(), newLogPolicy(log), newStatsPolicy()}
	if dstCred != nil {
		perCallPolicies = append(perRetryPolicies, NewDestReauthPolicy(dstCred))
	}
	if srcCred != nil {
		perRetryPolicies = append(perRetryPolicies, NewSourceAuthPolicy(srcCred))
	}
	retry.ShouldRetry = GetShouldRetry(&log)

	return azcore.ClientOptions{
		//APIVersion: ,
		//Cloud: ,
		//Logging: ,
		Retry:     retry,
		Telemetry: telemetry,
		//TracingProvider: ,
		Transport:        transport,
		PerCallPolicies:  perCallPolicies,
		PerRetryPolicies: perRetryPolicies,
	}
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Holds the status of transfers in this jptm
type jobPartProgressInfo struct {
	transfersCompleted int
	transfersSkipped   int
	transfersFailed    int
	completionChan     chan struct{}
}

// jobPartMgr represents the runtime information for a Job's Part
type jobPartMgr struct {
	// These fields represent the part's existence
	jobMgr          IJobMgr // Refers to this part's Job (for logging, cancelling, etc.)
	jobMgrInitState *jobMgrInitState
	filename        JobPartPlanFileName

	// sourceSAS defines the sas of the source of the Job. If the source is local Location, then sas is empty.
	// Since sas is not persisted in JobPartPlan file, it stripped from the source and stored in memory in JobPart Manager
	sourceSAS string
	// destinationSAS defines the sas of the destination of the Job. If the destination is local Location, then sas is empty.
	// Since sas is not persisted in JobPartPlan file, it stripped from the destination and stored in memory in JobPart Manager
	destinationSAS string

	// These fields hold the container/fileshare client of this jobPart,
	// whatever is appropriate for this scenario. Ex. For BlobFile, we
	// will have BlobService client in srcServiceClient and Fileservice in
	// dstServiceClient. For upload, srcService is nil, and likewise.
	srcServiceClient *common.ServiceClient
	dstServiceClient *common.ServiceClient

	srcIsOAuth bool // true if source is authenticated via oauth
	// When the part is schedule to run (inprogress), the below fields are used
	planMMF *JobPartPlanMMF // This Job part plan's MMF

	// Additional data shared by all of this Job Part's transfers; initialized when this jobPartMgr is created
	httpHeaders common.ResourceHTTPHeaders

	// Additional data shared by all of this Job Part's transfers; initialized when this jobPartMgr is created
	blockBlobTier common.BlockBlobTier

	// Additional data shared by all of this Job Part's transfers; initialized when this jobPartMgr is created
	pageBlobTier common.PageBlobTier

	// Additional data shared by all of this Job Part's transfers; initialized when this jobPartMgr is created
	putMd5 bool

	deleteDestinationFileIfNecessary bool

	metadata common.Metadata

	blobTags common.BlobTags

	blobTypeOverride common.BlobType // User specified blob type

	preserveLastModifiedTime bool

	newJobXfer newJobXfer // Method used to start the transfer

	priority common.JobPriority

	pacer pacer // Pacer is used to cap throughput

	slicePool common.ByteSlicePooler

	cacheLimiter            common.CacheLimiter
	fileCountLimiter        common.CacheLimiter
	exclusiveDestinationMap *common.ExclusiveStringMap

	// numberOfTransfersDone_doNotUse represents the number of transfer of JobPartOrder
	// which are either completed or failed
	// numberOfTransfersDone_doNotUse determines the final cancellation of JobPartOrder
	atomicTransfersDone      uint32
	atomicTransfersCompleted uint32
	atomicTransfersFailed    uint32
	atomicTransfersSkipped   uint32

	cpkOptions common.CpkOptions

	closeOnCompletion chan struct{}

	SetPropertiesFlags common.SetPropertiesFlags

	RehydratePriority common.RehydratePriorityType
}

func (jpm *jobPartMgr) getOverwritePrompter() *overwritePrompter {
	return jpm.jobMgr.getOverwritePrompter()
}

func (jpm *jobPartMgr) getFolderCreationTracker() FolderCreationTracker {
	if jpm.jobMgrInitState == nil || jpm.jobMgrInitState.folderCreationTracker == nil {
		panic("folderCreationTracker should have been initialized already")
	}

	return jpm.jobMgrInitState.folderCreationTracker
}

func (jpm *jobPartMgr) Plan() *JobPartPlanHeader {
	return jpm.planMMF.Plan()
}

// ScheduleTransfers schedules this job part's transfers. It is called when a new job part is ordered & is also called to resume a paused Job
func (jpm *jobPartMgr) ScheduleTransfers(jobCtx context.Context) {
	jobCtx = context.WithValue(jobCtx, ServiceAPIVersionOverride, DefaultServiceApiVersion)
	jpm.atomicTransfersDone = 0   // Reset the # of transfers done back to 0
	jpm.atomicTransfersFailed = 0 // Resets the # transfers failed back to 0 during resume operation
	// partplan file is opened and mapped when job part is added
	// jpm.planMMF = jpm.filename.Map() // Open the job part plan file & memory-map it in
	plan := jpm.planMMF.Plan()
	if plan.PartNum == 0 && plan.NumTransfers == 0 {
		/* This will wind down the transfer and report summary */
		plan.SetJobStatus(common.EJobStatus.Completed())
		return
	}

	// *** Open the job part: process any job part plan-setting used by all transfers ***
	dstData := plan.DstBlobData

	jpm.httpHeaders = common.ResourceHTTPHeaders{
		ContentType:        string(dstData.ContentType[:dstData.ContentTypeLength]),
		ContentEncoding:    string(dstData.ContentEncoding[:dstData.ContentEncodingLength]),
		ContentDisposition: string(dstData.ContentDisposition[:dstData.ContentDispositionLength]),
		ContentLanguage:    string(dstData.ContentLanguage[:dstData.ContentLanguageLength]),
		CacheControl:       string(dstData.CacheControl[:dstData.CacheControlLength]),
	}

	jpm.putMd5 = dstData.PutMd5
	jpm.blockBlobTier = dstData.BlockBlobTier
	jpm.pageBlobTier = dstData.PageBlobTier
	jpm.deleteDestinationFileIfNecessary = dstData.DeleteDestinationFileIfNecessary

	// For this job part, split the metadata string apart and create an common.Metadata out of it
	metadataString := string(dstData.Metadata[:dstData.MetadataLength])
	jpm.metadata = common.Metadata{}
	if len(metadataString) > 0 {
		var err error
		jpm.metadata, err = common.StringToMetadata(metadataString)
		if err != nil {
			panic("sanity check: metadata string should be valid at this point: " + metadataString)
		}
	}
	blobTagsStr := string(dstData.BlobTags[:dstData.BlobTagsLength])
	jpm.blobTags = common.BlobTags{}
	if len(blobTagsStr) > 0 {
		for _, keyAndValue := range strings.Split(blobTagsStr, "&") { // key/value pairs are separated by '&'
			kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
			key, _ := url.QueryUnescape(kv[0])
			value, _ := url.QueryUnescape(kv[1])
			jpm.blobTags[key] = value
		}
	}

	jpm.cpkOptions = common.CpkOptions{
		CpkInfo:           dstData.CpkInfo,
		CpkScopeInfo:      string(dstData.CpkScopeInfo[:dstData.CpkScopeInfoLength]),
		IsSourceEncrypted: dstData.IsSourceEncrypted,
	}

	jpm.SetPropertiesFlags = dstData.SetPropertiesFlags
	jpm.RehydratePriority = plan.RehydratePriority

	jpm.preserveLastModifiedTime = plan.DstLocalData.PreserveLastModifiedTime

	jpm.blobTypeOverride = plan.DstBlobData.BlobType
	jpm.newJobXfer = computeJobXfer(plan.FromTo, plan.DstBlobData.BlobType)

	jpm.priority = plan.Priority

	// *** Schedule this job part's transfers ***
	for t := uint32(0); t < plan.NumTransfers; t++ {
		jppt := plan.Transfer(t)
		ts := jppt.TransferStatus()
		if ts == common.ETransferStatus.Success() {
			jpm.ReportTransferDone(ts) // Don't schedule an already-completed/failed transfer
			continue
		}

		// If the transfer was failed, then while rescheduling the transfer marking it Started.
		if ts == common.ETransferStatus.Failed() {
			jppt.SetTransferStatus(common.ETransferStatus.Restarted(), true)
			if failedCount := atomic.LoadUint32(&jpm.atomicTransfersFailed); failedCount > 0 {
				atomic.AddUint32(&jpm.atomicTransfersFailed, ^uint32(0))
			} // Adding uint32 max is effectively subtracting 1
		}

		if _, dst, isFolder := plan.TransferSrcDstStrings(t); isFolder {
			// register the folder!
			if jpptFolderTracker, ok := jpm.getFolderCreationTracker().(JPPTCompatibleFolderCreationTracker); ok {
				if plan.FromTo.To().IsRemote() {
					uri, err := url.Parse(dst)
					common.PanicIfErr(err)
					uri.RawPath = ""
					uri.RawQuery = ""

					dst = uri.String()
				}

				jpptFolderTracker.RegisterPropertiesTransfer(dst, t)
			}
		}

		// Each transfer gets its own context (so any chunk can cancel the whole transfer) based off the job's context
		transferCtx, transferCancel := context.WithCancel(jobCtx)
		// Add the pipeline network stats to the context. This will be manually unset for all sourceInfoProvider contexts.
		transferCtx = withPipelineNetworkStats(transferCtx, jpm.jobMgr.PipelineNetworkStats())
		// Initialize a job part transfer manager
		jptm := &jobPartTransferMgr{
			jobPartMgr:          jpm,
			jobPartPlanTransfer: jppt,
			transferIndex:       t,
			ctx:                 transferCtx,
			cancel:              transferCancel,
			// TODO: insert the factory func interface in jptm.
			// numChunks will be set by the transfer's prologue method
		}

		//build transferInfo after we've set transferIndex
		jptm.transferInfo = jptm.Info()
		jpm.Log(common.LogDebug, fmt.Sprintf("scheduling JobID=%v, Part#=%d, Transfer#=%d, priority=%v", plan.JobID, plan.PartNum, t, plan.Priority))

		// ===== TEST KNOB
		relSrc, relDst := plan.TransferSrcDstRelatives(t)

		var err error
		if plan.FromTo.From().IsRemote() {
			relSrc, err = url.PathUnescape(relSrc)
		}
		relSrc = strings.TrimPrefix(relSrc, common.AZCOPY_PATH_SEPARATOR_STRING)
		common.PanicIfErr(err) // neither of these panics should happen, they already would have had a clean error.
		if plan.FromTo.To().IsRemote() {
			relDst, err = url.PathUnescape(relDst)
		}
		relDst = strings.TrimPrefix(relDst, common.AZCOPY_PATH_SEPARATOR_STRING)
		common.PanicIfErr(err)

		_, srcOk := DebugSkipFiles[relSrc]
		_, dstOk := DebugSkipFiles[relDst]
		if srcOk || dstOk {
			if jpm.ShouldLog(common.LogInfo) {
				jpm.Log(common.LogInfo, fmt.Sprintf("Transfer %d cancelled: %s", jptm.transferIndex, relSrc))
			}

			// cancel the transfer
			jptm.Cancel()
			jptm.SetStatus(common.ETransferStatus.Cancelled())
		} else {
			if len(DebugSkipFiles) != 0 && jpm.ShouldLog(common.LogInfo) {
				jpm.Log(common.LogInfo, fmt.Sprintf("Did not exclude: src: %s dst: %s", relSrc, relDst))
			}
		}
		// ===== TEST KNOB
		jpm.jobMgr.ScheduleTransfer(jpm.priority, jptm)

		// This sets the atomic variable atomicAllTransfersScheduled to 1
		// atomicAllTransfersScheduled variables is used in case of resume job
		// Since iterating the JobParts and scheduling transfer is independent
		// a variable is required which defines whether last part is resumed or not
		if plan.IsFinalPart {
			jpm.jobMgr.ConfirmAllTransfersScheduled()
		}
	}

	if plan.IsFinalPart {
		jpm.Log(common.LogInfo, "Final job part has been scheduled")
	}
}

func (jpm *jobPartMgr) ScheduleChunks(chunkFunc chunkFunc) {
	jpm.jobMgr.ScheduleChunk(jpm.priority, chunkFunc)
}

func (jpm *jobPartMgr) RescheduleTransfer(jptm IJobPartTransferMgr) {
	jpm.jobMgr.ScheduleTransfer(jpm.priority, jptm)
}

func (jpm *jobPartMgr) SlicePool() common.ByteSlicePooler {
	return jpm.slicePool
}

func (jpm *jobPartMgr) CacheLimiter() common.CacheLimiter {
	return jpm.cacheLimiter
}

func (jpm *jobPartMgr) FileCountLimiter() common.CacheLimiter {
	return jpm.fileCountLimiter
}

func (jpm *jobPartMgr) ExclusiveDestinationMap() *common.ExclusiveStringMap {
	return jpm.exclusiveDestinationMap
}

func (jpm *jobPartMgr) StartJobXfer(jptm IJobPartTransferMgr) {
	jpm.newJobXfer(jptm, jpm.pacer)
}

func (jpm *jobPartMgr) GetOverwriteOption() common.OverwriteOption {
	return jpm.Plan().ForceWrite
}

func (jpm *jobPartMgr) GetForceIfReadOnly() bool {
	return jpm.Plan().ForceIfReadOnly
}

func (jpm *jobPartMgr) AutoDecompress() bool {
	return jpm.Plan().AutoDecompress
}

func (jpm *jobPartMgr) resourceDstData(fullFilePath string, dataFileToXfer []byte) (headers common.ResourceHTTPHeaders,
	metadata common.Metadata, blobTags common.BlobTags, cpkOptions common.CpkOptions) {
	if jpm.planMMF.Plan().DstBlobData.NoGuessMimeType {
		return jpm.httpHeaders, jpm.metadata, jpm.blobTags, jpm.cpkOptions
	}

	return common.ResourceHTTPHeaders{
		ContentType:        jpm.inferContentType(fullFilePath, dataFileToXfer),
		ContentLanguage:    jpm.httpHeaders.ContentLanguage,
		ContentDisposition: jpm.httpHeaders.ContentDisposition,
		ContentEncoding:    jpm.httpHeaders.ContentEncoding,
		CacheControl:       jpm.httpHeaders.CacheControl,
	}, jpm.metadata, jpm.blobTags, jpm.cpkOptions
}

var EnvironmentMimeMap map[string]string

// TODO do we want these charset=utf-8?
var builtinTypes = map[string]string{
	".css":  "text/css",
	".gif":  "image/gif",
	".htm":  "text/html",
	".html": "text/html",
	".jpeg": "image/jpeg",
	".jpg":  "image/jpeg",
	".js":   "application/javascript",
	".mjs":  "application/javascript",
	".pdf":  "application/pdf",
	".png":  "image/png",
	".svg":  "image/svg+xml",
	".wasm": "application/wasm",
	".webp": "image/webp",
	".xml":  "text/xml",
}

func (jpm *jobPartMgr) inferContentType(fullFilePath string, dataFileToXfer []byte) string {
	fileExtension := filepath.Ext(fullFilePath)

	if contentType, ok := EnvironmentMimeMap[strings.ToLower(fileExtension)]; ok {
		return contentType
	}
	// short-circuit for common static website files
	// mime.TypeByExtension takes the registry into account, which is most often undesirable in practice
	if override, ok := builtinTypes[strings.ToLower(fileExtension)]; ok {
		return override
	}

	/*
	 * Below functions return utf-8 as default charset for text files. Discard
	 * charset if it exists, safer to omit charset instead of defaulting to
	 * a wrong one.
	 */
	if guessedType := mime.TypeByExtension(fileExtension); guessedType != "" {
		return strings.Split(guessedType, ";")[0]
	}

	return strings.Split(http.DetectContentType(dataFileToXfer), ";")[0]
}

func (jpm *jobPartMgr) BlobTypeOverride() common.BlobType {
	return jpm.blobTypeOverride
}

func (jpm *jobPartMgr) BlobTiers() (blockBlobTier common.BlockBlobTier, pageBlobTier common.PageBlobTier) {
	return jpm.blockBlobTier, jpm.pageBlobTier
}

func (jpm *jobPartMgr) CpkInfo() *blob.CPKInfo {
	return common.GetCpkInfo(jpm.cpkOptions.CpkInfo)
}

func (jpm *jobPartMgr) CpkScopeInfo() *blob.CPKScopeInfo {
	return common.GetCpkScopeInfo(jpm.cpkOptions.CpkScopeInfo)
}

func (jpm *jobPartMgr) IsSourceEncrypted() bool {
	return jpm.cpkOptions.IsSourceEncrypted
}

func (jpm *jobPartMgr) PropertiesToTransfer() common.SetPropertiesFlags {
	return jpm.SetPropertiesFlags
}
func (jpm *jobPartMgr) ShouldPutMd5() bool {
	return jpm.putMd5
}

func (jpm *jobPartMgr) DeleteDestinationFileIfNecessary() bool {
	return jpm.deleteDestinationFileIfNecessary
}

func (jpm *jobPartMgr) SAS() (string, string) {
	return jpm.sourceSAS, jpm.destinationSAS
}

func (jpm *jobPartMgr) SecurityInfoPersistenceManager() *securityInfoPersistenceManager {
	if jpm.jobMgrInitState == nil || jpm.jobMgrInitState.securityInfoPersistenceManager == nil {
		panic("SIPM should have been initialized already")
	}

	return jpm.jobMgrInitState.securityInfoPersistenceManager
}

func (jpm *jobPartMgr) FolderDeletionManager() common.FolderDeletionManager {
	if jpm.jobMgrInitState == nil || jpm.jobMgrInitState.folderDeletionManager == nil {
		panic("folder deletion manager should have been initialized already")
	}

	return jpm.jobMgrInitState.folderDeletionManager
}

func (jpm *jobPartMgr) localDstData() *JobPartPlanDstLocal {
	return &jpm.Plan().DstLocalData
}

func (jpm *jobPartMgr) deleteSnapshotsOption() common.DeleteSnapshotsOption {
	return jpm.Plan().DeleteSnapshotsOption
}

func (jpm *jobPartMgr) permanentDeleteOption() common.PermanentDeleteOption {
	return jpm.Plan().PermanentDeleteOption
}

func (jpm *jobPartMgr) updateJobPartProgress(status common.TransferStatus) {
	switch status {
	case common.ETransferStatus.Success():
		atomic.AddUint32(&jpm.atomicTransfersCompleted, 1)
	case common.ETransferStatus.Failed(), common.ETransferStatus.BlobTierFailure():
		atomic.AddUint32(&jpm.atomicTransfersFailed, 1)
	case common.ETransferStatus.SkippedEntityAlreadyExists(), common.ETransferStatus.SkippedBlobHasSnapshots():
		atomic.AddUint32(&jpm.atomicTransfersSkipped, 1)
	case common.ETransferStatus.Restarted(): // When a job is resumed, number of failed should reset to 0
		atomic.StoreUint32(&jpm.atomicTransfersFailed, 0)
	case common.ETransferStatus.Cancelled():
	default:
		jpm.Log(common.LogError, fmt.Sprintf("Unexpected status: %v", status.String()))
	}
}

// Call Done when a transfer has completed its epilog; this method returns the number of transfers completed so far
func (jpm *jobPartMgr) ReportTransferDone(status common.TransferStatus) (transfersDone uint32) {
	transfersDone = atomic.AddUint32(&jpm.atomicTransfersDone, 1)
	jpm.updateJobPartProgress(status)

	if transfersDone == jpm.planMMF.Plan().NumTransfers {
		jppi := jobPartProgressInfo{
			transfersCompleted: int(atomic.LoadUint32(&jpm.atomicTransfersCompleted)),
			transfersSkipped:   int(atomic.LoadUint32(&jpm.atomicTransfersSkipped)),
			transfersFailed:    int(atomic.LoadUint32(&jpm.atomicTransfersFailed)),
			completionChan:     jpm.closeOnCompletion,
		}
		jpm.Plan().SetJobPartStatus(common.EJobStatus.EnhanceJobStatusInfo(jppi.transfersSkipped > 0,
			jppi.transfersFailed > 0, jppi.transfersCompleted > 0))
		jpm.jobMgr.ReportJobPartDone(jppi)
		jpm.Log(common.LogInfo, fmt.Sprintf("JobID=%v, Part#=%d, TransfersDone=%d of %d",
			jpm.planMMF.Plan().JobID, jpm.planMMF.Plan().PartNum, transfersDone,
			jpm.planMMF.Plan().NumTransfers))
	}
	return transfersDone
}

// func (jpm *jobPartMgr) Cancel() { jpm.jobMgr.Cancel() }
func (jpm *jobPartMgr) Close() {
	jpm.planMMF.Unmap()
	// Clear other fields to all for GC
	jpm.httpHeaders = common.ResourceHTTPHeaders{}
	jpm.metadata = common.Metadata{}
	jpm.preserveLastModifiedTime = false

	// TODO: Delete file?
	/*if err := os.Remove(jpm.planFile.Name()); err != nil {
		jpm.Panic(fmt.Errorf("error removing Job Part Plan file %s. Error=%v", jpm.planFile.Name(), err))
	}*/
}

// TODO: added for debugging purpose. remove later
// Add 1 to the active number of goroutine performing the transfer or executing the chunkFunc
func (jpm *jobPartMgr) OccupyAConnection() {
	jpm.jobMgr.OccupyAConnection()
}

// Sub 1 from the active number of goroutine performing the transfer or executing the chunkFunc
// TODO: added for debugging purpose. remove later
func (jpm *jobPartMgr) ReleaseAConnection() {
	jpm.jobMgr.ReleaseAConnection()
}

func (jpm *jobPartMgr) ShouldLog(level common.LogLevel) bool  { return jpm.jobMgr.ShouldLog(level) }
func (jpm *jobPartMgr) Log(level common.LogLevel, msg string) { jpm.jobMgr.Log(level, msg) }
func (jpm *jobPartMgr) Panic(err error)                       { jpm.jobMgr.Panic(err) }
func (jpm *jobPartMgr) ChunkStatusLogger() common.ChunkStatusLogger {
	return jpm.jobMgr.ChunkStatusLogger()
}

func (jpm *jobPartMgr) SrcServiceClient() *common.ServiceClient {
	return jpm.srcServiceClient
}

func (jpm *jobPartMgr) DstServiceClient() *common.ServiceClient {
	return jpm.dstServiceClient
}

func (jpm *jobPartMgr) SourceIsOAuth() bool {
	return jpm.srcIsOAuth
}

/* Status update messages should not fail */
func (jpm *jobPartMgr) SendXferDoneMsg(msg xferDoneMsg) {
	jpm.jobMgr.SendXferDoneMsg(msg)
}

func (jpm *jobPartMgr) ResetFailedTransfersCount() {
	atomic.StoreUint32(&jpm.atomicTransfersFailed, 0)
}

// TODO: Can we delete this method?
// numberOfTransfersDone returns the numberOfTransfersDone_doNotUse of JobPartPlanInfo
// instance in thread safe manner
// func (jpm *jobPartMgr) numberOfTransfersDone() uint32 {	return atomic.LoadUint32(&jpm.numberOfTransfersDone_doNotUse)}

// setNumberOfTransfersDone sets the number of transfers done to a specific value
// in a thread safe manner
// func (jppi *jobPartPlanInfo) setNumberOfTransfersDone(val uint32) {
//	atomic.StoreUint32(&jPartPlanInfo.numberOfTransfersDone_doNotUse, val)
// }
