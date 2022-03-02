package ste

import (
	"context"
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/nitin-deamon/azure-storage-azcopy/v10/azbfs"
	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
	"golang.org/x/sync/semaphore"
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
	SAS() (string, string)
	//CancelJob()
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
	SourceProviderPipeline() pipeline.Pipeline
	getOverwritePrompter() *overwritePrompter
	getFolderCreationTracker() FolderCreationTracker
	SecurityInfoPersistenceManager() *securityInfoPersistenceManager
	FolderDeletionManager() common.FolderDeletionManager
	CpkInfo() common.CpkInfo
	CpkScopeInfo() common.CpkScopeInfo
	IsSourceEncrypted() bool
	/* Status Manager Updates */
	SendXferDoneMsg(msg xferDoneMsg)
}

type serviceAPIVersionOverride struct{}

// ServiceAPIVersionOverride is a global variable in package ste which is a key to Service Api Version Value set in the every Job's context.
var ServiceAPIVersionOverride = serviceAPIVersionOverride{}

// DefaultServiceApiVersion is the default value of service api version that is set as value to the ServiceAPIVersionOverride in every Job's context.
var DefaultServiceApiVersion = common.GetLifecycleMgr().GetEnvironmentVariable(common.EEnvironmentVariable.DefaultServiceApiVersion())

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

// NewAzcopyHTTPClient creates a new HTTP client.
// We must minimize use of this, and instead maximize re-use of the returned client object.
// Why? Because that makes our connection pooling more efficient, and prevents us exhausting the
// number of available network sockets on resource-constrained Linux systems. (E.g. when
// 'ulimit -Hn' is low).
func NewAzcopyHTTPClient(maxIdleConns int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: common.GlobalProxyLookup,
			DialContext: newDialRateLimiter(&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:           0, // No limit
			MaxIdleConnsPerHost:    maxIdleConns,
			IdleConnTimeout:        180 * time.Second,
			TLSHandshakeTimeout:    10 * time.Second,
			ExpectContinueTimeout:  1 * time.Second,
			DisableKeepAlives:      false,
			DisableCompression:     true, // must disable the auto-decompression of gzipped files, and just download the gzipped version. See https://github.com/Azure/azure-storage-azcopy/issues/374
			MaxResponseHeaderBytes: 0,
			//ResponseHeaderTimeout:  time.Duration{},
			//ExpectContinueTimeout:  time.Duration{},
		},
	}
}

// Prevents too many dials happening at once, because we've observed that that increases the thread
// count in the app, to several times more than is actually necessary - presumably due to a blocking OS
// call somewhere. It's tidier to avoid creating those excess OS threads.
// Even our change from Dial (deprecated) to DialContext did not replicate the effect of dialRateLimiter.
type dialRateLimiter struct {
	dialer *net.Dialer
	sem    *semaphore.Weighted
}

func newDialRateLimiter(dialer *net.Dialer) *dialRateLimiter {
	const concurrentDialsPerCpu = 10 // exact value doesn't matter too much, but too low will be too slow, and too high will reduce the beneficial effect on thread count
	return &dialRateLimiter{
		dialer,
		semaphore.NewWeighted(int64(concurrentDialsPerCpu * runtime.NumCPU())),
	}
}

func (d *dialRateLimiter) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	err := d.sem.Acquire(context.Background(), 1)
	if err != nil {
		return nil, err
	}
	defer d.sem.Release(1)

	return d.dialer.DialContext(ctx, network, address)
}

// newAzcopyHTTPClientFactory creates a HTTPClientPolicyFactory object that sends HTTP requests to a Go's default http.Client.
func newAzcopyHTTPClientFactory(pipelineHTTPClient *http.Client) pipeline.Factory {
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		return func(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
			r, err := pipelineHTTPClient.Do(request.WithContext(ctx))
			if err != nil {
				err = pipeline.NewError(err, "HTTP request failed")
			}
			return pipeline.NewHTTPResponse(r), err
		}
	})
}

// NewBlobPipeline creates a Pipeline using the specified credentials and options.
func NewBlobPipeline(c azblob.Credential, o azblob.PipelineOptions, r XferRetryOptions, p pacer, client *http.Client, statsAcc *PipelineNetworkStats) pipeline.Pipeline {
	if c == nil {
		panic("c can't be nil")
	}
	// Closest to API goes first; closest to the wire goes last
	f := []pipeline.Factory{
		azblob.NewTelemetryPolicyFactory(o.Telemetry),
		azblob.NewUniqueRequestIDPolicyFactory(),
		NewBlobXferRetryPolicyFactory(r),    // actually retry the operation
		newRetryNotificationPolicyFactory(), // record that a retry status was returned
		c,
		pipeline.MethodFactoryMarker(), // indicates at what stage in the pipeline the method factory is invoked
		//NewPacerPolicyFactory(p),
		NewVersionPolicyFactory(),
		NewRequestLogPolicyFactory(RequestLogOptions{
			LogWarningIfTryOverThreshold: o.RequestLog.LogWarningIfTryOverThreshold,
			SyslogDisabled:               common.IsForceLoggingDisabled(),
		}),
		newXferStatsPolicyFactory(statsAcc),
	}
	return pipeline.NewPipeline(f, pipeline.Options{HTTPSender: newAzcopyHTTPClientFactory(client), Log: o.Log})
}

// NewBlobFSPipeline creates a pipeline for transfers to and from BlobFS Service
// The blobFS operations currently in azcopy are supported by SharedKey Credentials
func NewBlobFSPipeline(c azbfs.Credential, o azbfs.PipelineOptions, r XferRetryOptions, p pacer, client *http.Client, statsAcc *PipelineNetworkStats) pipeline.Pipeline {
	if c == nil {
		panic("c can't be nil")
	}
	// Closest to API goes first; closest to the wire goes last
	f := []pipeline.Factory{
		azbfs.NewTelemetryPolicyFactory(o.Telemetry),
		azbfs.NewUniqueRequestIDPolicyFactory(),
		NewBFSXferRetryPolicyFactory(r),     // actually retry the operation
		newRetryNotificationPolicyFactory(), // record that a retry status was returned
	}

	f = append(f, c)

	f = append(f,
		pipeline.MethodFactoryMarker(), // indicates at what stage in the pipeline the method factory is invoked
		NewRequestLogPolicyFactory(RequestLogOptions{
			LogWarningIfTryOverThreshold: o.RequestLog.LogWarningIfTryOverThreshold,
			SyslogDisabled:               common.IsForceLoggingDisabled(),
		}),
		newXferStatsPolicyFactory(statsAcc))

	return pipeline.NewPipeline(f, pipeline.Options{HTTPSender: newAzcopyHTTPClientFactory(client), Log: o.Log})
}

// NewFilePipeline creates a Pipeline using the specified credentials and options.
func NewFilePipeline(c azfile.Credential, o azfile.PipelineOptions, r azfile.RetryOptions, p pacer, client *http.Client, statsAcc *PipelineNetworkStats) pipeline.Pipeline {
	if c == nil {
		panic("c can't be nil")
	}
	// Closest to API goes first; closest to the wire goes last
	f := []pipeline.Factory{
		azfile.NewTelemetryPolicyFactory(o.Telemetry),
		azfile.NewUniqueRequestIDPolicyFactory(),
		azfile.NewRetryPolicyFactory(r),     // actually retry the operation
		newRetryNotificationPolicyFactory(), // record that a retry status was returned
		c,
		pipeline.MethodFactoryMarker(), // indicates at what stage in the pipeline the method factory is invoked
		NewVersionPolicyFactory(),
		NewRequestLogPolicyFactory(RequestLogOptions{
			LogWarningIfTryOverThreshold: o.RequestLog.LogWarningIfTryOverThreshold,
			SyslogDisabled:               common.IsForceLoggingDisabled(),
		}),
		newXferStatsPolicyFactory(statsAcc),
	}
	return pipeline.NewPipeline(f, pipeline.Options{HTTPSender: newAzcopyHTTPClientFactory(client), Log: o.Log})
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Holds the status of transfers in this jptm
type jobPartProgressInfo struct {
	transfersCompleted int
	transfersSkipped   int
	transfersFailed    int
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

	pipeline pipeline.Pipeline // ordered list of Factory objects and an object implementing the HTTPSender interface
	// Currently, this only sees use in ADLSG2->ADLSG2 ACL transfers. TODO: Remove it when we can reliably get/set ACLs on blob.
	secondaryPipeline pipeline.Pipeline

	sourceProviderPipeline pipeline.Pipeline
	// TODO: Ditto
	secondarySourceProviderPipeline pipeline.Pipeline

	// used defensively to protect double init
	atomicPipelinesInitedIndicator uint32

	// numberOfTransfersDone_doNotUse represents the number of transfer of JobPartOrder
	// which are either completed or failed
	// numberOfTransfersDone_doNotUse determines the final cancellation of JobPartOrder
	atomicTransfersDone      uint32
	atomicTransfersCompleted uint32
	atomicTransfersFailed    uint32
	atomicTransfersSkipped   uint32

	cpkOptions common.CpkOptions
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

func (jpm *jobPartMgr) Plan() *JobPartPlanHeader { return jpm.planMMF.Plan() }

// ScheduleTransfers schedules this job part's transfers. It is called when a new job part is ordered & is also called to resume a paused Job
func (jpm *jobPartMgr) ScheduleTransfers(jobCtx context.Context) {
	jpm.atomicTransfersDone = 0 // Reset the # of transfers done back to 0
	// partplan file is opened and mapped when job part is added
	//jpm.planMMF = jpm.filename.Map() // Open the job part plan file & memory-map it in
	plan := jpm.planMMF.Plan()
	if plan.PartNum == 0 && plan.NumTransfers == 0 {
		/* This will wind down the transfer and report summary */
		plan.SetJobStatus(common.EJobStatus.Completed())
		return
	}

	// get the list of include / exclude transfers
	includeTransfer, excludeTransfer := jpm.jobMgr.IncludeExclude()
	if len(includeTransfer) > 0 || len(excludeTransfer) > 0 {
		panic("List of transfers is obsolete.")
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

	// For this job part, split the metadata string apart and create an common.Metadata out of it
	metadataString := string(dstData.Metadata[:dstData.MetadataLength])
	jpm.metadata = common.Metadata{}
	if len(metadataString) > 0 {
		for _, keyAndValue := range strings.Split(metadataString, ";") { // key/value pairs are separated by ';'
			kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
			jpm.metadata[kv[0]] = kv[1]
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

	jpm.preserveLastModifiedTime = plan.DstLocalData.PreserveLastModifiedTime

	jpm.blobTypeOverride = plan.DstBlobData.BlobType
	jpm.newJobXfer = computeJobXfer(plan.FromTo, plan.DstBlobData.BlobType)

	jpm.priority = plan.Priority

	jpm.createPipelines(jobCtx) // pipeline is created per job part manager

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
			jppt.SetTransferStatus(common.ETransferStatus.Started(), true)
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
		// Initialize a job part transfer manager
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
		relDst = strings.TrimPrefix(relSrc, common.AZCOPY_PATH_SEPARATOR_STRING)
		common.PanicIfErr(err)

		_, srcOk := DebugSkipFiles[relSrc]
		_, dstOk := DebugSkipFiles[relDst]
		if srcOk || dstOk {
			if jpm.ShouldLog(pipeline.LogInfo) {
				jpm.Log(pipeline.LogInfo, fmt.Sprintf("Transfer %d cancelled: %s", jptm.transferIndex, relSrc))
			}

			// cancel the transfer
			jptm.Cancel()
			jptm.SetStatus(common.ETransferStatus.Cancelled())
		} else {
			if len(DebugSkipFiles) != 0 && jpm.ShouldLog(pipeline.LogInfo) {
				jpm.Log(pipeline.LogInfo, fmt.Sprintf("Did not exclude: src: %s dst: %s", relSrc, relDst))
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
		jpm.Log(pipeline.LogInfo, "Final job part has been scheduled")
	}
}

func (jpm *jobPartMgr) ScheduleChunks(chunkFunc chunkFunc) {
	jpm.jobMgr.ScheduleChunk(jpm.priority, chunkFunc)
}

func (jpm *jobPartMgr) RescheduleTransfer(jptm IJobPartTransferMgr) {
	jpm.jobMgr.ScheduleTransfer(jpm.priority, jptm)
}

func (jpm *jobPartMgr) createPipelines(ctx context.Context) {
	if atomic.SwapUint32(&jpm.atomicPipelinesInitedIndicator, 1) != 0 {
		panic("init client and pipelines for same jobPartMgr twice")
	}

	fromTo := jpm.planMMF.Plan().FromTo
	credInfo := jpm.jobMgr.getInMemoryTransitJobState().CredentialInfo
	userAgent := common.UserAgent
	if fromTo.From() == common.ELocation.S3() {
		userAgent = common.S3ImportUserAgent
	} else if fromTo.From() == common.ELocation.GCP() {
		userAgent = common.GCPImportUserAgent
	} else if fromTo.From() == common.ELocation.Benchmark() || fromTo.To() == common.ELocation.Benchmark() {
		userAgent = common.BenchmarkUserAgent
	}
	userAgent = common.GetLifecycleMgr().AddUserAgentPrefix(common.UserAgent)

	credOption := common.CredentialOpOptions{
		LogInfo:  func(str string) { jpm.Log(pipeline.LogInfo, str) },
		LogError: func(str string) { jpm.Log(pipeline.LogError, str) },
		Panic:    jpm.Panic,
		CallerID: fmt.Sprintf("JobID=%v, Part#=%d", jpm.Plan().JobID, jpm.Plan().PartNum),
		Cancel:   jpm.jobMgr.Cancel,
	}
	// TODO: Consider to remove XferRetryPolicy and Options?
	xferRetryOption := XferRetryOptions{
		Policy:        0,
		MaxTries:      UploadMaxTries, // TODO: Consider to unify options.
		TryTimeout:    UploadTryTimeout,
		RetryDelay:    UploadRetryDelay,
		MaxRetryDelay: UploadMaxRetryDelay}

	var statsAccForSip *PipelineNetworkStats = nil // we don't accumulate stats on the source info provider

	// Create source info provider's pipeline for S2S copy.
	if fromTo == common.EFromTo.BlobBlob() || fromTo == common.EFromTo.BlobFile() {
		jpm.sourceProviderPipeline = NewBlobPipeline(
			azblob.NewAnonymousCredential(),
			azblob.PipelineOptions{
				Log: jpm.jobMgr.PipelineLogInfo(),
				Telemetry: azblob.TelemetryOptions{
					Value: userAgent,
				},
			},
			xferRetryOption,
			jpm.pacer,
			jpm.jobMgr.HttpClient(),
			statsAccForSip)

		// Consider the ADLSG2->ADLSG2 ACLs case
		if fromTo == common.EFromTo.BlobBlob() && jpm.Plan().PreservePermissions.IsTruthy() {
			jpm.secondarySourceProviderPipeline = NewBlobFSPipeline(
				azbfs.NewAnonymousCredential(),
				azbfs.PipelineOptions{
					Log: jpm.jobMgr.PipelineLogInfo(),
					Telemetry: azbfs.TelemetryOptions{
						Value: userAgent,
					},
				},
				xferRetryOption,
				jpm.pacer,
				jpm.jobMgr.HttpClient(),
				statsAccForSip)
		}
	}
	// Consider the file-local SDDL transfer case.
	if fromTo == common.EFromTo.FileBlob() || fromTo == common.EFromTo.FileFile() || fromTo == common.EFromTo.FileLocal() {
		jpm.sourceProviderPipeline = NewFilePipeline(
			azfile.NewAnonymousCredential(),
			azfile.PipelineOptions{
				Log: jpm.jobMgr.PipelineLogInfo(),
				Telemetry: azfile.TelemetryOptions{
					Value: userAgent,
				},
			},
			azfile.RetryOptions{
				Policy:        azfile.RetryPolicyExponential,
				MaxTries:      UploadMaxTries,
				TryTimeout:    UploadTryTimeout,
				RetryDelay:    UploadRetryDelay,
				MaxRetryDelay: UploadMaxRetryDelay,
			},
			jpm.pacer,
			jpm.jobMgr.HttpClient(),
			statsAccForSip)
	}

	// Create pipeline for data transfer.
	switch fromTo {
	case common.EFromTo.BlobTrash(), common.EFromTo.BlobLocal(), common.EFromTo.LocalBlob(), common.EFromTo.BenchmarkBlob(),
		common.EFromTo.BlobBlob(), common.EFromTo.FileBlob(), common.EFromTo.S3Blob(), common.EFromTo.GCPBlob():
		credential := common.CreateBlobCredential(ctx, credInfo, credOption)
		jpm.Log(pipeline.LogInfo, fmt.Sprintf("JobID=%v, credential type: %v", jpm.Plan().JobID, credInfo.CredentialType))
		jpm.pipeline = NewBlobPipeline(
			credential,
			azblob.PipelineOptions{
				Log: jpm.jobMgr.PipelineLogInfo(),
				Telemetry: azblob.TelemetryOptions{
					Value: userAgent,
				},
			},
			xferRetryOption,
			jpm.pacer,
			jpm.jobMgr.HttpClient(),
			jpm.jobMgr.PipelineNetworkStats())

		// Consider the ADLSG2->ADLSG2 ACLs case
		if fromTo == common.EFromTo.BlobBlob() && jpm.Plan().PreservePermissions.IsTruthy() {
			credential := common.CreateBlobFSCredential(ctx, credInfo, credOption)
			jpm.secondaryPipeline = NewBlobFSPipeline(
				credential,
				azbfs.PipelineOptions{
					Log: jpm.jobMgr.PipelineLogInfo(),
					Telemetry: azbfs.TelemetryOptions{
						Value: userAgent,
					},
				},
				xferRetryOption,
				jpm.pacer,
				jpm.jobMgr.HttpClient(),
				statsAccForSip)
		}
	// Create pipeline for Azure BlobFS.
	case common.EFromTo.BlobFSLocal(), common.EFromTo.LocalBlobFS(), common.EFromTo.BenchmarkBlobFS():
		credential := common.CreateBlobFSCredential(ctx, credInfo, credOption)
		jpm.Log(pipeline.LogInfo, fmt.Sprintf("JobID=%v, credential type: %v", jpm.Plan().JobID, credInfo.CredentialType))

		jpm.pipeline = NewBlobFSPipeline(
			credential,
			azbfs.PipelineOptions{
				Log: jpm.jobMgr.PipelineLogInfo(),
				Telemetry: azbfs.TelemetryOptions{
					Value: userAgent,
				},
			},
			xferRetryOption,
			jpm.pacer,
			jpm.jobMgr.HttpClient(),
			jpm.jobMgr.PipelineNetworkStats())
	// Create pipeline for Azure File.
	case common.EFromTo.FileTrash(), common.EFromTo.FileLocal(), common.EFromTo.LocalFile(), common.EFromTo.BenchmarkFile(),
		common.EFromTo.FileFile(), common.EFromTo.BlobFile():
		jpm.pipeline = NewFilePipeline(
			azfile.NewAnonymousCredential(),
			azfile.PipelineOptions{
				Log: jpm.jobMgr.PipelineLogInfo(),
				Telemetry: azfile.TelemetryOptions{
					Value: userAgent,
				},
			},
			azfile.RetryOptions{
				Policy:        azfile.RetryPolicyExponential,
				MaxTries:      UploadMaxTries,
				TryTimeout:    UploadTryTimeout,
				RetryDelay:    UploadRetryDelay,
				MaxRetryDelay: UploadMaxRetryDelay,
			},
			jpm.pacer,
			jpm.jobMgr.HttpClient(),
			jpm.jobMgr.PipelineNetworkStats())
	default:
		panic(fmt.Errorf("Unrecognized from-to: %q", fromTo.String()))
	}
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
	jpm.newJobXfer(jptm, jpm.pipeline, jpm.pacer)
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

func (jpm *jobPartMgr) CpkInfo() common.CpkInfo {
	return common.GetCpkInfo(jpm.cpkOptions.CpkInfo)
}

func (jpm *jobPartMgr) CpkScopeInfo() common.CpkScopeInfo {
	return common.GetCpkScopeInfo(jpm.cpkOptions.CpkScopeInfo)
}

func (jpm *jobPartMgr) IsSourceEncrypted() bool {
	return jpm.cpkOptions.IsSourceEncrypted
}

func (jpm *jobPartMgr) ShouldPutMd5() bool {
	return jpm.putMd5
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

func (jpm *jobPartMgr) updateJobPartProgress(status common.TransferStatus) {
	switch status {
	case common.ETransferStatus.Success():
		atomic.AddUint32(&jpm.atomicTransfersCompleted, 1)
	case common.ETransferStatus.Failed(), common.ETransferStatus.BlobTierFailure():
		atomic.AddUint32(&jpm.atomicTransfersFailed, 1)
	case common.ETransferStatus.SkippedEntityAlreadyExists(), common.ETransferStatus.SkippedBlobHasSnapshots():
		atomic.AddUint32(&jpm.atomicTransfersSkipped, 1)
	case common.ETransferStatus.Cancelled():
	default:
		jpm.Log(pipeline.LogError, fmt.Sprintf("Unexpected status: %v", status.String()))
	}
}

// Call Done when a transfer has completed its epilog; this method returns the number of transfers completed so far
func (jpm *jobPartMgr) ReportTransferDone(status common.TransferStatus) (transfersDone uint32) {
	transfersDone = atomic.AddUint32(&jpm.atomicTransfersDone, 1)
	jpm.updateJobPartProgress(status)

	//Add a safety count-check

	if jpm.ShouldLog(pipeline.LogInfo) {
		plan := jpm.Plan()
		jpm.Log(pipeline.LogInfo, fmt.Sprintf("JobID=%v, Part#=%d, TransfersDone=%d of %d", plan.JobID, plan.PartNum, transfersDone, plan.NumTransfers))
	}
	if transfersDone == jpm.planMMF.Plan().NumTransfers {
		jppi := jobPartProgressInfo{
			transfersCompleted: int(atomic.LoadUint32(&jpm.atomicTransfersCompleted)),
			transfersSkipped:   int(atomic.LoadUint32(&jpm.atomicTransfersSkipped)),
			transfersFailed:    int(atomic.LoadUint32(&jpm.atomicTransfersFailed)),
		}
		jpm.jobMgr.ReportJobPartDone(jppi)
	}
	return transfersDone
}

//func (jpm *jobPartMgr) Cancel() { jpm.jobMgr.Cancel() }
func (jpm *jobPartMgr) Close() {
	jpm.planMMF.Unmap()
	// Clear other fields to all for GC
	jpm.httpHeaders = common.ResourceHTTPHeaders{}
	jpm.metadata = common.Metadata{}
	jpm.preserveLastModifiedTime = false

	/*
	 * Set pipeline to nil, so that jpm/JobMgr can be GC'ed.
	 *
	 * TODO: We should not need to explicitly set this to nil but today we have a yet-unknown ref on pipeline which
	 *       is leaking JobMgr memory, so we cause that to be freed by force dropping this ref.
	 *
	 * Note: Force setting this to nil can technically result in crashes since the containing object is still around,
	 *       but we should be protected against that since we do this Close in a deferred manner, at least few minutes after the job completes.
	 */
	jpm.pipeline = nil

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

func (jpm *jobPartMgr) ShouldLog(level pipeline.LogLevel) bool  { return jpm.jobMgr.ShouldLog(level) }
func (jpm *jobPartMgr) Log(level pipeline.LogLevel, msg string) { jpm.jobMgr.Log(level, msg) }
func (jpm *jobPartMgr) Panic(err error)                         { jpm.jobMgr.Panic(err) }
func (jpm *jobPartMgr) ChunkStatusLogger() common.ChunkStatusLogger {
	return jpm.jobMgr.ChunkStatusLogger()
}

func (jpm *jobPartMgr) SourceProviderPipeline() pipeline.Pipeline {
	return jpm.sourceProviderPipeline
}

/* Status update messages should not fail */
func (jpm *jobPartMgr) SendXferDoneMsg(msg xferDoneMsg) {
	jpm.jobMgr.SendXferDoneMsg(msg)
}

// TODO: Can we delete this method?
// numberOfTransfersDone returns the numberOfTransfersDone_doNotUse of JobPartPlanInfo
// instance in thread safe manner
//func (jpm *jobPartMgr) numberOfTransfersDone() uint32 {	return atomic.LoadUint32(&jpm.numberOfTransfersDone_doNotUse)}

// setNumberOfTransfersDone sets the number of transfers done to a specific value
// in a thread safe manner
//func (jppi *jobPartPlanInfo) setNumberOfTransfersDone(val uint32) {
//	atomic.StoreUint32(&jPartPlanInfo.numberOfTransfersDone_doNotUse, val)
//}
