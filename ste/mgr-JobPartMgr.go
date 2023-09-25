package ste

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	azruntime "github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
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
	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"golang.org/x/sync/semaphore"
)

var _ IJobPartMgr = &jobPartMgr{}

// debug knob
var DebugSkipFiles = make(map[string]bool)

type IJobPartMgr interface {
	Plan() *JobPartPlanHeader
	ScheduleTransfers(jobCtx context.Context, commandLineMbpsCap float64)
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

	CredentialInfo() common.CredentialInfo
	ClientOptions() azcore.ClientOptions
	S2SSourceCredentialInfo() common.CredentialInfo
	S2SSourceClientOptions() azcore.ClientOptions
	CredentialOpOptions() *common.CredentialOpOptions

	Pipeline() pipeline.Pipeline
	SourceProviderPipeline() pipeline.Pipeline
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
			// ResponseHeaderTimeout:  time.Duration{},
			// ExpectContinueTimeout:  time.Duration{},
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

func NewClientOptions(retry policy.RetryOptions, telemetry policy.TelemetryOptions, transport policy.Transporter, statsAcc *PipelineNetworkStats, log LogOptions, trailingDot *common.TrailingDotOption, from *common.Location) azcore.ClientOptions {
	// Pipeline will look like
	// [includeResponsePolicy, newAPIVersionPolicy (ignored), NewTelemetryPolicy, perCall, NewRetryPolicy, perRetry, NewLogPolicy, httpHeaderPolicy, bodyDownloadPolicy]
	// TODO (gapra): Does this have to happen this happen here?
	log.RequestLogOptions.SyslogDisabled = common.IsForceLoggingDisabled()
	perCallPolicies := []policy.Policy{azruntime.NewRequestIDPolicy()}
	// TODO : Default logging policy is not equivalent to old one. tracing HTTP request
	perRetryPolicies := []policy.Policy{newRetryNotificationPolicy(), newVersionPolicy(), newColdTierPolicy(), NewTrailingDotPolicy(trailingDot, from), newLogPolicy(log), newStatsPolicy(statsAcc)}

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
		NewBFSXferRetryPolicyFactory(r),       // actually retry the operation
		newV1RetryNotificationPolicyFactory(), // record that a retry status was returned
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

	credInfo               common.CredentialInfo
	clientOptions          azcore.ClientOptions
	s2sSourceCredInfo      common.CredentialInfo
	s2sSourceClientOptions azcore.ClientOptions
	credOption             *common.CredentialOpOptions

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
func (jpm *jobPartMgr) ScheduleTransfers(jobCtx context.Context, commandLineMbpsCap float64) {
	jobCtx = context.WithValue(jobCtx, ServiceAPIVersionOverride, DefaultServiceApiVersion)
	jpm.atomicTransfersDone = 0 // Reset the # of transfers done back to 0
	// partplan file is opened and mapped when job part is added
	// jpm.planMMF = jpm.filename.Map() // Open the job part plan file & memory-map it in
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

	jpm.clientInfo()
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
			jppt.SetTransferStatus(common.ETransferStatus.Restarted(), true)
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
			commandLineMbpsCap: commandLineMbpsCap,
			// TODO: insert the factory func interface in jptm.
			// numChunks will be set by the transfer's prologue method
		}
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

func (jpm *jobPartMgr) clientInfo() {
	jobState := jpm.jobMgr.getInMemoryTransitJobState()

	// Destination credential
	if jpm.credInfo.CredentialType == common.ECredentialType.Unknown() {
		jpm.credInfo = jobState.CredentialInfo
	}
	fromTo := jpm.planMMF.Plan().FromTo

	// S2S source credential
	// Default credential type assumed to be SAS
	s2sSourceCredInfo := common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}
	// For Blob and BlobFS, there are other options for the source credential
	if (fromTo.IsS2S() || fromTo.IsDownload()) && (fromTo.From() == common.ELocation.Blob() || fromTo.From() == common.ELocation.BlobFS()) {
		if fromTo.To().CanForwardOAuthTokens() && jobState.S2SSourceCredentialType.IsAzureOAuth() {
			if jpm.s2sSourceCredInfo.CredentialType == common.ECredentialType.Unknown() {
				s2sSourceCredInfo = jobState.CredentialInfo.WithType(jobState.S2SSourceCredentialType)
			}
		} else if fromTo.IsDownload() && (jobState.CredentialInfo.CredentialType.IsAzureOAuth() || jobState.CredentialInfo.CredentialType == common.ECredentialType.SharedKey()) {
			s2sSourceCredInfo = jobState.CredentialInfo
		}
	}
	jpm.s2sSourceCredInfo = s2sSourceCredInfo

	jpm.credOption = &common.CredentialOpOptions{
		LogInfo:  func(str string) { jpm.Log(common.LogInfo, str) },
		LogError: func(str string) { jpm.Log(common.LogError, str) },
		Panic:    jpm.Panic,
		CallerID: fmt.Sprintf("JobID=%v, Part#=%d", jpm.Plan().JobID, jpm.Plan().PartNum),
		Cancel:   jpm.jobMgr.Cancel,
	}

	retryOptions := policy.RetryOptions{
		MaxRetries:    UploadMaxTries,
		TryTimeout:    UploadTryTimeout,
		RetryDelay:    UploadRetryDelay,
		MaxRetryDelay: UploadMaxRetryDelay,
	}

	var userAgent string
	if fromTo.From() == common.ELocation.S3() {
		userAgent = common.S3ImportUserAgent
	} else if fromTo.From() == common.ELocation.GCP() {
		userAgent = common.GCPImportUserAgent
	} else if fromTo.From() == common.ELocation.Benchmark() || fromTo.To() == common.ELocation.Benchmark() {
		userAgent = common.BenchmarkUserAgent
	} else {
		userAgent = common.GetLifecycleMgr().AddUserAgentPrefix(common.UserAgent)
	}
	telemetryOptions := policy.TelemetryOptions{ApplicationID: userAgent}

	httpClient := jpm.jobMgr.HttpClient()
	networkStats := jpm.jobMgr.PipelineNetworkStats()
	logOptions := jpm.jobMgr.PipelineLogInfo()

	var sourceTrailingDot *common.TrailingDotOption
	var trailingDot *common.TrailingDotOption
	var from *common.Location
	if (fromTo.IsS2S() || fromTo.IsDownload()) && (fromTo.From() == common.ELocation.File()) {
		sourceTrailingDot = &jpm.planMMF.Plan().DstFileData.TrailingDot
	}
	if fromTo.IsS2S() && fromTo.To() == common.ELocation.File() ||
		fromTo.IsUpload() && fromTo.To() == common.ELocation.File() ||
		fromTo.IsDownload() && fromTo.From() == common.ELocation.File() ||
		fromTo.IsSetProperties() && fromTo.From() == common.ELocation.File() ||
		fromTo.IsDelete() && fromTo.From() == common.ELocation.File() {
		trailingDot = &jpm.planMMF.Plan().DstFileData.TrailingDot
		if fromTo.IsS2S() {
			from = to.Ptr(fromTo.From())
		}
	}
	jpm.s2sSourceClientOptions = NewClientOptions(retryOptions, telemetryOptions, httpClient, nil, logOptions, sourceTrailingDot, nil)
	jpm.clientOptions = NewClientOptions(retryOptions, telemetryOptions, httpClient, networkStats, logOptions, trailingDot, from)
}

func (jpm *jobPartMgr) createPipelines(ctx context.Context) {
	if atomic.SwapUint32(&jpm.atomicPipelinesInitedIndicator, 1) != 0 {
		panic("init client and pipelines for same jobPartMgr twice")
	}
	fromTo := jpm.planMMF.Plan().FromTo
	credInfo := jpm.credInfo
	if jpm.credInfo.CredentialType == common.ECredentialType.Unknown() {
		credInfo = jpm.jobMgr.getInMemoryTransitJobState().CredentialInfo
	}
	var userAgent string
	if fromTo.From() == common.ELocation.S3() {
		userAgent = common.S3ImportUserAgent
	} else if fromTo.From() == common.ELocation.GCP() {
		userAgent = common.GCPImportUserAgent
	} else if fromTo.From() == common.ELocation.Benchmark() || fromTo.To() == common.ELocation.Benchmark() {
		userAgent = common.BenchmarkUserAgent
	} else {
		userAgent = common.GetLifecycleMgr().AddUserAgentPrefix(common.UserAgent)
	}

	credOption := common.CredentialOpOptions{
		LogInfo:  func(str string) { jpm.Log(common.LogInfo, str) },
		LogError: func(str string) { jpm.Log(common.LogError, str) },
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

	// Create source info provider's pipeline for S2S copy or download (in some cases).
	// BlobFS and Blob will utilize the Blob source info provider, as they are the "same" resource, but provide different details on both endpoints
	if (fromTo.IsS2S() || fromTo.IsDownload()) && (fromTo.From() == common.ELocation.Blob() || fromTo.From() == common.ELocation.BlobFS()) {
		// Prepare to pull dfs properties if we're working with BlobFS
		if fromTo.From() == common.ELocation.BlobFS() || jpm.Plan().PreservePermissions.IsTruthy() || jpm.Plan().PreservePOSIXProperties {
			credential := common.CreateBlobFSCredential(ctx, credInfo, credOption)
			jpm.secondarySourceProviderPipeline = NewBlobFSPipeline(
				credential,
				azbfs.PipelineOptions{
					Log: jpm.jobMgr.PipelineLogInfo().ToPipelineLogOptions(),
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

	switch {
	case fromTo.IsS2S() && (fromTo.To() == common.ELocation.Blob() || fromTo.To() == common.ELocation.BlobFS()), // destination determines pipeline for S2S, blobfs uses blob for S2S
		 fromTo.IsUpload() && fromTo.To() == common.ELocation.Blob(), // destination determines pipeline for upload
		 fromTo.IsDownload() && fromTo.From() == common.ELocation.Blob(), // source determines pipeline for download
		 fromTo.IsSetProperties() && (fromTo.From() == common.ELocation.Blob() || fromTo.From() == common.ELocation.BlobFS()), // source determines pipeline for set properties, blobfs uses blob for set properties
		 fromTo.IsDelete() && fromTo.From() == common.ELocation.Blob(): // ditto for delete
		jpm.Log(common.LogInfo, fmt.Sprintf("JobID=%v, credential type: %v", jpm.Plan().JobID, credInfo.CredentialType))

		// If we need to write specifically to the gen2 endpoint, we should have this available.
		if fromTo.To() == common.ELocation.BlobFS() || jpm.Plan().PreservePermissions.IsTruthy() || jpm.Plan().PreservePOSIXProperties {
			credential := common.CreateBlobFSCredential(ctx, credInfo, credOption)
			jpm.secondaryPipeline = NewBlobFSPipeline(
				credential,
				azbfs.PipelineOptions{
					Log: jpm.jobMgr.PipelineLogInfo().ToPipelineLogOptions(),
					Telemetry: azbfs.TelemetryOptions{
						Value: userAgent,
					},
				},
				xferRetryOption,
				jpm.pacer,
				jpm.jobMgr.HttpClient(),
				statsAccForSip)
		}
	case fromTo.IsUpload() && fromTo.To() == common.ELocation.BlobFS(), // Blobfs up/down/delete use the dfs endpoint
		 fromTo.IsDownload() && fromTo.From() == common.ELocation.BlobFS(),
		 fromTo.IsDelete() && fromTo.From() == common.ELocation.BlobFS():
		credential := common.CreateBlobFSCredential(ctx, credInfo, credOption)
		jpm.Log(common.LogInfo, fmt.Sprintf("JobID=%v, credential type: %v", jpm.Plan().JobID, credInfo.CredentialType))

		jpm.pipeline = NewBlobFSPipeline(
			credential,
			azbfs.PipelineOptions{
				Log: jpm.jobMgr.PipelineLogInfo().ToPipelineLogOptions(),
				Telemetry: azbfs.TelemetryOptions{
					Value: userAgent,
				},
			},
			xferRetryOption,
			jpm.pacer,
			jpm.jobMgr.HttpClient(),
			jpm.jobMgr.PipelineNetworkStats())
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

func (jpm *jobPartMgr) ShouldLog(level common.LogLevel) bool  { return jpm.jobMgr.ShouldLog(level) }
func (jpm *jobPartMgr) Log(level common.LogLevel, msg string) { jpm.jobMgr.Log(level, msg) }
func (jpm *jobPartMgr) Panic(err error)                         { jpm.jobMgr.Panic(err) }
func (jpm *jobPartMgr) ChunkStatusLogger() common.ChunkStatusLogger {
	return jpm.jobMgr.ChunkStatusLogger()
}

func (jpm *jobPartMgr) CredentialInfo() common.CredentialInfo {
	return jpm.credInfo
}

func (jpm *jobPartMgr) S2SSourceCredentialInfo() common.CredentialInfo {
	return jpm.s2sSourceCredInfo
}

func (jpm *jobPartMgr) ClientOptions() azcore.ClientOptions {
	return jpm.clientOptions
}

func (jpm *jobPartMgr) S2SSourceClientOptions() azcore.ClientOptions {
	return jpm.s2sSourceClientOptions
}

func (jpm *jobPartMgr) CredentialOpOptions() *common.CredentialOpOptions {
	return jpm.credOption
}

func (jpm *jobPartMgr) Pipeline() pipeline.Pipeline {
	return jpm.pipeline
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
// func (jpm *jobPartMgr) numberOfTransfersDone() uint32 {	return atomic.LoadUint32(&jpm.numberOfTransfersDone_doNotUse)}

// setNumberOfTransfersDone sets the number of transfers done to a specific value
// in a thread safe manner
// func (jppi *jobPartPlanInfo) setNumberOfTransfersDone(val uint32) {
//	atomic.StoreUint32(&jPartPlanInfo.numberOfTransfersDone_doNotUse, val)
// }
