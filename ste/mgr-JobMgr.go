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
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var _ IJobMgr = &jobMgr{}

type PartNumber = common.PartNumber

type IJobMgr interface {
	JobID() common.JobID
	JobPartMgr(partNum PartNumber) (IJobPartMgr, bool)
	// Throughput() XferThroughput
	// If existingPlanMMF is nil, a new MMF is opened.
	AddJobPart(args *AddJobPartArgs) IJobPartMgr

	ResumeTransfers(appCtx context.Context)
	ResetFailedTransfersCount()
	AllTransfersScheduled() bool
	ConfirmAllTransfersScheduled()
	ResetAllTransfersScheduled()
	GetTotalNumFilesProcessed() int64
	AddTotalNumFilesProcessed(numFiles int64)
	Reset(context.Context, string) IJobMgr
	PipelineLogInfo() LogOptions
	ReportJobPartDone(jobPartProgressInfo)
	Context() context.Context
	Cancel()
	// TODO: added for debugging purpose. remove later
	OccupyAConnection()
	// TODO: added for debugging purpose. remove later
	ReleaseAConnection()
	// TODO: added for debugging purpose. remove later
	ActiveConnections() int64
	GetPerfInfo() (displayStrings []string, constraint common.PerfConstraint)
	// Close()
	ChunkStatusLogger() common.ChunkStatusLogger
	PipelineNetworkStats() *PipelineNetworkStats
	getOverwritePrompter() *overwritePrompter
	common.ILoggerCloser

	/* Status related functions */
	SendJobPartCreatedMsg(msg JobPartCreatedMsg)
	SendXferDoneMsg(msg xferDoneMsg)
	ListJobSummary() common.ListJobSummaryResponse
	ResurrectSummary(js common.ListJobSummaryResponse)

	/* Ported from jobsAdmin() */
	ScheduleTransfer(priority common.JobPriority, jptm IJobPartTransferMgr)
	ScheduleChunk(priority common.JobPriority, chunkFunc chunkFunc)

	/* Some comment */
	IterateJobParts(readonly bool, f func(k common.PartNumber, v IJobPartMgr))
	TransferDirection() common.TransferDirection
	AddSuccessfulBytesInActiveFiles(n int64)
	SuccessfulBytesInActiveFiles() uint64
	CancelPauseJobOrder(desiredJobStatus common.JobStatus) common.CancelPauseResumeResponse
	IsDaemon() bool

	// Cleanup Functions
	DeferredCleanupJobMgr()
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func NewJobMgr(concurrency ConcurrencySettings, jobID common.JobID, appCtx context.Context, cpuMon common.CPUMonitor, level common.LogLevel,
	commandString string, tuner ConcurrencyTuner,
	pacer PacerAdmin, slicePool common.ByteSlicePooler, cacheLimiter common.CacheLimiter, fileCountLimiter common.CacheLimiter,
	jobLogger common.ILoggerResetable, daemonMode bool) IJobMgr {
	const channelSize = 100000
	// PartsChannelSize defines the number of JobParts which can be placed into the
	// parts channel. Any JobPart which comes from FE and partChannel is full,
	// has to wait and enumeration of transfer gets blocked till then.
	// TODO : PartsChannelSize Needs to be discussed and can change.
	const PartsChannelSize = 10000

	// partsCh is the channel in which all JobParts are put
	// for scheduling transfers. When the next JobPart order arrives
	// transfer engine creates the JobPartPlan file and
	// puts the JobPartMgr in partchannel
	// from which each part is picked up one by one
	// and transfers of that JobPart are scheduled
	partsCh := make(chan IJobPartMgr, PartsChannelSize)
	// Create normal & low transfer/chunk channels
	normalTransferCh, normalChunkCh := make(chan IJobPartTransferMgr, channelSize), make(chan chunkFunc, channelSize)
	lowTransferCh, lowChunkCh := make(chan IJobPartTransferMgr, channelSize), make(chan chunkFunc, channelSize)

	// atomicAllTransfersScheduled is set to 1 since this api is also called when new job part is ordered.
	enableChunkLogOutput := level == common.LogDebug

	/* Create book-keeping channels */
	jobPartProgressCh := make(chan jobPartProgressInfo)
	var jstm jobStatusManager
	jstm.respChan = make(chan common.ListJobSummaryResponse)
	jstm.listReq = make(chan struct{})
	jstm.partCreated = make(chan JobPartCreatedMsg, 100)
	jstm.xferDone = make(chan xferDoneMsg, 1000)
	jstm.xferDoneDrained = make(chan struct{})
	jstm.statusMgrDone = make(chan struct{})
	// Different logger for each job.
	if jobLogger == nil {
		jobLogger = common.NewJobLogger(jobID, common.ELogLevel.Debug(), common.LogPathFolder, "" /* logFileNameSuffix */)
		jobLogger.OpenLog()
	}

	jm := jobMgr{jobID: jobID, jobPartMgrs: newJobPartToJobPartMgr(),
		logger:               jobLogger,
		chunkStatusLogger:    common.NewChunkStatusLogger(jobID, cpuMon, common.LogPathFolder, enableChunkLogOutput),
		concurrency:          concurrency,
		overwritePrompter:    newOverwritePrompter(),
		pipelineNetworkStats: newPipelineNetworkStats(tuner), // let the stats coordinate with the concurrency tuner
		initMu:               &sync.Mutex{},
		jobPartProgress:      jobPartProgressCh,
		reportCancelCh:       make(chan struct{}, 1),
		coordinatorChannels: CoordinatorChannels{
			partsChannel:     partsCh,
			normalTransferCh: normalTransferCh,
			lowTransferCh:    lowTransferCh,
		},
		xferChannels: XferChannels{
			partsChannel:     partsCh,
			normalTransferCh: normalTransferCh,
			lowTransferCh:    lowTransferCh,
			normalChunckCh:   normalChunkCh,
			lowChunkCh:       lowChunkCh,
			closeTransferCh:  make(chan struct{}, 100),
			scheduleCloseCh:  make(chan struct{}, 1),
		},
		poolSizingChannels: poolSizingChannels{ // all deliberately unbuffered, because pool sizer routine works in lock-step with these - processing them as they happen, never catching up on populated buffer later
			entryNotificationCh: make(chan struct{}),
			exitNotificationCh:  make(chan struct{}),
			scalebackRequestCh:  make(chan struct{}),
			requestSlowTuneCh:   make(chan struct{}),
			done:                make(chan struct{}, 1),
		},
		concurrencyTuner: tuner,
		pacer:            pacer,
		slicePool:        slicePool,
		cacheLimiter:     cacheLimiter,
		fileCountLimiter: fileCountLimiter,
		cpuMon:           cpuMon,
		jstm:             &jstm,
		isDaemon:         daemonMode,
		/*Other fields remain zero-value until this job is scheduled */}
	jm.Reset(appCtx, commandString)
	// One routine constantly monitors the partsChannel.  It takes the JobPartManager from
	// the Channel and schedules the transfers of that JobPart.
	go jm.scheduleJobParts()
	// In addition to the main pool (which is governed ja.poolSizer), we spin up a separate set of workers to process initiation of transfers
	// (so that transfer initiation can't starve out progress on already-scheduled chunks.
	// (Not sure whether that can really happen, but this protects against it anyway.)
	// Perhaps MORE importantly, doing this separately gives us more CONTROL over how we interact with the file system.
	for cc := 0; cc < concurrency.TransferInitiationPoolSize.Value; cc++ {
		go jm.transferProcessor(cc)
	}

	go jm.reportJobPartDoneHandler()
	go jm.handleStatusUpdateMessage()

	return &jm
}

func (jm *jobMgr) getOverwritePrompter() *overwritePrompter {
	return jm.overwritePrompter
}

func (jm *jobMgr) Reset(appCtx context.Context, commandString string) IJobMgr {
	// jm.logger.OpenLog()
	// log the user given command to the job log file.
	// since the log file is opened in case of resume, list and many other operations
	// for which commandString passed is empty, the length check is added
	if len(commandString) > 0 {
		jm.logger.Log(common.LogError, fmt.Sprintf("Job-Command %s", commandString))
	}
	jm.logConcurrencyParameters()
	jm.ctx, jm.cancel = context.WithCancel(appCtx)
	atomic.StoreUint64(&jm.atomicNumberOfBytesCovered, 0)
	atomic.StoreUint64(&jm.atomicTotalBytesToXfer, 0)
	jm.partsDone = 0
	return jm
}

func (jm *jobMgr) logConcurrencyParameters() {
	level := common.LogWarning // log all this stuff at warning level, so that it can still be see it when running at that level. (It won't have the WARN prefix, because we don't add that)

	jm.logger.Log(level, fmt.Sprintf("Number of CPUs: %d", runtime.NumCPU()))
	// TODO: label max file buffer ram with how we obtained it (env var or default)
	jm.logger.Log(level, fmt.Sprintf("Max file buffer RAM %.3f GB",
		float32(jm.cacheLimiter.Limit())/(1024*1024*1024)))

	dynamicMessage := ""
	if jm.concurrency.AutoTuneMainPool() {
		dynamicMessage = " will be dynamically tuned up to "
	}
	jm.logger.Log(level, fmt.Sprintf("Max concurrent network operations: %s%d (%s)",
		dynamicMessage,
		jm.concurrency.MaxMainPoolSize.Value,
		jm.concurrency.MaxMainPoolSize.GetDescription()))

	jm.logger.Log(level, fmt.Sprintf("Check CPU usage when dynamically tuning concurrency: %t (%s)",
		jm.concurrency.CheckCpuWhenTuning.Value,
		jm.concurrency.CheckCpuWhenTuning.GetDescription()))

	jm.logger.Log(level, fmt.Sprintf("Max concurrent transfer initiation routines: %d (%s)",
		jm.concurrency.TransferInitiationPoolSize.Value,
		jm.concurrency.TransferInitiationPoolSize.GetDescription()))

	jm.logger.Log(level, fmt.Sprintf("Max enumeration routines: %d (%s)",
		jm.concurrency.EnumerationPoolSize.Value,
		jm.concurrency.EnumerationPoolSize.GetDescription()))

	jm.logger.Log(level, fmt.Sprintf("Parallelize getting file properties (file.Stat): %t (%s)",
		jm.concurrency.ParallelStatFiles.Value,
		jm.concurrency.ParallelStatFiles.GetDescription()))

	jm.logger.Log(level, fmt.Sprintf("Max open files when downloading: %d (auto-computed)",
		jm.concurrency.MaxOpenDownloadFiles))
}

// jobMgrInitState holds one-time init structures (such as SIPM), that initialize when the first part is added.
type jobMgrInitState struct {
	securityInfoPersistenceManager *securityInfoPersistenceManager
	folderCreationTracker          FolderCreationTracker
	folderDeletionManager          common.FolderDeletionManager
	exclusiveDestinationMapHolder  *atomic.Value
}

// jobMgr represents the runtime information for a Job
type jobMgr struct {
	// NOTE: for the 64 bit atomic functions to work on a 32 bit system, we have to guarantee the right 64-bit alignment
	// so the 64 bit integers are placed first in the struct to avoid future breaks
	// refer to: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	atomicNumberOfBytesCovered uint64
	atomicTotalBytesToXfer     uint64
	// atomicCurrentConcurrentConnections defines the number of active goroutines performing the transfer / executing the chunk func
	// TODO: added for debugging purpose. remove later
	atomicCurrentConcurrentConnections int64
	/* Pool sizer related values */
	atomicSuccessfulBytesInActiveFiles int64 // atomic 64-bit values should always be at the start of a struct to ensure alignment
	atomicCurrentMainPoolSize          int32
	// atomicAllTransfersScheduled defines whether all job parts have been iterated and resumed or not
	atomicAllTransfersScheduled     int32
	atomicFinalPartOrderedIndicator int32
	atomicTransferDirection         common.TransferDirection
	atomicTotalFilesProcessed       int64 // Number of files processed across multiple job parts
	concurrency                     ConcurrencySettings
	logger                          common.ILoggerResetable
	chunkStatusLogger               common.ChunkStatusLoggerCloser
	jobID                           common.JobID // The Job's unique ID
	ctx                             context.Context
	cancel                          context.CancelFunc
	pipelineNetworkStats            *PipelineNetworkStats

	jobPartMgrs jobPartToJobPartMgr // The map of part #s to JobPartMgrs

	// reportCancelCh to close the report thread.
	reportCancelCh chan struct{}

	// partsDone keep the count of completed part of the Job.
	partsDone uint32
	// throughput  common.CountPerSecond // TODO: Set LastCheckedTime to now

	// only a single instance of the prompter is needed for all transfers
	overwritePrompter *overwritePrompter

	initMu    *sync.Mutex
	initState *jobMgrInitState

	jobPartProgress chan jobPartProgressInfo

	coordinatorChannels CoordinatorChannels
	xferChannels        XferChannels
	poolSizingChannels  poolSizingChannels
	concurrencyTuner    ConcurrencyTuner
	cpuMon              common.CPUMonitor
	pacer               PacerAdmin
	slicePool           common.ByteSlicePooler
	cacheLimiter        common.CacheLimiter
	fileCountLimiter    common.CacheLimiter
	jstm                *jobStatusManager

	isDaemon bool /* is it running as service */
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (jm *jobMgr) Progress() (uint64, uint64) {
	return atomic.LoadUint64(&jm.atomicNumberOfBytesCovered),
		atomic.LoadUint64(&jm.atomicTotalBytesToXfer)
}

// func (jm *jobMgr) Throughput() XferThroughput { return jm.throughput }

// JobID returns the JobID that this jobMgr managers
func (jm *jobMgr) JobID() common.JobID { return jm.jobID }

// JobPartMgr looks up a job's part
func (jm *jobMgr) JobPartMgr(partNumber PartNumber) (IJobPartMgr, bool) {
	return jm.jobPartMgrs.Get(partNumber)
}

// Add 1 to the active number of goroutine performing the transfer or executing the chunkFunc
// TODO: added for debugging purpose. remove later
func (jm *jobMgr) OccupyAConnection() {
	atomic.AddInt64(&jm.atomicCurrentConcurrentConnections, 1)
}

// Sub 1 from the active number of goroutine performing the transfer or executing the chunkFunc
// TODO: added for debugging purpose. remove later
func (jm *jobMgr) ReleaseAConnection() {
	atomic.AddInt64(&jm.atomicCurrentConcurrentConnections, -1)
}

// returns the number of goroutines actively performing the transfer / executing the chunkFunc
// TODO: added for debugging purpose. remove later
func (jm *jobMgr) ActiveConnections() int64 {
	return atomic.LoadInt64(&jm.atomicCurrentConcurrentConnections)
}

// GetPerfStrings returns strings that may be logged for performance diagnostic purposes
// The number and content of strings may change as we enhance our perf diagnostics
func (jm *jobMgr) GetPerfInfo() (displayStrings []string, constraint common.PerfConstraint) {
	atomicTransferDirection := jm.atomicTransferDirection.AtomicLoad()

	// get data appropriate to our current transfer direction
	chunkStateCounts := jm.chunkStatusLogger.GetCounts(atomicTransferDirection)

	// convert the counts to simple strings for consumption by callers
	const format = "%c: %2d"
	result := make([]string, len(chunkStateCounts)+2)
	total := int64(0)
	for i, c := range chunkStateCounts {
		result[i] = fmt.Sprintf(format, c.WaitReason.Name[0], c.Count)
		total += c.Count
	}
	result[len(result)-2] = fmt.Sprintf(format, 'T', total)

	// Add an exact count of the number of running goroutines in the main pool
	// The states, above, that run inside that pool (basically the H and B states) will sum to
	// a value <= this value. But without knowing this value, its harder to be sure if they are at the limit
	// or not, especially if we are dynamically tuning the pool size.
	result[len(result)-1] = fmt.Sprintf(strings.Replace(format, "%c", "%s", -1), "GRs", jm.CurrentMainPoolSize())

	con := jm.chunkStatusLogger.GetPrimaryPerfConstraint(atomicTransferDirection, jm.PipelineNetworkStats())

	// logging from here is a bit of a hack
	// TODO: can we find a better way to get this info into the log?  The caller is at app level,
	//    not job level, so can't log it directly AFAICT.
	jm.logPerfInfo(result, con)

	return result, con
}

func (jm *jobMgr) logPerfInfo(displayStrings []string, constraint common.PerfConstraint) {
	constraintString := fmt.Sprintf("primary performance constraint is %s", constraint)
	msg := fmt.Sprintf("PERF: %s. States: %s", constraintString, strings.Join(displayStrings, ", "))
	jm.Log(common.LogInfo, msg)
}

type AddJobPartArgs struct {
	PartNum         PartNumber
	PlanFile        JobPartPlanFileName
	ExistingPlanMMF *JobPartPlanMMF

	// These clients are valid if this fits the FromTo. i.e if
	// we're uploading
	SrcClient  *common.ServiceClient
	DstClient  *common.ServiceClient
	SrcIsOAuth bool // true if source is authenticated via token

	ScheduleTransfers bool

	// This channel will be closed once all transfers in this part are done
	CompletionChan chan struct{}
}

// initializeJobPartPlanInfo func initializes the JobPartPlanInfo handler for given JobPartOrder
func (jm *jobMgr) AddJobPart(args *AddJobPartArgs) IJobPartMgr {
	jpm := &jobPartMgr{
		jobMgr:            jm,
		filename:          args.PlanFile,
		srcServiceClient:  args.SrcClient,
		dstServiceClient:  args.DstClient,
		pacer:             jm.pacer,
		slicePool:         jm.slicePool,
		cacheLimiter:      jm.cacheLimiter,
		fileCountLimiter:  jm.fileCountLimiter,
		closeOnCompletion: args.CompletionChan,
		srcIsOAuth:        args.SrcIsOAuth,
	}
	// If an existing plan MMF was supplied, re use it. Otherwise, init a new one.
	if args.ExistingPlanMMF == nil {
		jpm.planMMF = jpm.filename.Map()
	} else {
		jpm.planMMF = args.ExistingPlanMMF
	}

	jm.jobPartMgrs.Set(args.PartNum, jpm)
	jm.setFinalPartOrdered(args.PartNum, jpm.planMMF.Plan().IsFinalPart)
	jm.setDirection(jpm.Plan().FromTo)

	jm.initMu.Lock()
	defer jm.initMu.Unlock()
	if jm.initState == nil {
		var logger common.ILogger = jm
		jm.initState = &jobMgrInitState{
			securityInfoPersistenceManager: newSecurityInfoPersistenceManager(jm.ctx),
			folderCreationTracker:          NewFolderCreationTracker(jpm.Plan().Fpo, jpm.Plan()),
			folderDeletionManager:          common.NewFolderDeletionManager(jm.ctx, jpm.Plan().Fpo, logger),
			exclusiveDestinationMapHolder:  &atomic.Value{},
		}
		jm.initState.exclusiveDestinationMapHolder.Store(common.NewExclusiveStringMap(jpm.Plan().FromTo, runtime.GOOS))
	}
	jpm.jobMgrInitState = jm.initState // so jpm can use it as much as desired without locking (since the only mutation is the init in jobManager. As far as jobPartManager is concerned, the init state is read-only
	jpm.exclusiveDestinationMap = jm.getExclusiveDestinationMap(args.PartNum, jpm.Plan().FromTo)

	if args.ScheduleTransfers {
		// If the schedule transfer is set to true
		// Instead of the scheduling the Transfer for given JobPart
		// JobPart is put into the partChannel
		// from where it is picked up and scheduled
		// jpm.ScheduleTransfers(jm.ctx, make(map[string]int), make(map[string]int))
		jm.QueueJobParts(jpm)
	}
	return jpm
}

func (jm *jobMgr) AddJobOrder(order common.CopyJobPartOrderRequest) IJobPartMgr {
	jppfn := JobPartPlanFileName(fmt.Sprintf(JobPartPlanFileNameFormat, order.JobID.String(), 0, DataSchemaVersion))
	jppfn.Create(order) // Convert the order to a plan file

	jpm := &jobPartMgr{
		jobMgr:           jm,
		filename:         jppfn,
		sourceSAS:        order.SourceRoot.SAS,
		destinationSAS:   order.DestinationRoot.SAS,
		pacer:            jm.pacer,
		slicePool:        jm.slicePool,
		cacheLimiter:     jm.cacheLimiter,
		fileCountLimiter: jm.fileCountLimiter,
		srcIsOAuth:       order.S2SSourceCredentialType.IsAzureOAuth(),
	}
	jpm.planMMF = jpm.filename.Map()
	jm.jobPartMgrs.Set(order.PartNum, jpm)
	jm.setFinalPartOrdered(order.PartNum, jpm.planMMF.Plan().IsFinalPart)
	jm.setDirection(jpm.Plan().FromTo)

	jm.initMu.Lock()
	defer jm.initMu.Unlock()
	if jm.initState == nil {
		var logger common.ILogger = jm
		jm.initState = &jobMgrInitState{
			securityInfoPersistenceManager: newSecurityInfoPersistenceManager(jm.ctx),
			folderCreationTracker:          NewFolderCreationTracker(jpm.Plan().Fpo, jpm.Plan()),
			folderDeletionManager:          common.NewFolderDeletionManager(jm.ctx, jpm.Plan().Fpo, logger),
			exclusiveDestinationMapHolder:  &atomic.Value{},
		}
		jm.initState.exclusiveDestinationMapHolder.Store(common.NewExclusiveStringMap(jpm.Plan().FromTo, runtime.GOOS))
	}
	jpm.jobMgrInitState = jm.initState // so jpm can use it as much as desired without locking (since the only mutation is the init in jobManager. As far as jobPartManager is concerned, the init state is read-only
	jpm.exclusiveDestinationMap = jm.getExclusiveDestinationMap(order.PartNum, jpm.Plan().FromTo)

	jm.QueueJobParts(jpm)
	return jpm
}

func (jm *jobMgr) setFinalPartOrdered(partNum PartNumber, isFinalPart bool) {
	newVal := int32(common.Iff(isFinalPart, 1, 0))
	oldVal := atomic.SwapInt32(&jm.atomicFinalPartOrderedIndicator, newVal)
	if newVal == 0 && oldVal == 1 {
		// we just cleared the flag. Sanity check that.
		if partNum == 0 {
			// We can't complain because, when resuming a job, there are actually TWO calls made the ResurrectJob.
			// The effect is that all the parts are ordered... then all the parts are ordered _again_ (with new JobPartManagers replacing those from the first time)
			// (The first resurrect is from GetJobDetails and the second is from ResumeJobOrder)
			// So we don't object if the _first_ part clears the flag. The assumption we make, by allowing this special case here, is that
			// the first part will be scheduled before any higher-numbered part.  As long as that assumption is true, this is safe.
			// TODO: do we really need to to Resurrect the job twice?
		} else {
			// But we do object if any other part clears the flag, since that wouldn't make sense.
			panic("Error: another job part was scheduled after the final part")
		}

	}
}

// Remembers which direction we are running in (upload, download or neither (for service to service))
// It actually remembers the direction that our most recently-added job PART is running in,
// because that's where the fromTo information can be found,
// but we assume that all the job parts are running in the same direction.
// TODO: Optimize this when it's necessary for delete.
func (jm *jobMgr) setDirection(fromTo common.FromTo) {
	if fromTo.IsUpload() {
		jm.atomicTransferDirection.AtomicStore(common.ETransferDirection.Upload())
	}
	if fromTo.IsDownload() {
		jm.atomicTransferDirection.AtomicStore(common.ETransferDirection.Download())
		jm.RequestTuneSlowly()
	}
	if fromTo.IsS2S() {
		jm.atomicTransferDirection.AtomicStore(common.ETransferDirection.S2SCopy())
		jm.RequestTuneSlowly()
	}
}

// can't do this at time of constructing the jobManager, because it doesn't know fromTo at that time
func (jm *jobMgr) getExclusiveDestinationMap(partNum PartNumber, fromTo common.FromTo) *common.ExclusiveStringMap {
	return jm.initState.exclusiveDestinationMapHolder.Load().(*common.ExclusiveStringMap)
}

func (jm *jobMgr) PipelineNetworkStats() *PipelineNetworkStats {
	return jm.pipelineNetworkStats
}

// ScheduleTransfers schedules this job part's transfers. It is called when a new job part is ordered & is also called to resume a paused Job
func (jm *jobMgr) ResumeTransfers(appCtx context.Context) {
	jm.Reset(appCtx, "")
	// Since while creating the JobMgr, atomicAllTransfersScheduled is set to true
	// reset it to false while resuming it
	jm.ResetAllTransfersScheduled()
	jm.jobPartMgrs.Iterate(false, func(p common.PartNumber, jpm IJobPartMgr) {
		jm.QueueJobParts(jpm)
		// jpm.ScheduleTransfers(jm.ctx, includeTransfer, excludeTransfer)
	})
}

// When a previously job is resumed, ResetFailedTransfersCount
// resets the number of failed transfers
// persists the correct count of TotalBytesExpected
func (jm *jobMgr) ResetFailedTransfersCount() {
	// Ensure total bytes expected is correct
	totalBytesExpected := uint64(0)

	jm.jobPartMgrs.Iterate(false, func(partNum common.PartNumber, jpm IJobPartMgr) {
		jpm.ResetFailedTransfersCount()

		// After resuming a failed job, the percentComplete reporting needs to carry correct value for bytes expected.
		jpp := jpm.Plan()
		for t := uint32(0); t < jpp.NumTransfers; t++ {
			jppt := jpp.Transfer(t)
			totalBytesExpected += uint64(jppt.SourceSize)
		}
	})

	// Reset job summary in status manager
	summaryResp := jm.ListJobSummary()
	summaryResp.TransfersFailed = 0
	summaryResp.FailedTransfers = []common.TransferDetail{}
	summaryResp.TotalBytesExpected = totalBytesExpected

	jm.ResurrectSummary(summaryResp)
}

// AllTransfersScheduled returns whether Job has completely resumed or not
func (jm *jobMgr) AllTransfersScheduled() bool {
	return atomic.LoadInt32(&jm.atomicAllTransfersScheduled) == 1
}

// ConfirmAllTransfersScheduled sets the atomicAllTransfersScheduled to true
func (jm *jobMgr) ConfirmAllTransfersScheduled() {
	atomic.StoreInt32(&jm.atomicAllTransfersScheduled, 1)
}

// ResetAllTransfersScheduled sets the ResetAllTransfersScheduled to false
func (jm *jobMgr) ResetAllTransfersScheduled() {
	atomic.StoreInt32(&jm.atomicAllTransfersScheduled, 0)
}

func (jm *jobMgr) GetTotalNumFilesProcessed() int64 {
	return atomic.LoadInt64(&jm.atomicTotalFilesProcessed)
}

func (jm *jobMgr) AddTotalNumFilesProcessed(numFiles int64) {
	atomic.AddInt64(&jm.atomicTotalFilesProcessed, numFiles)
}

// ReportJobPartDone is called to report that a job part completed or failed
func (jm *jobMgr) ReportJobPartDone(progressInfo jobPartProgressInfo) {
	jm.jobPartProgress <- progressInfo
}

func (jm *jobMgr) reportJobPartDoneHandler() {
	var haveFinalPart bool
	var jobProgressInfo jobPartProgressInfo
	shouldLog := jm.ShouldLog(common.LogInfo)

	for {
		select {
		case <-jm.reportCancelCh:
			jobPart0Mgr, ok := jm.jobPartMgrs.Get(0)
			if ok {
				part0plan := jobPart0Mgr.Plan()
				if part0plan.JobStatus() == common.EJobStatus.InProgress() ||
					part0plan.JobStatus() == common.EJobStatus.Cancelling() {
					jm.Panic(fmt.Errorf("reportCancelCh received cancel event while job still not completed, Job(%s) in state: %s",
						jm.jobID.String(), part0plan.JobStatus()))
				}
			} else {
				jm.Log(common.LogError, "part0Plan of job invalid")
			}
			jm.Log(common.LogInfo, "reportJobPartDoneHandler done called")
			return

		case partProgressInfo := <-jm.jobPartProgress:
			jobPart0Mgr, ok := jm.jobPartMgrs.Get(0)
			if !ok {
				jm.Panic(fmt.Errorf("Failed to find Job %v, Part #0", jm.jobID))
			}
			part0Plan := jobPart0Mgr.Plan()
			jobStatus := part0Plan.JobStatus() // status of part 0 is status of job as a whole
			partsDone := atomic.AddUint32(&jm.partsDone, 1)
			jobProgressInfo.transfersCompleted += partProgressInfo.transfersCompleted
			jobProgressInfo.transfersSkipped += partProgressInfo.transfersSkipped
			jobProgressInfo.transfersFailed += partProgressInfo.transfersFailed

			if partProgressInfo.completionChan != nil {
				close(partProgressInfo.completionChan)
			}

			// If the last part is still awaited or other parts all still not complete,
			// JobPart 0 status is not changed (unless we are cancelling)
			haveFinalPart = atomic.LoadInt32(&jm.atomicFinalPartOrderedIndicator) == 1
			allKnownPartsDone := partsDone == jm.jobPartMgrs.Count()
			isCancelling := jobStatus == common.EJobStatus.Cancelling()
			shouldComplete := (haveFinalPart && allKnownPartsDone) || // If we have all of the parts, they should all exit cleanly, so the job can be resumed properly.
				(isCancelling && !haveFinalPart) // If we're cancelling, it's OK to try to exit early; the user already accepted this job cannot be resumed. Outgoing requests will fail anyway, so nothing can properly clean up.
			if shouldComplete {
				// Inform StatusManager that all parts are done.
				if jm.jstm.xferDone != nil {
					close(jm.jstm.xferDone)
				}

				// Wait  for all XferDone messages to be processed by statusManager. Front end
				// depends on JobStatus to determine if we've to quit job. Setting it here without
				// draining XferDone will make it report incorrect statistics.
				jm.waitToDrainXferDone()
				partDescription := "all parts of entire Job"
				if !haveFinalPart {
					if allKnownPartsDone {
						partDescription = "known parts of incomplete Job"
					} else {
						partDescription = "incomplete Job"
					}
				}
				if shouldLog {
					jm.Log(common.LogInfo, fmt.Sprintf("%s %s successfully completed, cancelled or paused", partDescription, jm.jobID.String()))
				}

				switch part0Plan.JobStatus() {
				case common.EJobStatus.Cancelling():
					part0Plan.SetJobStatus(common.EJobStatus.Cancelled())
					if shouldLog {
						jm.Log(common.LogInfo, fmt.Sprintf("%s %v successfully cancelled", partDescription, jm.jobID))
					}
				case common.EJobStatus.InProgress():
					part0Plan.SetJobStatus((common.EJobStatus).EnhanceJobStatusInfo(jobProgressInfo.transfersSkipped > 0,
						jobProgressInfo.transfersFailed > 0,
						jobProgressInfo.transfersCompleted > 0))
				}

				// reset counters
				atomic.StoreUint32(&jm.partsDone, 0)
				jobProgressInfo = jobPartProgressInfo{}

				// flush logs
				jm.chunkStatusLogger.FlushLog() // TODO: remove once we sort out what will be calling CloseLog (currently nothing)
			} //Else log and wait for next part to complete

			if shouldLog {
				jm.Log(common.LogInfo, fmt.Sprintf("is part of Job which %d total number of parts done ", partsDone))
			}
		}
	}
}

func (jm *jobMgr) Context() context.Context { return jm.ctx }
func (jm *jobMgr) Cancel() {
	jm.cancel()
	jm.jobPartProgress <- jobPartProgressInfo{} // in case we're waiting on another job part; we can just shoot in a zeroed out version & achieve a cancel immediately
}
func (jm *jobMgr) ShouldLog(level common.LogLevel) bool  { return jm.logger.ShouldLog(level) }
func (jm *jobMgr) Log(level common.LogLevel, msg string) { jm.logger.Log(level, msg) }
func (jm *jobMgr) PipelineLogInfo() LogOptions {
	return LogOptions{
		Log:       jm.Log,
		ShouldLog: func(level common.LogLevel) bool { return level <= jm.logger.MinimumLogLevel() },
	}
}
func (jm *jobMgr) Panic(err error) { jm.logger.Panic(err) }
func (jm *jobMgr) CloseLog() {
	jm.logger.CloseLog()
	jm.chunkStatusLogger.FlushLog()
}

// DeferredCleanupJobMgr cleanup all the jobMgr resources.
// Warning: DeferredCleanupJobMgr should be called from JobMgrCleanup().
//
//	As this function neither thread safe nor idempotent. So if DeferredCleanupJobMgr called
//	multiple times, it may stuck as receiving channel already closed. Where as JobMgrCleanup()
//	safe in that sense it will do the cleanup only once.
//
// TODO: Add JobsAdmin reference to each JobMgr so that in any circumstances JobsAdmin should not freed,
//
//	while jobMgr running. Whereas JobsAdmin store number JobMgr running  at any time.
//	At that point DeferredCleanupJobMgr() will delete jobMgr from jobsAdmin map.
func (jm *jobMgr) DeferredCleanupJobMgr() {
	jm.Log(common.LogInfo, "DeferredCleanupJobMgr called")

	time.Sleep(60 * time.Second)

	jm.Log(common.LogInfo, "DeferredCleanupJobMgr out of sleep")

	// Call jm.Cancel to signal routines workdone.
	// This will take care of any jobPartMgr release.
	jm.Cancel()

	// Transfer Thread Cleanup.
	jm.cleanupTransferRoutine()

	// Remove JobPartsMgr from jobPartMgr kv.
	jm.deleteJobPartsMgrs()

	// Close chunk status logger.
	jm.cleanupChunkStatusLogger()
	jm.Log(common.LogInfo, "DeferredCleanupJobMgr Exit, Closing the log")

	// Sleep for sometime so that all go routine done with cleanUp and log the progress in job log.
	time.Sleep(60 * time.Second)

	jm.logger.CloseLog()
}

func (jm *jobMgr) ChunkStatusLogger() common.ChunkStatusLogger {
	return jm.chunkStatusLogger
}

func (jm *jobMgr) cleanupChunkStatusLogger() {
	jm.chunkStatusLogger.FlushLog()
	jm.chunkStatusLogger.CloseLogger()
}

// PartsDone returns the number of the Job's parts that are either completed or failed
// func (jm *jobMgr) PartsDone() uint32 { return atomic.LoadUint32(&jm.partsDone) }

// SetPartsDone sets the number of Job's parts that are done (completed or failed)
// func (jm *jobMgr) SetPartsDone(partsDone uint32) { atomic.StoreUint32(&jm.partsDone, partsDone) }

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/* Infra ported to jobManager from JobsAdmin */

type CoordinatorChannels struct {
	partsChannel     chan<- IJobPartMgr         // Write Only
	normalTransferCh chan<- IJobPartTransferMgr // Write-only
	lowTransferCh    chan<- IJobPartTransferMgr // Write-only
}

type XferChannels struct {
	partsChannel     <-chan IJobPartMgr         // Read only
	normalTransferCh <-chan IJobPartTransferMgr // Read-only
	lowTransferCh    <-chan IJobPartTransferMgr // Read-only
	normalChunckCh   chan chunkFunc             // Read-write
	lowChunkCh       chan chunkFunc             // Read-write
	closeTransferCh  chan struct{}
	scheduleCloseCh  chan struct{}
}

type poolSizingChannels struct {
	entryNotificationCh chan struct{}
	exitNotificationCh  chan struct{}
	scalebackRequestCh  chan struct{}
	requestSlowTuneCh   chan struct{}
	done                chan struct{}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/* These functions are to integrate above into JobManager */

func (jm *jobMgr) CurrentMainPoolSize() int {
	return int(atomic.LoadInt32(&jm.atomicCurrentMainPoolSize))
}

func (jm *jobMgr) ScheduleTransfer(priority common.JobPriority, jptm IJobPartTransferMgr) {
	switch priority { // priority determines which channel handles the job part's transfers
	case common.EJobPriority.Normal():
		// jptm.SetChunkChannel(ja.xferChannels.normalChunckCh)
		jm.coordinatorChannels.normalTransferCh <- jptm
	case common.EJobPriority.Low():
		// jptm.SetChunkChannel(ja.xferChannels.lowChunkCh)
		jm.coordinatorChannels.lowTransferCh <- jptm
	default:
		jm.Panic(fmt.Errorf("invalid priority: %q", priority))
	}
}

func (jm *jobMgr) ScheduleChunk(priority common.JobPriority, chunkFunc chunkFunc) {
	switch priority { // priority determines which channel handles the job part's transfers
	case common.EJobPriority.Normal():
		jm.xferChannels.normalChunckCh <- chunkFunc
	case common.EJobPriority.Low():
		jm.xferChannels.lowChunkCh <- chunkFunc
	default:
		jm.Panic(fmt.Errorf("invalid priority: %q", priority))
	}
}

// QueueJobParts puts the given JobPartManager into the partChannel
// from where this JobPartMgr will be picked by a routine and
// its transfers will be scheduled
func (jm *jobMgr) QueueJobParts(jpm IJobPartMgr) {
	jm.coordinatorChannels.partsChannel <- jpm
}

// deleteJobPartsMgrs remove jobPartMgrs from jobPartToJobPartMgr kv.
func (jm *jobMgr) deleteJobPartsMgrs() {
	jm.Log(common.LogInfo, "deleteJobPartsMgrs enter")
	jm.jobPartMgrs.Iterate(false, func(k common.PartNumber, v IJobPartMgr) {
		v.Close()
		delete(jm.jobPartMgrs.m, k)
	})
	jm.Log(common.LogInfo, "deleteJobPartsMgrs exit")
}

// cleanupTransferRoutine closes all the Transfer thread.
// Note: Created the buffer channel so that, if somehow any thread missing(down), it should not stuck.
func (jm *jobMgr) cleanupTransferRoutine() {
	jm.reportCancelCh <- struct{}{}
	jm.xferChannels.scheduleCloseCh <- struct{}{}
	for cc := 0; cc < jm.concurrency.TransferInitiationPoolSize.Value; cc++ {
		jm.xferChannels.closeTransferCh <- struct{}{}
	}
}

// worker that sizes the chunkProcessor pool, dynamically if necessary
func (jm *jobMgr) poolSizer() {

	logConcurrency := func(targetConcurrency int, reason string) {
		switch reason {
		case ConcurrencyReasonNone,
			concurrencyReasonFinished,
			ConcurrencyReasonTunerDisabled:
			return
		default:
			msg := fmt.Sprintf("Trying %d concurrent connections (%s)", targetConcurrency, reason)
			common.GetLifecycleMgr().Info(msg)
			jm.Log(common.LogWarning, msg)
		}
	}

	nextWorkerId := 0
	actualConcurrency := 0
	lastBytesOnWire := int64(0)
	lastBytesTime := time.Now()
	hasHadTimeToStablize := false
	initialMonitoringInterval := time.Duration(4 * time.Second)
	expandedMonitoringInterval := time.Duration(8 * time.Second)
	throughputMonitoringInterval := initialMonitoringInterval
	slowTuneCh := jm.poolSizingChannels.requestSlowTuneCh

	// get initial pool size
	targetConcurrency, reason := jm.concurrencyTuner.GetRecommendedConcurrency(-1, jm.cpuMon.CPUContentionExists())
	logConcurrency(targetConcurrency, reason)

	// loop for ever, driving the actual concurrency towards the most up-to-date target
	for {
		// add or remove a worker if necessary
		if actualConcurrency < targetConcurrency {
			hasHadTimeToStablize = false
			nextWorkerId++
			go jm.chunkProcessor(nextWorkerId) // TODO: make sure this numbering is OK, even if we grow and shrink the pool (the id values don't matter right?)
		} else if actualConcurrency > targetConcurrency {
			hasHadTimeToStablize = false
			jm.poolSizingChannels.scalebackRequestCh <- struct{}{}
		} else if actualConcurrency == 0 && targetConcurrency == 0 {
			jm.Log(common.LogInfo, "Exits Pool sizer")
			return
		}

		// wait for something to happen (maybe ack from the worker of the change, else a timer interval)
		select {
		case <-jm.poolSizingChannels.done:
			targetConcurrency = 0
		case <-jm.poolSizingChannels.entryNotificationCh:
			// new worker has started
			actualConcurrency++
			atomic.StoreInt32(&jm.atomicCurrentMainPoolSize, int32(actualConcurrency))
		case <-jm.poolSizingChannels.exitNotificationCh:
			// worker has exited
			actualConcurrency--
			atomic.StoreInt32(&jm.atomicCurrentMainPoolSize, int32(actualConcurrency))
		case <-slowTuneCh:
			// we've been asked to tune more slowly
			// TODO: confirm we don't need this: expandedMonitoringInterval *= 2
			throughputMonitoringInterval = expandedMonitoringInterval
			slowTuneCh = nil // so we won't keep running this case at the expense of others)
		case <-time.After(throughputMonitoringInterval):
			if targetConcurrency != 0 && actualConcurrency == targetConcurrency { // scalebacks can take time. Don't want to do any tuning if actual is not yet aligned to target
				bytesOnWire := jm.pacer.GetTotalTraffic()
				if hasHadTimeToStablize {
					// throughput has had time to stabilize since last change, so we can meaningfully measure and act on throughput
					elapsedSeconds := time.Since(lastBytesTime).Seconds()
					bytes := bytesOnWire - lastBytesOnWire
					megabitsPerSec := (8 * float64(bytes) / elapsedSeconds) / (1000 * 1000)
					if megabitsPerSec > 4000 {
						throughputMonitoringInterval = expandedMonitoringInterval // start averaging throughputs over longer time period, since in some tests it takes a little longer to get a good average
					}
					targetConcurrency, reason = jm.concurrencyTuner.GetRecommendedConcurrency(int(megabitsPerSec), jm.cpuMon.CPUContentionExists())
					logConcurrency(targetConcurrency, reason)
				} else {
					// we weren't in steady state before, but given that throughputMonitoringInterval has now elapsed,
					// we'll deem that we are in steady state now (so can start measuring throughput from now)
					hasHadTimeToStablize = true
				}
				lastBytesOnWire = bytesOnWire
				lastBytesTime = time.Now()
			}
		}
	}
}

// RequestTuneSlowly is used to ask for a slower rate of auto-concurrency tuning.
// Necessary because if there's a download or S2S transfer going on, we need to measure throughputs over longer intervals to make
// the auto tuning work.
func (jm *jobMgr) RequestTuneSlowly() {
	select {
	case jm.poolSizingChannels.requestSlowTuneCh <- struct{}{}:
	default:
		return // channel already full, so don't need to add our signal there too, it already has one
	}
}

func (jm *jobMgr) scheduleJobParts() {
	startedPoolSizer := false
	for {
		select {
		case <-jm.xferChannels.scheduleCloseCh:
			jm.Log(common.LogInfo, "ScheduleJobParts done called")
			jm.poolSizingChannels.done <- struct{}{}
			return

		case jobPart := <-jm.xferChannels.partsChannel:

			if !startedPoolSizer {
				// spin up a GR to coordinate dynamic sizing of the main pool
				// It will automatically spin up the right number of chunk processors
				go jm.poolSizer()
				startedPoolSizer = true
			}
			jobPart.ScheduleTransfers(jm.Context())
		}
	}
}

// general purpose worker that reads in schedules chunk jobs, and executes chunk jobs
func (jm *jobMgr) chunkProcessor(workerID int) {
	jm.poolSizingChannels.entryNotificationCh <- struct{}{}                   // say we have started
	defer func() { jm.poolSizingChannels.exitNotificationCh <- struct{}{} }() // say we have exited

	for {
		// We check for scalebacks first to shrink goroutine pool
		// Then, we check chunks: normal & low priority
		select {
		case <-jm.poolSizingChannels.scalebackRequestCh:
			return
		default:
			select {
			case chunkFunc := <-jm.xferChannels.normalChunckCh:
				chunkFunc(workerID)
			default:
				select {
				case chunkFunc := <-jm.xferChannels.lowChunkCh:
					chunkFunc(workerID)
				default:
					time.Sleep(100 * time.Millisecond) // Sleep before looping around
					// TODO: Question: In order to safely support high goroutine counts,
					// do we need to review sleep duration, or find an approach that does not require waking every x milliseconds
					// For now, duration has been increased substantially from the previous 1 ms, to reduce cost of
					// the wake-ups.
				}
			}
		}
	}
}

// separate from the chunkProcessor, this dedicated worker that reads in and executes transfer initiation jobs
// (which in turn schedule chunks that get picked up by chunkProcessor)
func (jm *jobMgr) transferProcessor(workerID int) {
	startTransfer := func(jptm IJobPartTransferMgr) {
		if jptm.WasCanceled() {
			if jptm.ShouldLog(common.LogInfo) {
				jptm.Log(common.LogInfo, fmt.Sprintf(" is not picked up worked %d because transfer was cancelled", workerID))
			}
			jptm.SetStatus(common.ETransferStatus.Cancelled())
			jptm.ReportTransferDone()
		} else {
			// TODO fix preceding space
			jptm.Log(common.LogDebug, fmt.Sprintf("has worker %d which is processing TRANSFER %d", workerID, jptm.(*jobPartTransferMgr).transferIndex))
			jptm.StartJobXfer()
		}
	}

	for {
		// No scaleback check here, because this routine runs only in a small number of goroutines, so no need to kill them off
		select {
		case <-jm.xferChannels.closeTransferCh:
			jm.Log(common.LogInfo, "transferProcessor done called")
			return

		case jptm := <-jm.xferChannels.normalTransferCh:
			startTransfer(jptm)

		default:
			select {
			case jptm := <-jm.xferChannels.lowTransferCh:
				startTransfer(jptm)
			default:
				time.Sleep(10 * time.Millisecond) // Sleep before looping around
			}
		}
	}
}

func (jm *jobMgr) IterateJobParts(readonly bool, f func(k common.PartNumber, v IJobPartMgr)) {
	jm.jobPartMgrs.Iterate(readonly, f)
}

func (jm *jobMgr) TransferDirection() common.TransferDirection {
	return jm.atomicTransferDirection.AtomicLoad()
}

func (jm *jobMgr) AddSuccessfulBytesInActiveFiles(n int64) {
	atomic.AddInt64(&jm.atomicSuccessfulBytesInActiveFiles, n)
}

func (jm *jobMgr) SuccessfulBytesInActiveFiles() uint64 {
	n := atomic.LoadInt64(&jm.atomicSuccessfulBytesInActiveFiles)
	if n < 0 {
		n = 0 // should never happen, but would result in nasty over/underflow if it did
	}
	return uint64(n)
}

func (jm *jobMgr) CancelPauseJobOrder(desiredJobStatus common.JobStatus) common.CancelPauseResumeResponse {
	verb := common.Iff(desiredJobStatus == common.EJobStatus.Paused(), "pause", "cancel")
	jobID := jm.jobID

	// Search for the Part 0 of the Job, since the Part 0 status concludes the actual status of the Job
	jpm, found := jm.JobPartMgr(0)
	if !found {
		return common.CancelPauseResumeResponse{
			CancelledPauseResumed: false,
			ErrorMsg:              fmt.Sprintf("job with JobId %s has a missing 0th part", jobID.String()),
		}
	}

	jpp0 := jpm.Plan()
	status := jpp0.JobStatus()
	var jr common.CancelPauseResumeResponse
	switch status { // Current status
	case common.EJobStatus.Completed(): // You can't change state of a completed job
		jr = common.CancelPauseResumeResponse{
			CancelledPauseResumed: false,
			ErrorMsg:              fmt.Sprintf("Can't %s JobID=%v because it has already completed", verb, jobID),
			JobStatus:             status,
		}
	case common.EJobStatus.Cancelled():
		// If the status of Job is cancelled, it means that it has already been cancelled
		// No need to cancel further
		jr = common.CancelPauseResumeResponse{
			CancelledPauseResumed: false,
			ErrorMsg:              fmt.Sprintf("cannot cancel the job %s since it is already cancelled", jobID),
			JobStatus:             status,
		}
	case common.EJobStatus.Cancelling():
		// If the status of Job is cancelling, it means that it has already been requested for cancellation
		// No need to cancel further
		jr = common.CancelPauseResumeResponse{
			CancelledPauseResumed: true,
			ErrorMsg:              fmt.Sprintf("cannot cancel the job %s since it has already been requested for cancellation", jobID),
			JobStatus:             status,
		}
	case common.EJobStatus.InProgress():
		// If the Job status is in Progress and Job is not completely ordered
		// Job cannot be resumed later, hence graceful cancellation is not required
		// hence sending the response immediately. Response CancelPauseResumeResponse
		// returned has CancelledPauseResumed set to false, because that will let
		// Job immediately stop.
		fallthrough
	case common.EJobStatus.Paused(): // Logically, It's OK to pause an already-paused job
		jpp0.SetJobStatus(desiredJobStatus)
		msg := fmt.Sprintf("JobID=%v %s", jobID,
			common.Iff(desiredJobStatus == common.EJobStatus.Paused(), "paused", "canceled"))

		if jm.ShouldLog(common.LogInfo) {
			jm.Log(common.LogInfo, msg)
		}
		jm.Cancel() // Stop all inflight-chunks/transfer for this job (this includes all parts)
		jr = common.CancelPauseResumeResponse{
			CancelledPauseResumed: true,
			ErrorMsg:              msg,
			JobStatus:             status,
		}
	}
	return jr
}

func (jm *jobMgr) IsDaemon() bool {
	return jm.isDaemon
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type jobPartToJobPartMgr struct {
	nocopy common.NoCopy
	lock   sync.RWMutex
	m      map[PartNumber]IJobPartMgr
}

func newJobPartToJobPartMgr() jobPartToJobPartMgr {
	return jobPartToJobPartMgr{m: make(map[PartNumber]IJobPartMgr)}
}

func (m *jobPartToJobPartMgr) Count() uint32 {
	m.nocopy.Check()
	m.lock.RLock()
	count := uint32(len(m.m))
	m.lock.RUnlock()
	return count
}

func (m *jobPartToJobPartMgr) Set(key common.PartNumber, value IJobPartMgr) {
	m.nocopy.Check()
	m.lock.Lock()
	m.m[key] = value
	m.lock.Unlock()
}
func (m *jobPartToJobPartMgr) Get(key common.PartNumber) (value IJobPartMgr, ok bool) {
	m.nocopy.Check()
	m.lock.RLock()
	value, ok = m.m[key]
	m.lock.RUnlock()
	return
}
func (m *jobPartToJobPartMgr) Delete(key common.PartNumber) {
	m.nocopy.Check()
	m.lock.Lock()
	delete(m.m, key)
	m.lock.Unlock()
}

// We purposely disallow len
func (m *jobPartToJobPartMgr) Iterate(readonly bool, f func(k common.PartNumber, v IJobPartMgr)) {
	m.nocopy.Check()
	locker := sync.Locker(&m.lock)
	if readonly {
		locker = m.lock.RLocker()
	}
	locker.Lock()
	for k, v := range m.m {
		f(k, v)
	}
	locker.Unlock()
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// ThroughputState struct holds the attribute to monitor the through of an existing JobOrder
// type XferThroughput struct {
//	lastCheckedTime  time.Time
//	lastCheckedBytes int64
//	currentBytes     int64
// }

// getLastCheckedTime api returns the lastCheckedTime of ThroughputState instance in thread-safe manner
/*func (t *XferThroughput) LastCheckedTime() time.Time { return t.lastCheckedTime }

// updateLastCheckTime api updates the lastCheckedTime of ThroughputState instance in thread-safe manner
func (t *XferThroughput) SetLastCheckTime(currentTime time.Time) { t.lastCheckedTime = currentTime }

// getLastCheckedBytes api returns the lastCheckedBytes of ThroughputState instance in thread-safe manner
func (t *XferThroughput) LastCheckedBytes() int64 { return atomic.LoadInt64(&t.lastCheckedBytes) }

// updateLastCheckedBytes api updates the lastCheckedBytes of ThroughputState instance in thread-safe manner
func (t *XferThroughput) SetLastCheckedBytes(bytes int64) {
	atomic.StoreInt64(&t.lastCheckedBytes, bytes)
}

// getCurrentBytes api returns the currentBytes of ThroughputState instance in thread-safe manner
func (t *XferThroughput) CurrentBytes() int64 { return atomic.LoadInt64(&t.currentBytes) }

// updateCurrentBytes api adds the value in currentBytes of ThroughputState instance in thread-safe manner
func (t *XferThroughput) SetCurrentBytes(bytes int64) int64 {
	return atomic.AddInt64(&t.currentBytes, bytes)
}
*/
