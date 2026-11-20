package ste

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

const jobSessionCacheBytes int64 = 64 << 20

// JobExecutionSessionConfig is restricted to supported native Blob-to-Blob copies.
// Plans must be trusted outputs of the matching native planner.
// The caller must exclusively own the files AND destination objects until each
// admission's Wait returns (or Close joins them). This API does not implement leases.
type JobExecutionSessionConfig struct {
	JobID                           common.JobID
	PlanDirectory                   string // directory containing canonical native job/part filenames
	SourceRoot, DestinationRoot     string // credential-free, exact native-header roots
	SourceClient, DestinationClient *common.ServiceClient
	Logger                          common.ILoggerResetable // Optional unopened logger; session opens it and closes it after draining.
	SourceSAS, DestinationSAS       string                  // in-memory only; no discovery or persistence
	SourceIsOAuth                   bool                    // matches AddJobPartArgs.SrcIsOAuth in the fused path
	Concurrency                     *ConcurrencySettings
	BufferBytes                     int64   // zero retains the default 64 MiB budget
	CapMbps                         float64 // zero means uncapped, as in fused MainSTE
	ChunkChannelSize                int     // zero preserves the build profile's queue capacity
}

// jobSessionState is local to one worker binding, never distributed job status.
type jobSessionState struct {
	firstTransferStartOnce                         sync.Once
	routines                                       sync.WaitGroup
	mu                                             sync.Mutex
	metricsMu                                      sync.RWMutex
	status                                         common.JobStatus
	partsAdmitted, partsRetired, transfersAdmitted uint64
	firstTransferStart                             *JobExecutionTransferStart
	finalSummary                                   *common.ListJobSummaryResponse
}

func (s *jobSessionState) currentStatus(ctx context.Context) common.JobStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == common.EJobStatus.InProgress() && ctx.Err() != nil {
		s.status = common.EJobStatus.Cancelling()
	}
	return s.status
}

// Legacy jobs retain their aggregate status in original part zero.
func (jm *jobMgr) lifecycleStatus() (common.JobStatus, bool) {
	if jm.session != nil {
		return jm.session.currentStatus(jm.ctx), true
	}
	part, ok := jm.jobPartMgrs.Get(0)
	if !ok {
		return common.JobStatus(0), false
	}
	return part.Plan().JobStatus(), true
}

func (jm *jobMgr) startRoutine(f func()) {
	if jm.session == nil {
		go f()
		return
	}
	jm.session.routines.Add(1)
	go func() {
		defer jm.session.routines.Done()
		f()
	}()
}

// PlanExecutionResult is a snapshot, not an accounting delta.
// Completed includes successes persisted by earlier executions of this part.
// Drained means STE reported part completion and its mapping is closed.
type PlanExecutionResult struct {
	PartNumber                            common.PartNumber
	Status                                common.JobStatus
	Completed, Failed, Skipped, Cancelled uint32
	BytesTransferred                      uint64
	Drained                               bool
	Elapsed                               time.Duration
}

// JobExecutionSession retains one manager and in-memory local lifecycle state
// across FIFO admissions and independently retiring plans of one worker binding.
// Close joins all parts and the binding's routines.
// It does not support pause or resume.
// After cancellation, create a new session to replay the claimed plans.
type JobExecutionSession struct {
	mu        sync.Mutex
	jm        *jobMgr
	config    JobExecutionSessionConfig
	closed    bool
	active    sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// PlanExecution owns one admission's immutable result. Wait never returns
// early on cancellation: STE must report completion and the mapping must retire.
// Retirement does not depend on a consumer calling Wait.
type PlanExecution struct {
	done   chan struct{}
	result PlanExecutionResult
	err    error
}

func (e *PlanExecution) Wait() (PlanExecutionResult, error) {
	<-e.done
	return e.result, e.err
}

func NewJobExecutionSession(ctx context.Context, c JobExecutionSessionConfig) (*JobExecutionSession, error) {
	concurrency, bufferBytes, err := c.RuntimeSettings()
	if err != nil {
		return nil, err
	}
	if c.SourceIsOAuth && c.SourceSAS != "" {
		return nil, errors.New("source OAuth and source SAS cannot be combined")
	}
	if _, err := c.PlanPath(0); err != nil {
		return nil, err
	}
	if err := validateClaimedRoot(c.SourceRoot); err != nil {
		return nil, err
	}
	if err := validateClaimedRoot(c.DestinationRoot); err != nil {
		return nil, err
	}
	if c.SourceRoot == c.DestinationRoot {
		return nil, errors.New("source and destination roots must differ")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	logger := c.Logger
	if logger == nil {
		// Callers without a logger retain file-free execution.
		logger = common.NewJobLogger(c.JobID, common.ELogLevel.None(), "", "")
	}
	logger.OpenLog()
	var pacer PacerAdmin = NewNullAutoPacer()
	if c.CapMbps > 0 {
		pacer = NewTokenBucketPacer(int64(c.CapMbps*1000*1000/8), 0)
	}
	ctx = context.WithValue(ctx, ServiceAPIVersionOverride, DefaultServiceApiVersion)
	jm := newJobMgr(concurrency, c.JobID, ctx, common.NewNullCpuMonitor(), logger.MinimumLogLevel(), "",
		&NullConcurrencyTuner{FixedValue: concurrency.InitialMainPoolSize}, pacer,
		common.NewMultiSizeSlicePool(common.MaxBlockBlobBlockSize), common.NewCacheLimiter(bufferBytes),
		common.NewCacheLimiter(int64(concurrency.MaxOpenDownloadFiles)), logger, false,
		&jobSessionState{status: common.EJobStatus.InProgress()}, c.RuntimeChannelSizes())
	// Claimed execution admits only Blob->Blob file records. Use NoFolders resources
	// without retaining a plan header that will later be unmapped, even when a
	// fused header preserves its original folder-policy bytes.
	jm.initState = &jobMgrInitState{
		securityInfoPersistenceManager: newSecurityInfoPersistenceManager(jm.ctx),
		folderCreationTracker:          &nullFolderTracker{},
		folderDeletionManager:          common.NewFolderDeletionManager(jm.ctx, common.EFolderPropertiesOption.NoFolders(), jm),
		exclusiveDestinationMapHolder:  &atomic.Value{},
	}
	jm.initState.exclusiveDestinationMapHolder.Store(common.NewExclusiveStringMap(common.EFromTo.BlobBlob(), runtime.GOOS))
	jm.setDirection(common.EFromTo.BlobBlob())
	sourceCredentialType := enum.ECredentialType.Anonymous()
	if c.SourceIsOAuth {
		sourceCredentialType = enum.ECredentialType.OAuthToken()
	}
	jm.SetInMemoryTransitJobState(InMemoryTransitJobState{S2SSourceCredentialType: sourceCredentialType})
	return &JobExecutionSession{jm: jm, config: c}, nil
}

// Status describes only the local binding lifecycle, not aggregated plan outcomes.
// Completed/Cancelled is set after all admitted plans retire during Close.
func (s *JobExecutionSession) Status() common.JobStatus {
	return s.jm.session.currentStatus(s.jm.ctx)
}

func (s *JobExecutionSession) JobMgr() IJobMgr { return s.jm }

// RetainedParts counts only admitted original plans not yet retired.
func (s *JobExecutionSession) RetainedParts() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.jm.jobPartMgrs.Count()
}

// Cancel stops the entire binding. It is not a drain acknowledgement. Each
// execution's Wait (or Close) must return before relinquishing ownership.
func (s *JobExecutionSession) Cancel() { s.jm.Cancel() }

// Execute is the blocking convenience form of Submit followed by Wait.
func (s *JobExecutionSession) Execute(ctx context.Context, part common.PartNumber) (PlanExecutionResult, error) {
	e, err := s.Submit(ctx, part)
	if err != nil {
		return PlanExecutionResult{PartNumber: part}, err
	}
	return e.Wait()
}

// Submit maps and checks the original plan, then passes its mapping to the warm STE manager.
// It never waits for any part's completion, snapshot or acknowledgement. Native
// queue backpressure may block admission. The caller bounds outstanding admissions.
// Cancelling any admission cancels the binding, without releasing ownership early.
// Only the part number varies: JobID, directory, and credentials are bound once.
// The caller must already hold exclusive file/destination execution authority.
func (s *JobExecutionSession) Submit(ctx context.Context, part common.PartNumber) (*PlanExecution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := PlanExecutionResult{PartNumber: part}
	if s.closed {
		return nil, errors.New("job execution session closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.jm.ctx.Err(); err != nil {
		return nil, err
	}
	if _, exists := s.jm.jobPartMgrs.Get(part); exists {
		return nil, errors.New("part already admitted")
	}
	path, err := s.config.PlanPath(part)
	if err != nil {
		return nil, err
	}
	mmf, err := mapJobPartPlanFile(path)
	if err != nil {
		return nil, err
	}
	if err := s.config.validateClaimedHeader(mmf.Plan(), part); err != nil {
		mmf.Unmap()
		return nil, err
	}
	done := make(chan struct{})
	jpm := s.jm.AddJobPart(&AddJobPartArgs{
		PartNum: part, PlanFile: JobPartPlanFileName(path), ExistingPlanMMF: mmf,
		SrcClient: s.config.SourceClient, DstClient: s.config.DestinationClient,
		SrcIsOAuth:     s.config.SourceIsOAuth,
		CompletionChan: done,
	}).(*jobPartMgr)
	start := time.Now()
	callbackDone := make(chan struct{})
	// *** WORKFLOW 4.b.ii.a (cancellation hook): Cancel the binding when submission context ends; still await drain.
	stop := context.AfterFunc(ctx, func() { s.Cancel(); close(callbackDone) })
	jpm.sourceSAS, jpm.destinationSAS = s.config.SourceSAS, s.config.DestinationSAS
	s.jm.session.mu.Lock()
	s.jm.session.partsAdmitted++
	s.jm.session.transfersAdmitted += uint64(jpm.cachedNumTransfers)
	s.jm.session.mu.Unlock()
	if jpm.cachedNumTransfers == 0 {
		jpm.Plan().SetJobPartStatus(common.EJobStatus.Completed())
		close(done)
	} else {
		s.jm.QueueJobParts(jpm)
	}
	execution := &PlanExecution{done: make(chan struct{})}
	s.active.Add(1) // under admission lock, before Close may start waiting
	go func() {
		defer s.active.Done()
		<-done
		result.Status = jpm.Plan().JobPartStatus()
		// STE publishes these counters before reporting part completion.
		result.Completed = atomic.LoadUint32(&jpm.atomicTransfersCompleted)
		result.Failed = atomic.LoadUint32(&jpm.atomicTransfersFailed)
		result.Skipped = atomic.LoadUint32(&jpm.atomicTransfersSkipped)
		result.Cancelled = atomic.LoadUint32(&jpm.atomicTransfersCancelled)
		result.BytesTransferred = atomic.LoadUint64(&jpm.atomicBytesTransferred)
		// Retire exactly once after STE completion and snapshot. Close also
		// retires original transfer part zero, without changing global JobStatus.
		jpm.Close()
		s.jm.jobPartMgrs.Delete(part)
		if !stop() {
			<-callbackDone
		}
		result.Drained, result.Elapsed = true, time.Since(start)
		s.jm.session.mu.Lock()
		s.jm.session.partsRetired++
		s.jm.session.mu.Unlock()
		execution.result, execution.err = result, s.jm.ctx.Err()
		close(execution.done)
	}()
	return execution, nil
}

// Close is idempotent and graceful: it stops accepting work, joins all STE work
// and manager routines, then releases the binding's in-memory resources.
// It never invokes the legacy two-minute deferred cleanup or resets closed channels.
func (s *JobExecutionSession) Close() error {
	s.closeOnce.Do(func() { s.closeErr = s.close() })
	return s.closeErr
}

func (s *JobExecutionSession) close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	// *** WORKFLOW 5.a.i: Wait for all admitted plans to retire before draining accounting and releasing resources.
	s.active.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	jm := s.jm
	jm.session.metricsMu.Lock()
	defer jm.session.metricsMu.Unlock()
	// All admissions have retired; drain both accounting queues before freezing totals.
	close(jm.jstm.partCreated)
	close(jm.jstm.xferDone)
	<-jm.jstm.statusMgrDone
	summary := jm.ListJobSummary(false)
	jm.session.mu.Lock()
	jm.session.status = common.EJobStatus.EnhanceJobStatusInfo(
		summary.TransfersSkipped > 0, summary.TransfersFailed > 0, summary.TransfersCompleted > 0)
	if jm.ctx.Err() != nil {
		jm.session.status = common.EJobStatus.Cancelled()
	}
	jm.session.mu.Unlock()
	jm.cleanupTransferRoutine()
	jm.session.routines.Wait()
	finalSummary, _ := jm.getJobSummary(false)
	finalSummary.FailedTransfers, finalSummary.SkippedTransfers = nil, nil
	jm.session.finalSummary = &finalSummary
	jm.cancel()
	jm.deleteJobPartsMgrs()
	jm.cleanupChunkStatusLogger()
	jm.logger.CloseLog()
	jm.slicePool.Prune()
	pacerErr := jm.pacer.Close()
	jm.initState = nil
	jm.slicePool, jm.chunkStatusLogger = nil, nil
	jm.jstm.partCreated, jm.jstm.xferDone = nil, nil
	jm.coordinatorChannels = CoordinatorChannels{}
	jm.xferChannels = XferChannels{}
	jm.poolSizingChannels = poolSizingChannels{}
	s.config = JobExecutionSessionConfig{}
	return pacerErr
}
