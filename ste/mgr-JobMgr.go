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
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

var _ IJobMgr = &jobMgr{}

type PartNumber = common.PartNumber

// InMemoryTransitJobState defines job state transit in memory, and not in JobPartPlan file.
// Note: InMemoryTransitJobState should only be set when request come from cmd(FE) module to STE module.
// In memory CredentialInfo is currently maintained per job in STE, as FE could have many-to-one relationship with STE,
// i.e. different jobs could have different OAuth tokens requested from FE, and these jobs can run at same time in STE.
// This can be optimized if FE would no more be another module vs STE module.
type InMemoryTransitJobState struct {
	credentialInfo common.CredentialInfo
}

type IJobMgr interface {
	JobID() common.JobID
	JobPartMgr(partNum PartNumber) (IJobPartMgr, bool)
	//Throughput() XferThroughput
	AddJobPart(partNum PartNumber, planFile JobPartPlanFileName, sourceSAS string,
		destinationSAS string, scheduleTransfers bool) IJobPartMgr
	SetIncludeExclude(map[string]int, map[string]int)
	IncludeExclude() (map[string]int, map[string]int)
	ResumeTransfers(appCtx context.Context)
	AllTransfersScheduled() bool
	ConfirmAllTransfersScheduled()
	ResetAllTransfersScheduled()
	PipelineLogInfo() pipeline.LogOptions
	ReportJobPartDone() uint32
	Context() context.Context
	Cancel()
	// TODO: added for debugging purpose. remove later
	OccupyAConnection()
	// TODO: added for debugging purpose. remove later
	ReleaseAConnection()
	// TODO: added for debugging purpose. remove later
	ActiveConnections() int64
	GetPerfInfo() (displayStrings []string, constraint common.PerfConstraint)
	//Close()
	getInMemoryTransitJobState() InMemoryTransitJobState      // get in memory transit job state saved in this job.
	setInMemoryTransitJobState(state InMemoryTransitJobState) // set in memory transit job state saved in this job.
	ChunkStatusLogger() common.ChunkStatusLogger
	HttpClient() *http.Client
	getOverwritePrompter() *overwritePrompter
	common.ILoggerCloser
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func newJobMgr(concurrency ConcurrencySettings, appLogger common.ILogger, jobID common.JobID, appCtx context.Context, level common.LogLevel, commandString string, logFileFolder string) IJobMgr {
	// atomicAllTransfersScheduled is set to 1 since this api is also called when new job part is ordered.
	enableChunkLogOutput := level.ToPipelineLogLevel() == pipeline.LogDebug
	jm := jobMgr{jobID: jobID, jobPartMgrs: newJobPartToJobPartMgr(), include: map[string]int{}, exclude: map[string]int{},
		httpClient:        NewAzcopyHTTPClient(concurrency.MaxIdleConnections),
		logger:            common.NewJobLogger(jobID, level, appLogger, logFileFolder),
		chunkStatusLogger: common.NewChunkStatusLogger(jobID, logFileFolder, enableChunkLogOutput),
		concurrency:       concurrency,
		overwritePrompter: newOverwritePrompter(),
		/*Other fields remain zero-value until this job is scheduled */}
	jm.reset(appCtx, commandString)
	jm.logJobsAdminMessages()
	return &jm
}

func (jm *jobMgr) getOverwritePrompter() *overwritePrompter {
	return jm.overwritePrompter
}

func (jm *jobMgr) reset(appCtx context.Context, commandString string) IJobMgr {
	jm.logger.OpenLog()
	// log the user given command to the job log file.
	// since the log file is opened in case of resume, list and many other operations
	// for which commandString passed is empty, the length check is added
	if len(commandString) > 0 {
		jm.logger.Log(pipeline.LogInfo, fmt.Sprintf("Job-Command %s", commandString))
	}
	jm.logConcurrencyParameters()
	jm.ctx, jm.cancel = context.WithCancel(appCtx)
	atomic.StoreUint64(&jm.atomicNumberOfBytesCovered, 0)
	atomic.StoreUint64(&jm.atomicTotalBytesToXfer, 0)
	jm.partsDone = 0
	return jm
}

func (jm *jobMgr) logConcurrencyParameters() {
	jm.logger.Log(pipeline.LogInfo, fmt.Sprintf("Number of CPUs: %d", runtime.NumCPU()))
	// TODO: label max file buffer ram with how we obtained it (env var or default)
	jm.logger.Log(pipeline.LogInfo, fmt.Sprintf("Max file buffer RAM %.3f GB",
		float32(JobsAdmin.(*jobsAdmin).cacheLimiter.Limit())/(1024*1024*1024)))
	jm.logger.Log(pipeline.LogInfo, fmt.Sprintf("Max concurrent network operations: %d (%s)",
		jm.concurrency.MainPoolSize.Value,
		jm.concurrency.MainPoolSize.GetDescription()))
	jm.logger.Log(pipeline.LogInfo, fmt.Sprintf("Max concurrent transfer initiation routines: %d (%s)",
		jm.concurrency.TransferInitiationPoolSize.Value,
		jm.concurrency.TransferInitiationPoolSize.GetDescription()))
	jm.logger.Log(pipeline.LogInfo, fmt.Sprintf("Max open files when downloading: %d (auto-computed)",
		jm.concurrency.MaxOpenDownloadFiles))
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
	// atomicAllTransfersScheduled defines whether all job parts have been iterated and resumed or not
	atomicAllTransfersScheduled int32
	atomicTransferDirection     common.TransferDirection

	concurrency       ConcurrencySettings
	logger            common.ILoggerResetable
	chunkStatusLogger common.ChunkStatusLoggerCloser
	jobID             common.JobID // The Job's unique ID
	ctx               context.Context
	cancel            context.CancelFunc

	// Share the same HTTP Client across all job parts, so that the we maximize re-use of
	// its internal connection pool
	httpClient *http.Client

	jobPartMgrs jobPartToJobPartMgr // The map of part #s to JobPartMgrs
	// partsDone keep the count of completed part of the Job.
	partsDone uint32
	//throughput  common.CountPerSecond // TODO: Set LastCheckedTime to now

	inMemoryTransitJobState InMemoryTransitJobState
	// list of transfer mentioned to include only then while resuming the job
	include map[string]int
	// list of transfer mentioned to exclude while resuming the job
	exclude          map[string]int
	finalPartOrdered bool

	// only a single instance of the prompter is needed for all transfers
	overwritePrompter *overwritePrompter
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (jm *jobMgr) Progress() (uint64, uint64) {
	return atomic.LoadUint64(&jm.atomicNumberOfBytesCovered),
		atomic.LoadUint64(&jm.atomicTotalBytesToXfer)
}

//func (jm *jobMgr) Throughput() XferThroughput { return jm.throughput }

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
	jm.logJobsAdminMessages()
	atomicTransferDirection := jm.atomicTransferDirection.AtomicLoad()

	// get data appropriate to our current transfer direction
	chunkStateCounts := jm.chunkStatusLogger.GetCounts(atomicTransferDirection)

	// convert the counts to simple strings for consumption by callers
	const format = "%c: %2d"
	result := make([]string, len(chunkStateCounts)+1)
	total := int64(0)
	for i, c := range chunkStateCounts {
		result[i] = fmt.Sprintf(format, c.WaitReason.Name[0], c.Count)
		total += c.Count
	}
	result[len(result)-1] = fmt.Sprintf(format, 'T', total)

	con := jm.chunkStatusLogger.GetPrimaryPerfConstraint(atomicTransferDirection)

	// logging from here is a bit of a hack
	// TODO: can we find a better way to get this info into the log?  The caller is at app level,
	//    not job level, so can't log it directly AFAICT.
	jm.logPerfInfo(result, con)

	return result, con
}

func (jm *jobMgr) logPerfInfo(displayStrings []string, constraint common.PerfConstraint) {
	constraintString := fmt.Sprintf("primary performance constraint is %s", constraint)
	msg := fmt.Sprintf("PERF: %s. States: %s", constraintString, strings.Join(displayStrings, ", "))
	jm.Log(pipeline.LogInfo, msg)
}

// initializeJobPartPlanInfo func initializes the JobPartPlanInfo handler for given JobPartOrder
func (jm *jobMgr) AddJobPart(partNum PartNumber, planFile JobPartPlanFileName, sourceSAS string,
	destinationSAS string, scheduleTransfers bool) IJobPartMgr {
	jpm := &jobPartMgr{jobMgr: jm, filename: planFile, sourceSAS: sourceSAS,
		destinationSAS: destinationSAS, pacer: JobsAdmin.(*jobsAdmin).pacer,
		slicePool:        JobsAdmin.(*jobsAdmin).slicePool,
		cacheLimiter:     JobsAdmin.(*jobsAdmin).cacheLimiter,
		fileCountLimiter: JobsAdmin.(*jobsAdmin).fileCountLimiter}
	jpm.planMMF = jpm.filename.Map()
	jm.jobPartMgrs.Set(partNum, jpm)
	jm.finalPartOrdered = jpm.planMMF.Plan().IsFinalPart
	jm.setDirection(jpm.Plan().FromTo)
	if scheduleTransfers {
		// If the schedule transfer is set to true
		// Instead of the scheduling the Transfer for given JobPart
		// JobPart is put into the partChannel
		// from where it is picked up and scheduled
		//jpm.ScheduleTransfers(jm.ctx, make(map[string]int), make(map[string]int))
		JobsAdmin.QueueJobParts(jpm)
	}
	return jpm
}

// Remembers which direction we are running in (upload, download or neither (for service to service))
// It actually remembers the direction that our most recently-added job PART is running in,
// because that's where the fromTo information can be found,
// but we assume that all the job parts are running in the same direction.
// TODO: Optimize this when it's necessary for delete.
func (jm *jobMgr) setDirection(fromTo common.FromTo) {
	fromIsLocal := fromTo.From() == common.ELocation.Local()
	toIsLocal := fromTo.To() == common.ELocation.Local()

	isUpload := fromIsLocal && !toIsLocal
	isDownload := !fromIsLocal && toIsLocal
	isS2SCopy := fromTo.From().IsRemote() && fromTo.To().IsRemote()

	if isUpload {
		jm.atomicTransferDirection.AtomicStore(common.ETransferDirection.Upload())
	}
	if isDownload {
		jm.atomicTransferDirection.AtomicStore(common.ETransferDirection.Download())
	}
	if isS2SCopy {
		jm.atomicTransferDirection.AtomicStore(common.ETransferDirection.S2SCopy())
	}
}

func (jm *jobMgr) HttpClient() *http.Client {
	return jm.httpClient
}

// SetIncludeExclude sets the include / exclude list of transfers
// supplied with resume command to include or exclude mentioned transfers
func (jm *jobMgr) SetIncludeExclude(include, exclude map[string]int) {
	jm.include = include
	jm.exclude = exclude
}

// Returns the list of transfer mentioned to include / exclude
func (jm *jobMgr) IncludeExclude() (map[string]int, map[string]int) {
	return jm.include, jm.exclude
}

// ScheduleTransfers schedules this job part's transfers. It is called when a new job part is ordered & is also called to resume a paused Job
func (jm *jobMgr) ResumeTransfers(appCtx context.Context) {
	jm.reset(appCtx, "")
	// Since while creating the JobMgr, atomicAllTransfersScheduled is set to true
	// reset it to false while resuming it
	//jm.ResetAllTransfersScheduled()
	jm.jobPartMgrs.Iterate(false, func(p common.PartNumber, jpm IJobPartMgr) {
		JobsAdmin.QueueJobParts(jpm)
		//jpm.ScheduleTransfers(jm.ctx, includeTransfer, excludeTransfer)
	})
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

// ReportJobPartDone is called to report that a job part completed or failed
func (jm *jobMgr) ReportJobPartDone() uint32 {
	shouldLog := jm.ShouldLog(pipeline.LogInfo)
	partsDone := atomic.AddUint32(&jm.partsDone, 1)
	// If the last part is still awaited or other parts all still not complete,
	// JobPart 0 status is not changed.
	if partsDone != jm.jobPartMgrs.Count() || !jm.finalPartOrdered {
		if shouldLog {
			jm.Log(pipeline.LogInfo, fmt.Sprintf("is part of Job which %d total number of parts done ", partsDone))
		}
		return partsDone
	}

	if shouldLog {
		jm.Log(pipeline.LogInfo, fmt.Sprintf("all parts of Job %s successfully completed, cancelled or paused", jm.jobID.String()))
	}
	jobPart0Mgr, ok := jm.jobPartMgrs.Get(0)
	if !ok {
		jm.Panic(fmt.Errorf("Failed to find Job %v, Part #0", jm.jobID))
	}

	switch part0Plan := jobPart0Mgr.Plan(); part0Plan.JobStatus() {
	case common.EJobStatus.Cancelling():
		part0Plan.SetJobStatus(common.EJobStatus.Cancelled())
		if shouldLog {
			jm.Log(pipeline.LogInfo, fmt.Sprintf("all parts of Job %v successfully cancelled; cleaning up the Job", jm.jobID))
		}
		//jm.jobsInfo.cleanUpJob(jm.jobID)
	case common.EJobStatus.InProgress():
		part0Plan.SetJobStatus((common.EJobStatus).Completed())
	}

	jm.chunkStatusLogger.FlushLog() // TODO: remove once we sort out what will be calling CloseLog (currently nothing)

	return partsDone
}

func (jm *jobMgr) getInMemoryTransitJobState() InMemoryTransitJobState {
	return jm.inMemoryTransitJobState
}

// Note: InMemoryTransitJobState should only be set when request come from cmd(FE) module to STE module.
// And the state should no more be changed inside STE module.
func (jm *jobMgr) setInMemoryTransitJobState(state InMemoryTransitJobState) {
	jm.inMemoryTransitJobState = state
}

func (jm *jobMgr) Context() context.Context                { return jm.ctx }
func (jm *jobMgr) Cancel()                                 { jm.cancel() }
func (jm *jobMgr) ShouldLog(level pipeline.LogLevel) bool  { return jm.logger.ShouldLog(level) }
func (jm *jobMgr) Log(level pipeline.LogLevel, msg string) { jm.logger.Log(level, msg) }
func (jm *jobMgr) PipelineLogInfo() pipeline.LogOptions {
	return pipeline.LogOptions{
		Log:       jm.Log,
		ShouldLog: func(level pipeline.LogLevel) bool { return level <= jm.logger.MinimumLogLevel() },
	}
}
func (jm *jobMgr) Panic(err error) { jm.logger.Panic(err) }
func (jm *jobMgr) CloseLog() {
	jm.logger.CloseLog()
	jm.chunkStatusLogger.FlushLog()
}

func (jm *jobMgr) ChunkStatusLogger() common.ChunkStatusLogger {
	return jm.chunkStatusLogger
}

// TODO: find a better way for JobsAdmin to log (it doesn't have direct access to the job log, because it was originally designed to support multilpe jobs
func (jm *jobMgr) logJobsAdminMessages() {
	for {
		select {
		case msg := <-JobsAdmin.MessagesForJobLog():
			jm.Log(pipeline.LogInfo, msg)
		default:
			return
		}
	}
}

// PartsDone returns the number of the Job's parts that are either completed or failed
//func (jm *jobMgr) PartsDone() uint32 { return atomic.LoadUint32(&jm.partsDone) }

// SetPartsDone sets the number of Job's parts that are done (completed or failed)
//func (jm *jobMgr) SetPartsDone(partsDone uint32) { atomic.StoreUint32(&jm.partsDone, partsDone) }

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// ThroughputState struct holds the attribute to monitor the through of an existing JobOrder
//type XferThroughput struct {
//	lastCheckedTime  time.Time
//	lastCheckedBytes int64
//	currentBytes     int64
//}

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
