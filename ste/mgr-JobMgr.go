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
	AddJobPart(partNum PartNumber, planFile JobPartPlanFileName, scheduleTransfers bool) IJobPartMgr
	ResumeTransfers(appCtx context.Context, includeTransfer map[string]int, excludeTransfer map[string]int)
	PipelineLogInfo() pipeline.LogOptions
	ReportJobPartDone() uint32
	Context() context.Context
	Cancel()
	//Close()
	getInMemoryTransitJobState() InMemoryTransitJobState      // get in memory transit job state saved in this job.
	setInMemoryTransitJobState(state InMemoryTransitJobState) // set in memory transit job state saved in this job.

	common.ILoggerCloser
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func newJobMgr(appLogger common.ILogger, jobID common.JobID, appCtx context.Context, level common.LogLevel, commandString string) IJobMgr {
	jm := jobMgr{jobID: jobID, jobPartMgrs: newJobPartToJobPartMgr(), logger: common.NewJobLogger(jobID, level, appLogger) /*Other fields remain zero-value until this job is scheduled */}
	jm.reset(appCtx, commandString)
	return &jm
}

func (jm *jobMgr) reset(appCtx context.Context, commandString string) IJobMgr {
	jm.logger.OpenLog()
	// log the user given command to the job log file.
	// since the log file is opened in case of resume, list and many other operations
	// for which commandString passed is empty, the length check is added
	if len(commandString) > 0 {
		jm.logger.Log(pipeline.LogInfo, fmt.Sprintf("Job-Command %s", commandString))
	}
	jm.ctx, jm.cancel = context.WithCancel(appCtx)
	atomic.StoreUint64(&jm.atomicNumberOfBytesCovered, 0)
	atomic.StoreUint64(&jm.atomicTotalBytesToXfer, 0)
	jm.partsDone = 0
	return jm
}

// jobMgr represents the runtime information for a Job
type jobMgr struct {
	logger common.ILoggerResetable
	jobID  common.JobID // The Job's unique ID
	ctx    context.Context
	cancel context.CancelFunc

	jobPartMgrs jobPartToJobPartMgr // The map of part #s to JobPartMgrs
	// partsDone keep the count of completed part of the Job.
	partsDone uint32
	//throughput  common.CountPerSecond // TODO: Set LastCheckedTime to now

	inMemoryTransitJobState InMemoryTransitJobState

	finalPartOrdered           bool
	atomicNumberOfBytesCovered uint64
	atomicTotalBytesToXfer     uint64
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

// initializeJobPartPlanInfo func initializes the JobPartPlanInfo handler for given JobPartOrder
func (jm *jobMgr) AddJobPart(partNum PartNumber, planFile JobPartPlanFileName, scheduleTransfers bool) IJobPartMgr {
	jpm := &jobPartMgr{jobMgr: jm, filename: planFile, pacer: JobsAdmin.(*jobsAdmin).pacer}
	jpm.planMMF = jpm.filename.Map()
	jm.jobPartMgrs.Set(partNum, jpm)
	jm.finalPartOrdered = jpm.planMMF.Plan().IsFinalPart
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

// ScheduleTransfers schedules this job part's transfers. It is called when a new job part is ordered & is also called to resume a paused Job
func (jm *jobMgr) ResumeTransfers(appCtx context.Context, includeTransfer map[string]int, excludeTransfer map[string]int) {
	jm.reset(appCtx, "")
	jm.jobPartMgrs.Iterate(false, func(p common.PartNumber, jpm IJobPartMgr) {
		jpm.ScheduleTransfers(jm.ctx, includeTransfer, excludeTransfer)
	})
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
func (jm *jobMgr) CloseLog()       { jm.logger.CloseLog() }

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
	if !readonly {
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
