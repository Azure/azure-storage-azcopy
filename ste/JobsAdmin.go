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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var JobsAdminInitialized = make(chan bool, 1)

// JobAdmin is the singleton that manages ALL running Jobs, their parts, & their transfers
var JobsAdmin interface {
	NewJobPartPlanFileName(jobID common.JobID, partNumber common.PartNumber) JobPartPlanFileName

	// JobIDs returns point-in-time list of JobIDs
	JobIDs() []common.JobID

	// JobMgr returns the specified JobID's JobMgr
	JobMgr(jobID common.JobID) (IJobMgr, bool)
	JobMgrEnsureExists(jobID common.JobID, level common.LogLevel) IJobMgr

	// AddJobPartMgr associates the specified JobPartMgr with the Jobs Administrator
	//AddJobPartMgr(appContext context.Context, planFile JobPartPlanFileName) IJobPartMgr
	/*ScheduleTransfer(jptm IJobPartTransferMgr)*/
	ScheduleChunk(priority common.JobPriority, chunkFunc chunkFunc)
	ResurrectJobParts()

	// AppPathFolder returns the Azcopy application path folder.
	// JobPartPlanFile will be created inside this folder.
	AppPathFolder() string

	// returns the current value of bytesOverWire.
	BytesOverWire() int64

	//DeleteJob(jobID common.JobID)
	common.ILoggerCloser
}

func initJobsAdmin(appCtx context.Context, concurrentConnections int, targetRateInMBps int64, azcopyAppPathFolder string) {
	if JobsAdmin != nil {
		panic("initJobsAdmin was already called once")
	}

	const channelSize = 100000
	// Create normal & low transfer/chunk channels
	normalTransferCh, normalChunkCh := make(chan IJobPartTransferMgr, channelSize), make(chan chunkFunc, channelSize)
	lowTransferCh, lowChunkCh := make(chan IJobPartTransferMgr, channelSize), make(chan chunkFunc, channelSize)

	// Create suicide channel which is used to scale back on the number of workers
	suicideCh := make(chan SuicideJob, concurrentConnections)

	ja := &jobsAdmin{
		logger:        common.NewAppLogger(pipeline.LogInfo),
		jobIDToJobMgr: newJobIDToJobMgr(),
		planDir:       azcopyAppPathFolder,
		pacer:         newPacer(targetRateInMBps * 1024 * 1024),
		appCtx:        appCtx,
		coordinatorChannels: CoordinatorChannels{
			normalTransferCh: normalTransferCh,
			lowTransferCh:    lowTransferCh,
		},
		xferChannels: XferChannels{
			normalTransferCh: normalTransferCh,
			lowTransferCh:    lowTransferCh,
			normalChunckCh:   normalChunkCh,
			lowChunkCh:       lowChunkCh,
			suicideCh:        suicideCh,
		},
	}
	// create new context with the defaultService api version set as value to serviceAPIVersionOverride in the app context.
	ja.appCtx = context.WithValue(ja.appCtx, ServiceAPIVersionOverride, defaultServiceApiVersion)

	JobsAdmin = ja
	// Spin up the desired number of executionEngine workers to process transfers/chunks
	for cc := 0; cc < concurrentConnections; cc++ {
		go ja.transferAndChunkProcessor(cc)
	}
}

// general purpose worker that reads in transfer jobs, schedules chunk jobs, and executes chunk jobs
func (ja *jobsAdmin) transferAndChunkProcessor(workerID int) {
	startTransfer := func(jptm IJobPartTransferMgr) {
		if jptm.WasCanceled() {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf(" is not picked up worked %d because transfer was cancelled", workerID))
			}
			jptm.ReportTransferDone()
		} else {
			// TODO fix preceding space
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("has worker %d which is processing TRANSFER", workerID))
			}
			jptm.StartJobXfer()
		}
	}

	for {
		// We check for suicides first to shrink goroutine pool
		// Then, we check chunks: normal & low priority
		// Then, we check transfers: normal & low priority
		select {
		case <-ja.xferChannels.suicideCh:
			return
		default:
			select {
			case chunkFunc := <-ja.xferChannels.normalChunckCh:
				chunkFunc(workerID)
			default:
				select {
				case chunkFunc := <-ja.xferChannels.lowChunkCh:
					chunkFunc(workerID)
				default:
					select {
					case jptm := <-ja.xferChannels.normalTransferCh:
						startTransfer(jptm)
					default:
						select {
						case jptm := <-ja.xferChannels.lowTransferCh:
							startTransfer(jptm)
						default:
							time.Sleep(1 * time.Millisecond) // Sleep before looping around
						}
					}
				}
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// There will be only 1 instance of the jobsAdmin type.
// The coordinator uses this to manage all the running jobs and their job parts.
type jobsAdmin struct {
	logger        common.ILoggerCloser
	jobIDToJobMgr jobIDToJobMgr // Thread-safe map from each JobID to its JobInfo
	// Other global state can be stored in more fields here...
	planDir             string // Initialize to directory where Job Part Plans are stored
	coordinatorChannels CoordinatorChannels
	xferChannels        XferChannels
	appCtx              context.Context
	pacer               *pacer
	numOfEngineWorker   int
	// bytesOverWire defines the total bytes sent from azcopy to the service through sdk.
	// It is used to calculated the throughput of azcopy.
	bytesOverWire uint64
}

type CoordinatorChannels struct {
	normalTransferCh chan<- IJobPartTransferMgr // Write-only
	lowTransferCh    chan<- IJobPartTransferMgr // Write-only
}

type XferChannels struct {
	normalTransferCh <-chan IJobPartTransferMgr // Read-only
	lowTransferCh    <-chan IJobPartTransferMgr // Read-only
	normalChunckCh   chan chunkFunc             // Read-write
	lowChunkCh       chan chunkFunc             // Read-write
	suicideCh        <-chan SuicideJob          // Read-only
}

type SuicideJob struct{}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (ja jobsAdmin) NewJobPartPlanFileName(jobID common.JobID, partNumber common.PartNumber) JobPartPlanFileName {
	return JobPartPlanFileName(fmt.Sprintf(jobPartPlanFileNameFormat, jobID.String(), partNumber, DataSchemaVersion))
}

func (ja jobsAdmin) FileExtension() string {
	return fmt.Sprintf(".strV%05d", DataSchemaVersion)
}

// JobIDs returns point-in-time list of JobIDs
func (ja *jobsAdmin) JobIDs() []common.JobID {
	var jobIDs []common.JobID
	ja.jobIDToJobMgr.Iterate(false, func(k common.JobID, v IJobMgr) {
		jobIDs = append(jobIDs, k)
	})
	return jobIDs
}

// JobMgr returns the specified JobID's JobMgr if it exists
func (ja *jobsAdmin) JobMgr(jobID common.JobID) (IJobMgr, bool) {
	return ja.jobIDToJobMgr.Get(jobID)
}

// AppPathFolder returns the Azcopy application path folder.
// JobPartPlanFile will be created inside this folder.
func (ja *jobsAdmin) AppPathFolder() string {
	return ja.planDir
}

// JobMgrEnsureExists returns the specified JobID's IJobMgr if it exists or creates it if it doesn't already exit
// If it does exist, then the appCtx argument is ignored.
func (ja *jobsAdmin) JobMgrEnsureExists(jobID common.JobID,
	level common.LogLevel) IJobMgr {

	return ja.jobIDToJobMgr.EnsureExists(jobID,
		func() IJobMgr { return newJobMgr(ja.logger, jobID, ja.appCtx, level) }) // Return existing or new IJobMgr to caller
}

func (ja *jobsAdmin) ScheduleTransfer(priority common.JobPriority, jptm IJobPartTransferMgr) {
	switch priority { // priority determines which channel handles the job part's transfers
	case common.EJobPriority.Normal():
		//jptm.SetChunkChannel(ja.xferChannels.normalChunckCh)
		ja.coordinatorChannels.normalTransferCh <- jptm
	case common.EJobPriority.Low():
		//jptm.SetChunkChannel(ja.xferChannels.lowChunkCh)
		ja.coordinatorChannels.lowTransferCh <- jptm
	default:
		ja.Panic(fmt.Errorf("invalid priority: %q", priority))
	}
}

func (ja *jobsAdmin) ScheduleChunk(priority common.JobPriority, chunkFunc chunkFunc) {
	switch priority { // priority determines which channel handles the job part's transfers
	case common.EJobPriority.Normal():
		ja.xferChannels.normalChunckCh <- chunkFunc
	case common.EJobPriority.Low():
		ja.xferChannels.lowChunkCh <- chunkFunc
	default:
		ja.Panic(fmt.Errorf("invalid priority: %q", priority))
	}
}

func (ja *jobsAdmin) BytesOverWire() int64 {
	return atomic.LoadInt64(&ja.pacer.bytesTransferred)
}

// reconstructTheExistingJobParts reconstructs the in memory JobPartPlanInfo for existing memory map JobFile
func (ja *jobsAdmin) ResurrectJobParts() {
	// Get all the Job part plan files in the plan directory
	files := func(ext string) []os.FileInfo {
		var files []os.FileInfo
		filepath.Walk(ja.planDir, func(path string, fileInfo os.FileInfo, _ error) error {
			if !fileInfo.IsDir() && strings.HasSuffix(fileInfo.Name(), ext) {
				files = append(files, fileInfo)
			}
			return nil
		})
		return files
	}(fmt.Sprintf(".steV%d", DataSchemaVersion))

	// TODO : sort the file.
	for f := 0; f < len(files); f++ {
		planFile := JobPartPlanFileName(files[f].Name())
		jobID, partNum, err := planFile.Parse()
		if err != nil {
			continue
		}
		mmf := planFile.Map()
		//todo : call the compute transfer function here for each job.
		jm := ja.JobMgrEnsureExists(jobID, mmf.Plan().LogLevel)
		jm.AddJobPart(partNum, planFile, false)
	}
}

// TODO: I think something is wrong here: I think delete and cleanup should be merged together.
// DeleteJobInfo api deletes an entry of given JobId the JobsInfo
// TODO: add the clean up logic for all Jobparts.
func (ja *jobsAdmin) DeleteJob(jobID common.JobID) {
	ja.jobIDToJobMgr.Delete(jobID)
}

// cleanUpJob api unmaps all the memory map JobPartFile and deletes the JobPartFile
/*
	* Load PartMap for given JobId
    * Iterate through each part order of given Job and then shutdowns the JobInfo handler
    * Iterate through each part order of given Job and then shutdowns the JobInfo handler
	* Delete all the job part files stored on disk
    * Closes the logger file opened for logging logs related to given job
	* Removes the entry of given JobId from JobsInfo
*/

// TODO: take care fo this.
/*func (ja *jobsAdmin) cleanUpJob(jobID common.JobID) {
	jm, found := ja.JobMgr(jobID)
	if !found {
		ja.Panic(fmt.Errorf("no job found with JobID %v to clean up", jobID))
	}
	for p := PartNumber(0); true; p++ {
		jpm, found := jm.JobPartMgr(p)
		if !found { // TODO
		}
		// TODO: Fix jpm.planMMF.Unmap()	// unmapping the memory map JobPart file
		err := jpm.filename.Delete()
		if err != nil {
			ja.Panic(fmt.Errorf("error removing the job part file %s. Failed with following error %s", jpm.filename, err))
		}
		//TODO: jobHandler.shutDownHandler(ji.logger)
	}
	ji.closeLogForJob()
	// deletes the entry for given JobId from Map
	ja.DeleteJob(jobID)
}
*/
func (ja *jobsAdmin) ShouldLog(level pipeline.LogLevel) bool  { return ja.logger.ShouldLog(level) }
func (ja *jobsAdmin) Log(level pipeline.LogLevel, msg string) { ja.logger.Log(level, msg) }
func (ja *jobsAdmin) Panic(err error)                         { ja.logger.Panic(err) }
func (ja *jobsAdmin) CloseLog()                               { ja.logger.CloseLog() }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// The jobIDToJobMgr maps each JobID to its JobMgr
type jobIDToJobMgr struct {
	nocopy common.NoCopy
	lock   sync.RWMutex
	m      map[common.JobID]IJobMgr
}

func newJobIDToJobMgr() jobIDToJobMgr {
	return jobIDToJobMgr{m: make(map[common.JobID]IJobMgr)}
}

func (j *jobIDToJobMgr) Set(key common.JobID, value IJobMgr) {
	j.nocopy.Check()
	j.lock.Lock()
	j.m[key] = value
	j.lock.Unlock()
}

func (j *jobIDToJobMgr) Get(key common.JobID) (value IJobMgr, found bool) {
	j.nocopy.Check()
	j.lock.RLock()
	value, found = j.m[key]
	j.lock.RUnlock()
	return
}

func (j *jobIDToJobMgr) EnsureExists(jobID common.JobID, newJobMgr func() IJobMgr) IJobMgr {
	j.nocopy.Check()
	j.lock.Lock()

	// defined variables both jm & found above condition since defined variables might get re-initialized
	// in if condition if any variable in the left was not initialized.
	var jm IJobMgr
	var found bool

	// NOTE: We look up the desired IJobMgr and add it if it's not there atomically using a write lock
	if jm, found = j.m[jobID]; !found {
		jm = newJobMgr()
		j.m[jobID] = jm
	}
	j.lock.Unlock()
	return jm
}

func (j *jobIDToJobMgr) Delete(key common.JobID) {
	j.nocopy.Check()
	j.lock.Lock()
	delete(j.m, key)
	j.lock.Unlock()
}

func (j *jobIDToJobMgr) Iterate(write bool, f func(k common.JobID, v IJobMgr)) {
	j.nocopy.Check()
	locker := sync.Locker(&j.lock)
	if !write {
		locker = j.lock.RLocker()
	}
	locker.Lock()
	for k, v := range j.m {
		f(k, v)
	}
	locker.Unlock()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*func goroutinePoolTest() {
	const maxGoroutines = 10
	gp, die := &GoroutinePool{}, make(chan struct{}, maxGoroutines)
	setConcurrency := func(desiredConcurrency int32) {
		goroutinesToAdd := gp.Concurrency(desiredConcurrency)
		for g := int32(0); g < goroutinesToAdd; g++ {
			go worker(die)
		}
		for g := int32(0); g > goroutinesToAdd; g-- {
			die <- struct{}{}
		}
	}

	setConcurrency(2)
	time.Sleep(10 * time.Second)

	setConcurrency(10)
	time.Sleep(10 * time.Second)

	setConcurrency(1)
	time.Sleep(10 * time.Second)

	setConcurrency(0)
	time.Sleep(30 * time.Second)
}

var goroutinesInPool int32

func worker(die <-chan struct{}) {
	atomic.AddInt32(&goroutinesInPool, 1)
loop:
	for {
		fmt.Printf("Count #%d\n", atomic.LoadInt32(&goroutinesInPool))
		select {
		case <-die:
			break loop
		default:
			time.Sleep(time.Second * 4)
		}
	}
	fmt.Printf("Count %d\n", atomic.AddInt32(&goroutinesInPool, -1))
}

type GoroutinePool struct {
	nocopy      common.NoCopy
	concurrency int32
}

// Concurrency sets the desired concurrency and returns the number of goroutines that should be
// added/removed to achieve the desired concurrency. If this method returns a positive number,
// add the number of specified goroutines to the pool. If this method returns a negative number,
// kill the number of specified goroutines from the pool.
func (gp *GoroutinePool) Concurrency(concurrency int32) int32 {
	if concurrency < 0 {
		panic("concurrency must be >= 0")
	}
	gp.nocopy.Check()
	return concurrency - atomic.SwapInt32(&gp.concurrency, concurrency)
}*/
