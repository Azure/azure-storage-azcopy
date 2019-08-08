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
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"path"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

// sortPlanFiles is struct that implements len, swap and less than functions
// this struct is used to sort the JobPartPlan files of the same job on the basis
// of Part number
// TODO: can use the same struct to sort job part plan files on the basis of job number and part number
type sortPlanFiles struct{ Files []os.FileInfo }

// Less determines the comparison between two fileInfo's
// compares the part number of the Job Part files
func (spf sortPlanFiles) Less(i, j int) bool {
	_, parti, err := JobPartPlanFileName(spf.Files[i].Name()).Parse()
	if err != nil {
		panic(fmt.Errorf("error parsing the JobPartPlanfile name %s. Failed with error %s", spf.Files[i].Name(), err.Error()))
	}
	_, partj, err := JobPartPlanFileName(spf.Files[j].Name()).Parse()
	if err != nil {
		panic(fmt.Errorf("error parsing the JobPartPlanfile name %s. Failed with error %s", spf.Files[j].Name(), err.Error()))
	}
	return parti < partj
}

// Len determines the length of number of files
func (spf sortPlanFiles) Len() int { return len(spf.Files) }

func (spf sortPlanFiles) Swap(i, j int) { spf.Files[i], spf.Files[j] = spf.Files[j], spf.Files[i] }

// JobAdmin is the singleton that manages ALL running Jobs, their parts, & their transfers
var JobsAdmin interface {
	NewJobPartPlanFileName(jobID common.JobID, partNumber common.PartNumber) JobPartPlanFileName

	// JobIDDetails returns point-in-time list of JobIDDetails
	JobIDs() []common.JobID

	// JobMgr returns the specified JobID's JobMgr
	JobMgr(jobID common.JobID) (IJobMgr, bool)
	JobMgrEnsureExists(jobID common.JobID, level common.LogLevel, commandString string) IJobMgr

	// AddJobPartMgr associates the specified JobPartMgr with the Jobs Administrator
	//AddJobPartMgr(appContext context.Context, planFile JobPartPlanFileName) IJobPartMgr
	/*ScheduleTransfer(jptm IJobPartTransferMgr)*/
	ScheduleChunk(priority common.JobPriority, chunkFunc chunkFunc)

	ResurrectJob(jobId common.JobID, sourceSAS string, destinationSAS string) bool

	ResurrectJobParts()

	QueueJobParts(jpm IJobPartMgr)

	// AppPathFolder returns the Azcopy application path folder.
	// JobPartPlanFile will be created inside this folder.
	AppPathFolder() string

	// returns the current value of bytesOverWire.
	BytesOverWire() int64

	AddSuccessfulBytesInActiveFiles(n uint64)

	// returns number of bytes successfully transferred in transfers that are currently in progress
	SuccessfulBytesInActiveFiles() uint64

	//DeleteJob(jobID common.JobID)
	common.ILoggerCloser
}

func initJobsAdmin(appCtx context.Context, concurrency ConcurrencySettings, targetRateInMegaBitsPerSec int64, azcopyAppPathFolder string, azcopyLogPathFolder string) {
	if JobsAdmin != nil {
		panic("initJobsAdmin was already called once")
	}

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

	// Create suicide channel which is used to scale back on the number of workers
	// TODO: this is not used. Remove it.
	suicideCh := make(chan SuicideJob, concurrency.InitialMainPoolSize)

	planDir := path.Join(azcopyAppPathFolder, "plans")
	if err := os.Mkdir(planDir, os.ModeDir|os.ModePerm); err != nil && !os.IsExist(err) {
		common.PanicIfErr(err)
	}

	maxRamBytesToUse := getMaxRamForChunks()

	// default to a pacer that doesn't actually control the rate
	// (it just records total throughput, since for historical reasons we do that in the pacer)
	var pacer pacerAdmin = newNullAutoPacer()
	if targetRateInMegaBitsPerSec > 0 {
		// use the "networking mega" (based on powers of 10, not powers of 2, since that's what mega means in networking context)
		targetRateInBytesPerSec := targetRateInMegaBitsPerSec * 1000 * 1000 / 8
		unusedExpectedCoarseRequestByteCount := uint32(0)
		pacer = newTokenBucketPacer(targetRateInBytesPerSec, unusedExpectedCoarseRequestByteCount)
		// Note: as at July 2019, we don't currently have a shutdown method/event on JobsAdmin where this pacer
		// could be shut down. But, it's global anyway, so we just leave it running until application exit.
	}

	ja := &jobsAdmin{
		concurrency:      concurrency,
		logger:           common.NewAppLogger(pipeline.LogInfo, azcopyLogPathFolder),
		jobIDToJobMgr:    newJobIDToJobMgr(),
		logDir:           azcopyLogPathFolder,
		planDir:          planDir,
		pacer:            pacer,
		slicePool:        common.NewMultiSizeSlicePool(common.MaxBlockBlobBlockSize),
		cacheLimiter:     common.NewCacheLimiter(maxRamBytesToUse),
		fileCountLimiter: common.NewCacheLimiter(int64(concurrency.MaxOpenDownloadFiles)),
		appCtx:           appCtx,
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
			suicideCh:        suicideCh,
		},
	}
	// create new context with the defaultService api version set as value to serviceAPIVersionOverride in the app context.
	ja.appCtx = context.WithValue(ja.appCtx, ServiceAPIVersionOverride, DefaultServiceApiVersion)

	JobsAdmin = ja

	// Spin up slice pool pruner
	go ja.slicePoolPruneLoop()

	// One routine constantly monitors the partsChannel.  It takes the JobPartManager from
	// the Channel and schedules the transfers of that JobPart.
	go ja.scheduleJobParts()
	// Spin up the desired number of executionEngine workers to process chunks
	for cc := 0; cc < concurrency.InitialMainPoolSize; cc++ {
		go ja.chunkProcessor(cc)
	}
	// Spin up a separate set of workers to process initiation of transfers (so that transfer initiation can't starve
	// out progress on already-scheduled chunks. (Not sure whether that can really happen, but this protects against it
	// anyway.)
	// Perhaps MORE importantly, doing this separately gives us more CONTROL over how we interact with the file system.
	for cc := 0; cc < concurrency.TransferInitiationPoolSize.Value; cc++ {
		go ja.transferProcessor(cc)
	}
}

// Decide on a max amount of RAM we are willing to use. This functions as a cap, and prevents excessive usage.
// There's no measure of physical RAM in the STD library, so we guestimate conservatively, based on  CPU count (logical, not phyiscal CPUs)
// Note that, as at Feb 2019, the multiSizeSlicePooler uses additional RAM, over this level, since it includes the cache of
// currently-unnused, re-useable slices, that is not tracked by cacheLimiter.
// Also, block sizes that are not powers of two result in extra usage over and above this limit. (E.g. 100 MB blocks each
// count 100 MB towards this limit, but actually consume 128 MB)
func getMaxRamForChunks() int64 {

	// return the user-specified override value, if any
	envVar := common.EEnvironmentVariable.BufferGB()
	overrideString := common.GetLifecycleMgr().GetEnvironmentVariable(envVar)
	if overrideString != "" {
		overrideValue, err := strconv.ParseFloat(overrideString, 64)
		if err != nil {
			common.GetLifecycleMgr().Error(fmt.Sprintf("Cannot parse environment variable %s, due to error %s", envVar.Name, err))
		} else {
			return int64(overrideValue * 1024 * 1024 * 1024)
		}
	}

	// else use a sensible default
	// TODO maybe one day measure actual RAM available
	const gbToUsePerCpu = 0.5 // should be enough to support the amount of traffic 1 CPU can drive, and also less than the typical installed RAM-per-CPU
	maxTotalGB := float32(16) // Even 6 is enough at 10 Gbps with standard 8MB chunk size, but we need allow extra here to help if larger blob block sizes are selected by user, since then we need more memory to get enough chunks to have enough network-level concurrency
	if strconv.IntSize == 32 {
		maxTotalGB = 1 // 32-bit apps can only address 2 GB, and best to leave plenty for needs outside our cache (e.g. running the app itself)
	}
	gbToUse := float32(runtime.NumCPU()) * gbToUsePerCpu
	if gbToUse > maxTotalGB {
		gbToUse = maxTotalGB // cap it.
	}
	maxRamBytesToUse := int64(gbToUse * 1024 * 1024 * 1024)
	return maxRamBytesToUse
}

// QueueJobParts puts the given JobPartManager into the partChannel
// from where this JobPartMgr will be picked by a routine and
// its transfers will be scheduled
func (ja *jobsAdmin) QueueJobParts(jpm IJobPartMgr) {
	ja.coordinatorChannels.partsChannel <- jpm
}

// 1 single goroutine runs this method and InitJobsAdmin  kicks that goroutine off.
func (ja *jobsAdmin) scheduleJobParts() {
	for {
		jobPart := <-ja.xferChannels.partsChannel
		// If the job manager is not found for the JobId of JobPart
		// taken from partsChannel
		// there is an error in our code
		// this not should not happen since JobMgr is initialized before any
		// job part is added
		jobId := jobPart.Plan().JobID
		jm, found := ja.JobMgr(jobId)
		if !found {
			panic(fmt.Errorf("no job manager found for JobId %s", jobId.String()))
		}
		jobPart.ScheduleTransfers(jm.Context())
	}
}

// general purpose worker that reads in schedules chunk jobs, and executes chunk jobs
func (ja *jobsAdmin) chunkProcessor(workerID int) {
	for {
		// We check for suicides first to shrink goroutine pool
		// Then, we check chunks: normal & low priority
		select {
		case <-ja.xferChannels.suicideCh: // note: as at Dec 2018, this channel is not (yet) used
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
func (ja *jobsAdmin) transferProcessor(workerID int) {
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
		// No suicide check here, because this routine runs only in a small number of goroutines, so no need to kill them off
		select {
		case jptm := <-ja.xferChannels.normalTransferCh:
			startTransfer(jptm)
		default:
			select {
			case jptm := <-ja.xferChannels.lowTransferCh:
				startTransfer(jptm)
			default:
				time.Sleep(10 * time.Millisecond) // Sleep before looping around
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// There will be only 1 instance of the jobsAdmin type.
// The coordinator uses this to manage all the running jobs and their job parts.
type jobsAdmin struct {
	atomicSuccessfulBytesInActiveFiles int64
	concurrency                        ConcurrencySettings
	logger                             common.ILoggerCloser
	jobIDToJobMgr                      jobIDToJobMgr // Thread-safe map from each JobID to its JobInfo
	// Other global state can be stored in more fields here...
	logDir              string // Where log files are stored
	planDir             string // Initialize to directory where Job Part Plans are stored
	coordinatorChannels CoordinatorChannels
	xferChannels        XferChannels
	appCtx              context.Context
	pacer               pacerAdmin
	slicePool           common.ByteSlicePooler
	cacheLimiter        common.CacheLimiter
	fileCountLimiter    common.CacheLimiter
}

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
	suicideCh        <-chan SuicideJob          // Read-only
}

type SuicideJob struct{}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (ja *jobsAdmin) NewJobPartPlanFileName(jobID common.JobID, partNumber common.PartNumber) JobPartPlanFileName {
	return JobPartPlanFileName(fmt.Sprintf(jobPartPlanFileNameFormat, jobID.String(), partNumber, DataSchemaVersion))
}

func (ja *jobsAdmin) FileExtension() string {
	return fmt.Sprintf(".strV%05d", DataSchemaVersion)
}

// JobIDDetails returns point-in-time list of JobIDDetails
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
	level common.LogLevel, commandString string) IJobMgr {

	return ja.jobIDToJobMgr.EnsureExists(jobID,
		func() IJobMgr {
			// Return existing or new IJobMgr to caller
			return newJobMgr(ja.concurrency, ja.logger, jobID, ja.appCtx, level, commandString, ja.logDir)
		})
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
	return ja.pacer.GetTotalTraffic()
}

func (ja *jobsAdmin) AddSuccessfulBytesInActiveFiles(n uint64) {
	atomic.AddInt64(&ja.atomicSuccessfulBytesInActiveFiles, int64(n))
}

func (ja *jobsAdmin) SuccessfulBytesInActiveFiles() uint64 {
	return uint64(atomic.LoadInt64(&ja.atomicSuccessfulBytesInActiveFiles))
}

func (ja *jobsAdmin) ResurrectJob(jobId common.JobID, sourceSAS string, destinationSAS string) bool {
	// Search the existing plan files for the PartPlans for the given jobId
	// only the files which have JobId has prefix and DataSchemaVersion as Suffix
	// are include in the result
	files := func(prefix, ext string) []os.FileInfo {
		var files []os.FileInfo
		filepath.Walk(ja.planDir, func(path string, fileInfo os.FileInfo, _ error) error {
			if !fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(), prefix) && strings.HasSuffix(fileInfo.Name(), ext) {
				files = append(files, fileInfo)
			}
			return nil
		})
		return files
	}(jobId.String(), fmt.Sprintf(".steV%d", DataSchemaVersion))
	// If no files with JobId exists then return false
	if len(files) == 0 {
		return false
	}
	// sort the JobPartPlan files with respect to Part Number
	sort.Sort(sortPlanFiles{Files: files})
	for f := 0; f < len(files); f++ {
		planFile := JobPartPlanFileName(files[f].Name())
		jobID, partNum, err := planFile.Parse()
		if err != nil {
			continue
		}
		mmf := planFile.Map()
		jm := ja.JobMgrEnsureExists(jobID, mmf.Plan().LogLevel, "")
		jm.AddJobPart(partNum, planFile, sourceSAS, destinationSAS, false)
	}
	return true
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
		jm := ja.JobMgrEnsureExists(jobID, mmf.Plan().LogLevel, "")
		jm.AddJobPart(partNum, planFile, EMPTY_SAS_STRING, EMPTY_SAS_STRING, false)
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

func (ja *jobsAdmin) slicePoolPruneLoop() {
	// if something in the pool has been unused for this long, we probably don't need it
	const pruneInterval = 5 * time.Second

	ticker := time.NewTicker(pruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ja.slicePool.Prune()
		case <-ja.appCtx.Done():
			break
		}
	}
}

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
