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

package jobsAdmin

import (
	"context"
	"encoding/json"
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

	"github.com/Azure/azure-storage-azcopy/v10/pacer"
	"github.com/Azure/azure-storage-azcopy/v10/ste"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// sortPlanFiles is struct that implements len, swap and less than functions
// this struct is used to sort the JobPartPlan files of the same job on the basis
// of Part number
// TODO: can use the same struct to sort job part plan files on the basis of job number and part number
type sortPlanFiles struct{ Files []os.FileInfo }

// Less determines the comparison between two fileInfo's
// compares the part number of the Job Part files
func (spf sortPlanFiles) Less(i, j int) bool {
	_, parti, err := ste.JobPartPlanFileName(spf.Files[i].Name()).Parse()
	if err != nil {
		panic(fmt.Errorf("error parsing the JobPartPlanfile name %s. Failed with error %s", spf.Files[i].Name(), err.Error()))
	}
	_, partj, err := ste.JobPartPlanFileName(spf.Files[j].Name()).Parse()
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
	NewJobPartPlanFileName(jobID common.JobID, partNumber common.PartNumber) ste.JobPartPlanFileName

	// JobIDDetails returns point-in-time list of JobIDDetails
	JobIDs() []common.JobID

	// JobMgr returns the specified JobID's JobMgr
	JobMgr(jobID common.JobID) (ste.IJobMgr, bool)
	JobMgrEnsureExists(jobID common.JobID, level common.LogLevel, commandString string, jobErrorHandler common.JobErrorHandler) ste.IJobMgr

	// AddJobPartMgr associates the specified JobPartMgr with the Jobs Administrator
	//AddJobPartMgr(appContext context.Context, planFile JobPartPlanFileName) IJobPartMgr
	/*ScheduleTransfer(jptm IJobPartTransferMgr)*/
	ResurrectJob(jobId common.JobID, srcServiceClient *common.ServiceClient, dstServiceClient *common.ServiceClient, srcIsOAuth bool, jobErrorHandler common.JobErrorHandler) bool

	// returns the current value of bytesOverWire.
	BytesOverWire() int64

	//DeleteJob(jobID common.JobID)

	TryGetPerformanceAdvice(bytesInJob uint64, filesInJob uint32, fromTo common.FromTo, dir common.TransferDirection, p *ste.PipelineNetworkStats) []common.PerformanceAdvice

	SetConcurrencySettingsToAuto()
	GetConcurrencySettings() (int, bool)

	// JobMgrCleanUp do the JobMgr cleanup.
	JobMgrCleanUp(jobId common.JobID)
	MessageHandler(inputChan <-chan *common.LCMMsg)
}

func initJobsAdmin(appCtx context.Context, concurrency ste.ConcurrencySettings, targetRateInMegaBitsPerSec float64) error {
	if JobsAdmin != nil {
		panic("initJobsAdmin was already called once")
	}

	cpuMon := common.NewNullCpuMonitor()
	// One day, we might monitor CPU as the app runs in all cases (and report CPU as possible constraint like we do with disk).
	// But for now, we only monitor it when tuning the GR pool size.
	if concurrency.AutoTuneMainPool() && concurrency.CheckCpuWhenTuning.Value {
		// let our CPU monitor self-calibrate BEFORE we start doing any real work TODO: remove if we switch to gopsutil
		cpuMon = common.NewCalibratedCpuUsageMonitor()
	}

	maxRamBytesToUse, err := getMaxRamForChunks()
	if err != nil {
		return err
	}

	// use the "networking mega" (based on powers of 10, not powers of 2, since that's what mega means in networking context)
	targetRateInBytesPerSec := int64(targetRateInMegaBitsPerSec * 1000 * 1000 / 8)
	requestPacer := pacer.New(pacer.NewBandwidthRecorder(targetRateInBytesPerSec, pacer.DefaultBandwidthRecorderWindowSeconds, appCtx), appCtx)
	// Note: as at July 2019, we don't currently have a shutdown method/event on JobsAdmin where this pacer
	// could be shut down. But, it's global anyway, so we just leave it running until application exit.
	ja := &jobsAdmin{
		concurrency:        concurrency,
		jobIDToJobMgr:      newJobIDToJobMgr(),
		pacer:              requestPacer,
		slicePool:          common.NewMultiSizeSlicePool(common.MaxBlockBlobBlockSize),
		cacheLimiter:       common.NewCacheLimiter(maxRamBytesToUse),
		fileCountLimiter:   common.NewCacheLimiter(int64(concurrency.MaxOpenDownloadFiles)),
		cpuMonitor:         cpuMon,
		appCtx:             appCtx,
		commandLineMbpsCap: targetRateInMegaBitsPerSec,
	}
	// create new context with the defaultService api version set as value to serviceAPIVersionOverride in the app context.
	ja.appCtx = context.WithValue(ja.appCtx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// create concurrency tuner...
	// ... but don't spin up the main pool. That is done when
	// the first piece of work actually arrives. Why do it then?
	// So that we don't start tuning with no traffic to process, since doing so skews
	// the tuning results and, in the worst case, leads to "completion" of tuning before any traffic has been sent.
	ja.concurrencyTuner = ja.createConcurrencyTuner()

	JobsAdmin = ja

	// Spin up slice pool pruner
	go ja.slicePoolPruneLoop()

	return nil
}

// Decide on a max amount of RAM we are willing to use. This functions as a cap, and prevents excessive usage.
// There's no measure of physical RAM in the STD library, so we guesstimate conservatively, based on  CPU count (logical, not physical CPUs)
// Note that, as at Feb 2019, the multiSizeSlicePooler uses additional RAM, over this level, since it includes the cache of
// currently-unused, reusable slices, that is not tracked by cacheLimiter.
// Also, block sizes that are not powers of two result in extra usage over and above this limit. (E.g. 100 MB blocks each
// count 100 MB towards this limit, but actually consume 128 MB)
func getMaxRamForChunks() (int64, error) {

	// return the user-specified override value, if any
	envVar := common.EEnvironmentVariable.BufferGB()
	overrideString := common.GetEnvironmentVariable(envVar)
	if overrideString != "" {
		overrideValue, err := strconv.ParseFloat(overrideString, 64)
		if err != nil {
			return 0, fmt.Errorf("Cannot parse environment variable %s, due to error %v", envVar.Name, err)
		} else {
			return int64(overrideValue * 1024 * 1024 * 1024), nil
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
	return maxRamBytesToUse, nil
}

func (ja *jobsAdmin) createConcurrencyTuner() ste.ConcurrencyTuner {
	if ja.concurrency.AutoTuneMainPool() {
		t := ste.NewAutoConcurrencyTuner(ja.concurrency.InitialMainPoolSize, ja.concurrency.MaxMainPoolSize.Value, BenchmarkResults)
		if !t.RequestCallbackWhenStable(func() { ja.recordTuningCompleted(true) }) {
			panic("could not register tuning completion callback")
		}
		return t
	} else {
		ja.recordTuningCompleted(false)
		return &ste.NullConcurrencyTuner{FixedValue: ja.concurrency.InitialMainPoolSize}
	}
}

func (ja *jobsAdmin) recordTuningCompleted(showOutput bool) {
	// remember how many bytes were transferred during tuning, so we can exclude them from our post-tuning throughput calculations
	atomic.StoreInt64(&ja.atomicBytesTransferredWhileTuning, ja.BytesOverWire())
	atomic.StoreInt64(&ja.atomicTuningEndSeconds, time.Now().Unix())

	if showOutput {
		msg := "Automatic concurrency tuning completed."
		if BenchmarkResults {
			msg += " Recording of performance stats will begin now."
		}
		common.GetLifecycleMgr().Info("")
		common.GetLifecycleMgr().Info(msg)
		if BenchmarkResults {
			common.GetLifecycleMgr().Info("")
			common.GetLifecycleMgr().Info("*** After a minute or two, you may cancel the job with CTRL-C to trigger early analysis of the stats. ***")
			common.GetLifecycleMgr().Info("*** You do not need to wait for whole job to finish.                                                  ***")
		}
		common.GetLifecycleMgr().Info("")
		common.LogToJobLogWithPrefix(msg, common.LogInfo)
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var BenchmarkResults = false

// There will be only 1 instance of the jobsAdmin type.
// The coordinator uses this to manage all the running jobs and their job parts.
type jobsAdmin struct {
	atomicBytesTransferredWhileTuning int64
	atomicTuningEndSeconds            int64
	concurrency                       ste.ConcurrencySettings
	jobIDToJobMgr                     jobIDToJobMgr // Thread-safe map from each JobID to its JobInfo
	// Other global state can be stored in more fields here...
	appCtx             context.Context
	pacer              pacer.Interface
	slicePool          common.ByteSlicePooler
	cacheLimiter       common.CacheLimiter
	fileCountLimiter   common.CacheLimiter
	concurrencyTuner   ste.ConcurrencyTuner
	commandLineMbpsCap float64
	cpuMonitor         common.CPUMonitor
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (ja *jobsAdmin) NewJobPartPlanFileName(jobID common.JobID, partNumber common.PartNumber) ste.JobPartPlanFileName {
	return ste.JobPartPlanFileName(fmt.Sprintf(ste.JobPartPlanFileNameFormat, jobID.String(), partNumber, ste.DataSchemaVersion))
}

// JobIDDetails returns point-in-time list of JobIDDetails
func (ja *jobsAdmin) JobIDs() []common.JobID {
	var jobIDs []common.JobID
	ja.jobIDToJobMgr.Iterate(false, func(k common.JobID, v ste.IJobMgr) {
		jobIDs = append(jobIDs, k)
	})
	return jobIDs
}

// JobMgr returns the specified JobID's JobMgr if it exists
func (ja *jobsAdmin) JobMgr(jobID common.JobID) (ste.IJobMgr, bool) {
	return ja.jobIDToJobMgr.Get(jobID)
}

// JobMgrEnsureExists returns the specified JobID's IJobMgr if it exists or creates it if it doesn't already exit
// If it does exist, then the appCtx argument is ignored.
func (ja *jobsAdmin) JobMgrEnsureExists(jobID common.JobID,
	level common.LogLevel, commandString string, jobErrorHandler common.JobErrorHandler) ste.IJobMgr {

	return ja.jobIDToJobMgr.EnsureExists(jobID,
		func() ste.IJobMgr {
			// Return existing or new IJobMgr to caller
			return ste.NewJobMgr(ja.concurrency, jobID, ja.appCtx, ja.cpuMonitor, level, commandString, ja.concurrencyTuner, ja.pacer, ja.slicePool, ja.cacheLimiter, ja.fileCountLimiter, common.AzcopyCurrentJobLogger, false, jobErrorHandler)
		})
}

// JobMgrCleanup cleans up the jobMgr identified by the given jobId. It undoes what NewJobMgr() does, basically it does the following:
// 1. Stop all go routines started to process this job.
// 2. Release the memory allocated for this JobMgr instance.
// Note: this is not thread safe and only one goroutine should call this for a job.
func (ja *jobsAdmin) JobMgrCleanUp(jobId common.JobID) {
	// First thing get the jobMgr.
	jm, found := ja.JobMgr(jobId)

	if found {
		/*
		 * Change log level to Info, so that we can capture these messages in job log file.
		 * These log messages useful in debuggability and tells till what stage cleanup done.
		 */
		jm.Log(common.LogInfo, "JobMgrCleanUp Enter")

		// Delete the jobMgr from jobIDtoJobMgr map, so that next call will fail.
		ja.DeleteJob(jobId)

		jm.Log(common.LogInfo, "Job deleted from jobMgr map")

		/*
		 * Rest of jobMgr related cleanup done by DeferredCleanupJobMgr function.
		 * Now that we have removed the jobMgr from the map, no new caller will find it and hence cannot start any
		 * new activity using the jobMgr. We cleanup the resources of the jobMgr in a deferred manner as a safety net
		 * to allow processing any messages that may be in transit.
		 *
		 * NOTE: This is not really required but we don't want to miss any in-transit messages as some of the TODOs in
		 *               the code suggest.
		 */
		go jm.DeferredCleanupJobMgr()
	}
}

func (ja *jobsAdmin) BytesOverWire() int64 {
	return ja.pacer.GetTotalTraffic()
}

func (ja *jobsAdmin) UpdateTargetBandwidth(newTarget int64) {
	if newTarget < 0 {
		return
	}
	ja.pacer.RequestHardLimit(newTarget)
}

/*
func (ja *jobsAdmin) AddSuccessfulBytesInActiveFiles(n int64) {
	atomic.AddInt64(&ja.atomicSuccessfulBytesInActiveFiles, n)
}

func (ja *jobsAdmin) SuccessfulBytesInActiveFiles() uint64 {
	n := atomic.LoadInt64(&ja.atomicSuccessfulBytesInActiveFiles)
	if n < 0 {
		n = 0 // should never happen, but would result in nasty over/underflow if it did
	}
	return uint64(n)
}
*/

func (ja *jobsAdmin) ResurrectJob(jobId common.JobID,
	srcServiceClient *common.ServiceClient,
	dstServiceClient *common.ServiceClient,
	srcIsOAuth bool, jobErrorHandler common.JobErrorHandler) bool {
	// Search the existing plan files for the PartPlans for the given jobId
	// only the files which are not empty and have JobId has prefix and DataSchemaVersion as Suffix
	// are include in the result
	files := func(prefix, ext string) []os.FileInfo {
		var files []os.FileInfo
		_ = filepath.Walk(common.AzcopyJobPlanFolder, func(path string, fileInfo os.FileInfo, _ error) error {
			if !fileInfo.IsDir() && fileInfo.Size() != 0 && strings.HasPrefix(fileInfo.Name(), prefix) && strings.HasSuffix(fileInfo.Name(), ext) {
				files = append(files, fileInfo)
			}
			return nil
		})
		return files
	}(jobId.String(), fmt.Sprintf(".steV%d", ste.DataSchemaVersion))
	// If no files with JobId exists then return false
	if len(files) == 0 {
		return false
	}
	// sort the JobPartPlan files with respect to Part Number
	sort.Sort(sortPlanFiles{Files: files})
	for f := 0; f < len(files); f++ {
		planFile := ste.JobPartPlanFileName(files[f].Name())
		jobID, partNum, err := planFile.Parse()
		if err != nil {
			continue
		}
		mmf := planFile.Map()
		jm := ja.JobMgrEnsureExists(jobID, mmf.Plan().LogLevel, "", jobErrorHandler)
		args := &ste.AddJobPartArgs{
			PartNum:           partNum,
			PlanFile:          planFile,
			ExistingPlanMMF:   mmf,
			SrcClient:         srcServiceClient,
			DstClient:         dstServiceClient,
			SrcIsOAuth:        srcIsOAuth,
			ScheduleTransfers: false,
			CompletionChan:    nil,
		}
		jm.AddJobPart(args)
	}

	jm, _ := ja.JobMgr(jobId)
	js := resurrectJobSummary(jm)
	jm.ResurrectSummary(js)

	return true
}

func (ja *jobsAdmin) SetConcurrencySettingsToAuto() {
	// Setting initial pool size to 4 and max pool size to 3,000
	ja.concurrency.InitialMainPoolSize = 4
	ja.concurrency.MaxMainPoolSize = &ste.ConfiguredInt{Value: 3000, IsUserSpecified: false, EnvVarName: common.EEnvironmentVariable.ConcurrencyValue().Name, DefaultSourceDesc: "auto-tuning limit"}

	// recreate the concurrency tuner.
	// Tuner isn't called until the first job part is scheduled for transfer, so it is safe to update it before that.
	ja.concurrencyTuner = ja.createConcurrencyTuner()
}

func (ja *jobsAdmin) GetConcurrencySettings() (int, bool) {
	// return a copy of the concurrency settings, so that caller cannot modify the original
	return ja.concurrency.EnumerationPoolSize.Value, ja.concurrency.ParallelStatFiles.Value
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

// TODO: take care of this.
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

func (ja *jobsAdmin) TryGetPerformanceAdvice(bytesInJob uint64, filesInJob uint32, fromTo common.FromTo, dir common.TransferDirection, p *ste.PipelineNetworkStats) []common.PerformanceAdvice {
	if !BenchmarkResults {
		return make([]common.PerformanceAdvice, 0)
	}

	megabitsPerSec := float64(0)
	finalReason, finalConcurrency := ja.concurrencyTuner.GetFinalState()

	secondsAfterTuning := float64(0)
	tuningEndSeconds := atomic.LoadInt64(&ja.atomicTuningEndSeconds)
	if tuningEndSeconds > 0 {
		bytesTransferredAfterTuning := ja.BytesOverWire() - atomic.LoadInt64(&ja.atomicBytesTransferredWhileTuning)
		secondsAfterTuning = time.Since(time.Unix(tuningEndSeconds, 0)).Seconds()
		megabitsPerSec = (8 * float64(bytesTransferredAfterTuning) / secondsAfterTuning) / (1000 * 1000)
	}

	// if we we didn't run enough after the end of tuning, due to too little time or too close the slow patch as throughput winds down approaching 100%,
	// then pretend that we didn't get any tuning result at all
	percentCompleteAtTuningStart := 100 * float64(atomic.LoadInt64(&ja.atomicBytesTransferredWhileTuning)) / float64(bytesInJob)
	if finalReason != ste.ConcurrencyReasonTunerDisabled && (secondsAfterTuning < 10 || percentCompleteAtTuningStart > 95) {
		finalReason = ste.ConcurrencyReasonNone
	}

	averageBytesPerFile := int64(0)
	if filesInJob > 0 {
		averageBytesPerFile = int64(bytesInJob / uint64(filesInJob))
	}

	isToAzureFiles := fromTo.To() == common.ELocation.File()
	a := ste.NewPerformanceAdvisor(p, ja.commandLineMbpsCap, int64(megabitsPerSec), finalReason, finalConcurrency, dir, averageBytesPerFile, isToAzureFiles)
	return a.GetAdvice()
}

func (ja *jobsAdmin) MessageHandler(inputChan <-chan *common.LCMMsg) {
	toBitsPerSec := func(megaBitsPerSec int64) int64 {
		return megaBitsPerSec * 1000 * 1000 / 8
	}

	const minIntervalBetweenPerfAdjustment = time.Minute
	lastPerfAdjustTime := time.Now().Add(-2 * minIntervalBetweenPerfAdjustment)
	var err error

	for {
		msg := <-inputChan
		var msgType common.LCMMsgType
		_ = msgType.Parse(msg.Req.MsgType) // MsgType is already verified by LCM
		switch msgType {
		case common.ELCMMsgType.PerformanceAdjustment():
			var resp common.PerfAdjustmentResp
			var perfAdjustmentReq common.PerfAdjustmentReq

			if time.Since(lastPerfAdjustTime) < minIntervalBetweenPerfAdjustment {
				err = fmt.Errorf("Performance Adjustment already in progress. Please try after %s",
					lastPerfAdjustTime.Add(minIntervalBetweenPerfAdjustment).Format(time.RFC3339))
			}

			if e := json.Unmarshal([]byte(msg.Req.Value), &perfAdjustmentReq); e != nil {
				err = fmt.Errorf("parsing %s failed with %s", msg.Req.Value, e.Error())
			}

			if perfAdjustmentReq.Throughput < 0 {
				err = fmt.Errorf("invalid value %d for cap-mbps. cap-mpbs should be greater than 0",
					perfAdjustmentReq.Throughput)
			}

			if err == nil {
				lastPerfAdjustTime = time.Now()
				ja.UpdateTargetBandwidth(toBitsPerSec(perfAdjustmentReq.Throughput))

				resp.Status = true
				resp.AdjustedThroughPut = perfAdjustmentReq.Throughput
				resp.NextAdjustmentAfter = lastPerfAdjustTime.Add(minIntervalBetweenPerfAdjustment)
				resp.Err = ""
			} else {
				resp.Status = false
				resp.AdjustedThroughPut = -1
				resp.NextAdjustmentAfter = lastPerfAdjustTime.Add(minIntervalBetweenPerfAdjustment)
				resp.Err = err.Error()
			}

			msg.SetResponse(&common.LCMMsgResp{
				TimeStamp: time.Now(),
				MsgType:   msg.Req.MsgType,
				Value:     resp,
				Err:       err,
			})

			msg.Reply()

		default:
		}

	}

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// The jobIDToJobMgr maps each JobID to its JobMgr
type jobIDToJobMgr struct {
	nocopy common.NoCopy
	lock   sync.RWMutex
	m      map[common.JobID]ste.IJobMgr
}

func newJobIDToJobMgr() jobIDToJobMgr {
	return jobIDToJobMgr{m: make(map[common.JobID]ste.IJobMgr)}
}

func (j *jobIDToJobMgr) Set(key common.JobID, value ste.IJobMgr) {
	j.nocopy.Check()
	j.lock.Lock()
	j.m[key] = value
	j.lock.Unlock()
}

func (j *jobIDToJobMgr) Get(key common.JobID) (value ste.IJobMgr, found bool) {
	j.nocopy.Check()
	j.lock.RLock()
	value, found = j.m[key]
	j.lock.RUnlock()
	return
}

func (j *jobIDToJobMgr) EnsureExists(jobID common.JobID, newJobMgr func() ste.IJobMgr) ste.IJobMgr {
	j.nocopy.Check()
	j.lock.Lock()

	// defined variables both jm & found above condition since defined variables might get re-initialized
	// in if condition if any variable in the left was not initialized.
	var jm ste.IJobMgr
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

func (j *jobIDToJobMgr) Iterate(write bool, f func(k common.JobID, v ste.IJobMgr)) {
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
