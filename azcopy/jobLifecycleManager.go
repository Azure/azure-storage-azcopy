// Copyright © 2025 Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

type jobProgressTracker interface {
	// Start - calls OnStart
	Start()
	// CheckProgress checks the progress of the job and returns the number of transfers completed so far and whether the job is done
	CheckProgress() (uint32, bool)
	// CompletedEnumeration checks whether the enumeration is complete
	CompletedEnumeration() bool // Whether we should prompt before cancelling
	// GetJobID returns the JobID of the job being tracked
	GetJobID() common.JobID
	// GetElapsedTime returns the elapsed time since the job started
	GetElapsedTime() time.Duration
}

type jobLifecycleManager struct {
	completionFuncs []func()
	completionChan  chan struct{}
	mutex           sync.RWMutex
	done            bool
	completed       bool
	lastError       error
	handler         common.LifecycleMgr
	jobLogger       common.ILoggerResetable
	stopChan        chan struct{}
	stopOnce        sync.Once
	progressStarted bool
	stopped         bool
	progressWG      sync.WaitGroup
	progressDone    chan struct{}
}

// NewJobLifecycleManager creates a new JobLifecycle instance that implements the JobLifecycle interface.
// This can be used by copy, sync, and resume operations to manage job lifecycle.
//
// The job supports adaptive progress reporting that:
// - Starts with 2-second intervals
// - Reduces to 2-minute intervals for large jobs (>1M transfers), or 5 seconds in Mover
// - Matches the behavior of AzCopy's lifecycle manager
// - Logs frequency changes via the Info() method
func NewJobLifecycleManager(handler common.LifecycleMgr, loggers ...common.ILoggerResetable) *jobLifecycleManager {
	if handler == nil {
		handler = common.GetLifecycleMgr()
	}
	jlcm := &jobLifecycleManager{
		completionFuncs: make([]func(), 0),
		completionChan:  make(chan struct{}),
		stopChan:        make(chan struct{}),
		progressDone:    make(chan struct{}),
		handler:         handler,
	}
	if len(loggers) > 0 {
		jlcm.jobLogger = loggers[0]
	}

	return jlcm
}

func (j *jobLifecycleManager) JobLogger() common.ILoggerResetable {
	return j.jobLogger
}

// RegisterCloseFunc registers per-job cleanup. Callbacks can reenter the manager,
// but must not wait for the completion that their own return is responsible for.
func (j *jobLifecycleManager) RegisterCloseFunc(f func()) {
	if f == nil {
		return
	}
	j.mutex.Lock()
	if j.completed {
		j.mutex.Unlock()
		f()
		return
	}
	j.completionFuncs = append(j.completionFuncs, f)
	j.mutex.Unlock()
}

func (j *jobLifecycleManager) OnComplete() {
	j.finish(nil)
}

func (j *jobLifecycleManager) Error(err string) {
	if err == "" {
		err = "job failed"
	}
	j.finish(errors.New(j.handler.SanitizeLogMessage(err)))
}

func (j *jobLifecycleManager) finish(err error) {
	j.mutex.Lock()
	if j.done {
		j.mutex.Unlock()
		return
	}
	j.done = true
	j.lastError = err
	j.mutex.Unlock()
	j.stopOnce.Do(func() { close(j.stopChan) })

	// Completion callbacks may reenter the manager or join the reporter. Never invoke them
	// while holding its lock or from the reporter goroutine itself.
	go func() {
		j.progressWG.Wait()
		for {
			j.mutex.Lock()
			callbacks := j.completionFuncs
			j.completionFuncs = nil
			if len(callbacks) == 0 {
				j.completed = true
				close(j.completionChan)
				j.mutex.Unlock()
				return
			}
			j.mutex.Unlock()
			for _, callback := range callbacks {
				j.runCloseFunc(callback)
			}
		}
	}()
}

func (j *jobLifecycleManager) runCloseFunc(callback func()) {
	defer func() {
		if recovered := recover(); recovered != nil {
			message := common.NewAzCopyLogSanitizer().SanitizeLogMessage(fmt.Sprintf("job completion callback panic: %v", recovered))
			j.mutex.Lock()
			defer j.mutex.Unlock()
			if j.lastError == nil {
				j.lastError = errors.New(message)
			}
		}
	}()
	callback()
}

func (j *jobLifecycleManager) GetError() error {
	j.mutex.RLock()
	defer j.mutex.RUnlock()
	return j.lastError
}

func (j *jobLifecycleManager) Wait() error {
	<-j.completionChan
	return j.GetError()
}

// Stop joins progress reporting before the caller cleans up job resources. It never
// closes a client, a job manager, or any shared pools. Do not call it from a progress callback.
func (j *jobLifecycleManager) Stop() {
	_ = j.StopContext(context.Background())
}

// StopContext bounds the reporter join, including a host callback that has not returned.
func (j *jobLifecycleManager) StopContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("a cleanup context is required")
	}
	j.mutex.Lock()
	j.stopped = true
	started := j.progressStarted
	j.mutex.Unlock()
	j.stopOnce.Do(func() { close(j.stopChan) })
	if !started {
		return nil
	}
	select {
	case <-j.progressDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CancelAndDrain requires all enumeration/dispatch producers to have stopped.
// A non-nil result means resources must be retained rather than cleaned up.
func (j *jobLifecycleManager) CancelAndDrain(ctx context.Context, jobID common.JobID) error {
	drainErr := jobsAdmin.CancelAndDrainJob(ctx, jobID)
	stopErr := j.StopContext(ctx)
	if drainErr != nil {
		return drainErr
	}

	if stopErr != nil {
		return stopErr
	}
	j.finish(context.Canceled)
	select {
	case <-j.completionChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RequestCancellation unblocks engine work before the caller joins dispatch producers.
func (j *jobLifecycleManager) RequestCancellation(jobID common.JobID) error {
	return jobsAdmin.RequestJobCancellation(jobID)
}

func (j *jobLifecycleManager) InitiateProgressReporting(ctx context.Context, reporter jobProgressTracker) {
	j.mutex.Lock()
	if j.done || j.stopped || j.progressStarted {
		j.mutex.Unlock()
		return
	}
	j.progressStarted = true
	j.progressWG.Add(1)
	j.mutex.Unlock()
	common.SetJobCancellationRequestsEnabled(ctx, true)

	started := false
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				j.Error(fmt.Sprintf("progress reporting panic: %v", recovered))
			}
		}()
		reporter.Start()
		started = true
	}()
	if !started {
		common.SetJobCancellationRequestsEnabled(ctx, false)
		j.progressWG.Done()
		close(j.progressDone)
		return
	}

	// Start progress reporting in a separate goroutine with adaptive frequency
	go func() {
		defer func() {
			j.progressWG.Done()
			close(j.progressDone)
		}()
		defer common.SetJobCancellationRequestsEnabled(ctx, false)
		// Recover from any panic to prevent waiting indefinitely
		defer func() {
			if r := recover(); r != nil {
				j.Error(fmt.Sprintf("progress reporting panic: %v", r))
			}
		}()

		// Progress reporting configuration (exactly like lifecycleMgr)
		const progressFrequencyThreshold = 1000000
		var oldCount, newCount uint32
		wait := 2 * time.Second
		lastFetchTime := time.Now().Add(-wait) // Start fetching immediately

		cancelCalled := false
		cancelChannel := ctx.Done()
		cancelRequests, approveCancellation := common.JobCancellationRequests(ctx)

		for {
			j.mutex.RLock()
			isDone := j.done
			j.mutex.RUnlock()

			if isDone {
				break
			}

			cancelRequested := false
			interactiveCancellation := false
			select {
			case <-j.stopChan:
				return
			case <-time.After(wait):
				if time.Since(lastFetchTime) >= wait {

					newCount, isDone = reporter.CheckProgress()
					lastFetchTime = time.Now()
					if isDone {
						// OnComplete will mark the job as done to bring down the progress reporter and then call the user provided Handler
						j.OnComplete()
						return
					}
				}
			case <-cancelChannel:
				cancelChannel = nil
				cancelRequested = true
			case _, ok := <-cancelRequests:
				if !ok {
					cancelRequests = nil
					continue
				}
				cancelRequested = true
				interactiveCancellation = ctx.Err() == nil
			}

			if cancelRequested {
				cancelCalled = true
				wait = 2 * time.Second
				j.handler.Info("Cancellation requested. Beginning clean shutdown...")
				if interactiveCancellation && !reporter.CompletedEnumeration() {
					answer := j.handler.Prompt("The enumeration (source only for copy, source/destination comparison for sync) is not complete, "+
						"cancelling the job at this point means it cannot be resumed.",
						common.PromptDetails{
							PromptType: common.EPromptType.Cancel(),
							ResponseOptions: []common.ResponseOption{
								common.EResponseOption.Yes(),
								common.EResponseOption.No(),
							},
						})

					if answer != common.EResponseOption.Yes() && answer != common.EResponseOption.Default() {
						// user aborted cancel - continue monitoring but don't cancel
						cancelCalled = false
						continue
					}
				}
				if interactiveCancellation && approveCancellation != nil {
					approveCancellation()
				}
				// Context cancellation is irrevocable; process it only once. Interactive
				// requests are disabled only after approval, not after a declined prompt.
				cancelChannel = nil
				cancelRequests = nil
				if pending, ok := reporter.(interface{ firstPartOrdered() bool }); ok && !pending.firstPartOrdered() {
					err := ctx.Err()
					if err == nil {
						err = context.Canceled
					}
					j.finish(err)
					return
				}
				// schedule job cancellation
				// reporter will continue to report progress until the job is fully cancelled or completed
				jobID := reporter.GetJobID()
				err := j.cancelJob(jobID)
				if err != nil {
					j.Error("error occurred while cancelling the job " + jobID.String() + ": " + err.Error())
					return
				}
				continue
			}

			// Adjust frequency based on transfer count (exactly like lifecycle manager)
			if !cancelCalled {
				if newCount >= progressFrequencyThreshold {
					// Reduce progress reporting frequency for large jobs to save CPU costs
					wait = 2 * time.Minute
					if buildmode.IsMover {
						wait = 5 * time.Second
					}
					if oldCount < progressFrequencyThreshold {
						j.handler.Info(fmt.Sprintf("Reducing progress output frequency to %v, because there are over %d files", wait, progressFrequencyThreshold))
					}
				}
			}

			oldCount = newCount
		}
	}()
}

func (j *jobLifecycleManager) cancelJob(jobID common.JobID) error {
	if jobID.IsEmpty() {
		return errors.New("cancel job requires the JobID")
	}
	if jobsAdmin.JobsAdmin == nil {
		return errors.New("storage transfer engine is not initialized")
	}
	j.handler.Info("Canceling job started")
	resp := jobsAdmin.CancelPauseJobOrder(jobID, common.EJobStatus.Cancelling(), j)
	if !resp.CancelledPauseResumed {
		if manager, ok := jobsAdmin.JobsAdmin.JobMgr(jobID); ok {
			summary := manager.ListJobSummary(false)
			if summary.JobStatus.IsJobDone() {
				return nil
			}
		}
		return errors.New(resp.ErrorMsg)
	}
	return nil
}
