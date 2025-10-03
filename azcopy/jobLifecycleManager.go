package azcopy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

type JobProgressTracker interface {
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

type JobLifecycleManager struct {
	completionFuncs []func()
	completionChan  chan struct{}
	errorChan       chan string
	mutex           sync.RWMutex
	done            bool
	lastError       string
	handler         *common.JobUIHooks
}

// NewJobLifecycleManager creates a new JobLifecycle instance that implements the JobLifecycle interface.
// This can be used by copy, sync, and resume operations to manage job lifecycle.
//
// The job supports adaptive progress reporting that:
// - Starts with 2-second intervals
// - Automatically reduces to 2-minute intervals for large jobs (>1M transfers)
// - Matches the behavior of AzCopy's lifecycle manager
// - Logs frequency changes via the Info() method
func NewJobLifecycleManager(handler *common.JobUIHooks) *JobLifecycleManager {
	jlcm := &JobLifecycleManager{
		completionFuncs: make([]func(), 0),
		completionChan:  make(chan struct{}, 1),
		errorChan:       make(chan string, 1),
		done:            false,
		handler:         handler,
	}

	return jlcm
}

func (j *JobLifecycleManager) RegisterCloseFunc(f func()) {
	j.completionFuncs = append(j.completionFuncs, f)
}

func (j *JobLifecycleManager) OnComplete() {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	if j.done {
		return // prevent multiple completions
	}

	j.done = true

	// Execute completion functions
	for _, fn := range j.completionFuncs {
		if fn != nil {
			fn()
		}
	}

	// Signal completion (non-blocking)
	select {
	case j.completionChan <- struct{}{}:
	default:
	}
}

// TODO : rename interface method to OnError to match other methods
func (j *JobLifecycleManager) Error(err string) {
	j.OnError(err)
}

func (j *JobLifecycleManager) OnError(err string) {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	if j.done {
		return // prevent multiple errors
	}

	j.done = true
	j.lastError = err

	// Execute completion functions
	for _, fn := range j.completionFuncs {
		if fn != nil {
			fn()
		}
	}

	// Signal error (non-blocking)
	select {
	case j.errorChan <- err:
	default:
	}
}

func (j *JobLifecycleManager) GetError() error {
	j.mutex.RLock()
	defer j.mutex.RUnlock()
	return common.Iff(j.lastError == "", nil, errors.New(j.lastError))
}

func (j *JobLifecycleManager) Wait() error {
	j.mutex.RLock()
	isDone := j.done
	lastError := j.lastError
	j.mutex.RUnlock()

	if isDone {
		if lastError != "" {
			return errors.New(lastError)
		}
		return nil
	}

	// wait until OnComplete or OnError is called
	select {
	case <-j.completionChan:
		return nil
	case errMsg := <-j.errorChan:
		return errors.New(errMsg)
	}
}

func (j *JobLifecycleManager) InitiateProgressReporting(ctx context.Context, reporter JobProgressTracker) {
	reporter.Start()

	// Start progress reporting in a separate goroutine with adaptive frequency
	go func() {
		// Recover from any panic to prevent waiting indefinitely
		defer func() {
			if r := recover(); r != nil {
				j.OnError(fmt.Sprintf("progress reporting panic: %v", r))
			}
		}()

		// Progress reporting configuration (exactly like lifecycleMgr)
		const progressFrequencyThreshold = 1000000
		var oldCount, newCount uint32
		wait := 2 * time.Second
		lastFetchTime := time.Now().Add(-wait) // Start fetching immediately

		cancelCalled := false

		for {
			j.mutex.RLock()
			isDone := j.done
			j.mutex.RUnlock()

			if isDone {
				break
			}

			// Time-based progress reporting (exactly like lifecycle manager's logic)
			select {
			case <-time.After(wait):
				if time.Since(lastFetchTime) >= wait {

					newCount, isDone = reporter.CheckProgress()
					lastFetchTime = time.Now()
					if isDone {
						// OnComplete will mark the job as done to bring down the progress reporter and then call the user provided Handler
						j.OnComplete()
					}
				}
			case <-ctx.Done():
				cancelCalled = true
				j.handler.Info("Cancellation requested. Beginning clean shutdown...")
				if !reporter.CompletedEnumeration() {
					answer := j.handler.Prompt("The enumeration (source only for copy, source/destination comparison for sync) is not complete, "+
						"cancelling the job at this point means it cannot be resumed.",
						common.PromptDetails{
							PromptType: common.EPromptType.Cancel(),
							ResponseOptions: []common.ResponseOption{
								common.EResponseOption.Yes(),
								common.EResponseOption.No(),
							},
						})

					if answer != common.EResponseOption.Yes() {
						// user aborted cancel - continue monitoring but don't cancel
						cancelCalled = false
						continue
					}
				}
				// schedule job cancellation
				// reporter will continue to report progress until the job is fully cancelled or completed
				jobID := reporter.GetJobID()
				err := j.cancelJob(jobID)
				if err != nil {
					j.OnError("error occurred while cancelling the job " + jobID.String() + ": " + err.Error())
				}
				continue
			}

			// Adjust frequency based on transfer count (exactly like lifecycle manager)
			if !cancelCalled {
				if newCount >= progressFrequencyThreshold {
					// Reduce progress reporting frequency for large jobs to save CPU costs
					wait = 2 * time.Minute
					if oldCount < progressFrequencyThreshold {
						j.handler.Info(fmt.Sprintf("Reducing progress output frequency to %v, because there are over %d files", wait, progressFrequencyThreshold))
					}
				}
			}

			oldCount = newCount
		}
	}()
}

func (j *JobLifecycleManager) cancelJob(jobID common.JobID) error {
	if jobID.IsEmpty() {
		return errors.New("cancel job requires the JobID")
	}
	resp := jobsAdmin.CancelPauseJobOrder(jobID, common.EJobStatus.Cancelling(), j)
	if !resp.CancelledPauseResumed {
		return errors.New(resp.ErrorMsg)
	}
	return nil
}
