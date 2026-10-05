package ste

import (
	"context"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func (jm *jobMgr) closeXferDone() {
	jm.xferDoneCloseOnce.Do(func() {
		if jm.xferDoneInput != nil {
			close(jm.xferDoneInput)
		}
	})
}

// RequestCancellation does not wait for the completion reporter, so producers
// blocked behind engine queues can unwind before the drain barrier is entered.
func (jm *jobMgr) RequestCancellation() {
	if part, ok := jm.jobPartMgrs.Get(0); ok {
		plan := part.Plan()
		status := plan.JobStatus()
		if !status.IsJobDone() {
			plan.SetJobStatus(common.EJobStatus.Cancelling())
		}
	}
	jm.cancel()
	jm.dispatchHardlinkParts(jm.hardlinkGate.cancel())
}

// CancelAndDrain requires enumeration and dispatch producers to be joined first.
// It cancels actual work, then waits for scheduling, transfer epilogues and part
// completion handling before allowing callers to release job resources.
func (jm *jobMgr) CancelAndDrain(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("a cleanup context is required")
	}
	part, ok := jm.jobPartMgrs.Get(0)
	if !ok {
		jm.cancel()
		return fmt.Errorf("cannot drain job %s: part zero is missing", jm.jobID)
	}
	plan := part.Plan()
	jm.RequestCancellation()
	jm.partCreatedCloseOnce.Do(func() {
		if jm.partCreatedInput != nil {
			close(jm.partCreatedInput)
		}
	})

	// Wake completion handling without an unbounded send if a failed consumer has
	// stopped responding. The caller must retain resources on any timeout.
	jm.drainTracker.add(1)
	select {
	case jm.jobPartProgress <- jobPartProgressInfo{}:
	case <-jm.reportLoopDone:
		jm.drainTracker.done(1)
	case <-ctx.Done():
		jm.drainTracker.done(1)
		return ctx.Err()
	}
	if err := jm.drainTracker.wait(ctx); err != nil {
		return err
	}

	jm.closeXferDone()
	select {
	case <-jm.jstm.xferDoneDrained:
	case <-ctx.Done():
		return ctx.Err()
	}
	if plan.JobStatus() == common.EJobStatus.Cancelling() {
		plan.SetJobStatus(common.EJobStatus.Cancelled())
	}
	return nil
}
