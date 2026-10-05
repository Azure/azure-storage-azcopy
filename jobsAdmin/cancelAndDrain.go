package jobsAdmin

import (
	"context"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type jobDrainer interface {
	CancelAndDrain(context.Context) error
}

// RequestJobCancellation starts cancellation before callers join blocked dispatch
// producers. It neither resurrects jobs nor waits for their completion.
func RequestJobCancellation(jobID common.JobID) error {
	if jobID.IsEmpty() {
		return fmt.Errorf("a job ID is required for cancellation")
	}
	if JobsAdmin == nil {
		return nil
	}
	manager, found := JobsAdmin.JobMgr(jobID)
	if !found {
		return nil
	}
	requester, ok := manager.(interface{ RequestCancellation() })
	if !ok {
		return fmt.Errorf("job %s does not support nonblocking cancellation", jobID)
	}
	requester.RequestCancellation()
	return nil
}

// CancelAndDrainJob never resurrects a missing job or releases resources. Callers
// must stop/join producers first, then clean up only if this barrier succeeds.
func CancelAndDrainJob(ctx context.Context, jobID common.JobID) error {
	return cancelAndDrainJob(ctx, jobID, func(id common.JobID) (jobDrainer, error) {
		if JobsAdmin == nil {
			return nil, nil
		}
		manager, found := JobsAdmin.JobMgr(id)
		if !found {
			return nil, nil
		}
		drainer, ok := manager.(jobDrainer)
		if !ok {
			return nil, fmt.Errorf("job %s does not support draining active transfers", id)
		}
		return drainer, nil
	})
}

func cancelAndDrainJob(ctx context.Context, jobID common.JobID, lookup func(common.JobID) (jobDrainer, error)) error {
	if ctx == nil {
		return fmt.Errorf("a cleanup context is required")
	}
	if jobID.IsEmpty() {
		return fmt.Errorf("a job ID is required for cancellation and drain")
	}
	drainer, err := lookup(jobID)
	if err != nil || drainer == nil {
		return err
	}
	return drainer.CancelAndDrain(ctx)
}
