package ste

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type drainTestPart struct {
	IJobPartMgr
	plan *JobPartPlanHeader
}

func (p *drainTestPart) Plan() *JobPartPlanHeader { return p.plan }

// Only status/report consumers are fake. Tests exercise the concrete engine
// cancellation/drain implementation without creating files, clients or worker pools.
func newDrainTestJob(t *testing.T, status common.JobStatus) (*jobMgr, *JobPartPlanHeader) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	plan := &JobPartPlanHeader{}
	plan.SetJobStatus(status)
	manager := &jobMgr{
		jobID:            common.NewJobID(),
		ctx:              ctx,
		cancel:           cancel,
		jobPartMgrs:      newJobPartToJobPartMgr(),
		jobPartProgress:  make(chan jobPartProgressInfo),
		reportLoopDone:   make(chan struct{}),
		xferDoneInput:    make(chan xferDoneMsg),
		partCreatedInput: make(chan JobPartCreatedMsg),
		jstm:             &jobStatusManager{xferDoneDrained: make(chan struct{})},
	}
	manager.jobPartMgrs.Set(0, &drainTestPart{plan: plan})

	stop := make(chan struct{})
	reportsStopped := make(chan struct{})
	statusStopped := make(chan struct{})
	go func() {
		defer close(reportsStopped)
		for {
			select {
			case <-manager.jobPartProgress:
				manager.drainTracker.done(1)
			case <-stop:
				return
			}
		}
	}()
	go func() {
		defer close(statusStopped)
		select {
		case <-manager.xferDoneInput:
			close(manager.jstm.xferDoneDrained)
		case <-stop:
		}
	}()
	t.Cleanup(func() {
		cancel()
		close(stop)
		<-reportsStopped
		<-statusStopped
	})
	return manager, plan
}

func TestEngineDrainQueueRegistration(t *testing.T) {
	manager, _ := newDrainTestJob(t, common.EJobStatus.InProgress())
	queue := make(chan IJobPartMgr, 1)
	manager.coordinatorChannels.partsChannel = queue
	part := &jobPartMgr{cachedNumTransfers: 2}
	manager.QueueJobParts(part)
	if <-queue != part || part.drainTracker != &manager.drainTracker {
		t.Fatal("queued part was not attached to the job drain tracker")
	}
	manager.drainTracker.mu.Lock()
	pending := manager.drainTracker.pending
	manager.drainTracker.mu.Unlock()
	if pending != 3 {
		t.Fatalf("queued work must include two transfers and their scheduler: got %d", pending)
	}
	manager.drainTracker.done(3)
}

func TestEngineDrainDoesNotTrustTerminalStatus(t *testing.T) {
	for _, test := range []struct {
		name  string
		units uint64
	}{
		{name: "queued-part", units: 1},
		{name: "inflight-transfer", units: 1},
		{name: "completion-report", units: 1},
		{name: "queued-and-inflight", units: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager, _ := newDrainTestJob(t, common.EJobStatus.Cancelled())
			manager.drainTracker.add(test.units)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if err := manager.CancelAndDrain(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("terminal status incorrectly bypassed pending work: %v", err)
			}
			select {
			case <-manager.xferDoneInput:
				t.Fatal("completion input closed while queued or inflight work remained")
			default:
			}
			manager.drainTracker.done(test.units)
			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := manager.CancelAndDrain(ctx); err != nil {
				t.Fatalf("actual completion did not release the barrier: %v", err)
			}
		})
	}
}

func TestEngineDrainZeroAndCompletedJobs(t *testing.T) {
	for _, initial := range []common.JobStatus{common.EJobStatus.InProgress(), common.EJobStatus.Completed()} {
		t.Run(initial.String(), func(t *testing.T) {
			manager, plan := newDrainTestJob(t, initial)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := manager.CancelAndDrain(ctx); err != nil {
				t.Fatal(err)
			}
			expected := common.EJobStatus.Cancelled()
			if initial == common.EJobStatus.Completed() {
				expected = initial
			}
			if plan.JobStatus() != expected || !errors.Is(manager.ctx.Err(), context.Canceled) {
				t.Fatal("drain failed to cancel work or changed a completed job's status")
			}
			if err := manager.CancelAndDrain(ctx); err != nil {
				t.Fatalf("repeated drain must be safe: %v", err)
			}
		})
	}
}

func TestEngineDrainCancelledCleanupContextRetainsWork(t *testing.T) {
	manager, _ := newDrainTestJob(t, common.EJobStatus.InProgress())
	manager.drainTracker.add(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := manager.CancelAndDrain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled cleanup context did not stop the wait: %v", err)
	}
	if manager.ctx.Err() == nil {
		t.Fatal("engine cancellation was not requested")
	}
	select {
	case <-manager.xferDoneInput:
		t.Fatal("timeout must not release an active completion stream")
	default:
	}
	manager.drainTracker.done(1)
}
