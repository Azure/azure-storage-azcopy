package jobsAdmin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type fakeJobDrainer struct {
	called chan struct{}
	done   chan struct{}
	err    error
}

func (d *fakeJobDrainer) CancelAndDrain(ctx context.Context) error {
	close(d.called)
	select {
	case <-d.done:
		return d.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestCancelAndDrainWaitsForBackend(t *testing.T) {
	drainer := &fakeJobDrainer{called: make(chan struct{}), done: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- cancelAndDrainJob(ctx, common.NewJobID(), func(common.JobID) (jobDrainer, error) {
			return drainer, nil
		})
	}()
	<-drainer.called
	select {
	case <-result:
		t.Fatal("backend cancellation request was mistaken for a completed drain")
	default:
	}
	close(drainer.done)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestCancelAndDrainPropagatesTimeout(t *testing.T) {
	drainer := &fakeJobDrainer{called: make(chan struct{}), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := cancelAndDrainJob(ctx, common.NewJobID(), func(common.JobID) (jobDrainer, error) {
		return drainer, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("failed drains must prevent cleanup: %v", err)
	}
}

func TestCancelAndDrainDoesNotResurrectMissingJob(t *testing.T) {
	err := cancelAndDrainJob(context.Background(), common.NewJobID(), func(common.JobID) (jobDrainer, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
