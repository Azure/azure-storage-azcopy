package cmd

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type jobCancellationTestHost struct {
	common.LifecycleMgr
	signals chan os.Signal
}

func (h *jobCancellationTestHost) CancellationChannel() <-chan os.Signal { return h.signals }

func TestLibraryJobContextReceivesHostCancellation(t *testing.T) {
	previousCommand, previousCommon := GetLifecycleMgr(), common.GetLifecycleMgr()
	t.Cleanup(func() {
		glcm = previousCommand
		common.SetLifecycleMgr(previousCommon)
	})
	host := &jobCancellationTestHost{signals: make(chan os.Signal, 1)}
	SetLifecycleMgr(host)
	ctx, cancel := WithJobCancellation(context.Background())
	defer cancel()
	common.SetJobCancellationRequestsEnabled(ctx, true)
	requests, approveCancellation := common.JobCancellationRequests(ctx)
	host.signals <- os.Interrupt
	select {
	case <-requests:
	case <-time.After(time.Second):
		t.Fatal("host cancellation request did not reach the library job context")
	}
	if ctx.Err() != nil {
		t.Fatal("interactive cancellation cancelled the job before approval")
	}
	approveCancellation()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("approved cancellation did not cancel the job context")
	}
}

func TestLibraryJobContextCleanupWithoutHostSignal(t *testing.T) {
	previousCommand, previousCommon := GetLifecycleMgr(), common.GetLifecycleMgr()
	t.Cleanup(func() {
		glcm = previousCommand
		common.SetLifecycleMgr(previousCommon)
	})
	SetLifecycleMgr(&jobCancellationTestHost{signals: make(chan os.Signal)})
	ctx, cancel := WithJobCancellation(context.Background())
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("job cleanup did not cancel the bridged context")
	}
}

func TestLibraryJobContextPreflightCancellation(t *testing.T) {
	previousCommand, previousCommon := GetLifecycleMgr(), common.GetLifecycleMgr()
	t.Cleanup(func() {
		glcm = previousCommand
		common.SetLifecycleMgr(previousCommon)
	})
	host := &jobCancellationTestHost{signals: make(chan os.Signal, 1)}
	SetLifecycleMgr(host)
	ctx, cancel := WithJobCancellation(context.Background())
	defer cancel()
	host.signals <- os.Interrupt
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("preflight or dry-run cancellation waited for a nonexistent reporter")
	}
}
