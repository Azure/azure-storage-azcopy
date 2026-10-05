package ste

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

// No Plan method is provided: the phase gate must never dereference an unmapped plan.
type hardlinkGateTestPart struct{ IJobPartMgr }

func TestM5HardlinksWaitForFinalAndMixedCompletion(t *testing.T) {
	var gate hardlinkPartGate
	link := &hardlinkGateTestPart{}
	gate.register(0, common.EJobPartType.Mixed(), false)
	gate.register(1, common.EJobPartType.Hardlink(), false)
	require.Empty(t, gate.enqueue(link))
	require.Empty(t, gate.complete(0), "an early mixed completion is not end-of-enumeration")
	gate.register(2, common.EJobPartType.Mixed(), true)
	require.Empty(t, gate.complete(1), "hardlink state cannot bypass unfinished mixed data")
	require.Equal(t, []IJobPartMgr{link}, gate.complete(2))
	require.Empty(t, gate.complete(2), "hardlinks must be released only once")
}

func TestM5HardlinksOnlyAndZeroMixed(t *testing.T) {
	t.Run("hardlinks-only", func(t *testing.T) {
		var gate hardlinkPartGate
		link := &hardlinkGateTestPart{}
		gate.register(0, common.EJobPartType.Hardlink(), true)
		require.Equal(t, []IJobPartMgr{link}, gate.enqueue(link))
	})
	t.Run("empty-mixed-part", func(t *testing.T) {
		var gate hardlinkPartGate
		link := &hardlinkGateTestPart{}
		gate.register(0, common.EJobPartType.Mixed(), false)
		gate.register(1, common.EJobPartType.Hardlink(), true)
		require.Empty(t, gate.enqueue(link))
		require.Equal(t, []IJobPartMgr{link}, gate.complete(0))
	})
}

func TestM5HardlinkResumeResetsCompletedMixedParts(t *testing.T) {
	var gate hardlinkPartGate
	gate.register(0, common.EJobPartType.Mixed(), false)
	gate.register(1, common.EJobPartType.Hardlink(), true)
	gate.complete(0)
	gate.reset()
	// Resume registers all plans before queueing any part, even in arbitrary map order.
	gate.register(1, common.EJobPartType.Hardlink(), true)
	gate.register(0, common.EJobPartType.Mixed(), false)
	link := &hardlinkGateTestPart{}
	require.Empty(t, gate.enqueue(link))
	require.Equal(t, []IJobPartMgr{link}, gate.complete(0))
}

func TestM5HardlinkGateRejectsMissingMixedRegistration(t *testing.T) {
	var gate hardlinkPartGate
	gate.register(1, common.EJobPartType.Hardlink(), true)
	link := &hardlinkGateTestPart{}
	require.Empty(t, gate.enqueue(link))
	gate.register(0, common.EJobPartType.Mixed(), false)
	require.Equal(t, []IJobPartMgr{link}, gate.complete(0))
}

func TestM5HardlinkCancellationReleasesWithoutFinalPart(t *testing.T) {
	var gate hardlinkPartGate
	first, second := &hardlinkGateTestPart{}, &hardlinkGateTestPart{}
	gate.register(0, common.EJobPartType.Mixed(), false)
	gate.register(1, common.EJobPartType.Hardlink(), false)
	require.Empty(t, gate.enqueue(first))
	require.Equal(t, []IJobPartMgr{first}, gate.cancel())
	require.Empty(t, gate.cancel())
	gate.register(2, common.EJobPartType.Hardlink(), false)
	require.Equal(t, []IJobPartMgr{second}, gate.enqueue(second))
}

func TestM5DeferredHardlinksParticipateInRealDrain(t *testing.T) {
	queue := make(chan IJobPartMgr)
	manager := &jobMgr{coordinatorChannels: CoordinatorChannels{partsChannel: queue}}
	part := &jobPartMgr{
		cachedPartNum: 1, cachedNumTransfers: 2,
		cachedJobPartType: common.EJobPartType.Hardlink(),
	}
	manager.hardlinkGate.register(0, common.EJobPartType.Mixed(), false)
	manager.hardlinkGate.register(1, common.EJobPartType.Hardlink(), false)
	manager.QueueJobParts(part)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, manager.drainTracker.wait(ctx), context.Canceled,
		"deferred hardlinks count before their scheduling channel send")

	// Cancellation release must not block a completion consumer on a full queue.
	released := make(chan struct{})
	go func() {
		manager.dispatchHardlinkParts(manager.hardlinkGate.cancel())
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("phase release blocked on scheduling-channel capacity")
	}
	require.True(t, errors.Is(manager.drainTracker.wait(ctx), context.Canceled))
	select {
	case got := <-queue:
		require.Same(t, part, got)
	case <-time.After(time.Second):
		t.Fatal("cancelled deferred part was not released for normal completion accounting")
	}
	part.drainTracker.done(3) // scheduler + two transfer epilogues
	deadline, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	require.NoError(t, manager.drainTracker.wait(deadline))
}
