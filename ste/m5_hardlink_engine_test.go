package ste

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

type hardlinkSchedulingTestMgr struct {
	IJobMgr
	reports   []jobPartProgressInfo
	confirmed bool
	onReport  func()
}

func (m *hardlinkSchedulingTestMgr) ReportJobPartDone(progress jobPartProgressInfo) {
	m.reports = append(m.reports, progress)
	if m.onReport != nil {
		m.onReport()
	}
}
func (m *hardlinkSchedulingTestMgr) ConfirmAllTransfersScheduled() { m.confirmed = true }
func (*hardlinkSchedulingTestMgr) Log(common.LogLevel, string)     {}
func (*hardlinkSchedulingTestMgr) ShouldLog(common.LogLevel) bool  { return false }

func TestM5ZeroMixedPartReportsCompletion(t *testing.T) {
	name := createM5Plan(t, nil)
	mapped := name.Map()
	defer mapped.Unmap()
	mapped.Plan().JobPartType = common.EJobPartType.Mixed()
	mapped.Plan().IsFinalPart = false
	manager := &hardlinkSchedulingTestMgr{}
	var work jobWorkTracker
	work.add(1)
	part := &jobPartMgr{
		jobMgr: manager, filename: name, planMMF: mapped,
		cachedPartNum: 0, cachedNumTransfers: 0, cachedIsFinalPart: false,
		drainTracker: &work,
	}
	part.ScheduleTransfers(context.Background())
	require.Len(t, manager.reports, 1)
	require.NotNil(t, manager.reports[0].partNum)
	require.Equal(t, PartNumber(0), *manager.reports[0].partNum)
	require.Equal(t, common.EJobStatus.Completed(), mapped.Plan().JobPartStatus())
	require.False(t, manager.confirmed)
	require.NoError(t, work.wait(context.Background()))
}

func TestM5ZeroFinalPartConfirmsScheduling(t *testing.T) {
	name := createM5Plan(t, nil)
	mapped := name.Map()
	defer mapped.Unmap()
	manager := &hardlinkSchedulingTestMgr{}
	part := &jobPartMgr{
		jobMgr: manager, filename: name, planMMF: mapped,
		cachedIsFinalPart: true,
	}
	part.ScheduleTransfers(context.Background())
	require.True(t, manager.confirmed)
	require.Len(t, manager.reports, 1)
}

func TestM5ActualCancellationFlushesDeferredHardlinks(t *testing.T) {
	manager, _ := newDrainTestJob(t, common.EJobStatus.InProgress())
	queue := make(chan IJobPartMgr, 1)
	manager.coordinatorChannels.partsChannel = queue
	part := &jobPartMgr{cachedPartNum: 1, cachedJobPartType: common.EJobPartType.Hardlink(), cachedNumTransfers: 1}
	manager.hardlinkGate.register(0, common.EJobPartType.Mixed(), false)
	manager.hardlinkGate.register(1, common.EJobPartType.Hardlink(), false)
	manager.QueueJobParts(part)
	manager.RequestCancellation()
	select {
	case got := <-queue:
		require.Same(t, part, got)
	case <-time.After(time.Second):
		t.Fatal("engine cancellation left preserved hardlink transfers permanently deferred")
	}
	// Simulate the scheduler and cancelled transfer completion after release.
	part.drainTracker.done(2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, manager.CancelAndDrain(ctx))
}

func TestM5ResumedSuccessfulFinalPartConfirmsAfterUnmap(t *testing.T) {
	name := createM5Plan(t, []common.CopyTransfer{{Source: "file", Destination: "file", EntityType: common.EEntityType.File()}})
	mapped := name.Map()
	defer func() {
		if mapped != nil {
			mapped.Unmap()
		}
	}()
	mapped.Plan().Transfer(0).SetTransferStatus(common.ETransferStatus.Success(), true)
	manager := &hardlinkSchedulingTestMgr{onReport: func() {
		mapped.Unmap()
		mapped = nil
	}}
	var work jobWorkTracker
	work.add(2)
	part := &jobPartMgr{
		jobMgr: manager, filename: name, planMMF: mapped,
		cachedPartNum: 1, cachedNumTransfers: 1, cachedIsFinalPart: true,
		drainTracker: &work,
	}
	part.ScheduleTransfers(context.Background())
	require.True(t, manager.confirmed)
	require.Len(t, manager.reports, 1)
	require.Nil(t, mapped, "resumed successful part must permit early unmapping")
	require.NoError(t, work.wait(context.Background()))
}
