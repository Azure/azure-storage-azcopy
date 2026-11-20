package ste

import (
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// JobExecutionProgress projects native summary counters alongside session diagnostics.
// Completed includes replayed successes; TransfersCompleted and BytesCompleted
// count only successful transfers in this session, not replay or partial chunks.
// It retains no transfer paths, credentials, error lists or plan mappings.
type JobExecutionProgress struct {
	Status                                         common.JobStatus
	PartsAdmitted, PartsRetired, TransfersAdmitted uint64
	Completed, Failed, Skipped                     uint64
	BytesCompleted                                 uint64
	TransfersCompleted                             uint64
	Channels                                       ChannelStats
	Network                                        JobExecutionNetworkProgress
	FirstTransferStart                             *JobExecutionTransferStart
}

// JobExecutionTransferStart marks entry into a live native transfer handler,
// not plan admission, a replayed success, or the first HTTP request/payload byte.
// Timestamp retains its monotonic clock in process for handoff measurements.
type JobExecutionTransferStart struct {
	Timestamp     time.Time
	PartNumber    uint32
	TransferIndex uint32
}

type JobExecutionNetworkProgress struct {
	E2EMilliseconds, ConnectionWaitMilliseconds, WireMilliseconds int
	ConnectionsReused, ConnectionsNew                             int64
	ServerBusyPercent                                             float32
}

// Progress remains safe during admission, retirement and after Close. It never
// takes the admission lock, which may be held while native queues apply pressure.
func (s *JobExecutionSession) Progress() JobExecutionProgress {
	state := s.jm.session
	state.metricsMu.RLock()
	defer state.metricsMu.RUnlock()
	state.mu.Lock()
	p := JobExecutionProgress{
		PartsAdmitted: state.partsAdmitted, PartsRetired: state.partsRetired,
		TransfersAdmitted: state.transfersAdmitted, FirstTransferStart: state.firstTransferStart,
	}
	if p.FirstTransferStart != nil {
		start := *p.FirstTransferStart
		p.FirstTransferStart = &start
	}
	p.Status = state.status
	state.mu.Unlock()
	js := s.jm.ListJobSummary(false)
	p.Completed, p.Failed, p.Skipped = js.TransfersCompleted, js.TransfersFailed, js.TransfersSkipped
	p.TransfersCompleted, p.BytesCompleted = js.TransfersCompletedThisExecution, js.BytesTransferredThisExecution
	if p.Status == common.EJobStatus.InProgress() && s.jm.ctx.Err() != nil {
		p.Status = common.EJobStatus.Cancelling()
	}
	if s.jm.jstm != nil {
		p.Channels = s.jm.GetChannelStats()
	}
	if stats := s.jm.PipelineNetworkStats(); stats != nil {
		p.Network = JobExecutionNetworkProgress{
			E2EMilliseconds:            stats.AverageE2EMilliseconds(),
			ConnectionWaitMilliseconds: stats.AverageConnWaitMs(), WireMilliseconds: stats.AverageWireMs(),
			ConnectionsReused: stats.ConnReusedCount(), ConnectionsNew: stats.ConnNewCount(),
			ServerBusyPercent: stats.TotalServerBusyPercentage(),
		}
	}
	return p
}

func (s *jobSessionState) transferStarted(transfer IJobPartTransferMgr) {
	s.firstTransferStartOnce.Do(func() {
		now := time.Now()
		part, index := transfer.TransferIndex()
		s.mu.Lock()
		defer s.mu.Unlock()
		s.firstTransferStart = &JobExecutionTransferStart{
			Timestamp: now, PartNumber: part, TransferIndex: index,
		}
	})
}
