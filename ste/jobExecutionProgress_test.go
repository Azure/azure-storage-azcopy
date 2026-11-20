package ste

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func newAccountingTestManager(t *testing.T, session *jobSessionState) *jobMgr {
	t.Helper()
	jm := &jobMgr{session: session, jstm: &jobStatusManager{
		partCreated: make(chan JobPartCreatedMsg), xferDone: make(chan xferDoneMsg),
		listReq: make(chan listRequest), respChan: make(chan common.ListJobSummaryResponse),
		xferDoneDrained: make(chan struct{}), statusMgrDone: make(chan struct{}),
	}}
	go jm.handleStatusUpdateMessage()
	t.Cleanup(func() {
		close(jm.jstm.partCreated)
		close(jm.jstm.xferDone)
		jm.waitToDrainXferDone()
		jm.ListJobSummary()
		<-jm.jstm.statusMgrDone
	})
	return jm
}

func TestSessionActualTransferCounters(t *testing.T) {
	jm := newAccountingTestManager(t, &jobSessionState{})
	jm.ResurrectSummary(common.ListJobSummaryResponse{TotalTransfers: 1<<32 - 1, TransfersCompleted: 1<<32 - 1})
	jm.SendJobPartCreatedMsg(JobPartCreatedMsg{TotalTransfers: 2})
	for _, msg := range []common.TransferDetail{
		{TransferStatus: common.ETransferStatus.Success(), TransferSize: 4},
		{TransferStatus: common.ETransferStatus.Success(), TransferSize: 0},
		{TransferStatus: common.ETransferStatus.Failed(), TransferSize: 10},
		{TransferStatus: common.ETransferStatus.Cancelled(), TransferSize: 10},
		{TransferStatus: common.ETransferStatus.SkippedEntityAlreadyExists(), TransferSize: 10},
	} {
		jm.SendXferDoneMsg(xferDoneMsg{TransferDetail: msg})
	}
	if p := jm.ListJobSummary(false); p.TransfersCompletedThisExecution != 2 || p.BytesTransferredThisExecution != 4 ||
		p.TotalTransfers != 1<<32+1 || p.TransfersCompleted != 1<<32+1 ||
		p.TransfersFailed != 1 || p.TransfersSkipped != 1 || p.BytesFailed != 10 {
		t.Fatalf("actual-transfer counters lost zero-byte successes or included unsuccessful transfers: %+v", p)
	}
	jm.SendXferDoneMsg(xferDoneMsg{
		TransferDetail: common.TransferDetail{TransferStatus: common.ETransferStatus.Success(), TransferSize: 9},
		replayed:       true,
	})
	if p := jm.ListJobSummary(false); p.TransfersCompleted != 1<<32+2 || p.TotalBytesTransferred != 13 ||
		p.TransfersCompletedThisExecution != 2 || p.BytesTransferredThisExecution != 4 {
		t.Fatalf("replay inflated execution counters or lost native totals: %+v", p)
	}
}

func TestLegacySummaryCollectorAndListReset(t *testing.T) {
	jstm := &jobStatusManager{
		partCreated: make(chan JobPartCreatedMsg), xferDone: make(chan xferDoneMsg),
		listReq: make(chan listRequest), respChan: make(chan common.ListJobSummaryResponse),
		xferDoneDrained: make(chan struct{}), statusMgrDone: make(chan struct{}),
	}
	jm := &jobMgr{jstm: jstm}
	done := make(chan struct{})
	go func() { defer close(done); jm.handleStatusUpdateMessage() }()
	jm.SendJobPartCreatedMsg(JobPartCreatedMsg{TotalTransfers: 2, FileTransfers: 2, TotalBytesEnumerated: 12, IsFinalPart: true})
	jm.SendXferDoneMsg(xferDoneMsg{TransferDetail: common.TransferDetail{TransferStatus: common.ETransferStatus.Success(), TransferSize: 5}})
	jm.SendXferDoneMsg(xferDoneMsg{TransferDetail: common.TransferDetail{TransferStatus: common.ETransferStatus.Failed(), TransferSize: 7}})
	close(jstm.xferDone)
	jm.waitToDrainXferDone()
	before := jm.ListJobSummary(false)
	if before.TotalTransfers != 2 || before.TotalBytesExpected != 12 || before.TotalBytesTransferred != 5 ||
		before.TransfersCompleted != 1 || before.TransfersFailed != 1 || len(before.FailedTransfers) != 1 || !before.CompleteJobOrdered {
		t.Fatalf("legacy queued accounting changed: %+v", before)
	}
	before.FailedTransfers[0].ErrorMessage = "caller-owned copy"
	if got := jm.ListJobSummary(false); len(got.FailedTransfers) != 1 || got.FailedTransfers[0].ErrorMessage != "" {
		t.Fatal("non-reset summary consumed or exposed the collector's failure list", got)
	}
	last := jm.ListJobSummary()
	<-done
	if len(last.FailedTransfers) != 1 || len(jm.ListJobSummary(false).FailedTransfers) != 0 {
		t.Fatal("legacy failed-transfer list was lost or replayed after reset")
	}
	last.TransfersFailed = 0
	last.FailedTransfers = nil
	jm.ResurrectSummary(last)
	if got := jm.ListJobSummary(false); got.TransfersFailed != 0 || got.TransfersCompleted != 1 {
		t.Fatal("resurrection after collector shutdown lost counters", got)
	}
}

func TestSessionPartZeroAdmissionProtections(t *testing.T) {
	for _, scenario := range []string{"duplicate", "cancelled-context", "cancelled-session", "closed"} {
		t.Run(scenario, func(t *testing.T) {
			s, err := NewJobExecutionSession(context.Background(), JobExecutionSessionConfig{
				JobID: common.NewJobID(), PlanDirectory: t.TempDir(),
				SourceRoot: "https://source.invalid/src", DestinationRoot: "https://destination.invalid/dst",
			})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := context.Canceled.Error()
			switch scenario {
			case "duplicate":
				// Reserve the admission deterministically, without starting a transfer.
				s.jm.jobPartMgrs.Set(0, &jobPartMgr{})
				defer s.jm.jobPartMgrs.Delete(0)
				want = "part already admitted"
			case "cancelled-context":
				cancel()
			case "cancelled-session":
				s.Cancel()
			case "closed":
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				want = "job execution session closed"
			}
			if execution, err := s.Submit(ctx, 0); execution != nil || err == nil || err.Error() != want {
				t.Fatalf("part-zero admission bypassed %s protection: execution=%v error=%v", scenario, execution, err)
			}
			if progress := s.Progress(); progress.PartsAdmitted != 0 {
				t.Fatal("rejected part zero was admitted", progress)
			}
		})
	}
}

func TestSessionSummaryCloseDrainsQueues(t *testing.T) {
	s, err := NewJobExecutionSession(context.Background(), JobExecutionSessionConfig{
		JobID: common.NewJobID(), PlanDirectory: t.TempDir(),
		SourceRoot: "https://source.invalid/src", DestinationRoot: "https://destination.invalid/dst",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	jstm := s.jm.jstm
	// Hold the handler's reply so both accounting queues have a backlog at shutdown.
	jstm.listReq <- listRequest{}
	replyPending := true
	defer func() {
		if replyPending {
			<-jstm.respChan
		}
	}()
	count := min(64, cap(jstm.partCreated), cap(jstm.xferDone))
	if count == 0 {
		t.Fatal("fixture requires buffered accounting queues")
	}
	for i := 0; i < count; i++ {
		s.jm.SendJobPartCreatedMsg(JobPartCreatedMsg{TotalTransfers: 1, FileTransfers: 1, TotalBytesEnumerated: 3, IsFinalPart: i == 0})
		s.jm.SendXferDoneMsg(xferDoneMsg{TransferDetail: common.TransferDetail{
			TransferStatus: common.ETransferStatus.Failed(), TransferSize: 3, ErrorMessage: "failed",
		}})
	}
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	<-jstm.respChan
	replyPending = false
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session did not drain its accounting queues")
	}
	var readers sync.WaitGroup
	failures := make(chan int, 8)
	for range cap(failures) {
		readers.Add(1)
		go func() {
			defer readers.Done()
			p, _ := s.JobMgr().GetJobSummary(true)
			if p.TotalTransfers != uint64(count) || p.TransfersFailed != uint64(count) ||
				p.TotalBytesExpected != uint64(3*count) || p.BytesFailed != uint64(3*count) ||
				p.JobStatus != common.EJobStatus.Failed() || !p.CompleteJobOrdered {
				t.Errorf("final summary was frozen before accounting drained: %+v", p)
			}
			failures <- len(p.FailedTransfers)
		}()
	}
	readers.Wait()
	close(failures)
	total := 0
	for n := range failures {
		total += n
	}
	if total != count {
		t.Fatal("post-close reads lost or repeated failure details", total, count)
	}
}

func TestSessionCancelledSummaryDrainsWithoutSuccess(t *testing.T) {
	s, err := NewJobExecutionSession(context.Background(), JobExecutionSessionConfig{
		JobID: common.NewJobID(), PlanDirectory: t.TempDir(),
		SourceRoot: "https://source.invalid/src", DestinationRoot: "https://destination.invalid/dst",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const count = 32
	for range count {
		s.jm.SendJobPartCreatedMsg(JobPartCreatedMsg{TotalTransfers: 1, FileTransfers: 1, TotalBytesEnumerated: 3})
		s.jm.SendXferDoneMsg(xferDoneMsg{TransferDetail: common.TransferDetail{
			TransferStatus: common.ETransferStatus.Cancelled(), TransferSize: 3,
		}})
	}
	s.Cancel()
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled transfers prevented session accounting drain")
	}
	select {
	case <-s.jm.jstm.xferDoneDrained:
	default:
		t.Fatal("canceled transfer messages did not signal drain")
	}
	p, _ := s.JobMgr().GetJobSummary(true)
	if p.TotalTransfers != count || p.TotalBytesExpected != 3*count || !p.CompleteJobOrdered ||
		p.JobStatus != common.EJobStatus.Cancelled() || p.TransfersCompleted != 0 ||
		p.TransfersCompletedThisExecution != 0 || p.TotalBytesTransferred != 0 ||
		p.BytesTransferredThisExecution != 0 || p.TransfersFailed != 0 || p.TransfersSkipped != 0 || p.BytesFailed != 0 {
		t.Fatalf("canceled transfers changed successful accounting or final status: %+v", p)
	}
	progress := s.Progress()
	if progress.Status != common.EJobStatus.Cancelled() || progress.Completed != 0 || progress.TransfersCompleted != 0 {
		t.Fatal("drained cancellation became successful progress", progress)
	}
	for _, record := range []any{p, progress} {
		data, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"TransfersCancelled", "Cancelled"} {
			if _, ok := fields[name]; ok {
				t.Fatalf("aggregate cancellation field %s still reported: %s", name, data)
			}
		}
	}
}

func TestSessionSummaryIsolationAndClose(t *testing.T) {
	var sessions []*JobExecutionSession
	for _, bytes := range []int64{17, 29} {
		s, err := NewJobExecutionSession(context.Background(), JobExecutionSessionConfig{
			JobID: common.NewJobID(), PlanDirectory: t.TempDir(),
			SourceRoot: "https://source.invalid/src", DestinationRoot: "https://destination.invalid/dst",
		})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if err := s.jm.pacer.RequestTrafficAllocation(context.Background(), bytes); err != nil {
			t.Fatal(err)
		}
		s.jm.SendJobPartCreatedMsg(JobPartCreatedMsg{TotalTransfers: 1, FileTransfers: 1, TotalBytesEnumerated: uint64(bytes)})
		s.jm.SendXferDoneMsg(xferDoneMsg{TransferDetail: common.TransferDetail{
			TransferStatus: common.ETransferStatus.Success(), TransferSize: uint64(bytes),
		}})
		deadline := time.Now().Add(5 * time.Second)
		for p := s.jm.ListJobSummary(false); p.TransfersCompleted != 1 || p.TotalTransfers != 1; p = s.jm.ListJobSummary(false) {
			if time.Now().After(deadline) {
				t.Fatal("queued summary did not catch up", p)
			}
			time.Sleep(time.Millisecond)
		}
		sessions = append(sessions, s)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			js, _ := sessions[0].JobMgr().GetJobSummary(true)
			if js.BytesOverWire != 17 || js.TotalBytesTransferred != 17 || js.TransfersCompleted != 1 {
				t.Errorf("summary changed during retirement: %+v", js)
				return
			}
		}
	}()
	if err := sessions[0].Close(); err != nil {
		t.Fatal(err)
	}
	<-done
	first, _ := sessions[0].JobMgr().GetJobSummary(true)
	second, _ := sessions[1].JobMgr().GetJobSummary(true)
	if first.JobStatus != common.EJobStatus.Completed() || !first.CompleteJobOrdered ||
		second.JobStatus != common.EJobStatus.InProgress() || second.CompleteJobOrdered ||
		second.BytesOverWire != 29 || second.TotalBytesTransferred != 29 {
		t.Fatalf("jobs shared lifecycle or traffic: first=%+v second=%+v", first, second)
	}
}

type failingSessionJobMgr struct{ IJobMgr }

func (jm failingSessionJobMgr) ScheduleTransfer(_ common.JobPriority, transfer IJobPartTransferMgr) {
	transfer.SetStatus(common.ETransferStatus.Failed())
	transfer.ReportTransferDone()
}

func TestSessionNativePartAccounting(t *testing.T) {
	for _, test := range []struct {
		name     string
		session  *jobSessionState
		replay   bool
		statuses []common.TransferStatus
		want     PlanExecutionResult
	}{
		{
			name: "mixed", session: &jobSessionState{},
			statuses: []common.TransferStatus{
				common.ETransferStatus.Success(), common.ETransferStatus.Failed(),
				common.ETransferStatus.BlobTierFailure(), common.ETransferStatus.TierAvailabilityCheckFailure(),
				common.ETransferStatus.SkippedEntityAlreadyExists(), common.ETransferStatus.SkippedBlobHasSnapshots(),
				common.ETransferStatus.Restarted(),
			},
			want: PlanExecutionResult{Completed: 1, Failed: 4, Skipped: 2, Status: common.EJobStatus.CompletedWithErrorsAndSkipped()},
		},
		{
			name: "cancelled", session: &jobSessionState{},
			statuses: []common.TransferStatus{
				common.ETransferStatus.Success(), common.ETransferStatus.Failed(), common.ETransferStatus.Cancelled(),
			},
			want: PlanExecutionResult{Completed: 1, Failed: 1, Cancelled: 1, Status: common.EJobStatus.Cancelled()},
		},
		{
			name:     "legacy-cancellation",
			statuses: []common.TransferStatus{common.ETransferStatus.Success(), common.ETransferStatus.Cancelled()},
			want:     PlanExecutionResult{Completed: 1, Status: common.EJobStatus.Completed()},
		},
		{
			name: "replayed-failures", session: &jobSessionState{}, replay: true,
			statuses: []common.TransferStatus{common.ETransferStatus.Failed(), common.ETransferStatus.Failed()},
			want:     PlanExecutionResult{Failed: 2, Status: common.EJobStatus.Failed()},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			job := common.NewJobID()
			file := JobPartPlanFileName(filepath.Join(t.TempDir(), "part"))
			order := common.CopyJobPartOrderRequest{
				JobID: job, FromTo: common.EFromTo.BlobBlob(),
				SourceRoot:      common.ResourceString{Value: "https://source.invalid/src"},
				DestinationRoot: common.ResourceString{Value: "https://destination.invalid/dst"},
			}
			for range test.statuses {
				order.Transfers.List = append(order.Transfers.List, common.CopyTransfer{
					Source: "/file", Destination: "/file", EntityType: common.EEntityType.File(),
				})
			}
			if err := file.Create(order); err != nil {
				t.Fatal(err)
			}
			mmf := file.Map()
			defer mmf.Unmap()
			jm := newAccountingTestManager(t, test.session)
			jm.jobPartProgress = make(chan jobPartProgressInfo, 1)
			jm.logger = common.NewJobLogger(job, common.ELogLevel.None(), "", "")
			jpm := &jobPartMgr{
				jobMgr: jm, planMMF: mmf, sessionState: jm.session,
				cachedJobID: job, cachedNumTransfers: uint32(len(test.statuses)),
				srcServiceClient: &common.ServiceClient{}, dstServiceClient: &common.ServiceClient{},
			}
			var reporters sync.WaitGroup
			for i, status := range test.statuses {
				if test.replay {
					mmf.Plan().Transfer(uint32(i)).SetTransferStatus(status, true)
					continue
				}
				if test.session != nil {
					reporters.Add(1)
					go func(status common.TransferStatus) {
						defer reporters.Done()
						jpm.ReportTransferDone(status)
					}(status)
				} else {
					jpm.ReportTransferDone(status)
				}
			}
			reporters.Wait()
			if test.replay {
				// Fail each retry before scheduling the next previously failed record.
				jpm.jobMgr = failingSessionJobMgr{jm}
				jpm.ScheduleTransfers(context.Background())
			}
			select {
			case result := <-jm.jobPartProgress:
				if uint32(result.transfersCompleted) != test.want.Completed ||
					uint32(result.transfersFailed) != test.want.Failed ||
					uint32(result.transfersSkipped) != test.want.Skipped ||
					jpm.atomicTransfersCancelled != test.want.Cancelled ||
					mmf.Plan().JobPartStatus() != test.want.Status {
					t.Fatalf("native outcome=%+v cancelled=%d status=%s, want %+v",
						result, jpm.atomicTransfersCancelled, mmf.Plan().JobPartStatus(), test.want)
				}
			default:
				t.Fatal("native part completion was not reported")
			}
		})
	}
}
