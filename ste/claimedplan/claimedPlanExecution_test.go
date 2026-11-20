package claimedplan_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

// Use the real native writer directly; no exported fixture or replacement plan.
func writeNativePlan(t *testing.T, c ste.JobExecutionSessionConfig, path string, part common.PartNumber, edit func(*common.CopyJobPartOrderRequest)) common.PartNumber {
	t.Helper()
	if common.AzcopyJobPlanFolder != "" {
		t.Fatal("test requires an unset global plan directory")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	order := common.CopyJobPartOrderRequest{
		JobID: c.JobID, PartNum: part, IsFinalPart: true,
		FromTo: common.EFromTo.BlobBlob(), ForceWrite: common.EOverwriteOption.True(),
		Priority: common.EJobPriority.Normal(), LogLevel: common.ELogLevel.None(),
		SourceRoot: common.ResourceString{Value: c.SourceRoot}, DestinationRoot: common.ResourceString{Value: c.DestinationRoot},
		Fpo: common.EFolderPropertiesOption.NoFolders(), CommandString: "native test",
		BlobAttributes: common.BlobTransferAttributes{BlobType: common.EBlobType.BlockBlob()},
	}
	if edit != nil {
		edit(&order)
	}
	ste.JobPartPlanFileName(path).Create(order)
	return part
}

func TestClaimedPartExecutesOriginalNativePlan(t *testing.T) {
	c := runtimeConfig(t)
	c.BufferBytes = 128 << 20
	var requests atomic.Uint32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("unexpected copy request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		requests.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()
	c.SourceRoot, c.DestinationRoot = server.URL+"/account/src", server.URL+"/account/dst"
	client, err := service.NewClientWithNoCredential(server.URL+"/account", nil)
	if err != nil {
		t.Fatal(err)
	}
	c.SourceClient = common.NewServiceClient(client, nil, nil)
	c.DestinationClient = c.SourceClient
	common.GetLifecycleMgr().E2EEnableAwaitAllowOpenFiles(false)
	s, err := ste.NewJobExecutionSession(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// A final plan does not close this session. Native widths expand without
	// truncation, and native transfer part zero is not a shared status anchor.
	for _, part := range []common.PartNumber{0, 42, 100000, 4294967295} {
		path, err := c.PlanPath(part)
		if err != nil {
			t.Fatal(err)
		}
		plan := writeNativePlan(t, c, path, part, func(o *common.CopyJobPartOrderRequest) {
			if part == 42 {
				return
			}
			o.BlobAttributes.BlockSizeInBytes, o.BlobAttributes.PutBlobSizeInBytes = 1, 1
			o.Transfers = common.Transfers{List: []common.CopyTransfer{
				{Source: "/a", Destination: "/a", SourceSize: 2, EntityType: common.EEntityType.File(), BlobType: blob.BlobTypeBlockBlob},
				{Source: "/b", Destination: "/b", SourceSize: 2, EntityType: common.EEntityType.File(), BlobType: blob.BlobTypeBlockBlob},
			}}
			if part == 4294967295 {
				o.BlobAttributes.BlockSizeInBytes, o.BlobAttributes.PutBlobSizeInBytes = 48<<20, 48<<20
				for i := range o.Transfers.List {
					o.Transfers.List[i].SourceSize = 96 << 20
				}
			}
		})
		if part == 42 {
			// Sparse padding exercises large-plan admission without a large test buffer.
			if err := os.Truncate(path, 64<<20+1); err != nil {
				t.Fatal(err)
			}
		}
		mmf := ste.JobPartPlanFileName(path).Map()
		if part == 0 {
			// A persisted legacy job status must not control this session.
			mmf.Plan().SetJobStatus(common.EJobStatus.Failed())
		}
		before := *mmf.Plan()
		var expectedBytes uint64
		for i := uint32(0); i < before.NumTransfers; i++ {
			expectedBytes += uint64(mmf.Plan().Transfer(i).SourceSize)
		}
		mmf.Unmap()
		result, err := s.Execute(context.Background(), plan)
		if err != nil || !result.Drained || result.PartNumber != part || result.Status != common.EJobStatus.Completed() ||
			result.Completed != before.NumTransfers || result.BytesTransferred != expectedBytes || s.RetainedParts() != 0 {
			t.Fatalf("original plan did not retire: %+v, %v", result, err)
		}
		mmf = ste.JobPartPlanFileName(path).Map()
		after := *mmf.Plan()
		mmf.Unmap()
		if after.JobID != before.JobID || after.PartNum != before.PartNum || after.NumTransfers != before.NumTransfers ||
			after.SourceRoot != before.SourceRoot || after.DestinationRoot != before.DestinationRoot ||
			after.JobStatus() != before.JobStatus() || after.JobPartStatus() != common.EJobStatus.Completed() {
			t.Fatal("original identity/global status changed or original file was not updated")
		}
		if s.Status() != common.EJobStatus.InProgress() {
			t.Fatal("part completion or persisted part-zero status changed the session lifecycle", s.Status())
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	summary, _ := s.JobMgr().GetJobSummary(true)
	if s.Status() != common.EJobStatus.Completed() || summary.JobStatus != common.EJobStatus.Completed() ||
		summary.TransfersCompleted != 6 || !summary.CompleteJobOrdered {
		t.Fatal("closed session did not derive status from its summary independently of part zero", summary)
	}
	// Each of six transfers sends two blocks and commits before its part retires.
	if got := requests.Load(); got != 18 {
		t.Fatalf("parts retired without completing their chunk requests: got %d, want 18", got)
	}
	entries, err := os.ReadDir(c.PlanDirectory)
	if err != nil || len(entries) != 4 {
		t.Fatalf("unexpected synthesized plan artifacts: %v, %v", entries, err)
	}
}

func TestSessionSummariesWithoutPartZero(t *testing.T) {
	jobID := common.NewJobID()
	var sessions []*ste.JobExecutionSession
	for _, part := range []common.PartNumber{42, 87} {
		c := runtimeConfig(t)
		c.JobID = jobID
		s, err := ste.NewJobExecutionSession(context.Background(), c)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		path, _ := c.PlanPath(part)
		writeNativePlan(t, c, path, part, func(o *common.CopyJobPartOrderRequest) {
			o.Transfers = common.Transfers{List: []common.CopyTransfer{{
				Source: "/a", Destination: "/a", SourceSize: int64(part), EntityType: common.EEntityType.File(),
			}}}
		})
		mmf := ste.JobPartPlanFileName(path).Map()
		mmf.Plan().Transfer(0).SetTransferStatus(common.ETransferStatus.Success(), true)
		mmf.Unmap()
		result, err := s.Execute(context.Background(), part)
		if err != nil || result.BytesTransferred != uint64(part) {
			t.Fatal("replayed plan lost successful bytes", result, err)
		}
		js, _ := s.JobMgr().GetJobSummary(true)
		deadline := time.Now().Add(5 * time.Second)
		for js.TotalTransfers != 1 || js.TransfersCompleted != 1 {
			if time.Now().After(deadline) {
				t.Fatal("queued summary did not catch up", js)
			}
			time.Sleep(time.Millisecond)
			js, _ = s.JobMgr().GetJobSummary(true)
		}
		if js.ErrorMsg != "" || js.TransfersCompleted != 1 || js.TotalBytesTransferred != uint64(part) ||
			js.TotalTransfers != 1 || js.TotalBytesExpected != uint64(part) ||
			js.BytesTransferredThisExecution != 0 || js.TransfersCompletedThisExecution != 0 ||
			js.JobStatus != common.EJobStatus.InProgress() || js.CompleteJobOrdered || len(js.PerfStrings) == 0 {
			t.Fatalf("sparse session summary lost stats or completed prematurely: %+v", js)
		}
		sessions = append(sessions, s)
	}
	sessions[0].Cancel()
	if err := sessions[0].Close(); err != nil {
		t.Fatal(err)
	}
	first, _ := sessions[0].JobMgr().GetJobSummary(true)
	second, _ := sessions[1].JobMgr().GetJobSummary(true)
	if first.JobID != jobID || second.JobID != jobID ||
		first.JobStatus != common.EJobStatus.Cancelled() || !first.CompleteJobOrdered ||
		second.JobStatus != common.EJobStatus.InProgress() || second.CompleteJobOrdered ||
		first.TotalBytesTransferred != 42 || second.TotalBytesTransferred != 87 {
		t.Fatal("sessions sharing a job ID lost independent summaries after close", first, second)
	}
}

func TestClaimedPartReplaysNativeSuccesses(t *testing.T) {
	c := runtimeConfig(t)
	path, err := c.PlanPath(1)
	if err != nil {
		t.Fatal(err)
	}
	part := writeNativePlan(t, c, path, 1, func(o *common.CopyJobPartOrderRequest) {
		o.Transfers = common.Transfers{List: []common.CopyTransfer{
			{Source: "/a", Destination: "/a", EntityType: common.EEntityType.File(), BlobType: blob.BlobTypeBlockBlob},
			{Source: "/b", Destination: "/b", EntityType: common.EEntityType.File(), BlobType: blob.BlobTypeBlockBlob},
		}}
	})
	mmf := ste.JobPartPlanFileName(path).Map()
	for i := uint32(0); i < mmf.Plan().NumTransfers; i++ {
		mmf.Plan().Transfer(i).SetTransferStatus(common.ETransferStatus.Success(), true)
	}
	mmf.Unmap()
	common.AzcopyJobPlanFolder = t.TempDir()
	defer func() { common.AzcopyJobPlanFolder = "" }()
	s, err := ste.NewJobExecutionSession(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	result, err := s.Execute(context.Background(), part)
	if err != nil || !result.Drained || result.Status != common.EJobStatus.Completed() ||
		result.Completed != 2 ||
		result.Failed != 0 || result.Skipped != 0 || result.Cancelled != 0 {
		t.Fatalf("native replay accounting changed: %+v, %v", result, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if progress := s.Progress(); progress.Completed != 2 ||
		progress.TransfersCompleted != 0 || progress.BytesCompleted != 0 || s.RetainedParts() != 0 {
		t.Fatalf("replayed records counted as new transfers or retained: %+v", progress)
	}
}

func TestClaimedPartAdmissionAndUnsupportedTransfers(t *testing.T) {
	for _, name := range []string{"relative", "outside-run", "traversal", "missing", "directory", "wrong-job", "wrong-part", "part-zero-wrong-job", "part-zero-wrong-part", "wrong-root", "non-blob", "folder-record", "missing-clients", "schema", "truncated", "arbitrary-name"} {
		t.Run(name, func(t *testing.T) {
			c := runtimeConfig(t)
			part := common.PartNumber(5)
			if name == "part-zero-wrong-job" || name == "part-zero-wrong-part" {
				part = 0
			}
			path, err := c.PlanPath(part)
			if err != nil {
				t.Fatal(err)
			}
			plan := writeNativePlan(t, c, path, part, func(o *common.CopyJobPartOrderRequest) {
				switch name {
				case "wrong-job", "part-zero-wrong-job":
					o.JobID = common.NewJobID()
				case "wrong-part", "part-zero-wrong-part":
					o.PartNum++
				case "wrong-root":
					o.SourceRoot.Value = "https://other.invalid/src"
				case "non-blob":
					o.FromTo = common.EFromTo.BlobFSBlob()
				case "folder-record", "missing-clients":
					entity := common.EEntityType.File()
					if name == "folder-record" {
						entity = common.EEntityType.Folder()
					}
					o.Transfers = common.Transfers{List: []common.CopyTransfer{{Source: "/file", Destination: "/file", EntityType: entity, BlobType: blob.BlobTypeBlockBlob}}}
				}
			})
			switch name {
			case "relative":
				c.PlanDirectory = "relative"
			case "outside-run":
				c.PlanDirectory = t.TempDir()
			case "traversal":
				c.PlanDirectory += string(filepath.Separator) + ".." + string(filepath.Separator) + filepath.Base(c.PlanDirectory)
			case "missing":
				plan++
			case "directory":
				plan++
				dir, err := c.PlanPath(plan)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
			case "schema":
				mmf := ste.JobPartPlanFileName(path).Map()
				mmf.Plan().Version++
				mmf.Unmap()
			case "truncated":
				if err := os.Truncate(path, 8); err != nil {
					t.Fatal(err)
				}
			case "arbitrary-name":
				other := filepath.Join(c.PlanDirectory, "published.plan")
				if err := os.Rename(path, other); err != nil {
					t.Fatal(err)
				}
				path = other
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			s, err := ste.NewJobExecutionSession(context.Background(), c)
			if err != nil {
				if name == "relative" || name == "traversal" {
					return // Invalid binding rejected even before admission.
				}
				t.Fatal(err)
			}
			defer s.Close()
			execution, err := s.Submit(context.Background(), plan)
			if name == "folder-record" || name == "missing-clients" {
				if err != nil {
					t.Fatal(err)
				}
				result, err := execution.Wait()
				if err != nil || !result.Drained || result.Status != common.EJobStatus.Failed() || result.Failed != 1 ||
					s.RetainedParts() != 0 || s.Progress().TransfersCompleted != 0 {
					t.Fatalf("unsupported transfer was not failed by STE: %+v, %v", result, err)
				}
				return
			}
			if err == nil {
				t.Fatal("execution accepted invalid locator or native contents")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) || s.RetainedParts() != 0 {
				t.Fatal("rejected admission modified or retained the original plan", err)
			}
		})
	}
}

func TestBoundNativePlanPath(t *testing.T) {
	c := runtimeConfig(t)
	for _, part := range []common.PartNumber{0, 42, 99999, 100000, 4294967295} {
		path, err := c.PlanPath(part)
		want := filepath.Join(c.PlanDirectory, fmt.Sprintf("%s--%05d.steV%d", c.JobID.String(), part, ste.DataSchemaVersion))
		if err != nil || path != want {
			t.Fatalf("part %d: path %q, want %q, error %v", part, path, want, err)
		}
	}
	for _, name := range []string{"zero-job", "relative", "unclean"} {
		t.Run(name, func(t *testing.T) {
			bad := c
			switch name {
			case "zero-job":
				bad.JobID = common.JobID{}
			case "relative":
				bad.PlanDirectory = "relative"
			case "unclean":
				bad.PlanDirectory += string(filepath.Separator) + "."
			}
			if _, err := bad.PlanPath(42); err == nil {
				t.Fatal("invalid binding accepted")
			}
		})
	}
}
