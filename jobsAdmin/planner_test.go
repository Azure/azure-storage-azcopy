package jobsAdmin

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

func plannerOrder(t *testing.T) common.CopyJobPartOrderRequest {
	t.Helper()
	previousAdmin, previousFolder := JobsAdmin, common.AzcopyJobPlanFolder
	JobsAdmin, common.AzcopyJobPlanFolder = nil, ""
	t.Cleanup(func() { JobsAdmin, common.AzcopyJobPlanFolder = previousAdmin, previousFolder })
	return common.CopyJobPartOrderRequest{
		JobID: common.NewJobID(), FromTo: common.EFromTo.BlobBlob(), IsFinalPart: true,
		SourceRoot:      common.ResourceString{Value: "https://source.blob.core.windows.net/source", SAS: "sig=runtime"},
		DestinationRoot: common.ResourceString{Value: "https://target.blob.core.windows.net/target", SAS: "sig=runtime"},
		BlobAttributes:  common.BlobTransferAttributes{BlobType: common.EBlobType.BlockBlob()},
		PlanOnly:        &common.PlanOnlyOptions{Context: context.Background(), Directory: t.TempDir()},
		Transfers: common.Transfers{
			List: []common.CopyTransfer{
				{Source: "/a", Destination: "/a", SourceSize: 3, EntityType: common.EEntityType.File()},
				{Source: "/b", Destination: "/b", SourceSize: 4, EntityType: common.EEntityType.File()},
			},
			TotalSizeInBytes: 7, FileTransferCount: 2,
		},
	}
}

func TestPlannerOnlyCreatesNativePlanWithoutSTE(t *testing.T) {
	order := plannerOrder(t)
	plans, objects, bytes := 0, 0, uint64(0)
	order.PlanOnly.OnPartCreated = func(transfers common.Transfers) error {
		plans++
		objects += len(transfers.List)
		bytes += transfers.TotalSizeInBytes
		if transfers.FileTransferCount != 2 {
			t.Error("existing transfer counters changed")
		}
		return nil
	}
	response := ExecuteNewCopyJobPartOrder(order)
	if !response.JobStarted || response.ErrorMsg != "" || JobsAdmin != nil || plans != 1 || objects != 2 || bytes != 7 {
		t.Fatalf("response=%+v admin=%v plans=%d objects=%d bytes=%d", response, JobsAdmin, plans, objects, bytes)
	}
	name := ste.JobPartPlanFileName(filepath.Join(order.PlanOnly.Directory, string(NewJobPartPlanFileName(order.JobID, 0))))
	data, err := os.ReadFile(string(name))
	if err != nil || strings.Contains(string(data), "sig=runtime") {
		t.Fatalf("invalid native output: %v", err)
	}
	mapped := name.Map()
	header := mapped.Plan()
	if header.JobID != order.JobID || header.PartNum != 0 || !header.IsFinalPart || header.NumTransfers != 2 ||
		header.Transfer(0).SourceSize != 3 || order.PartNum != 0 {
		t.Error("native format, identity or scanner order changed")
	}
	mapped.Unmap()
}

func TestPlannerOnlyFailurePreservesEarlierCounters(t *testing.T) {
	order := plannerOrder(t)
	plans, objects, bytes := 0, 0, uint64(0)
	var observed error
	order.PlanOnly.OnPartCreated = func(transfers common.Transfers) error {
		plans++
		objects += len(transfers.List)
		bytes += transfers.TotalSizeInBytes
		return nil
	}
	order.PlanOnly.ReportError = func(err error) { observed = err }
	if response := ExecuteNewCopyJobPartOrder(order); !response.JobStarted {
		t.Fatal(response)
	}
	order.PartNum = 1
	occupied := filepath.Join(order.PlanOnly.Directory, string(NewJobPartPlanFileName(order.JobID, 1)))
	if err := os.Mkdir(occupied, 0700); err != nil {
		t.Fatal(err)
	}
	response := ExecuteNewCopyJobPartOrder(order)
	if response.JobStarted || response.ErrorMsg == "" || observed == nil || plans != 1 || objects != 2 || bytes != 7 {
		t.Fatalf("write failure lost earlier counters: response=%+v plans=%d objects=%d bytes=%d error=%v",
			response, plans, objects, bytes, observed)
	}
}

func TestPlannerOnlyEmptyAndCancelledOrders(t *testing.T) {
	for _, scenario := range []string{"empty", "cancelled", "cancel-after-create"} {
		t.Run(scenario, func(t *testing.T) {
			order := plannerOrder(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			order.PlanOnly.Context = ctx
			counters := 0
			var observed error
			order.PlanOnly.ReportError = func(err error) { observed = err }
			order.PlanOnly.OnPartCreated = func(common.Transfers) error { counters++; cancel(); return nil }
			switch scenario {
			case "empty":
				order.Transfers = common.Transfers{}
			case "cancelled":
				cancel()
			}
			response := ExecuteNewCopyJobPartOrder(order)
			files, err := os.ReadDir(order.PlanOnly.Directory)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "empty" {
				if !response.JobStarted || counters != 0 || len(files) != 0 || observed != nil {
					t.Fatal("empty order created a file or lost its native acknowledgement")
				}
			} else {
				want := 0
				if scenario == "cancel-after-create" {
					want = 1
				}
				if response.JobStarted || !errors.Is(observed, context.Canceled) || counters != want || len(files) != want {
					t.Fatalf("cancellation/counters incorrect: response=%+v counters=%d files=%d error=%v",
						response, counters, len(files), observed)
				}
			}
		})
	}
}

type fusedEntryProbe struct {
	*jobsAdmin
	entered bool
}

func (p *fusedEntryProbe) JobMgrEnsureExists(common.JobID, common.LogLevel, string) ste.IJobMgr {
	p.entered = true
	panic("fused-jobmgr-probe")
}

func TestPlannerNilModeRetainsFusedJobMgrPath(t *testing.T) {
	order := plannerOrder(t)
	common.AzcopyJobPlanFolder = order.PlanOnly.Directory
	order.PlanOnly = nil
	probe := &fusedEntryProbe{}
	JobsAdmin = probe
	defer func() {
		if recovered := recover(); recovered != "fused-jobmgr-probe" || !probe.entered {
			t.Errorf("default mode bypassed JobMgr: %v", recovered)
		}
		name := NewJobPartPlanFileName(order.JobID, 0)
		if !name.Exists() {
			t.Error("fused path did not call Create before JobMgr")
		}
	}()
	ExecuteNewCopyJobPartOrder(order)
	t.Fatal("default mode returned planner-only success")
}
