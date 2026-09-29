package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

func TestPlannerScanningLoggerLifecycle(t *testing.T) {
	previousJob, previousPath, previousLevel := Client.CurrentJobID, common.LogPathFolder, LogLevel
	previousLogger := azcopyScanningLogger
	defer func() {
		CloseScanningLogger()
		Client.CurrentJobID, common.LogPathFolder, LogLevel = previousJob, previousPath, previousLevel
		azcopyScanningLogger = previousLogger
	}()
	common.LogPathFolder, LogLevel = t.TempDir(), common.LogError
	for range 2 {
		Client.CurrentJobID = common.NewJobID()
		OpenScanningLogger()
		azcopyScanningLogger.Log(common.LogError, "scanner diagnostic fixture")
		CloseScanningLogger()
		if azcopyScanningLogger != nil {
			t.Fatal("closed scanner logger leaked into the next job")
		}
		data, err := os.ReadFile(filepath.Join(common.LogPathFolder, Client.CurrentJobID.String()+"-scanning.log"))
		if err != nil || !strings.Contains(string(data), "scanner diagnostic fixture") || !strings.Contains(string(data), "Closing Log") {
			t.Fatalf("scanner diagnostic was not persisted and closed: %v", err)
		}
	}
}

func TestPlannerBatchDispatchAndLegacyDefault(t *testing.T) {
	previous := jobsAdmin.ExecuteNewCopyJobPartOrder
	legacyCalls := 0
	var orders []common.CopyJobPartOrderRequest
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		if order.PlanOnly == nil {
			legacyCalls++
		}
		orders = append(orders, order)
		return common.CopyJobPartOrderResponse{JobStarted: true}
	}
	defer func() { jobsAdmin.ExecuteNewCopyJobPartOrder = previous }()
	template := &common.CopyJobPartOrderRequest{
		JobID: common.NewJobID(), FromTo: common.EFromTo.BlobBlob(), PlanOnly: &common.PlanOnlyOptions{},
	}
	processor := newCopyTransferProcessor(template, 1, common.ResourceString{}, common.ResourceString{}, nil, nil, false, false)
	transfers := common.Transfers{List: []common.CopyTransfer{{Source: "/object", SourceSize: 7}}}
	if err := processor.dispatchPart(dispatchItem{partNum: 3, transfers: transfers}); err != nil {
		t.Fatal(err)
	}
	template.PartNum = 4
	template.Transfers = transfers
	if started, err := processor.dispatchFinalPart(); !started || err != nil {
		t.Fatalf("final dispatch: started=%v err=%v", started, err)
	}
	if legacyCalls != 0 || len(orders) != 2 || orders[0].PartNum != 3 || orders[1].PartNum != 4 ||
		!orders[1].IsFinalPart || orders[0].Transfers.List[0].Source != "/object" {
		t.Fatal("planner-only mode or scanner batches were lost at native dispatch")
	}
	notStarted := false
	processor.dispatchOnce.Do(func() { notStarted = true })
	if !notStarted {
		t.Fatal("standalone output started the asynchronous STE dispatch pipeline")
	}
	template.PlanOnly = nil
	processor.sendPartToSte()
	if legacyCalls != 1 {
		t.Fatal("nil output changed the legacy dispatch")
	}
}

func TestPlannerExistingBatchLimits(t *testing.T) {
	previous := UseSyncOrchestrator
	UseSyncOrchestrator = true
	defer func() { UseSyncOrchestrator = previous }()
	for _, test := range []struct {
		name        string
		sizes       []int64
		zeroObjects int
		limit       uint64
		counts      []int
		bytes       []uint64
	}{
		{name: "empty", limit: 10_000_000_000, counts: []int{0}, bytes: []uint64{0}},
		{name: "exact-count", zeroObjects: 10_000, limit: 10_000_000_000, counts: []int{10_000}, bytes: []uint64{0}},
		{name: "count-overflow", zeroObjects: 10_001, limit: 10_000_000_000, counts: []int{10_000, 1}, bytes: []uint64{0, 0}},
		{name: "exact-bytes", sizes: []int64{4_000_000_000, 6_000_000_000}, limit: 10_000_000_000,
			counts: []int{2}, bytes: []uint64{10_000_000_000}},
		{name: "byte-overflow", sizes: []int64{6_000_000_000, 5_000_000_000}, limit: 10_000_000_000,
			counts: []int{1, 1}, bytes: []uint64{6_000_000_000, 5_000_000_000}},
		{name: "full-then-zero", sizes: []int64{10_000_000_000, 0}, limit: 10_000_000_000,
			counts: []int{1, 1}, bytes: []uint64{10_000_000_000, 0}},
		{name: "isolate-oversized", sizes: []int64{1, 11_000_000_000, 12_000_000_000, 2}, limit: 10_000_000_000,
			counts: []int{1, 1, 1, 1}, bytes: []uint64{1, 11_000_000_000, 12_000_000_000, 2}},
		{name: "legacy-count-only", sizes: []int64{11_000_000_000, 12_000_000_000},
			counts: []int{2}, bytes: []uint64{23_000_000_000}},
	} {
		t.Run(test.name, func(t *testing.T) {
			template := &common.CopyJobPartOrderRequest{JobID: common.NewJobID(), FromTo: common.EFromTo.BlobBlob()}
			if test.limit != 0 {
				template.PlanOnly = &common.PlanOnlyOptions{}
			}
			processor := newCopyTransferProcessor(template, 10_000, common.ResourceString{}, common.ResourceString{}, nil, nil, false, false)
			processor.maxBytesPerPart = test.limit
			var orders []common.CopyJobPartOrderRequest
			previousDispatch := jobsAdmin.ExecuteNewCopyJobPartOrder
			defer func() { jobsAdmin.ExecuteNewCopyJobPartOrder = previousDispatch }()
			jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
				orders = append(orders, order)
				return common.CopyJobPartOrderResponse{JobStarted: true}
			}
			for i := 0; i < test.zeroObjects+len(test.sizes); i++ {
				var size int64
				if i < len(test.sizes) {
					size = test.sizes[i]
				}
				if err := processor.scheduleCopyTransfer(StoredObject{
					name: fmt.Sprint(i), relativePath: fmt.Sprint(i), size: size, entityType: common.EEntityType.File(),
				}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := processor.dispatchFinalPart(); err != nil {
				t.Fatal(err)
			}
			if len(orders) != len(test.counts) {
				t.Fatalf("parts=%d want %d", len(orders), len(test.counts))
			}
			for i, order := range orders {
				if len(order.Transfers.List) != test.counts[i] || order.Transfers.TotalSizeInBytes != test.bytes[i] ||
					order.Transfers.FileTransferCount != uint32(test.counts[i]) ||
					order.PartNum != common.PartNumber(i) || order.IsFinalPart != (i == len(orders)-1) {
					t.Fatalf("unexpected batch %d: count=%d bytes=%d part=%d final=%v",
						i, len(order.Transfers.List), order.Transfers.TotalSizeInBytes, order.PartNum, order.IsFinalPart)
				}
			}
		})
	}
}

func TestPlannerConcurrentByteCappedScheduling(t *testing.T) {
	previous := UseSyncOrchestrator
	UseSyncOrchestrator = true
	defer func() { UseSyncOrchestrator = previous }()
	template := &common.CopyJobPartOrderRequest{
		JobID: common.NewJobID(), FromTo: common.EFromTo.BlobBlob(), PlanOnly: &common.PlanOnlyOptions{},
	}
	processor := newCopyTransferProcessor(template, 10_000, common.ResourceString{}, common.ResourceString{}, nil, nil, false, false)
	processor.maxBytesPerPart = 10_000_000_000
	objects, parts := 0, 0
	previousDispatch := jobsAdmin.ExecuteNewCopyJobPartOrder
	defer func() { jobsAdmin.ExecuteNewCopyJobPartOrder = previousDispatch }()
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		if order.Transfers.TotalSizeInBytes > 10_000_000_000 || len(order.Transfers.List) == 0 {
			t.Errorf("invalid byte-capped batch")
		}
		objects += len(order.Transfers.List)
		parts++
		return common.CopyJobPartOrderResponse{JobStarted: true}
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := processor.scheduleCopyTransfer(StoredObject{
				name: fmt.Sprint(i), relativePath: fmt.Sprint(i), size: 3_000_000_000, entityType: common.EEntityType.File(),
			}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if _, err := processor.dispatchFinalPart(); err != nil {
		t.Fatal(err)
	}
	if objects != 100 || parts != 34 {
		t.Fatalf("objects=%d parts=%d", objects, parts)
	}
}

func TestPlannerDispatchFailure(t *testing.T) {
	template := &common.CopyJobPartOrderRequest{JobID: common.NewJobID(), PlanOnly: &common.PlanOnlyOptions{}}
	processor := newCopyTransferProcessor(template, 1, common.ResourceString{}, common.ResourceString{}, nil, nil, false, false)
	previousDispatch := jobsAdmin.ExecuteNewCopyJobPartOrder
	defer func() { jobsAdmin.ExecuteNewCopyJobPartOrder = previousDispatch }()
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return common.CopyJobPartOrderResponse{ErrorMsg: common.CopyJobPartOrderErrorType("share failed")}
	}
	if err := processor.dispatchPart(dispatchItem{}); err == nil {
		t.Fatal("output error was lost")
	}
	if _, err := processor.dispatchFinalPart(); err == nil {
		t.Fatal("final output error was lost")
	}
}

func TestPlannerDeletionObserverPreservesNativeDelete(t *testing.T) {
	failure := errors.New("delete denied")
	var observed error
	calls := 0
	processor := &interactiveDeleteProcessor{
		deleter: func(StoredObject) error { calls++; return failure },
	}
	reportSyncDeletionErrors(processor, &common.PlanOnlyOptions{ReportError: func(err error) { observed = err }})
	if err := processor.deleter(StoredObject{}); !errors.Is(err, failure) || !errors.Is(observed, failure) || calls != 1 {
		t.Fatalf("native deletion was replaced or its failure hidden: %v %v %d", err, observed, calls)
	}
}
