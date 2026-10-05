package azcopy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"
)

func TestM3ProcessorAbortSkipsQueuedPartsAndIsIdempotent(t *testing.T) {
	original := jobsAdmin.ExecuteNewCopyJobPartOrder
	var dispatched atomic.Int32
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		dispatched.Add(1)
		return common.CopyJobPartOrderResponse{JobStarted: true}
	}
	t.Cleanup(func() { jobsAdmin.ExecuteNewCopyJobPartOrder = original })

	for _, firstError := range []error{nil, errors.New("dispatch failed")} {
		processor := &copyTransferProcessor{
			dispatchCh: make(chan dispatchItem, 2), dispatchDone: make(chan struct{}),
			dispatchErr: firstError,
		}
		processor.dispatchCh <- dispatchItem{partNum: 0}
		processor.dispatchCh <- dispatchItem{partNum: 1}
		assert.NoError(t, processor.AbortAndWait(context.Background()))
		assert.NoError(t, processor.AbortAndWait(context.Background()))
		expected := firstError
		if expected == nil {
			expected = context.Canceled
		}
		assert.ErrorIs(t, processor.waitForDispatchPipeline(), expected)
		assert.Empty(t, processor.dispatchCh)
		assert.Zero(t, dispatched.Load())
	}

	synchronous := &copyTransferProcessor{}
	assert.NoError(t, synchronous.AbortAndWait(context.Background()))
	assert.NoError(t, synchronous.waitForDispatchPipeline())
}

func TestM3ProcessorAbortWaitsForActiveDispatch(t *testing.T) {
	original := jobsAdmin.ExecuteNewCopyJobPartOrder
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	processor := &copyTransferProcessor{
		dispatchCh: make(chan dispatchItem, 1), dispatchDone: make(chan struct{}),
	}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		close(entered)
		<-release
		return common.CopyJobPartOrderResponse{JobStarted: true}
	}
	t.Cleanup(func() {
		unblock()
		assert.NoError(t, processor.AbortAndWait(context.Background()))
		jobsAdmin.ExecuteNewCopyJobPartOrder = original
	})
	assert.NoError(t, processor.dispatchPart(dispatchItem{partNum: 0}))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch did not start")
	}

	aborted := make(chan struct{})
	var drainCalled atomic.Bool
	executor := &transferExecutor{opts: &CookedTransferOptions{}, processor: processor}
	go func() {
		err := executor.cancelAndDrain(func() {}, common.NewJobID(), func(context.Context, common.JobID) error {
			drainCalled.Store(true)
			return nil
		})
		assert.NoError(t, err)
		close(aborted)
	}()
	assert.Eventually(t, func() bool { return processor.getDispatchError() == context.Canceled }, time.Second, time.Millisecond)
	select {
	case <-aborted:
		t.Error("abort returned before the in-flight dispatch finished")
	case <-time.After(20 * time.Millisecond):
	}
	assert.False(t, drainCalled.Load())
	unblock()
	select {
	case <-aborted:
	case <-time.After(5 * time.Second):
		t.Fatal("abort did not finish after the dispatch returned")
	}
	assert.True(t, drainCalled.Load())
	assert.ErrorIs(t, processor.waitForDispatchPipeline(), context.Canceled)
}

func TestM3ProcessorAbortTimesOutDuringBlockedDispatch(t *testing.T) {
	original := jobsAdmin.ExecuteNewCopyJobPartOrder
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	processor := &copyTransferProcessor{
		dispatchCh: make(chan dispatchItem, 1), dispatchDone: make(chan struct{}),
		dispatchTemplate: common.CopyJobPartOrderRequest{JobID: common.NewJobID()},
	}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		close(entered)
		<-release
		return common.CopyJobPartOrderResponse{JobStarted: true}
	}
	t.Cleanup(func() {
		unblock()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		assert.NoError(t, processor.AbortAndWait(cleanupCtx))
		jobsAdmin.ExecuteNewCopyJobPartOrder = original
	})
	assert.NoError(t, processor.dispatchPart(dispatchItem{partNum: 0}))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch did not start")
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, processor.AbortAndWait(cleanupCtx), context.DeadlineExceeded)
	assert.ErrorIs(t, processor.getDispatchError(), context.Canceled)
	select {
	case <-processor.dispatchDone:
		t.Error("blocked dispatch was incorrectly reported as joined")
	default:
	}

	unblock()
	joinCtx, stopJoin := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopJoin()
	assert.NoError(t, processor.AbortAndWait(joinCtx))
	assert.ErrorIs(t, processor.waitForDispatchPipeline(), context.Canceled)
}

func TestM3CopyRejectsNilContext(t *testing.T) {
	client := Client{}
	_, err := client.Copy(nil, "source", "destination", CopyOptions{})
	assert.EqualError(t, err, "a context is required for copy")
}

func TestM3CopyDrainPreservesCancellationRequestError(t *testing.T) {
	operationCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := &transferExecutor{opts: &CookedTransferOptions{}}
	called := false
	err := executor.cancelAndDrain(cancel, common.JobID{}, func(context.Context, common.JobID) error {
		called = true
		return nil
	})
	assert.ErrorContains(t, err, "a job ID is required")
	assert.False(t, called)
	assert.ErrorIs(t, operationCtx.Err(), context.Canceled)
}

func TestM3CopyDrainUsesFreshBoundedContext(t *testing.T) {
	operationCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobID := common.NewJobID()
	drainErr := errors.New("drain did not finish")
	executor := &transferExecutor{opts: &CookedTransferOptions{}}
	called := false
	err := executor.cancelAndDrain(cancel, jobID, func(cleanupCtx context.Context, id common.JobID) error {
		called = true
		assert.ErrorIs(t, operationCtx.Err(), context.Canceled)
		assert.NoError(t, cleanupCtx.Err())
		deadline, bounded := cleanupCtx.Deadline()
		assert.True(t, bounded)
		assert.Greater(t, time.Until(deadline), time.Duration(0))
		assert.LessOrEqual(t, time.Until(deadline), 30*time.Second)
		assert.Equal(t, jobID, id)
		return drainErr
	})
	assert.True(t, called)
	assert.ErrorIs(t, err, drainErr)
}

func TestM3CopyDryrunCancelsWithoutCallingEngineDrain(t *testing.T) {
	operationCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := &transferExecutor{opts: &CookedTransferOptions{dryrun: true}}
	err := executor.cancelAndDrain(cancel, common.NewJobID(), func(context.Context, common.JobID) error {
		t.Error("dry-run called the engine drain barrier")
		return nil
	})
	assert.NoError(t, err)
	assert.ErrorIs(t, operationCtx.Err(), context.Canceled)
}

func TestM3CopyPathMappingPreservesRemoteNames(t *testing.T) {
	options := CopyPathOptions{
		Source:      common.ResourceString{Value: "https://source.blob.core.windows.net/container/root"},
		Destination: common.ResourceString{Value: "https://destination.blob.core.windows.net/container"},
		FromTo:      common.EFromTo.BlobBlob(),
	}
	object := traverser.StoredObject{RelativePath: `prefix/back\slash`, EntityType: common.EEntityType.File()}
	assert.Equal(t, "/prefix/back%5Cslash", options.MakeEscapedRelativePath(true, true, object))
	assert.Equal(t, "/prefix/back%5Cslash", options.MakeEscapedRelativePath(false, true, object))
	object.RelativePath = "\x00"
	assert.Equal(t, "\x00", options.MakeEscapedRelativePath(false, true, object))
}

func TestM3CopyRemoteWildcardAndLiteralStar(t *testing.T) {
	for _, test := range []struct {
		source, expected string
		strip            bool
	}{
		{"https://account.blob.core.windows.net/container/root/*", "https://account.blob.core.windows.net/container/root", true},
		{"https://account.blob.core.windows.net/container/root/%2A", "https://account.blob.core.windows.net/container/root/%2A", false},
		{"https://account.blob.core.windows.net/container/root/back%5Cslash", "https://account.blob.core.windows.net/container/root/back%5Cslash", false},
	} {
		got, strip, err := StripTrailingWildcardOnRemoteSource(test.source, common.ELocation.Blob())
		assert.NoError(t, err)
		assert.Equal(t, test.expected, got)
		assert.Equal(t, test.strip, strip)
	}
	_, _, err := StripTrailingWildcardOnRemoteSource("https://account.blob.core.windows.net/%zz", common.ELocation.Blob())
	assert.Error(t, err)
}

func TestM3CopyProcessorFilePropertiesAndCallbacks(t *testing.T) {
	template := &common.CopyJobPartOrderRequest{FromTo: common.EFromTo.BlobBlob()}
	var orders []common.CopyJobPartOrderRequest
	firstCalls, finalCalls := 0, 0
	processor := NewCopyTransferProcessor(true, template, 1, common.ResourceString{}, common.ResourceString{},
		func(started bool) { assert.True(t, started); firstCalls++ },
		func() { finalCalls++ }, false, true,
		func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			orders = append(orders, order)
			return common.CopyJobPartOrderResponse{JobStarted: true}
		})
	assert.NoError(t, processor.ScheduleTransfer(common.CopyTransfer{
		Source: "/metadata-only", Destination: "/metadata-only", EntityType: common.EEntityType.FileProperties(),
	}))
	assert.NoError(t, processor.ScheduleTransfer(common.CopyTransfer{
		Source: "/data", Destination: "/data", EntityType: common.EEntityType.File(), SourceSize: 7,
	}))
	started, err := processor.DispatchFinalPart()
	assert.NoError(t, err)
	assert.True(t, started)
	if assert.Len(t, orders, 2) {
		assert.EqualValues(t, 1, orders[0].Transfers.FilePropertyTransferCount)
		assert.Zero(t, orders[0].Transfers.TotalSizeInBytes)
		assert.EqualValues(t, 7, orders[1].Transfers.TotalSizeInBytes)
		assert.True(t, orders[1].IsFinalPart)
	}
	assert.Equal(t, 1, firstCalls)
	assert.Equal(t, 1, finalCalls)
}

func TestM3EmptyCopyProcessorCompletesEnumeration(t *testing.T) {
	template := &common.CopyJobPartOrderRequest{FromTo: common.EFromTo.BlobBlob()}
	firstCalls, finalCalls := 0, 0
	processor := NewCopyTransferProcessor(true, template, 1, common.ResourceString{}, common.ResourceString{},
		func(bool) { firstCalls++ }, func() { finalCalls++ }, false, true,
		func(common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			return common.CopyJobPartOrderResponse{ErrorMsg: common.ECopyJobPartOrderErrorType.NoTransfersScheduledErr()}
		})
	started, err := processor.DispatchFinalPart()
	assert.False(t, started)
	assert.ErrorIs(t, err, NothingScheduledError)
	assert.Zero(t, firstCalls)
	assert.Equal(t, 1, finalCalls)
}

func TestM3CopyProcessorDecompressionPath(t *testing.T) {
	template := &common.CopyJobPartOrderRequest{FromTo: common.EFromTo.BlobLocal(), AutoDecompress: true}
	processor := NewCopyTransferProcessor(true, template, 10, common.ResourceString{}, common.ResourceString{},
		nil, nil, false, false, nil)
	assert.NoError(t, processor.ScheduleSyncRemoveSetPropertiesTransfer(traverser.StoredObject{
		RelativePath: "data.gz", EntityType: common.EEntityType.File(), ContentEncoding: "gzip",
	}))
	if assert.Len(t, template.Transfers.List, 1) {
		assert.Equal(t, "/data", template.Transfers.List[0].Destination)
	}
}

func TestValidateProtocolCompatibility(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	// Test cases where validation should NOT be called (no File locations involved)
	testCases := []struct {
		name           string
		fromTo         common.FromTo
		shouldValidate bool
		description    string
	}{
		{
			name:           "S3ToBlob",
			fromTo:         common.EFromTo.S3Blob(),
			shouldValidate: false,
			description:    "S3 to Blob should not validate (neither side is File)",
		},
		{
			name:           "GCPToBlob",
			fromTo:         common.EFromTo.GCPBlob(),
			shouldValidate: false,
			description:    "GCP to Blob should not validate (neither side is File)",
		},
		{
			name:           "LocalToBlob",
			fromTo:         common.EFromTo.LocalBlob(),
			shouldValidate: false,
			description:    "Local to Blob should not validate (neither side is File)",
		},
		{
			name:           "BlobToLocal",
			fromTo:         common.EFromTo.BlobLocal(),
			shouldValidate: false,
			description:    "Blob to Local should not validate (neither side is File)",
		},
		{
			name:           "BlobToBlob",
			fromTo:         common.EFromTo.BlobBlob(),
			shouldValidate: false,
			description:    "Blob to Blob should not validate (neither side is File)",
		},
		{
			name:           "LocalToBlobFS",
			fromTo:         common.EFromTo.LocalBlobFS(),
			shouldValidate: false,
			description:    "Local to BlobFS should not validate (neither side is File)",
		},
		{
			name:           "LocalToFile",
			fromTo:         common.EFromTo.LocalFile(),
			shouldValidate: true,
			description:    "Local to File should validate (destination is File)",
		},
		{
			name:           "FileToLocal",
			fromTo:         common.EFromTo.FileLocal(),
			shouldValidate: true,
			description:    "File to Local should validate (source is File)",
		},
		{
			name:           "LocalToFileNFS",
			fromTo:         common.EFromTo.LocalFileNFS(),
			shouldValidate: true,
			description:    "Local to FileNFS should validate (destination is FileNFS)",
		},
		{
			name:           "FileNFSToLocal",
			fromTo:         common.EFromTo.FileNFSLocal(),
			shouldValidate: true,
			description:    "FileNFS to Local should validate (source is FileNFS)",
		},
		{
			name:           "FileToFile",
			fromTo:         common.EFromTo.FileFile(),
			shouldValidate: true,
			description:    "File to File should validate (both sides are File)",
		},
		{
			name:           "FileNFSToFileNFS",
			fromTo:         common.EFromTo.FileNFSFileNFS(),
			shouldValidate: true,
			description:    "FileNFS to FileNFS should validate (both sides are FileNFS)",
		},
		{
			name:           "FileToBlob",
			fromTo:         common.EFromTo.FileBlob(),
			shouldValidate: true,
			description:    "File to Blob should validate (source is File)",
		},
		{
			name:           "BlobToFile",
			fromTo:         common.EFromTo.BlobFile(),
			shouldValidate: true,
			description:    "Blob to File should validate (destination is File)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create dummy resource strings
			src := common.ResourceString{Value: "https://source.example.com/path"}
			dst := common.ResourceString{Value: "https://dest.example.com/path"}

			// For non-File transfers, we can pass nil service clients since validation should be skipped
			// For File transfers, we would need proper service clients, but we're testing the conditional logic
			var srcClient, dstClient *common.ServiceClient

			if !tc.shouldValidate {
				// Test that validation is skipped when no File locations are involved
				// This should not panic even with nil service clients
				err := ValidateProtocolCompatibility(ctx, tc.fromTo, src, dst, srcClient, dstClient)
				a.NoError(err, "validateProtocolCompatibility should not fail for %s: %s", tc.name, tc.description)
			} else {
				// For File transfers, we expect the function to attempt validation
				// Since we're passing nil service clients, we expect it to fail gracefully
				// This tests that the conditional logic correctly identifies File transfers
				err := ValidateProtocolCompatibility(ctx, tc.fromTo, src, dst, srcClient, dstClient)
				// We expect an error here because we're passing nil service clients for File transfers
				// The important thing is that it doesn't panic and attempts validation
				if tc.fromTo.From().IsFile() || tc.fromTo.To().IsFile() {
					a.Error(err, "validateProtocolCompatibility should attempt validation for %s and fail with nil clients: %s", tc.name, tc.description)
				}
			}
		})
	}
}

func TestValidateProtocolCompatibility_ConditionalLogic(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	// Test the specific conditional logic
	src := common.ResourceString{Value: "https://source.example.com/path"}
	dst := common.ResourceString{Value: "https://dest.example.com/path"}

	// Test that S3->Blob doesn't call validation (should not panic with nil clients)
	err := ValidateProtocolCompatibility(ctx, common.EFromTo.S3Blob(), src, dst, nil, nil)
	a.NoError(err, "S3->Blob should skip validation and not panic with nil service clients")

	// Test that GCP->Blob doesn't call validation (should not panic with nil clients)
	err = ValidateProtocolCompatibility(ctx, common.EFromTo.GCPBlob(), src, dst, nil, nil)
	a.NoError(err, "GCP->Blob should skip validation and not panic with nil service clients")

	// Test that Local->Blob doesn't call validation (should not panic with nil clients)
	err = ValidateProtocolCompatibility(ctx, common.EFromTo.LocalBlob(), src, dst, nil, nil)
	a.NoError(err, "Local->Blob should skip validation and not panic with nil service clients")
}
