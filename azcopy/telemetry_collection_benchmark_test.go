//go:build telemetryperf

package azcopy

import (
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

var telemetryCollectionBenchmarkResult telemetry.JobFinishedEvent

func BenchmarkTelemetryCollection(benchmark *testing.B) {
	for _, mode := range []string{"disabled", "enabled"} {
		for _, path := range []string{"file.bin", "first/second/third/file.bin"} {
			benchmark.Run(mode+"/"+path, func(benchmark *testing.B) {
				var tracker *sourceShapeTracker
				if mode == "enabled" {
					agent := &telemetryAgent{enabled: true}
					tracker = newSourceShapeTracker(common.ELocation.Blob(), common.ESymlinkHandlingType.Skip(), common.EHardlinkHandlingType.Follow())
					tracker.isActive = agent.isActive
				}
				object := traverser.StoredObject{EntityType: common.EEntityType.File(), RelativePath: path, ContainerName: "container", Size: 16 * 1024}
				benchmark.ReportAllocs()
				for benchmark.Loop() {
					if err := tracker.recordScanned(object); err != nil {
						benchmark.Fatal(err)
					}
					tracker.recordScheduled(object)
				}
			})
		}
	}
	benchmark.Run("snapshot-and-finished-summary", func(benchmark *testing.B) {
		tracker := newSourceShapeTracker(common.ELocation.Blob(), common.ESymlinkHandlingType.Skip(), common.EHardlinkHandlingType.Follow())
		object := traverser.StoredObject{EntityType: common.EEntityType.File(), RelativePath: "file.bin", Size: 64 * 1024 * 1024}
		if err := tracker.recordScanned(object); err != nil {
			benchmark.Fatal(err)
		}
		tracker.recordScheduled(object)
		summary := common.ListJobSummaryResponse{JobStatus: common.EJobStatus.Completed(), TotalTransfers: 1, FileTransfers: 1,
			TransfersCompleted: 1, TotalBytesTransferred: uint64(object.Size), TotalBytesExpected: uint64(object.Size),
			StorageHTTPAttemptCount: 16, AverageE2EMilliseconds: 200}
		started := time.Now()
		ended := started.Add(10 * time.Second)
		benchmark.ReportAllocs()
		for benchmark.Loop() {
			telemetryCollectionBenchmarkResult = buildFinishedEvent(telemetry.ResourceAttributes{}, telemetry.JobDimensions{Command: "copy"},
				"job", "attempt", started, ended, summary, 10*time.Second, time.Second, 9*time.Second, tracker.snapshot())
		}
	})
}
