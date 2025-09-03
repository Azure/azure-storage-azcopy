//go:build smslidingwindow
// +build smslidingwindow

// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"context"
	"fmt"
	_ "net/http/pprof"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// --- BEGIN Throttling and Concurrency Configuration ---
const (
	defaultMaxDirectoryChildCount int64 = 100_000 // Max files per active directory

	// These are static limits as of now. This can be dynamically adjusted later
	// by the StatsMonitor based on available system resources.
	maxActiveGoRoutines      int64   = 50_000 // Max active goroutines, used for dynamic limits
	maxMemoryUsageMultiplier float64 = 0.7    // Max memory usage percentage, used for dynamic limits

	throttleLogIntervalSecs       int           = 60 * 60                               // How often to log during throttling
	semaphoreThrottleWaitInterval time.Duration = time.Duration(time.Millisecond * 100) // How often to check semaphore after throttle limit is hit
	semaphoreWaitInterval         time.Duration = time.Duration(time.Millisecond * 50)  // How often to check semaphore status

	// Performance tuning constants
	filesPerGBMemory = 1_000_000 // Files per GB of memory

	// Hysteresis percentages to prevent oscillation
	throttleEngageThreshold  = 100 // Engage throttling at 100% of limit
	throttleReleaseThreshold = 85  // Release throttling at 85% of limit

	memoryEngageThreshold  = 80.0 // Engage at 80% memory usage
	memoryReleaseThreshold = 70.0 // Release at 70% memory usage

	goroutineEngageThreshold  = 100 // Engage at 100% of goroutine limit
	goroutineReleaseThreshold = 85  // Release at 85% of goroutine limit

	// Defaults
	defaultPhysicalMemoryGB uint64 = 8 // Default physical memory in GB, used if sysinfo fails
	defaultNumCores         int32  = 4 // Default number of CPU cores, used if runtime.NumCPU() fails

	// Traversal concurrency settings
	defaultTargetSlotRatio float64 = 0.25 // Target slots = 25% of source slots by default

	directorySizeBuffer = 1024

	gbToBytesMultiplier = 1024 * 1024 * 1024
	mbToBytesMultiplier = 1024 * 1024
)

// Global variables that control the throttling behavior and resource limits.
// These are configured based on system capabilities and transfer scenarios.
var (
	enableDebugLogs    bool = false
	enableThrottleLogs bool = true
	startGoProfiling   bool = false

	// Core concurrency settings
	crawlParallelism                  int32
	maxActiveFiles                    int64
	maxActivelyEnumeratingDirectories int64

	// Traversal concurrency settings
	targetSlotRatio float64 = defaultTargetSlotRatio // Configurable ratio for target slots

	// Counters
	activeDirectories         atomic.Int64
	srcDirEnumerating         atomic.Int64 // Number of directories currently being enumerated at source
	dstDirEnumerating         atomic.Int64 // Number of directories currently being enumerated at destination
	totalFilesInIndexer       atomic.Int64
	totalDirectoriesProcessed atomic.Uint64 // never decremented

	dstDirEnumerationSkippedBasedOnCTime atomic.Uint32

	// Throttling control flags
	enableThrottling               bool = true
	enableFileBasedThrottling      bool = false
	enableMemoryBasedThrottling    bool = true
	enableGoroutineBasedThrottling bool = true

	// Dynamic limits
	activeFilesLimit          atomic.Int64 // Dynamic limit for active files managed by StatsMonitor
	enumeratingDirectoryLimit atomic.Int64 // Dynamic limit for actively enumerating directories
)

func initializeTestModeLimits() {
	// In test mode, we set fixed limits for easier testing
	maxActiveFiles = 10000 // not being used right now
	maxActivelyEnumeratingDirectories = 100

	enableDebugLogs = false
	enableThrottleLogs = false
	startGoProfiling = false

	enableThrottling = true
	enableFileBasedThrottling = false
	enableMemoryBasedThrottling = false
	enableGoroutineBasedThrottling = false

	activeFilesLimit.Store(maxActiveFiles)
	enumeratingDirectoryLimit.Store(maxActivelyEnumeratingDirectories)
	crawlParallelism = 4
}

// initializeLimits initializes the concurrency and memory limits based on system resources
// and the transfer scenario (FromTo). It sets MaxActiveFiles based on available memory
// and CrawlParallelism based on CPU cores with scenario-specific multipliers.
func initializeLimits(orchestratorOptions *SyncOrchestratorOptions) {

	if common.IsSyncOrchTestModeSet() {
		initializeTestModeLimits()
		return
	}

	memory, err := common.GetTotalPhysicalMemory()
	if err != nil {
		syncOrchestratorLog(
			common.LogWarning,
			fmt.Sprintf("Failed to get total physical memory: %v. Using default - 8GB", err))
		memory = int64(defaultPhysicalMemoryGB)
	}

	memoryGB := memory / gbToBytesMultiplier                                              // Convert to GB
	maxActiveFiles = int64(float64(memoryGB)*maxMemoryUsageMultiplier) * filesPerGBMemory // Set based on physical memory, 1 million files per GB

	maxDirectoryDirectChildCount := defaultMaxDirectoryChildCount
	crawlParallelism = int32(EnumerationParallelism)

	if orchestratorOptions != nil && orchestratorOptions.valid && orchestratorOptions.maxDirectoryDirectChildCount > 0 {
		maxDirectoryDirectChildCount = int64(orchestratorOptions.GetMaxDirectoryDirectChildCount())
		crawlParallelism = orchestratorOptions.parallelTraversers
	}

	if maxDirectoryDirectChildCount > maxActiveFiles {
		syncOrchestratorLog(
			common.LogWarning,
			fmt.Sprintf("Max directory direct child count (%d) exceeds max active files (%d), adjusting to prevent OOM", maxDirectoryDirectChildCount, maxActiveFiles))
		maxDirectoryDirectChildCount = maxActiveFiles // Prevent deadlock by ensuring at least one directory can be processed
	}

	// Validate if the crawl parallelism is within acceptable limits
	// We need to check how many directories can be enumerated in parallel based on system memory
	safeParallelismLimit := GetSafeParallelismLimit(maxActiveFiles, maxDirectoryDirectChildCount, orchestratorOptions.fromTo)
	if crawlParallelism > safeParallelismLimit {
		syncOrchestratorLog(
			common.LogWarning,
			fmt.Sprintf("Crawl parallelism (%d) exceeds safe limit (%d), adjusting to prevent OOM", crawlParallelism, safeParallelismLimit),
			true)
		crawlParallelism = safeParallelismLimit
	}

	syncOrchestratorLog(common.LogInfo, fmt.Sprintf(
		"Crawl parallelism = %d, Indexer capacity = %d, Max child count = %d",
		crawlParallelism,
		maxActiveFiles,
		maxDirectoryDirectChildCount))

	maxActivelyEnumeratingDirectories = maxActiveFiles / maxDirectoryDirectChildCount // Ensure at least one directory can be processed
	activeFilesLimit.Store(maxActiveFiles)
	enumeratingDirectoryLimit.Store(maxActivelyEnumeratingDirectories) // Set initial limit for actively enumerating directories
}

// --- END Throttling and Concurrency Configuration ---

func GetSafeParallelismLimit(maxActiveFiles, maxChildCount int64, fromTo common.FromTo) int32 {
	limit := int32(max(maxActiveFiles/maxChildCount, 1))

	switch fromTo {
	case common.EFromTo.LocalBlob(), common.EFromTo.LocalBlobFS(), common.EFromTo.LocalFile():
		return min(limit, 48)
	case common.EFromTo.S3Blob():
		return min(limit, 64)
	default:
		return min(limit, 48)
	}
}

// GetNumCPU returns the number of logical CPU cores available on the system.
// It uses runtime.NumCPU() and falls back to a default value if the call fails.
func GetNumCPU() int32 {
	// Use runtime.NumCPU() to get the number of logical CPUs
	numCores := runtime.NumCPU()
	if numCores <= 0 {
		// Fallback to default if NumCPU fails
		numCores = int(defaultNumCores)
	}
	syncOrchestratorLog(common.LogInfo, fmt.Sprintf("Number of CPU cores: %d", numCores))
	return int32(numCores)
}

// ThrottleSemaphore manages directory processing concurrency using a semaphore pattern.
// It controls how many directories can be processed simultaneously and includes
// throttling logic based on file indexer size and system resource usage.
type ThrottleSemaphore struct {
	// Separate semaphores for source and target traversals
	sourceSemaphore chan struct{}
	targetSemaphore chan struct{}

	// Legacy semaphore for backward compatibility
	semaphore chan struct{}

	waitingForSourceSemaphore atomic.Int32
	waitingForTargetSemaphore atomic.Int32

	// Monitoring
	lastLogTime time.Time

	statsMonitor *common.SystemStatsMonitor

	// Hysteresis state tracking
	isThrottling       bool         // Current throttling state
	throttleStateMutex sync.RWMutex // Protect throttling state

	// Individual throttle states for different resources
	fileThrottleActive      bool
	memoryThrottleActive    bool
	goroutineThrottleActive bool

	// Context-based cancellation instead of channel
	ctx    context.Context
	cancel context.CancelFunc
}

// NewThrottleSemaphore creates and initializes a new ThrottleSemaphore with pre-filled tokens
// and starts the associated stats monitor for dynamic throttling adjustments.
func NewThrottleSemaphore(parentCtx context.Context, jobID common.JobID) *ThrottleSemaphore {
	// Create child context for this semaphore
	ctx, cancel := context.WithCancel(parentCtx)

	// Calculate target semaphore capacity (25% of source capacity by default, configurable)
	sourceCapacity := crawlParallelism
	targetCapacity := int32(float64(sourceCapacity) * targetSlotRatio)
	if targetCapacity < 1 {
		targetCapacity = 1 // Ensure at least 1 slot for target
	}

	ds := &ThrottleSemaphore{
		sourceSemaphore: make(chan struct{}, sourceCapacity),
		targetSemaphore: make(chan struct{}, targetCapacity),
		lastLogTime:     time.Now(),
		ctx:             ctx,
		cancel:          cancel,

		// Initialize hysteresis state
		isThrottling:            false,
		fileThrottleActive:      false,
		memoryThrottleActive:    false,
		goroutineThrottleActive: false,
	}

	// Pre-fill source semaphore with tokens
	for i := int32(0); i < sourceCapacity; i++ {
		ds.sourceSemaphore <- struct{}{}
	}

	// Pre-fill target semaphore with tokens
	for i := int32(0); i < targetCapacity; i++ {
		ds.targetSemaphore <- struct{}{}
	}

	RegisterGlobalCustomStatsCallback(common.SyncOrchestratorId, ds.getOrchestratorStats)

	return ds
}

// Close gracefully shuts down the ThrottleSemaphore by stopping the stats monitor.
// This should be called when the semaphore is no longer needed to prevent resource leaks.
func (ds *ThrottleSemaphore) Close() {

	ForceCollectGlobalCustomStats(common.SyncOrchestratorId)
	UnregisterGlobalCustomStatsCallback(common.SyncOrchestratorId)

	// Cancel the context to stop all monitoring goroutines
	if ds.cancel != nil {
		ds.cancel()
	}
	syncOrchestratorLog(common.LogInfo, "Stopping ThrottleSemaphore and releasing resources")
}

// AcquireSourceSlot blocks until a source traversal slot is available and throttling conditions allow processing.
// Source slots have higher capacity (same as crawlParallelism) to allow faster source traversal.
// Returns an error if the context is cancelled during acquisition.
func (ds *ThrottleSemaphore) AcquireSourceSlot(ctx context.Context) error {
	ds.waitingForSourceSemaphore.Add(1)
	defer ds.waitingForSourceSemaphore.Add(-1)

	for {
		select {
		case <-ds.sourceSemaphore:
			// Got source semaphore token
			if ds.shouldThrottle() {
				// Should throttle - put token back and wait
				ds.sourceSemaphore <- struct{}{}

				// Cancellation-aware sleep
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(semaphoreThrottleWaitInterval):
					continue
				}
			}

			// Track active source directory processors
			// Note: Not incrementing activeDirectories here as it's handled at directory level
			return nil

		default:
			// No source semaphore available - wait
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(semaphoreWaitInterval):
				continue
			}
		}
	}
}

// AcquireTargetSlot blocks until a target traversal slot is available and throttling conditions allow processing.
// Target slots have lower capacity (configurable percentage of source capacity) to limit slower target operations.
// Returns an error if the context is cancelled during acquisition.
func (ds *ThrottleSemaphore) AcquireTargetSlot(ctx context.Context) error {
	ds.waitingForTargetSemaphore.Add(1)
	defer ds.waitingForTargetSemaphore.Add(-1)

	for {
		select {
		case <-ds.targetSemaphore:
			// Got target semaphore token
			if ds.shouldThrottle() {
				// Should throttle - put token back and wait
				ds.targetSemaphore <- struct{}{}

				// Cancellation-aware sleep
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(semaphoreThrottleWaitInterval):
					continue
				}
			}

			// Track active target directory processors
			// Note: Not incrementing activeDirectories here as it's handled at directory level
			return nil

		default:
			// No target semaphore available - wait
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(semaphoreWaitInterval):
				continue
			}
		}
	}
}

// ReleaseSourceSlot returns a source semaphore token.
// This should be called when source traversal is complete.
func (ds *ThrottleSemaphore) ReleaseSourceSlot() {
	ds.sourceSemaphore <- struct{}{} // Put source token back
}

// ReleaseTargetSlot returns a target semaphore token.
// This should be called when target traversal is complete.
func (ds *ThrottleSemaphore) ReleaseTargetSlot() {
	ds.targetSemaphore <- struct{}{} // Put target token back
}

// shouldThrottle determines whether directory processing should be throttled with hysteresis
// to prevent oscillating behavior between throttled and non-throttled states.
func (ds *ThrottleSemaphore) shouldThrottle() bool {
	ds.throttleStateMutex.Lock()
	defer ds.throttleStateMutex.Unlock()

	// Check each resource with hysteresis
	fileThrottle := enableFileBasedThrottling && ds.shouldThrottleBasedOnFiles()
	memoryThrottle := enableMemoryBasedThrottling && ds.shouldThrottleBasedOnMemory()
	goroutineThrottle := enableGoroutineBasedThrottling && ds.shouldThrottleBasedOnGoroutines()

	// Update overall throttling state
	previousState := ds.isThrottling
	ds.isThrottling = fileThrottle || memoryThrottle || goroutineThrottle

	// Log state changes
	if enableThrottleLogs && (previousState != ds.isThrottling) {
		var reasons []string

		if fileThrottle {
			reasons = append(reasons, "FILES")
		}
		if memoryThrottle {
			reasons = append(reasons, "MEMORY")
		}
		if goroutineThrottle {
			reasons = append(reasons, "GOROUTINES")
		}

		if ds.isThrottling {
			syncOrchestratorLog(common.LogWarning, fmt.Sprintf("THROTTLE ENGAGED: %s", strings.Join(reasons, ", ")))
		} else {
			syncOrchestratorLog(common.LogInfo, "THROTTLE RELEASED: All resources below release thresholds")
		}
	}

	return ds.isThrottling
}

// shouldThrottleBasedOnFiles applies hysteresis to file indexer throttling
func (ds *ThrottleSemaphore) shouldThrottleBasedOnFiles() bool {
	currentFiles := totalFilesInIndexer.Load()
	activeFilesLimit := activeFilesLimit.Load()

	// Calculate usage percentage
	usagePercent := float64(currentFiles) * 100.0 / float64(activeFilesLimit)

	if !ds.fileThrottleActive {
		// Not currently throttling - check if we should start
		if usagePercent >= throttleEngageThreshold {
			ds.fileThrottleActive = true
			ds.logThrottling("FILES: Engaging throttle at %.1f%% (%d/%d files)",
				usagePercent, currentFiles, activeFilesLimit)
			return true
		}
	} else {
		// Currently throttling - check if we should stop
		if usagePercent <= throttleReleaseThreshold {
			ds.fileThrottleActive = false
			ds.logThrottling("FILES: Releasing throttle at %.1f%% (%d/%d files)",
				usagePercent, currentFiles, activeFilesLimit)
			return false
		}
		// Stay throttled
		return true
	}

	return false
}

// shouldThrottleBasedOnMemory applies hysteresis to memory pressure throttling
func (ds *ThrottleSemaphore) shouldThrottleBasedOnMemory() bool {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	totalMemoryBytes, err := common.GetTotalPhysicalMemory()
	if err != nil {
		totalMemoryBytes = int64(defaultPhysicalMemoryGB) * gbToBytesMultiplier
	}
	usagePercent := float64(memStats.Sys) / float64(totalMemoryBytes) * 100

	if !ds.memoryThrottleActive {
		// Not currently throttling - check if we should start
		if usagePercent >= memoryEngageThreshold {
			ds.memoryThrottleActive = true
			ds.logThrottling(fmt.Sprintf("MEMORY: Engaging throttle at %.1f%% usage", usagePercent))
			return true
		}
	} else {
		// Currently throttling - check if we should stop
		if usagePercent <= memoryReleaseThreshold {
			ds.memoryThrottleActive = false
			ds.logThrottling(fmt.Sprintf("MEMORY: Releasing throttle at %.1f%% usage", usagePercent))
			return false
		}
		// Stay throttled
		return true
	}

	return false
}

// shouldThrottleBasedOnGoroutines applies hysteresis to goroutine throttling
func (ds *ThrottleSemaphore) shouldThrottleBasedOnGoroutines() bool {
	currentGoroutines := int64(runtime.NumGoroutine())

	// Calculate usage percentage
	usagePercent := float64(currentGoroutines) * 100.0 / float64(maxActiveGoRoutines)

	if !ds.goroutineThrottleActive {
		// Not currently throttling - check if we should start
		if usagePercent >= goroutineEngageThreshold {
			ds.goroutineThrottleActive = true
			ds.logThrottling("GOROUTINES: Engaging throttle at %.1f%% (%d/%d goroutines)",
				usagePercent, currentGoroutines, maxActiveGoRoutines)
			return true
		}
	} else {
		// Currently throttling - check if we should stop
		if usagePercent <= goroutineReleaseThreshold {
			ds.goroutineThrottleActive = false
			ds.logThrottling("GOROUTINES: Releasing throttle at %.1f%% (%d/%d goroutines)",
				usagePercent, currentGoroutines, maxActiveGoRoutines)
			return false
		}
		// Stay throttled
		return true
	}

	return false
}

func (ds *ThrottleSemaphore) logThrottling(msgFmt string, args ...interface{}) {

	if !enableThrottleLogs {
		return
	}

	now := time.Now()
	if now.Sub(ds.lastLogTime) > time.Duration(throttleLogIntervalSecs)*time.Second {
		syncOrchestratorLog(common.LogWarning, msgFmt)
		ds.lastLogTime = now
	}
}

// getOrchestratorStats returns orchestrator-specific metrics for the custom stats callback.
// It provides real-time visibility into the sync orchestrator's internal state including
// indexer size, directory processing counters, and enumeration activity.
func (ds *ThrottleSemaphore) getOrchestratorStats() []common.CustomStatEntry {
	stats := []common.CustomStatEntry{
		{Key: "Indexer", Value: fmt.Sprintf("%d", totalFilesInIndexer.Load())},
		{Key: "Active", Value: fmt.Sprintf("%d", activeDirectories.Load())},
		{Key: "SrcEnum", Value: fmt.Sprintf("%d", srcDirEnumerating.Load())},
		{Key: "DstEnum", Value: fmt.Sprintf("%d", dstDirEnumerating.Load())},
		{Key: "Done", Value: fmt.Sprintf("%d", totalDirectoriesProcessed.Load())},
	}

	waitingSource := ds.waitingForSourceSemaphore.Load()
	if waitingSource != 0 {
		stats = append(stats, common.CustomStatEntry{Key: "WaitSrc", Value: fmt.Sprintf("%d", waitingSource)})
	}

	waitingTarget := ds.waitingForTargetSemaphore.Load()
	if waitingTarget != 0 {
		stats = append(stats, common.CustomStatEntry{Key: "WaitTgt", Value: fmt.Sprintf("%d", waitingTarget)})
	}

	ctimeSkip := dstDirEnumerationSkippedBasedOnCTime.Load()
	if ctimeSkip != 0 {
		stats = append(stats, common.CustomStatEntry{Key: "CTimeSkip", Value: fmt.Sprintf("%d", ctimeSkip)})
	}

	return stats
}
