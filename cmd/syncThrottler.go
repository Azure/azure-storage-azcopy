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

const (
	SyncThrottlingTestMode = false // Set to true to enable throttling test mode
)

// --- BEGIN Throttling and Concurrency Configuration ---
const (
	absoluteMaxActiveFiles     int64 = 10_000_000 // Absolute max active files, used for dynamic limits
	maxFilesPerActiveDirectory int64 = 1_000_000  // Max files per active directory

	// These are static limits as of now. This can be dynamically adjusted later
	// by the StatsMonitor based on available system resources.
	maxActiveGoRoutines   int64 = 50_000 // Max active goroutines, used for dynamic limits
	maxMemoryUsagePercent int32 = 80     // Max memory usage percentage, used for dynamic limits

	throttleLogIntervalSecs       int           = 60 * 60                               // How often to log during throttling
	semaphoreThrottleWaitInterval time.Duration = time.Duration(time.Millisecond * 100) // How often to check semaphore after throttle limit is hit
	semaphoreWaitInterval         time.Duration = time.Duration(time.Millisecond * 50)  // How often to check semaphore status

	// Performance tuning constants
	filesPerGBMemory          = 500_000 // Files per GB of memory
	snapshotRetentionCount    = 50      // Number of snapshots to keep
	consistencyThreshold      = 10      // Samples needed for consistency
	adjustmentCooldownMinutes = 2       // Minutes between adjustments

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

	// Counters
	activeDirectories            atomic.Int64
	activeDirectoriesEnumerating atomic.Int64 // Number of directories currently being enumerated
	totalFilesInIndexer          atomic.Int64
	totalDirectoriesProcessed    atomic.Uint64 // never decremented

	// Throttling control flags
	enableThrottling               bool = true
	enableEnumerationThrottling    bool = true // Throttle directory enumeration to prevent deadlocks
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
	enableEnumerationThrottling = true // Throttle directory enumeration to prevent deadlocks
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

	if SyncThrottlingTestMode {
		initializeTestModeLimits()
		return
	}

	memory, err := common.GetTotalPhysicalMemory()
	if err != nil {
		glcm.Warn(fmt.Sprintf("Failed to get total physical memory: %v", err))
		memory = int64(defaultPhysicalMemoryGB)
	}
	memoryGB := memory / gbToBytesMultiplier            // Convert to GB
	maxActiveFiles = int64(memoryGB) * filesPerGBMemory // Set based on physical memory, 1 million files per GB

	maxDirectoryDirectChildCount := maxFilesPerActiveDirectory // Default max direct children per directory
	crawlParallelism = int32(EnumerationParallelism)

	if orchestratorOptions != nil && orchestratorOptions.valid && orchestratorOptions.maxDirectoryDirectChildCount > 0 {
		maxDirectoryDirectChildCount = int64(orchestratorOptions.GetMaxDirectoryDirectChildCount())
		crawlParallelism = orchestratorOptions.parallelTraversers
	}

	if maxDirectoryDirectChildCount > maxActiveFiles {
		glcm.Warn(fmt.Sprintf("Max directory direct child count (%d) exceeds max active files (%d), adjusting to prevent deadlock", maxDirectoryDirectChildCount, maxActiveFiles))
		maxDirectoryDirectChildCount = maxActiveFiles // Prevent deadlock by ensuring at least one directory can be processed
	}

	maxActivelyEnumeratingDirectories = maxActiveFiles / maxDirectoryDirectChildCount // Ensure at least one directory can be processed
	activeFilesLimit.Store(maxActiveFiles)
	enumeratingDirectoryLimit.Store(maxActivelyEnumeratingDirectories) // Set initial limit for actively enumerating directories
}

// --- END Throttling and Concurrency Configuration ---

// GetNumCPU returns the number of logical CPU cores available on the system.
// It uses runtime.NumCPU() and falls back to a default value if the call fails.
func GetNumCPU() int32 {
	// Use runtime.NumCPU() to get the number of logical CPUs
	numCores := runtime.NumCPU()
	if numCores <= 0 {
		// Fallback to default if NumCPU fails
		numCores = int(defaultNumCores)
	}
	glcm.Info(fmt.Sprintf("Number of CPU cores: %d", numCores))
	return int32(numCores)
}

// ThrottleSemaphore manages directory processing concurrency using a semaphore pattern.
// It controls how many directories can be processed simultaneously and includes
// throttling logic based on file indexer size and system resource usage.
type ThrottleSemaphore struct {
	// Semaphore for directory processing
	semaphore chan struct{}

	waitingForSemaphore atomic.Int32

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

	ds := &ThrottleSemaphore{
		semaphore:   make(chan struct{}, crawlParallelism),
		lastLogTime: time.Now(),
		ctx:         ctx,
		cancel:      cancel,

		// Initialize hysteresis state
		isThrottling:            false,
		fileThrottleActive:      false,
		memoryThrottleActive:    false,
		goroutineThrottleActive: false,
	}

	// Pre-fill semaphore with tokens
	for i := int32(0); i < crawlParallelism; i++ {
		ds.semaphore <- struct{}{}
	}

	// Start semaphore monitoring with context
	go ds.semaphoreMonitor(ctx)

	RegisterGlobalCustomStatsCallback(getOrchestratorStats)

	return ds
}

// Close gracefully shuts down the ThrottleSemaphore by stopping the stats monitor.
// This should be called when the semaphore is no longer needed to prevent resource leaks.
func (ds *ThrottleSemaphore) Close() {
	// Cancel the context to stop all monitoring goroutines
	if ds.cancel != nil {
		ds.cancel()
	}

	UnregisterGlobalCustomStatsCallback()
}

// AcquireSlot blocks until a semaphore slot is available and throttling conditions allow processing.
// It implements context-aware cancellation and respects throttling limits to prevent system overload.
// Returns an error if the context is cancelled during acquisition.
func (ds *ThrottleSemaphore) AcquireSlot(ctx context.Context) error {
	// This blocks until semaphore is available AND throttling allows it
	ds.waitingForSemaphore.Add(1)
	defer ds.waitingForSemaphore.Add(-1)

	for {
		select {

		case <-ds.semaphore:
			// Got semaphore token
			if ds.shouldThrottle() {
				// Should throttle - put token back and wait
				ds.semaphore <- struct{}{}

				// Cancellation-aware sleep
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(semaphoreThrottleWaitInterval):
					continue
				}
			}

			// Track active directory processors
			activeDirectories.Add(1)
			return nil

		default:
			// No semaphore available - wait
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(semaphoreWaitInterval):
				continue
			}
		}
	}
}

// ReleaseSlot returns a semaphore token and decrements the active directory counter.
// This should be called when directory processing is complete to allow other operations to proceed.
func (ds *ThrottleSemaphore) ReleaseSlot() {
	ds.semaphore <- struct{}{} // Put token back
	activeDirectories.Add(-1)  // Decrement active directory count
}

// shouldThrottle determines whether directory processing should be throttled with hysteresis
// to prevent oscillating behavior between throttled and non-throttled states.
func (ds *ThrottleSemaphore) shouldThrottle() bool {
	ds.throttleStateMutex.Lock()
	defer ds.throttleStateMutex.Unlock()

	// Check each resource with hysteresis
	enumerationThrottling := enableEnumerationThrottling && ds.shouldThrottlingDirectoryEnumeration()
	fileThrottle := enableFileBasedThrottling && ds.shouldThrottleBasedOnFiles()
	memoryThrottle := enableMemoryBasedThrottling && ds.shouldThrottleBasedOnMemory()
	goroutineThrottle := enableGoroutineBasedThrottling && ds.shouldThrottleBasedOnGoroutines()

	// Update overall throttling state
	previousState := ds.isThrottling
	ds.isThrottling = enumerationThrottling || fileThrottle || memoryThrottle || goroutineThrottle

	// Log state changes
	if enableThrottleLogs && (previousState != ds.isThrottling) {
		var reasons []string

		if enumerationThrottling {
			reasons = append(reasons, "DIRECTORY ENUMERATION")
		}

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
			glcm.Info(fmt.Sprintf("THROTTLE ENGAGED: %s", strings.Join(reasons, ", ")))
		} else {
			glcm.Info("THROTTLE RELEASED: All resources below release thresholds")
		}
	}

	return ds.isThrottling
}

func (ds *ThrottleSemaphore) shouldThrottlingDirectoryEnumeration() bool {
	currentActivelyEnumerating := activeDirectoriesEnumerating.Load()
	enumeratingLimit := enumeratingDirectoryLimit.Load()

	// We want to do simple throttling here without any hysteresis
	if currentActivelyEnumerating >= enumeratingLimit {
		if enableThrottleLogs {
			glcm.Info(fmt.Sprintf("DIRECTORY ENUMERATION THROTTLED: %d directories actively enumerating (limit: %d)",
				currentActivelyEnumerating, enumeratingLimit))
		}
		return true // Throttle if we hit the limit
	}

	return false // No throttling needed
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
		glcm.Info(msgFmt)
		ds.lastLogTime = now
	}
}

// semaphoreMonitor provides detailed monitoring and logging of semaphore utilization.
// It runs in a separate goroutine and periodically reports on contention and resource usage.
// The monitoring helps identify performance bottlenecks and system stress conditions.
func (ds *ThrottleSemaphore) semaphoreMonitor(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(throttleLogIntervalSecs) * time.Second) // More frequent monitoring
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if enableThrottling {
				waiting := ds.waitingForSemaphore.Load()
				active := activeDirectories.Load()
				queued := totalDirectoriesProcessed.Load()
				files := totalFilesInIndexer.Load()
				limit := activeFilesLimit.Load()

				// Only log if there's activity or contention
				if waiting > 5 || active > 5 {
					if enableThrottleLogs {
						WarnStdoutAndScanningLog(fmt.Sprintf(
							"[INFO] Active Dirs: %d | Wait: %d | Total: %d | Active Files: %d/%d",
							active, waiting, queued, files, limit))
					}
				}

				// Alert on high contention
				if waiting > int32(float64(crawlParallelism)*0.8) {
					if enableThrottleLogs {
						WarnStdoutAndScanningLog(fmt.Sprintf(
							"[WARN] Severe contention: %d directories waiting (exceeds semaphore capacity of %d)",
							waiting, crawlParallelism))
					}
				}
			}
		}
	}
}

// getOrchestratorStats returns orchestrator-specific metrics for the custom stats callback.
// It provides real-time visibility into the sync orchestrator's internal state including
// indexer size, directory processing counters, and enumeration activity.
func getOrchestratorStats() map[string]string {
	return map[string]string{
		"IndexerSize":            fmt.Sprintf("%d", totalFilesInIndexer.Load()),
		"ActiveDirectories":      fmt.Sprintf("%d", activeDirectories.Load()),
		"ProcessedDirectories":   fmt.Sprintf("%d", totalDirectoriesProcessed.Load()),
		"EnumeratingDirectories": fmt.Sprintf("%d", activeDirectoriesEnumerating.Load()),
	}
}
