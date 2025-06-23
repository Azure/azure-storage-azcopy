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
	enableDebugLogs    bool = false
	enableThrottleLogs bool = true
	startGoProfiling   bool = true

	// Throttling control flags
	enableThrottling               bool = true
	enableFileBasedThrottling      bool = true
	enableMemoryBasedThrottling    bool = true
	enableGoroutineBasedThrottling bool = true
	onlyReduceLimits               bool = true // If true, only reduce limits, never increase

	absoluteMaxActiveFiles     int64 = 10_000_000 // Absolute max active files, used for dynamic limits
	maxFilesPerActiveDirectory int64 = 80_000     // Max files per active directory

	// These are static limits as of now. This can be dynamically adjusted later
	// by the StatsMonitor based on available system resources.
	maxActiveGoRoutines   int64 = 50_000 // Max active goroutines, used for dynamic limits
	maxMemoryUsagePercent int32 = 80     // Max memory usage percentage, used for dynamic limits

	throttleLogIntervalSecs       int           = 60 * 60                               // How often to log during throttling
	semaphoreThrottleWaitInterval time.Duration = time.Duration(time.Millisecond * 100) // How often to check semaphore after throttle limit is hit
	semaphoreWaitInterval         time.Duration = time.Duration(time.Millisecond * 50)  // How often to check semaphore status

	// Performance tuning constants
	filesPerGBMemory          = 500_000 // Files per GB of memory
	crawlMultiplierLocal      = 4       // Local to remote multiplier
	crawlMultiplierS3         = 8       // S3 to blob multiplier
	crawlMultiplierDefault    = 2       // Default multiplier
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
	defaultPhysicalMemoryGB uint64 = 16 // Default physical memory in GB, used if sysinfo fails
	defaultNumCores         int32  = 8  // Default number of CPU cores, used if runtime.NumCPU() fails

	directorySizeBuffer = 1024

	gbToBytesMultiplier = 1024 * 1024 * 1024
	mbToBytesMultiplier = 1024 * 1024
)

// Global variables that control the throttling behavior and resource limits.
// These are configured based on system capabilities and transfer scenarios.
var (
	// Core concurrency settings
	crawlParallelism int32
	maxActiveFiles   int64

	// Counters
	activeDirectories         atomic.Int64
	totalFilesInIndexer       atomic.Int64
	totalDirectoriesProcessed atomic.Uint64 // never decremented

	// Dynamic limits
	activeFilesLimit atomic.Int64 // Dynamic limit for active files managed by StatsMonitor
)

// initializeLimits initializes the concurrency and memory limits based on system resources
// and the transfer scenario (FromTo). It sets MaxActiveFiles based on available memory
// and CrawlParallelism based on CPU cores with scenario-specific multipliers.
func initializeLimits(fromTo common.FromTo) {
	maxActiveFiles = int64(GetTotalPhysicalMemoryGB()) * filesPerGBMemory // Set based on physical memory, 1 million files per GB
	activeFilesLimit.Store(maxActiveFiles)

	var multiplier int
	switch fromTo.From() {
	case common.ELocation.Local():
		// Local to remote, use default limits
		// Parallelism will deal with parallel File I/O operations
		multiplier = crawlMultiplierLocal
	case common.ELocation.S3():
		// S3 to blob, use higher limits
		// parallelism will deal with API calls
		multiplier = crawlMultiplierS3
	default:
		// Default case, use moderate limits
		multiplier = crawlMultiplierDefault
	}

	crawlParallelism = int32(runtime.NumCPU() * multiplier) // Set parallelism based on CPU cores
}

// --- END Throttling and Concurrency Configuration ---

// GetTotalPhysicalMemoryGB retrieves the total physical memory available on the system in GB.
// It uses syscall.Sysinfo to get system information and falls back to a default value
// if the system call fails.
func GetTotalPhysicalMemoryGB() uint64 {
	/*var sysInfo syscall.Sysinfo_t
	if err := syscall.Sysinfo(&sysInfo); err != nil {
	} else {
		// Convert from bytes to GB
		totalGB = (uint64(sysInfo.Totalram)) * uint64(sysInfo.Unit) / gbToBytesMultiplier
	}*/
	return defaultPhysicalMemoryGB

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
	glcm.Info(fmt.Sprintf("Number of CPU cores: %d", numCores))
	return int32(numCores)
}

// DirSemaphore manages directory processing concurrency using a semaphore pattern.
// It controls how many directories can be processed simultaneously and includes
// throttling logic based on file indexer size and system resource usage.
type DirSemaphore struct {
	// Semaphore for directory processing
	dirSemaphore chan struct{}

	waitingForSemaphore atomic.Int32

	// Monitoring
	lastLogTime time.Time

	statsMonitor *StatsMonitor

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

// NewDirSemaphore creates and initializes a new DirSemaphore with pre-filled tokens
// and starts the associated stats monitor for dynamic throttling adjustments.
func NewDirSemaphore(parentCtx context.Context) *DirSemaphore {
	// Create child context for this semaphore
	ctx, cancel := context.WithCancel(parentCtx)

	ds := &DirSemaphore{
		dirSemaphore: make(chan struct{}, crawlParallelism),
		lastLogTime:  time.Now(),
		ctx:          ctx,
		cancel:       cancel,

		// Initialize hysteresis state
		isThrottling:            false,
		fileThrottleActive:      false,
		memoryThrottleActive:    false,
		goroutineThrottleActive: false,
	}

	// Pre-fill semaphore with tokens
	for i := int32(0); i < crawlParallelism; i++ {
		ds.dirSemaphore <- struct{}{}
	}

	ds.statsMonitor = NewStatsMonitor()
	ds.statsMonitor.Start(ctx) // Pass context to stats monitor

	// Start semaphore monitoring with context
	go ds.semaphoreMonitor(ctx)

	return ds
}

// Close gracefully shuts down the DirSemaphore by stopping the stats monitor.
// This should be called when the semaphore is no longer needed to prevent resource leaks.
func (ds *DirSemaphore) Close() {
	// Cancel the context to stop all monitoring goroutines
	if ds.cancel != nil {
		ds.cancel()
	}

	// Stop the stats monitor
	ds.statsMonitor.Stop()
}

// AcquireSlot blocks until a semaphore slot is available and throttling conditions allow processing.
// It implements context-aware cancellation and respects throttling limits to prevent system overload.
// Returns an error if the context is cancelled during acquisition.
func (ds *DirSemaphore) AcquireSlot(ctx context.Context) error {
	// This blocks until semaphore is available AND throttling allows it
	ds.waitingForSemaphore.Add(1)
	defer ds.waitingForSemaphore.Add(-1)

	for {
		select {

		case <-ds.dirSemaphore:
			// Got semaphore token
			if ds.shouldThrottle() {
				// Should throttle - put token back and wait
				ds.dirSemaphore <- struct{}{}

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
func (ds *DirSemaphore) ReleaseSlot() {
	ds.dirSemaphore <- struct{}{} // Put token back
	activeDirectories.Add(-1)     // Decrement active directory count
}

// shouldThrottle determines whether directory processing should be throttled with hysteresis
// to prevent oscillating behavior between throttled and non-throttled states.
func (ds *DirSemaphore) shouldThrottle() bool {
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
			glcm.Info(fmt.Sprintf("THROTTLE ENGAGED: %s", strings.Join(reasons, ", ")))
		} else {
			glcm.Info("THROTTLE RELEASED: All resources below release thresholds")
		}
	}

	return ds.isThrottling
}

// shouldThrottleBasedOnFiles applies hysteresis to file indexer throttling
func (ds *DirSemaphore) shouldThrottleBasedOnFiles() bool {
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
func (ds *DirSemaphore) shouldThrottleBasedOnMemory() bool {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	totalMemoryBytes := GetTotalPhysicalMemoryGB() * gbToBytesMultiplier
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
func (ds *DirSemaphore) shouldThrottleBasedOnGoroutines() bool {
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

func (ds *DirSemaphore) logThrottling(msgFmt string, args ...interface{}) {

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
func (ds *DirSemaphore) semaphoreMonitor(ctx context.Context) {
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

// StatsState represents different operational states of the system based on resource utilization.
// These states drive dynamic adjustments to concurrency limits and throttling behavior.
type StatsState int

const (
	StateOptimal       StatsState = iota // 80-100%
	StateUnderutilized                   // < 60% - increase directories
	StateCritical                        // > 120% - decrease directories
	StateAboveOptimal                    // 100-120% - slight decrease
	StateBelowOptimal                    // 60-80% - slight increase
)

// StatsSnapshot captures a point-in-time view of system performance metrics.
// These snapshots are used for trend analysis and dynamic limit adjustments.
type StatsSnapshot struct {
	Timestamp          time.Time
	IndexerSize        int64
	ActiveDirectories  int64
	MemoryUsageMB      uint64
	UtilizationPercent float64
}

// StatsMonitor analyzes system performance over time and dynamically adjusts
// concurrency limits to maintain optimal throughput while preventing resource exhaustion.
// It implements a feedback control system with configurable thresholds and cooldown periods.
type StatsMonitor struct {
	// Target configuration
	targetIndexerSize      int64 // Target
	optimalRangeMin        int64 // 80% of target
	optimalRangeMax        int64 // 100% of target
	criticalThreshold      int64 // 120% of target - must reduce
	underutilizedThreshold int64 // 60% of target - can increase

	// Analysis parameters
	consistencyThreshold int           // Number of consecutive samples needed
	adjustmentCooldown   time.Duration // Minimum time between adjustments
	lastAdjustmentTime   time.Time

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Rest of the fields...
	snapshots          []StatsSnapshot
	snapshotMutex      sync.RWMutex
	maxSnapshots       int32 // Max number of snapshots to keep
	monitoringInterval time.Duration
	stopMonitoring     chan struct{}
	monitoringWG       sync.WaitGroup
}

// NewStatsMonitor creates and configures a new StatsMonitor with target thresholds
// based on the maximum active files limit. It sets up analysis parameters including
// consistency requirements and adjustment cooldown periods.
func NewStatsMonitor() *StatsMonitor {
	targetSize := int64(maxActiveFiles)

	return &StatsMonitor{
		// Target thresholds
		targetIndexerSize:      targetSize,
		optimalRangeMin:        targetSize * 80 / 100,
		optimalRangeMax:        targetSize,
		criticalThreshold:      targetSize * 120 / 100,
		underutilizedThreshold: targetSize * 60 / 100,

		// Analysis parameters
		consistencyThreshold: consistencyThreshold,                    // Need 10 consecutive samples
		adjustmentCooldown:   time.Minute * adjustmentCooldownMinutes, // Wait 2 minutes between adjustments

		// Monitoring
		snapshots:          make([]StatsSnapshot, 0, 50),
		maxSnapshots:       50,               // Keep last 50 snapshots
		monitoringInterval: time.Second * 20, // Check every 20 seconds
		stopMonitoring:     make(chan struct{}),
	}
}

// Start begins the monitoring loop in a separate goroutine.
// It starts periodic sampling and analysis of system performance metrics
// to enable dynamic throttling adjustments.
func (sm *StatsMonitor) Start(parentCtx context.Context) {
	// Create child context
	sm.ctx, sm.cancel = context.WithCancel(parentCtx)

	sm.monitoringWG.Add(1)
	go sm.monitoringLoop()

	glcm.Info(fmt.Sprintf(
		"Started monitoring for active throttling (Active files limit: %d, Crawl parallelism: %d)",
		activeFilesLimit.Load(),
		crawlParallelism))
}

// Stop gracefully shuts down the monitoring loop and waits for completion.
// This should be called during cleanup to prevent goroutine leaks.
func (sm *StatsMonitor) Stop() {
	// Cancel context
	if sm.cancel != nil {
		sm.cancel()
	}

	close(sm.stopMonitoring)
	sm.monitoringWG.Wait()
}

// monitoringLoop is the main monitoring routine that runs periodically to:
// 1. Take performance snapshots
// 2. Analyze trends and system state
// 3. Calculate and apply optimal limit adjustments
// It runs until the stop signal is received.
func (sm *StatsMonitor) monitoringLoop() {
	defer sm.monitoringWG.Done()

	ticker := time.NewTicker(sm.monitoringInterval)
	defer ticker.Stop()
	logCounter := 0

	for {
		select {
		case <-sm.ctx.Done():
			// Context cancelled - shutdown gracefully
			return

		case <-ticker.C:
			// Check if context is still valid before processing
			if sm.ctx.Err() != nil {
				return
			}

			snapshot := sm.takeSnapshot()
			sm.addSnapshot(snapshot)
			logCounter++

			// Check for adjustment
			currentLimit := activeFilesLimit.Load()
			newLimit, adjusted := sm.calculateOptimalLimit(currentLimit)
			if adjusted {
				activeFilesLimit.Store(newLimit)
				sm.lastAdjustmentTime = time.Now()
			}
		}
	}
}

// analyzeStats examines recent performance samples to determine the current system state.
// It requires a minimum number of consistent samples before making state determinations
// to avoid oscillating adjustments based on temporary fluctuations.
func (sm *StatsMonitor) analyzeStats(samples []StatsSnapshot) StatsState {
	if len(samples) < sm.consistencyThreshold {
		return StateOptimal // Not enough data
	}

	// Check last N samples for consistency
	recentSamples := samples[len(samples)-sm.consistencyThreshold:]

	// Count samples in each range
	var optimal, underutilized, critical, aboveOptimal, belowOptimal int

	for _, sample := range recentSamples {
		size := sample.IndexerSize

		switch {
		case size >= sm.criticalThreshold:
			critical++
		case size >= sm.optimalRangeMax:
			aboveOptimal++
		case size >= sm.optimalRangeMin:
			optimal++
		case size >= sm.underutilizedThreshold:
			belowOptimal++
		default:
			underutilized++
		}
	}

	// Determine state based on majority of samples
	majority := (sm.consistencyThreshold + 1) / 2

	if critical >= majority {
		return StateCritical
	} else if underutilized >= majority {
		return StateUnderutilized
	} else if aboveOptimal >= majority {
		return StateAboveOptimal
	} else if belowOptimal >= majority {
		return StateBelowOptimal
	} else {
		return StateOptimal
	}
}

// calculateOptimalLimit determines the optimal active files limit based on recent performance data.
// It implements a feedback control system that:
// - Reduces limits when system is overloaded (critical state)
// - Increases limits when system is underutilized
// - Respects cooldown periods to prevent oscillation
// Returns the new limit and whether an adjustment was made.
func (sm *StatsMonitor) calculateOptimalLimit(currentLimit int64) (int64, bool) {
	sm.snapshotMutex.RLock()
	defer sm.snapshotMutex.RUnlock()

	if currentLimit == absoluteMaxActiveFiles {
		return currentLimit, false // Already at absolute max, no adjustment needed
	}

	if len(sm.snapshots) < sm.consistencyThreshold {
		return currentLimit, false // Not enough data to adjust
	}

	// Check cooldown period
	if time.Since(sm.lastAdjustmentTime) < sm.adjustmentCooldown {
		return currentLimit, false // Still in cooldown
	}

	state := sm.analyzeStats(sm.snapshots)

	var newLimit int64
	var reason string

	switch state {
	case StateCritical:
		// > target consistently - REDUCE directories aggressively
		newLimit = int64(float64(currentLimit) * 0.8) // 20% reduction
		reason = fmt.Sprintf(
			"CRITICAL: Indexer size > %dM files - reducing directories",
			sm.criticalThreshold/1000000)

	case StateAboveOptimal:
		// slightly higher than target - slight reduction
		newLimit = int64(float64(currentLimit) * 0.9) // 10% reduction
		reason = "Above optimal range - slight reduction"

	case StateUnderutilized:
		// < target consistently - INCREASE directories aggressively
		newLimit = int64(float64(currentLimit) * 1.3) // 30% increase
		reason = fmt.Sprintf("UNDERUTILIZED: Indexer size < %dM files - increasing directories",
			sm.underutilizedThreshold/1000000)

	case StateBelowOptimal:
		// slightly less than target - slight increase
		newLimit = int64(float64(currentLimit) * 1.1) // 10% increase
		reason = "Below optimal range - slight increase"

	case StateOptimal:
		// 4M - 5M files - maintain current limit
		return currentLimit, false

	default:
		return currentLimit, false
	}

	if newLimit > currentLimit && onlyReduceLimits {
		// If we are only reducing limits, skip increases
		return currentLimit, false
	}

	newLimit = min(newLimit, absoluteMaxActiveFiles) // Cap at absolute max

	if newLimit != currentLimit {
		glcm.Info(fmt.Sprintf("ADJUSTMENT: %s | %d -> %d files",
			reason, currentLimit, newLimit))
	}

	return newLimit, (newLimit != currentLimit)
}

// takeSnapshot captures current system performance metrics including:
// - File indexer size and active directory count
// - Memory usage statistics
// - Utilization percentage relative to target
// Returns a StatsSnapshot for trend analysis.
func (sm *StatsMonitor) takeSnapshot() StatsSnapshot {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	currentIndexerSize := totalFilesInIndexer.Load()
	activeDirs := activeDirectories.Load()

	// Calculate utilization percentage
	utilizationPercent := float64(currentIndexerSize) / float64(sm.targetIndexerSize) * 100

	return StatsSnapshot{
		Timestamp:          time.Now(),
		IndexerSize:        currentIndexerSize,
		ActiveDirectories:  activeDirs,
		MemoryUsageMB:      memStats.HeapInuse / mbToBytesMultiplier,
		UtilizationPercent: utilizationPercent,
	}
}

// addSnapshot adds a new performance snapshot to the monitoring history.
// It maintains a sliding window of recent snapshots by removing old entries
// when the maximum snapshot count is exceeded.
func (sm *StatsMonitor) addSnapshot(snapshot StatsSnapshot) {
	sm.snapshotMutex.Lock()
	defer sm.snapshotMutex.Unlock()

	sm.snapshots = append(sm.snapshots, snapshot)

	if len(sm.snapshots) > int(sm.maxSnapshots) {
		sm.snapshots = sm.snapshots[1:]
	}
}
