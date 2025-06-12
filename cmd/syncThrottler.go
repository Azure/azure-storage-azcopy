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
	"bufio"
	"context"
	"fmt"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	enableThrottling bool = false
	enableDebugLogs  bool = false
)

// --- BEGIN Throttling and Concurrency Configuration ---
var (
	// Core concurrency settings
	CrawlParallelism         int = 4  // Default concurrent directory processors
	MaxConcurrentDirectories int = 50 // Semaphore limit for directory processing

	// Goroutine thresholds
	GoroutineThreshold    int32 = 30000       // Max total goroutines before throttling
	GoroutineLowThreshold int32 = 0.8 * 30000 // Resume threshold (80% of max)

	// Memory thresholds (in MB)
	MemoryLowThresholdMB      uint64 = 2048 // Resume processing (2GB)
	MemoryHighThresholdMB     uint64 = 3072 // Start throttling (3GB)
	MemoryCriticalThresholdMB uint64 = 4096 // Emergency stop (4GB)

	// Monitoring and logging
	MonitorIntervalSecs     int = 10 // Stats logging interval (reduced for better visibility)
	ThrottleLogIntervalSecs int = 5  // How often to log during throttling

	// Throttling control flags
	EnableGoroutineThrottling bool = false
	EnableMemoryThrottling    bool = false
	EnableDirectoryThrottling bool = true
	EnableAdaptiveThrottling  bool = true

	// Adaptive controller weights (must sum to 1.0)
	// These weights will be normalized based on control flags above
	GoroutineWeight float64 = 0.4
	MemoryWeight    float64 = 0.4
	DirectoryWeight float64 = 0.2
)

// --- END Throttling and Concurrency Configuration ---

// Global state variables
var (
	totalGoroutines        atomic.Int32
	activeDirProcessors    atomic.Int32
	waitingForSemaphore    atomic.Int32
	totalDirectoriesQueued atomic.Uint64
	dirSemaphore           chan struct{}
)

// Cached metrics with atomic operations for thread safety
type CachedSystemMetrics struct {
	// Fast-changing metrics (updated every 2 seconds)
	goroutines      atomic.Int32  // Current goroutine count
	activeDirs      atomic.Int32  // Active directory processors
	waitingDirs     atomic.Int32  // Directories waiting for semaphore
	totalQueuedDirs atomic.Uint64 // Total directories queued for processing

	// Expensive metrics (updated every 5 seconds)
	heapAllocMB      atomic.Uint64 // Heap allocated memory in MB
	heapSysMB        atomic.Uint64 // Heap system memory in MB
	gcPauses         atomic.Uint64 // Total GC pause time in milliseconds
	numGC            atomic.Uint32 // Number of GC cycles
	residentMemoryMB atomic.Uint64 // RSS memory from /proc/*/status
	virtualMemoryMB  atomic.Uint64 // Virtual memory from /proc/*/statm

	// Timestamps for freshness checking
	lastExpensiveMetricsUpdate atomic.Int64 // Unix timestamp for expensive metrics
	lastFastMetricsUpdate      atomic.Int64 // Unix timestamp for fast metrics

	mutex sync.RWMutex
}

// SystemMetrics holds current system performance metrics
type SystemMetrics struct {
	Timestamp              time.Time
	Goroutines             int32
	ActiveDirectories      int32
	WaitingDirectories     int32
	TotalQueuedDirectories uint64
	VirtualMemoryMB        uint64
	ResidentMemoryMB       uint64
	CPUUsagePercent        float64
	GCPauses               uint64
	HeapAllocMB            uint64
	HeapSysMB              uint64
	NumGC                  uint32
}

// AdaptiveThrottleController manages multi-metric throttling decisions
type AdaptiveThrottleController struct {
	maxGoroutines int32
	maxMemoryMB   uint64
	maxActiveDirs int32

	// Throttling state
	isThrottling      bool
	throttleStartTime time.Time
	lastLogTime       time.Time

	mutex sync.RWMutex
}

// MetricReliability tracks which metrics are currently working
type MetricReliability struct {
	goroutineMetricsOK bool
	memoryMetricsOK    bool
	lastGoroutineError time.Time
	lastMemoryError    time.Time
	mutex              sync.RWMutex
}

// Memory state tracking for error handling
type memoryMetricsState struct {
	lastValidRSS      uint64
	lastValidVM       uint64
	consecutiveErrors int
	lastErrorTime     time.Time
	mutex             sync.RWMutex
}

var (
	globalMetricsCache = &CachedSystemMetrics{}
	metricReliability  = &MetricReliability{
		goroutineMetricsOK: true,
		memoryMetricsOK:    true,
	}
	memoryState = &memoryMetricsState{}
)

// Constants for update intervals
const (
	FastMetricsUpdateInterval      = 2 * time.Second  // For goroutines + active dirs
	ExpensiveMetricsUpdateInterval = 5 * time.Second  // For memory + GC stats
	StaleDataThreshold             = 10 * time.Second // Consider data stale after this
)

// Optimized memory reading with error caching
var (
	memoryReadCache = struct {
		rss       uint64
		vm        uint64
		lastRead  time.Time
		cacheTime time.Duration
		mutex     sync.RWMutex
	}{
		cacheTime: 1 * time.Second, // Cache for 1 second
	}
)

// NewAdaptiveThrottleController creates a new adaptive throttle controller
func NewAdaptiveThrottleController() *AdaptiveThrottleController {
	return &AdaptiveThrottleController{
		maxGoroutines: GoroutineThreshold,
		maxMemoryMB:   MemoryCriticalThresholdMB,
		maxActiveDirs: int32(MaxConcurrentDirectories),
	}
}

// Lightweight metrics getter - uses cached values
func GetSystemMetricsOptimized() SystemMetrics {
	cache := globalMetricsCache
	cache.mutex.RLock()
	defer cache.mutex.RUnlock()

	now := time.Now()

	// Check data freshness with better variable names
	expensiveMetricsAge := now.Unix() - cache.lastExpensiveMetricsUpdate.Load()
	fastMetricsAge := now.Unix() - cache.lastFastMetricsUpdate.Load()

	metrics := SystemMetrics{
		Timestamp:              now,
		Goroutines:             cache.goroutines.Load(),
		ActiveDirectories:      cache.activeDirs.Load(),
		WaitingDirectories:     waitingForSemaphore.Load(),
		TotalQueuedDirectories: totalDirectoriesQueued.Load(),
		HeapAllocMB:            cache.heapAllocMB.Load(),
		HeapSysMB:              cache.heapSysMB.Load(),
		GCPauses:               cache.gcPauses.Load(),
		NumGC:                  cache.numGC.Load(),
		ResidentMemoryMB:       cache.residentMemoryMB.Load(),
		VirtualMemoryMB:        cache.virtualMemoryMB.Load(),
	}

	// Add data freshness indicators with better descriptions
	if expensiveMetricsAge > int64(StaleDataThreshold.Seconds()) {
		WarnStdoutAndScanningLog(fmt.Sprintf("Warning: Expensive metrics (memory/GC) are %d seconds old", expensiveMetricsAge))
	}
	if fastMetricsAge > int64(StaleDataThreshold.Seconds()) {
		WarnStdoutAndScanningLog(fmt.Sprintf("Warning: Fast metrics (goroutines/dirs) are %d seconds old", fastMetricsAge))
	}

	return metrics
}

// Background goroutine for updating expensive metrics (memory + GC stats)
func updateExpensiveMetricsBackground(ctx context.Context) {
	ticker := time.NewTicker(ExpensiveMetricsUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updateExpensiveMetrics()
		}
	}
}

// Background goroutine for updating fast-changing metrics (goroutines + active directories)
func updateFastMetricsBackground(ctx context.Context) {
	ticker := time.NewTicker(FastMetricsUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updateFastMetrics()
		}
	}
}

// Update expensive metrics: memory stats, GC stats, heap stats
func updateExpensiveMetrics() {
	cache := globalMetricsCache

	// Get runtime memory stats (expensive operation - this rarely fails)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Get system memory info with error handling
	rss, rssErr := getRSSMemoryOptimized()
	vm, vmErr := getTotalVirtualMemoryOptimized()

	memoryState.mutex.Lock()

	// Handle RSS memory reading
	if rssErr != nil {
		memoryState.consecutiveErrors++
		memoryState.lastErrorTime = time.Now()

		// Log error periodically (not every failure to avoid spam)
		if memoryState.consecutiveErrors == 1 || memoryState.consecutiveErrors%10 == 0 {
			WarnStdoutAndScanningLog(fmt.Sprintf(
				"Warning: Failed to read RSS memory (attempt %d): %v. Using last valid value: %dMB",
				memoryState.consecutiveErrors, rssErr, memoryState.lastValidRSS))
		}

		// Use last known good value
		rss = memoryState.lastValidRSS

		// If we've never had a good reading, use heap as fallback
		if rss == 0 {
			rss = m.HeapSys / 1024 / 1024 // Heap system memory as approximation
			WarnStdoutAndScanningLog(fmt.Sprintf(
				"Using heap system memory as RSS fallback: %dMB", rss))
		}

		// Update reliability tracking
		metricReliability.mutex.Lock()
		if metricReliability.memoryMetricsOK {
			WarnStdoutAndScanningLog("Memory metrics failed - switching to directory fallback throttling")
		}
		metricReliability.memoryMetricsOK = false
		metricReliability.lastMemoryError = time.Now()
		metricReliability.mutex.Unlock()
	} else {
		// Successful read - reset error state
		if memoryState.consecutiveErrors > 0 {
			WarnStdoutAndScanningLog(fmt.Sprintf(
				"RSS memory reading recovered after %d failures", memoryState.consecutiveErrors))
			memoryState.consecutiveErrors = 0
		}
		memoryState.lastValidRSS = rss

		// Update reliability tracking
		metricReliability.mutex.Lock()
		if !metricReliability.memoryMetricsOK {
			WarnStdoutAndScanningLog("Memory metrics recovered - re-enabling memory throttling")
		}
		metricReliability.memoryMetricsOK = true
		metricReliability.mutex.Unlock()
	}

	// Handle Virtual Memory reading similarly
	if vmErr != nil {
		if memoryState.lastValidVM == 0 {
			vm = rss * 2 // Simple fallback: assume VM is roughly 2x RSS
		} else {
			vm = memoryState.lastValidVM
		}
	} else {
		memoryState.lastValidVM = vm
	}

	memoryState.mutex.Unlock()

	// Update cache atomically with validated values
	cache.heapAllocMB.Store(m.HeapAlloc / 1024 / 1024)
	cache.heapSysMB.Store(m.HeapSys / 1024 / 1024)
	cache.gcPauses.Store(m.PauseTotalNs / 1000000)
	cache.numGC.Store(m.NumGC)
	cache.residentMemoryMB.Store(rss) // Now guaranteed to be non-zero
	cache.virtualMemoryMB.Store(vm)
	cache.lastExpensiveMetricsUpdate.Store(time.Now().Unix())
}

// Update fast-changing metrics: goroutine count, active directory processors
func updateFastMetrics() {
	cache := globalMetricsCache

	// Goroutine count (this can theoretically fail in extreme cases)
	defer func() {
		if r := recover(); r != nil {
			metricReliability.mutex.Lock()
			if metricReliability.goroutineMetricsOK {
				WarnStdoutAndScanningLog(fmt.Sprintf(
					"Goroutine metrics failed (panic: %v) - switching to directory fallback throttling", r))
			}
			metricReliability.goroutineMetricsOK = false
			metricReliability.lastGoroutineError = time.Now()
			metricReliability.mutex.Unlock()
		}
	}()

	// These are very fast operations (just reading counters)
	current := runtime.NumGoroutine()
	activeDirs := activeDirProcessors.Load()

	metricReliability.mutex.Lock()
	if !metricReliability.goroutineMetricsOK {
		WarnStdoutAndScanningLog("Goroutine metrics recovered - re-enabling goroutine throttling")
	}
	metricReliability.goroutineMetricsOK = true
	metricReliability.mutex.Unlock()

	cache.goroutines.Store(int32(current))
	cache.activeDirs.Store(activeDirs)
	cache.lastFastMetricsUpdate.Store(time.Now().Unix())
}

// Check which metrics are currently reliable
func GetMetricReliability() (goroutineOK, memoryOK bool) {
	metricReliability.mutex.RLock()
	defer metricReliability.mutex.RUnlock()

	// Consider metrics unreliable if they failed recently (within last 30 seconds)
	now := time.Now()
	goroutineRecent := now.Sub(metricReliability.lastGoroutineError) < 30*time.Second
	memoryRecent := now.Sub(metricReliability.lastMemoryError) < 30*time.Second

	goroutineOK = metricReliability.goroutineMetricsOK && !goroutineRecent
	memoryOK = metricReliability.memoryMetricsOK && !memoryRecent

	return goroutineOK, memoryOK
}

// Enhanced memory reading with better error handling
func getRSSMemoryOptimized() (uint64, error) {
	cache := &memoryReadCache
	cache.mutex.RLock()

	// Return cached value if recent enough
	if time.Since(cache.lastRead) < cache.cacheTime {
		rss := cache.rss
		cache.mutex.RUnlock()
		return rss, nil
	}
	cache.mutex.RUnlock()

	// Need to read fresh data
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	// Double-check after acquiring write lock
	if time.Since(cache.lastRead) < cache.cacheTime {
		return cache.rss, nil
	}

	// Read fresh data with retries
	rss, err := getRSSMemoryWithRetry(3) // Try up to 3 times
	if err == nil {
		cache.rss = rss
		cache.lastRead = time.Now()
	} else {
		// Don't update cache on error - keep last good value
		WarnStdoutAndScanningLog(fmt.Sprintf("RSS memory read failed after retries: %v", err))
	}

	return rss, err
}

func getTotalVirtualMemoryOptimized() (uint64, error) {
	cache := &memoryReadCache
	cache.mutex.RLock()

	if time.Since(cache.lastRead) < cache.cacheTime {
		vm := cache.vm
		cache.mutex.RUnlock()
		return vm, nil
	}
	cache.mutex.RUnlock()

	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	if time.Since(cache.lastRead) < cache.cacheTime {
		return cache.vm, nil
	}

	vm, err := getTotalVirtualMemoryWithRetry(3)
	if err == nil {
		cache.vm = vm
		cache.lastRead = time.Now()
	}

	return vm, err
}

// RSS memory reading with retry logic
func getRSSMemoryWithRetry(maxRetries int) (uint64, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		rss, err := getRSSMemoryDirect()
		if err == nil {
			return rss, nil
		}

		lastErr = err

		// Wait before retry (exponential backoff)
		if attempt < maxRetries-1 {
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
		}
	}

	return 0, fmt.Errorf("failed after %d attempts, last error: %v", maxRetries, lastErr)
}

// Virtual memory reading with retry logic
func getTotalVirtualMemoryWithRetry(maxRetries int) (uint64, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		vm, err := getTotalVirtualMemoryDirect()
		if err == nil {
			return vm, nil
		}

		lastErr = err

		if attempt < maxRetries-1 {
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
		}
	}

	return 0, fmt.Errorf("failed after %d attempts, last error: %v", maxRetries, lastErr)
}

// Enhanced /proc reading with better error handling
func getRSSMemoryDirect() (uint64, error) {
	pid := os.Getpid()
	filePath := fmt.Sprintf("/proc/%d/status", pid)

	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open %s: %w", filePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				return 0, fmt.Errorf("unexpected VmRSS line format: %s", line)
			}

			rssKB, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("failed to parse VmRSS value '%s': %w", fields[1], err)
			}

			rssMB := rssKB / 1024
			if rssMB == 0 {
				return 0, fmt.Errorf("RSS memory reading returned 0 MB (raw: %d kB)", rssKB)
			}

			return rssMB, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error scanning %s: %w", filePath, err)
	}

	return 0, fmt.Errorf("VmRSS not found in %s", filePath)
}

func getTotalVirtualMemoryDirect() (uint64, error) {
	// Open /proc/self/statm
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, fmt.Errorf("failed to read /proc/self/statm: %w", err)
	}

	// Parse the first field (total virtual memory pages)
	fields := strings.Fields(string(data))
	if len(fields) < 1 {
		return 0, fmt.Errorf("unexpected format in /proc/self/statm")
	}

	// Convert pages to bytes (assuming 4KB pages)
	pages, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse virtual memory pages: %w", err)
	}

	pageSize := uint64(os.Getpagesize()) // Get system page size
	vmMB := (pages * pageSize) / 1024 / 1024

	if vmMB == 0 {
		return 0, fmt.Errorf("virtual memory reading returned 0 MB")
	}

	return vmMB, nil
}

// Updated throttle controller with fallback logic
func (atc *AdaptiveThrottleController) CalculateThrottleScoreOptimized() (float64, string, SystemMetrics) {
	metrics := GetSystemMetricsOptimized()
	goroutineOK, memoryOK := GetMetricReliability()

	var reasons []string
	var score float64
	var totalActiveWeight float64
	var goroutineComponent, memoryComponent, directoryComponent float64

	// Only use goroutine throttling if metrics are reliable
	if EnableGoroutineThrottling && goroutineOK {
		goroutineRatio := float64(metrics.Goroutines) / float64(atc.maxGoroutines)
		goroutineComponent = goroutineRatio * GoroutineWeight
		totalActiveWeight += GoroutineWeight

		if goroutineRatio > 0.8 {
			reasons = append(reasons, fmt.Sprintf("goroutines=%.1f%% (%d/%d)",
				goroutineRatio*100, metrics.Goroutines, atc.maxGoroutines))
		}
	} else if EnableGoroutineThrottling && !goroutineOK {
		reasons = append(reasons, "goroutine-metrics-failed")
	}

	// Only use memory throttling if metrics are reliable
	if EnableMemoryThrottling && memoryOK {
		memoryRatio := float64(metrics.ResidentMemoryMB) / float64(atc.maxMemoryMB)
		memoryComponent = memoryRatio * MemoryWeight
		totalActiveWeight += MemoryWeight

		if memoryRatio > 0.75 {
			reasons = append(reasons, fmt.Sprintf("memory=%.1f%% (%dMB/%dMB)",
				memoryRatio*100, metrics.ResidentMemoryMB, atc.maxMemoryMB))
		}
	} else if EnableMemoryThrottling && !memoryOK {
		reasons = append(reasons, "memory-metrics-failed")
	}

	// Use directory throttling if enabled, OR as fallback if other metrics failed
	useDirThrottling := EnableDirectoryThrottling ||
		(EnableGoroutineThrottling && !goroutineOK) ||
		(EnableMemoryThrottling && !memoryOK)

	if useDirThrottling {
		dirRatio := float64(metrics.ActiveDirectories) / float64(atc.maxActiveDirs)
		directoryComponent = dirRatio * DirectoryWeight
		totalActiveWeight += DirectoryWeight

		if dirRatio > 0.8 {
			dirStatus := "directories"
			if !goroutineOK || !memoryOK {
				dirStatus = "directories(fallback)"
			}
			reasons = append(reasons, fmt.Sprintf("%s=%.1f%% (%d/%d)",
				dirStatus, dirRatio*100, metrics.ActiveDirectories, atc.maxActiveDirs))
		}
	}

	// Normalize the score based on active weights
	if totalActiveWeight > 0 {
		score = (goroutineComponent + memoryComponent + directoryComponent) / totalActiveWeight
	} else {
		// All throttling disabled/failed - never throttle
		score = 0
		reasons = append(reasons, "all throttling disabled or failed")
	}

	reason := strings.Join(reasons, ", ")
	return score, reason, metrics
}

// Add method to check if memory metrics are reliable
func (atc *AdaptiveThrottleController) AreMemoryMetricsReliable() bool {
	memoryState.mutex.RLock()
	defer memoryState.mutex.RUnlock()

	// Consider unreliable if we've had many consecutive errors recently
	recentErrors := memoryState.consecutiveErrors > 5
	oldErrors := time.Since(memoryState.lastErrorTime) < 30*time.Second

	return !(recentErrors && oldErrors)
}

// Correct hysteresis implementation
func (atc *AdaptiveThrottleController) ShouldThrottle() (bool, string, SystemMetrics) {
	score, reason, metrics := atc.CalculateThrottleScoreOptimized()
	shouldThrottle := EnableAdaptiveThrottling && score > 1.0 // Start at 100%
	return shouldThrottle, reason, metrics
}

func (atc *AdaptiveThrottleController) ContinueThrottle() (bool, string, SystemMetrics) {
	score, reason, metrics := atc.CalculateThrottleScoreOptimized()
	// Continue until score drops to 80% (not score * 0.8 > 100%)
	continueThrottle := EnableAdaptiveThrottling && score > 0.8 // Stop at 80%
	return continueThrottle, reason, metrics
}

// PerformThrottling executes the throttling logic with context cancellation support
func (atc *AdaptiveThrottleController) PerformThrottling(ctx context.Context, dirPath string) error {
	atc.mutex.Lock()
	if !atc.isThrottling {
		atc.isThrottling = true
		atc.throttleStartTime = time.Now()
		atc.lastLogTime = time.Now()
	}
	atc.mutex.Unlock()

	var waitCount int64

	for {
		shouldContinue, reason, metrics := atc.ContinueThrottle() // Already uses optimized version
		if !shouldContinue {
			break
		}

		// Log periodically during throttling
		if time.Since(atc.lastLogTime) >= time.Duration(ThrottleLogIntervalSecs)*time.Second {
			duration := time.Since(atc.throttleStartTime).Round(time.Second)
			WarnStdoutAndScanningLog(fmt.Sprintf(
				"[THROTTLE] Dir: %s | Duration: %v | Reason: %s | Goroutines: %d | Memory: %dMB | ActiveDirs: %d | WaitingDirs: %d",
				dirPath, duration, reason, metrics.Goroutines, metrics.ResidentMemoryMB,
				metrics.ActiveDirectories, metrics.WaitingDirectories))
			atc.lastLogTime = time.Now()
		}

		// Wait with context cancellation support
		select {
		case <-ctx.Done():
			WarnStdoutAndScanningLog(fmt.Sprintf("Throttling cancelled for dir: %s due to context: %v", dirPath, ctx.Err()))
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			waitCount++
		}

		// Force garbage collection every 50 iterations during memory pressure
		// if waitCount%50 == 0 && EnableMemoryThrottling {
		// 	runtime.GC()
		// }
	}

	atc.mutex.Lock()
	if atc.isThrottling {
		duration := time.Since(atc.throttleStartTime).Round(time.Second)
		WarnStdoutAndScanningLog(fmt.Sprintf(
			"[THROTTLE-END] Dir: %s | Total duration: %v | Resumed processing", dirPath, duration))
		atc.isThrottling = false
	}
	atc.mutex.Unlock()

	return nil
}

// Legacy monitoring function for compatibility
func monitorGoroutines(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second) // Fast sampling for goroutine count
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := runtime.NumGoroutine()
			totalGoroutines.Store(int32(current))
		}
	}
}

// Enhanced monitoring function with metric reliability status
func enhancedSyncMonitor(ctx context.Context) {
	WarnStdoutAndScanningLog("Starting Enhanced SyncMonitor with Fallback Logic...")

	ticker := time.NewTicker(time.Duration(MonitorIntervalSecs) * time.Second)
	defer ticker.Stop()

	var lastMetrics SystemMetrics

	for {
		select {
		case <-ctx.Done():
			WarnStdoutAndScanningLog("Enhanced SyncMonitor received termination signal. Exiting...")
			return
		case <-ticker.C:
			metrics := GetSystemMetricsOptimized()
			goroutineOK, memoryOK := GetMetricReliability()

			// Calculate rates if we have previous metrics
			var gcRate, memAllocRate, dirRate string
			if lastMetrics.Timestamp != (time.Time{}) {
				timeDiff := metrics.Timestamp.Sub(lastMetrics.Timestamp).Seconds()
				if timeDiff > 0 {
					gcDiff := metrics.NumGC - lastMetrics.NumGC
					gcRate = fmt.Sprintf(" | GC/s: %.1f", float64(gcDiff)/timeDiff)

					memDiff := int64(metrics.HeapAllocMB) - int64(lastMetrics.HeapAllocMB)
					memAllocRate = fmt.Sprintf(" | MemΔ: %+dMB", memDiff)

					queuedDiff := metrics.TotalQueuedDirectories - lastMetrics.TotalQueuedDirectories
					dirRate = fmt.Sprintf(" | DirRate: %.1f/s", float64(queuedDiff)/timeDiff)
				}
			}

			// Calculate semaphore utilization
			var semaphoreInfo string
			if EnableDirectoryThrottling {
				utilization := float64(metrics.ActiveDirectories) / float64(MaxConcurrentDirectories) * 100
				semaphoreInfo = fmt.Sprintf(" | Semaphore: %d/%d (%.1f%%) | Waiting: %d",
					metrics.ActiveDirectories, MaxConcurrentDirectories, utilization, metrics.WaitingDirectories)
			}

			// Create reliability indicators
			var reliabilityStatus []string
			if !goroutineOK {
				reliabilityStatus = append(reliabilityStatus, "GOR-FAIL")
			}
			if !memoryOK {
				reliabilityStatus = append(reliabilityStatus, "MEM-FAIL")
			}
			if len(reliabilityStatus) > 0 {
				reliabilityStatus = append([]string{"DIR-FALLBACK"}, reliabilityStatus...)
			}

			reliabilityStr := ""
			if len(reliabilityStatus) > 0 {
				reliabilityStr = fmt.Sprintf(" | Status: %s", strings.Join(reliabilityStatus, ","))
			}

			// Format comprehensive status log
			status := fmt.Sprintf(
				"\n[SYNC-MONITOR-FALLBACK] %s | Goroutines: %d | RSS: %dMB | Heap: %dMB/%dMB | GC: %dms%s%s%s%s",
				metrics.Timestamp.Format("15:04:05"),
				metrics.Goroutines,
				metrics.ResidentMemoryMB,
				metrics.HeapAllocMB,
				metrics.HeapSysMB,
				metrics.GCPauses,
				semaphoreInfo,
				dirRate,
				gcRate,
				memAllocRate,
			)

			if len(reliabilityStatus) > 0 {
				status += reliabilityStr
			}

			WarnStdoutAndScanningLog(status)

			// Alert if too many directories are waiting
			if metrics.WaitingDirectories > int32(MaxConcurrentDirectories/2) {
				WarnStdoutAndScanningLog(fmt.Sprintf(
					"[SEMAPHORE-ALERT] High semaphore contention: %d directories waiting for %d slots",
					metrics.WaitingDirectories, MaxConcurrentDirectories))
			}

			lastMetrics = metrics
		}
	}
}

// Add a specific semaphore monitoring function for detailed tracking
func semaphoreMonitor(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second) // More frequent monitoring
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if EnableDirectoryThrottling {
				waiting := waitingForSemaphore.Load()
				active := activeDirProcessors.Load()
				queued := totalDirectoriesQueued.Load()

				// Only log if there's activity or contention
				if waiting > 0 || active > 0 {
					utilization := float64(active) / float64(MaxConcurrentDirectories) * 100
					if enableDebugLogs {
						WarnStdoutAndScanningLog(fmt.Sprintf(
							"[SEMAPHORE-STATUS] Active: %d/%d (%.1f%%) | Waiting: %d | Total Queued: %d",
							active, MaxConcurrentDirectories, utilization, waiting, queued))
					}
				}

				// Alert on high contention
				if waiting > int32(MaxConcurrentDirectories) {
					if enableDebugLogs {
						WarnStdoutAndScanningLog(fmt.Sprintf(
							"[SEMAPHORE-WARNING] Severe contention: %d directories waiting (exceeds semaphore capacity of %d)",
							waiting, MaxConcurrentDirectories))
					}
				}
			}
		}
	}
}

// Diagnostic function to check current throttling strategy
func GetCurrentThrottlingStrategy() string {
	goroutineOK, memoryOK := GetMetricReliability()

	var activeStrategies []string
	var fallbackStrategies []string

	if EnableGoroutineThrottling && goroutineOK {
		activeStrategies = append(activeStrategies, "Goroutine")
	} else if EnableGoroutineThrottling && !goroutineOK {
		fallbackStrategies = append(fallbackStrategies, "Goroutine→Directory")
	}

	if EnableMemoryThrottling && memoryOK {
		activeStrategies = append(activeStrategies, "Memory")
	} else if EnableMemoryThrottling && !memoryOK {
		fallbackStrategies = append(fallbackStrategies, "Memory→Directory")
	}

	if EnableDirectoryThrottling {
		activeStrategies = append(activeStrategies, "Directory")
	}

	strategy := fmt.Sprintf("Active: [%s]", strings.Join(activeStrategies, ", "))
	if len(fallbackStrategies) > 0 {
		strategy += fmt.Sprintf(" | Fallbacks: [%s]", strings.Join(fallbackStrategies, ", "))
	}

	return strategy
}

// Add diagnostics function
func GetMemoryMetricsDiagnostics() string {
	memoryState.mutex.RLock()
	defer memoryState.mutex.RUnlock()

	return fmt.Sprintf(
		"Memory Metrics: LastValidRSS=%dMB, LastValidVM=%dMB, ConsecutiveErrors=%d, LastError=%v",
		memoryState.lastValidRSS, memoryState.lastValidVM,
		memoryState.consecutiveErrors, memoryState.lastErrorTime)
}

// Initialize the background metrics collection
func StartMetricsCollection(ctx context.Context) {
	// Initialize cache with current values
	updateExpensiveMetrics()
	updateFastMetrics()

	// Start background updaters with descriptive names
	go updateExpensiveMetricsBackground(ctx)
	go updateFastMetricsBackground(ctx)
}
