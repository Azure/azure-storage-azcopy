package common

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	"github.com/shirou/gopsutil/v3/process"
)

var GlobalSystemStatsMonitor *SystemStatsMonitor

// CustomStatsCallback defines a function type for collecting custom application-specific metrics
// The callback should return a map of metric names to their values as strings
// Example: map[string]string{"active_transfers": "42", "queued_files": "1234", "error_count": "5"}
type CustomStatsCallback func() map[string]string

// CustomCallbackManager manages multiple custom stats callbacks
type CustomCallbackManager struct {
	callbacks map[string]CustomStatsCallback
	mutex     sync.RWMutex
}

// NewCustomCallbackManager creates a new callback manager
func NewCustomCallbackManager() *CustomCallbackManager {
	return &CustomCallbackManager{
		callbacks: make(map[string]CustomStatsCallback),
	}
}

// RegisterCallback registers a callback with a unique identifier
func (m *CustomCallbackManager) RegisterCallback(id string, callback CustomStatsCallback) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.callbacks[id] = callback
}

// UnregisterCallback removes a callback by its identifier
func (m *CustomCallbackManager) UnregisterCallback(id string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.callbacks, id)
}

// UnregisterAllCallbacks removes all registered callbacks
func (m *CustomCallbackManager) UnregisterAllCallbacks() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.callbacks = make(map[string]CustomStatsCallback)
}

// CollectAllMetrics calls all registered callbacks and combines their results
func (m *CustomCallbackManager) CollectAllMetrics() map[string]string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	result := make(map[string]string)

	for callbackID, callback := range m.callbacks {
		if callback != nil {
			if metrics := callback(); metrics != nil {
				for key, value := range metrics {
					// Prefix metric names with callback ID to avoid conflicts
					prefixedKey := fmt.Sprintf("%s.%s", callbackID, key)
					result[prefixedKey] = value
				}
			}
		}
	}

	return result
}

// GetCallbackIDs returns a list of all registered callback IDs
func (m *CustomCallbackManager) GetCallbackIDs() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	ids := make([]string, 0, len(m.callbacks))
	for id := range m.callbacks {
		ids = append(ids, id)
	}
	return ids
}

// SystemStatsSnapshot represents a point-in-time view of system metrics
type SystemStatsSnapshot struct {
	Timestamp time.Time

	// CPU metrics
	CPUPercent       float64
	LoadAverage1Min  float64
	LoadAverage5Min  float64
	LoadAverage15Min float64

	// Memory metrics
	MemoryPercent     float64
	MemoryUsedMB      uint64
	MemoryAvailableMB uint64
	MemoryFreeMB      uint64
	MemoryTotalMB     uint64

	// Swap metrics
	SwapPercent float64
	SwapUsedMB  uint64
	SwapFreeMB  uint64

	// Disk metrics (for monitored paths)
	DiskMetrics map[string]DiskPathMetrics

	// Overall disk space metrics (filesystem-level)
	DiskSpaceUsedPercent float64
	DiskSpaceFreeMB      uint64
	DiskSpaceTotalMB     uint64

	// Network I/O rates (MB/s)
	NetworkReadMBps  float64
	NetworkWriteMBps float64
	NetworkReadOps   uint64
	NetworkWriteOps  uint64

	// Disk I/O rates (MB/s)
	DiskReadMBps  float64
	DiskWriteMBps float64
	DiskReadOps   uint64
	DiskWriteOps  uint64

	// File descriptor metrics
	OpenFileDescriptors   int64
	MaxFileDescriptors    int64
	FileDescriptorPercent float64

	// Process-specific metrics
	ProcessCPUPercent float64
	ProcessMemoryMB   uint64
	ProcessThreads    int32
	GoRoutinesCount   int64
	GoMemoryMB        uint64
	GoGCCount         uint32

	// Custom application metrics
	CustomMetrics map[string]string
}

// DiskPathMetrics represents folder size metrics for a specific path
type DiskPathMetrics struct {
	Path           string
	FolderSizeMB   uint64 // Actual size of the folder contents
	FileCount      uint64 // Number of files in the folder
	DirectoryCount uint64 // Number of subdirectories in the folder
}

// NetworkIOState tracks network I/O state for rate calculations
type NetworkIOState struct {
	BytesSent   uint64
	BytesRecv   uint64
	PacketsSent uint64
	PacketsRecv uint64
	Timestamp   time.Time
}

// DiskIOState tracks disk I/O state for rate calculations
type DiskIOState struct {
	ReadBytes  uint64
	WriteBytes uint64
	ReadOps    uint64
	WriteOps   uint64
	Timestamp  time.Time
}

// StatsMonitorConfig contains configuration for the stats monitor
type StatsMonitorConfig struct {
	// Monitoring interval
	Interval time.Duration

	// Paths to monitor for disk usage
	MonitorPaths []string

	// Logger for conditional logging (can be nil)
	Logger ILoggerResetable

	// Custom stats callback manager for application-specific metrics (can be nil)
	CustomCallbackManager *CustomCallbackManager

	// Logging conditions
	LogConditions LogConditions
}

// LogConditions defines when to log stats
type LogConditions struct {
	// Always log at this interval (0 = never log automatically)
	LogInterval time.Duration

	// Conditional logging thresholds
	CPUThreshold            float64 // Log if CPU > threshold
	MemoryThreshold         float64 // Log if memory > threshold
	DiskThreshold           float64 // Log if any monitored disk > threshold
	LoadThreshold           float64 // Log if load average > threshold
	NetworkMBpsThreshold    float64 // Log if combined network I/O > threshold
	DiskIOThreshold         float64 // Log if combined disk I/O > threshold
	FileDescriptorThreshold float64 // Log if file descriptor usage > threshold (percentage)
}

// AveragingState tracks accumulated values for averaging fluctuating metrics
// This provides smoother logging values by averaging CPU, Network I/O, Disk I/O,
// and File Descriptor metrics between log intervals instead of showing point-in-time spikes
type AveragingState struct {
	// Accumulated values since last log
	cpuSum              float64
	networkReadMBpsSum  float64
	networkWriteMBpsSum float64
	diskReadMBpsSum     float64
	diskWriteMBpsSum    float64
	fileDescriptorSum   float64

	// Sample count for averaging
	sampleCount int

	// Last reset time
	lastReset time.Time
}

// SystemStatsMonitor monitors system metrics using gopsutil
type SystemStatsMonitor struct {
	config StatsMonitorConfig
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Current snapshot (protected by mutex)
	currentSnapshot *SystemStatsSnapshot
	snapshotMutex   sync.RWMutex

	// State for rate calculations
	lastNetworkIO NetworkIOState
	lastDiskIO    DiskIOState

	// Logging state
	lastLogTime time.Time

	// Averaging state for fluctuating metrics
	averagingState AveragingState
	averagingMutex sync.Mutex

	// Process handle for current process
	currentProcess *process.Process
}

// NewSystemStatsMonitor creates a new system stats monitor
func NewSystemStatsMonitor(config StatsMonitorConfig) (*SystemStatsMonitor, error) {
	// Get current process handle
	currentProc, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return nil, fmt.Errorf("failed to get current process: %w", err)
	}

	// Set default interval if not specified
	if config.Interval == 0 {
		config.Interval = 5 * time.Second
	}

	// Set default monitor paths if not specified
	if len(config.MonitorPaths) == 0 {
		if wd, err := os.Getwd(); err == nil {
			config.MonitorPaths = []string{wd}
		} else {
			config.MonitorPaths = []string{"/"}
		}
	}

	monitor := &SystemStatsMonitor{
		config:         config,
		currentProcess: currentProc,
	}

	return monitor, nil
}

// Start begins monitoring in a background goroutine
func (m *SystemStatsMonitor) Start(parentCtx context.Context) {
	m.ctx, m.cancel = context.WithCancel(parentCtx)

	// Log static system information once at startup
	m.logStaticSystemInfo()

	m.wg.Add(1)
	go m.monitorLoop()
}

// Stop stops the monitoring
func (m *SystemStatsMonitor) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

// GetSnapshot returns the current point-in-time snapshot
func (m *SystemStatsMonitor) GetSnapshot() SystemStatsSnapshot {
	m.snapshotMutex.RLock()
	defer m.snapshotMutex.RUnlock()

	if m.currentSnapshot == nil {
		// Return empty snapshot if monitoring hasn't started
		return SystemStatsSnapshot{
			Timestamp:     time.Now(),
			DiskMetrics:   make(map[string]DiskPathMetrics),
			CustomMetrics: make(map[string]string),
		}
	}

	// Return a copy of the current snapshot
	snapshot := *m.currentSnapshot

	// Deep copy disk metrics map
	snapshot.DiskMetrics = make(map[string]DiskPathMetrics)
	for k, v := range m.currentSnapshot.DiskMetrics {
		snapshot.DiskMetrics[k] = v
	}

	// Deep copy custom metrics map
	snapshot.CustomMetrics = make(map[string]string)
	for k, v := range m.currentSnapshot.CustomMetrics {
		snapshot.CustomMetrics[k] = v
	}

	return snapshot
}

// logStaticSystemInfo logs static system information once at startup
func (m *SystemStatsMonitor) logStaticSystemInfo() {
	if m.config.Logger == nil {
		return
	}

	// Collect static system information
	staticInfo := make([]string, 0)

	// CPU information
	if cpuInfo, err := cpu.Info(); err == nil && len(cpuInfo) > 0 {
		staticInfo = append(staticInfo, fmt.Sprintf("CPU: %s (%d cores)", cpuInfo[0].ModelName, len(cpuInfo)))
	} else {
		staticInfo = append(staticInfo, fmt.Sprintf("CPU: %d cores", runtime.NumCPU()))
	}

	// Memory information
	if vmStat, err := mem.VirtualMemory(); err == nil {
		staticInfo = append(staticInfo, fmt.Sprintf("Memory: %.1fGB total", float64(vmStat.Total)/(1024*1024*1024)))
	}

	// File descriptor limits
	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err == nil {
		staticInfo = append(staticInfo, fmt.Sprintf("FD Limit: %d/%d (soft/hard)", rlimit.Cur, rlimit.Max))
	}

	// Go version and architecture
	staticInfo = append(staticInfo, fmt.Sprintf("Go: %s %s/%s", runtime.Version(), runtime.GOOS, runtime.GOARCH))

	// Process ID
	staticInfo = append(staticInfo, fmt.Sprintf("PID: %d", os.Getpid()))

	// Log the static information on multiple lines for better readability
	m.config.Logger.Log(LogInfo, "=== SYSTEM INFORMATION ===")
	for _, info := range staticInfo {
		m.config.Logger.Log(LogInfo, fmt.Sprintf("• %s", info))
	}

	// Log detailed stats monitoring configuration and methodology
	m.logStatsMonitoringDetails()
}

// logStatsMonitoringDetails logs comprehensive information about stats collection methodology
func (m *SystemStatsMonitor) logStatsMonitoringDetails() {
	if m.config.Logger == nil {
		return
	}

	// Log monitoring configuration
	configMsg := fmt.Sprintf("Stats Monitoring: Collection interval=%v, Log interval=%v, Monitored paths=%v",
		m.config.Interval,
		m.config.LogConditions.LogInterval,
		m.config.MonitorPaths)
	m.config.Logger.Log(LogInfo, configMsg)

	// Log threshold configuration
	thresholdMsg := fmt.Sprintf("Alert Thresholds: CPU=%.1f%%, Memory=%.1f%%, FileDescriptor=%.1f%%, Load=%.2f, NetworkIO=%.1fMB/s, DiskIO=%.1fMB/s",
		m.config.LogConditions.CPUThreshold,
		m.config.LogConditions.MemoryThreshold,
		m.config.LogConditions.FileDescriptorThreshold,
		m.config.LogConditions.LoadThreshold,
		m.config.LogConditions.NetworkMBpsThreshold,
		m.config.LogConditions.DiskIOThreshold)
	m.config.Logger.Log(LogInfo, thresholdMsg)

	// Log detailed methodology explanation
	methodologyLines := []string{
		"=== STATS COLLECTION METHODOLOGY ===",
		"• COLLECTION: System metrics collected every " + m.config.Interval.String() + " via gopsutil library",
		"• AVERAGING: CPU, Network I/O, Disk I/O, and File Descriptors are averaged between log intervals to smooth fluctuations",
		"• POINT-IN-TIME: Memory, Load, Disk Space, Process metrics use latest values (less volatile)",
		"",
		"=== METRIC DETAILS ===",
		"• CPU: System CPU percentage (100ms sample via gopsutil, then averaged)",
		"• Memory: Virtual memory usage percentage and MB used (via /proc/meminfo)",
		"• Load: 1/5/15 minute load averages from OS (already time-averaged by kernel)",
		"• Network I/O: MB/s read/write rates calculated from /proc/net/dev byte counters (averaged)",
		"• Disk I/O: MB/s read/write rates from /sys/block/*/stat across all disks (averaged)",
		"• File Descriptors: Count from /proc/PID/fd directory, limits from getrlimit() (averaged)",
		"• Disk Space: Filesystem usage from statvfs() system call (point-in-time)",
		"• Process: Current process CPU/memory/threads via /proc/PID/stat (point-in-time)",
		"• Go Runtime: Goroutines, heap memory, GC count from runtime package (point-in-time)",
		"• Custom Metrics: Application-specific via registered callback functions",
		"",
		"=== LOGGING BEHAVIOR ===",
		"• Automatic logging every " + fmt.Sprintf("%.0f", m.config.LogConditions.LogInterval.Seconds()) + "s (if configured)",
		"• Threshold-based alerts when metrics exceed configured limits",
		"• Averaged metrics prevent false alarms from temporary spikes",
		"• Log format: CPU:X% Mem:X%(XMB) Load:X.X Net:X.X/X.XMB/s Disk:X.X/X.XMB/s FDs:X(X%) ...",
	}

	for _, line := range methodologyLines {
		m.config.Logger.Log(LogInfo, line)
	}
}

// monitorLoop is the main monitoring loop
func (m *SystemStatsMonitor) monitorLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.Interval)
	defer ticker.Stop()

	// Take initial snapshot
	snapshot := m.collectSnapshot()
	m.updateSnapshot(snapshot)

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			snapshot := m.collectSnapshot()
			m.updateSnapshot(snapshot)

			// Accumulate fluctuating metrics for averaging
			m.accumulateMetricsForAveraging(snapshot)

			m.checkAndLog(snapshot)
		}
	}
}

// collectSnapshot collects all system metrics
func (m *SystemStatsMonitor) collectSnapshot() SystemStatsSnapshot {
	snapshot := SystemStatsSnapshot{
		Timestamp:     time.Now(),
		DiskMetrics:   make(map[string]DiskPathMetrics),
		CustomMetrics: make(map[string]string),
	}

	// Collect CPU metrics
	m.collectCPUMetrics(&snapshot)

	// Collect memory metrics
	m.collectMemoryMetrics(&snapshot)

	// Collect disk metrics for monitored paths
	m.collectDiskMetrics(&snapshot)

	// Collect disk space metrics for filesystems
	m.collectDiskSpaceMetrics(&snapshot)

	// Collect network I/O metrics
	m.collectNetworkMetrics(&snapshot)

	// Collect disk I/O metrics
	m.collectDiskIOMetrics(&snapshot)

	// Collect file descriptor metrics
	m.collectFileDescriptorMetrics(&snapshot)

	// Collect process-specific metrics
	m.collectProcessMetrics(&snapshot)

	// Collect Go runtime metrics
	m.collectGoMetrics(&snapshot)

	// Collect custom application metrics
	m.collectCustomMetrics(&snapshot)

	return snapshot
}

// accumulateMetricsForAveraging accumulates fluctuating metrics for averaging
func (m *SystemStatsMonitor) accumulateMetricsForAveraging(snapshot SystemStatsSnapshot) {
	m.averagingMutex.Lock()
	defer m.averagingMutex.Unlock()

	// Initialize if first sample or reset
	if m.averagingState.lastReset.IsZero() {
		m.averagingState.lastReset = snapshot.Timestamp
	}

	// Accumulate fluctuating metrics
	m.averagingState.cpuSum += snapshot.CPUPercent
	m.averagingState.networkReadMBpsSum += snapshot.NetworkReadMBps
	m.averagingState.networkWriteMBpsSum += snapshot.NetworkWriteMBps
	m.averagingState.diskReadMBpsSum += snapshot.DiskReadMBps
	m.averagingState.diskWriteMBpsSum += snapshot.DiskWriteMBps
	m.averagingState.fileDescriptorSum += float64(snapshot.OpenFileDescriptors)
	m.averagingState.sampleCount++
}

// getAveragedMetrics returns averaged values for fluctuating metrics and resets the accumulator
func (m *SystemStatsMonitor) getAveragedMetrics() (avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors float64) {
	m.averagingMutex.Lock()
	defer m.averagingMutex.Unlock()

	// Calculate averages if we have samples
	if m.averagingState.sampleCount > 0 {
		avgCPU = m.averagingState.cpuSum / float64(m.averagingState.sampleCount)
		avgNetRead = m.averagingState.networkReadMBpsSum / float64(m.averagingState.sampleCount)
		avgNetWrite = m.averagingState.networkWriteMBpsSum / float64(m.averagingState.sampleCount)
		avgDiskRead = m.averagingState.diskReadMBpsSum / float64(m.averagingState.sampleCount)
		avgDiskWrite = m.averagingState.diskWriteMBpsSum / float64(m.averagingState.sampleCount)
		avgFileDescriptors = m.averagingState.fileDescriptorSum / float64(m.averagingState.sampleCount)
	}

	// Reset accumulator for next interval
	m.averagingState = AveragingState{
		lastReset: time.Now(),
	}

	return
}

// GetAveragingStats returns current averaging statistics for debugging
func (m *SystemStatsMonitor) GetAveragingStats() (sampleCount int, duration time.Duration) {
	m.averagingMutex.Lock()
	defer m.averagingMutex.Unlock()

	if !m.averagingState.lastReset.IsZero() {
		duration = time.Since(m.averagingState.lastReset)
	}
	return m.averagingState.sampleCount, duration
}

// collectCPUMetrics collects CPU-related metrics
func (m *SystemStatsMonitor) collectCPUMetrics(snapshot *SystemStatsSnapshot) {
	// CPU percentage (use shorter interval for responsiveness)
	if cpuPercents, err := cpu.Percent(100*time.Millisecond, false); err == nil && len(cpuPercents) > 0 {
		snapshot.CPUPercent = cpuPercents[0]
	}

	// Load averages
	if loadStat, err := load.Avg(); err == nil {
		snapshot.LoadAverage1Min = loadStat.Load1
		snapshot.LoadAverage5Min = loadStat.Load5
		snapshot.LoadAverage15Min = loadStat.Load15
	}
}

// collectMemoryMetrics collects memory-related metrics
func (m *SystemStatsMonitor) collectMemoryMetrics(snapshot *SystemStatsSnapshot) {
	// Virtual memory
	if vmStat, err := mem.VirtualMemory(); err == nil {
		snapshot.MemoryPercent = vmStat.UsedPercent
		snapshot.MemoryUsedMB = vmStat.Used / (1024 * 1024)
		snapshot.MemoryAvailableMB = vmStat.Available / (1024 * 1024)
		snapshot.MemoryFreeMB = vmStat.Free / (1024 * 1024)
		snapshot.MemoryTotalMB = vmStat.Total / (1024 * 1024)
	}

	// Swap memory
	if swapStat, err := mem.SwapMemory(); err == nil {
		snapshot.SwapPercent = swapStat.UsedPercent
		snapshot.SwapUsedMB = swapStat.Used / (1024 * 1024)
		snapshot.SwapFreeMB = swapStat.Free / (1024 * 1024)
	}
}

// collectDiskMetrics collects folder size metrics for monitored paths
func (m *SystemStatsMonitor) collectDiskMetrics(snapshot *SystemStatsSnapshot) {
	for _, path := range m.config.MonitorPaths {
		if folderSize, fileCount, dirCount, err := m.calculateFolderSize(path); err == nil {
			snapshot.DiskMetrics[path] = DiskPathMetrics{
				Path:           path,
				FolderSizeMB:   folderSize / (1024 * 1024),
				FileCount:      fileCount,
				DirectoryCount: dirCount,
			}
		}
	}
}

// collectDiskSpaceMetrics collects overall disk/filesystem space metrics
func (m *SystemStatsMonitor) collectDiskSpaceMetrics(snapshot *SystemStatsSnapshot) {
	// Just get disk space from the first monitored path (or current directory as fallback)
	monitorPath := "."
	if len(m.config.MonitorPaths) > 0 {
		monitorPath = m.config.MonitorPaths[0]
	}

	if diskStat, err := disk.Usage(monitorPath); err == nil {
		snapshot.DiskSpaceUsedPercent = diskStat.UsedPercent
		snapshot.DiskSpaceFreeMB = diskStat.Free / (1024 * 1024)
		snapshot.DiskSpaceTotalMB = diskStat.Total / (1024 * 1024)
	}
}

// calculateFolderSize calculates the total size, file count, and directory count of a folder
func (m *SystemStatsMonitor) calculateFolderSize(folderPath string) (uint64, uint64, uint64, error) {
	var totalSize uint64
	var fileCount uint64
	var dirCount uint64

	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Continue walking even if we can't access some files/directories
			return nil
		}

		if info.IsDir() {
			dirCount++
		} else {
			fileCount++
			totalSize += uint64(info.Size())
		}

		return nil
	})

	return totalSize, fileCount, dirCount, err
}

// collectNetworkMetrics collects network I/O metrics and calculates rates
func (m *SystemStatsMonitor) collectNetworkMetrics(snapshot *SystemStatsSnapshot) {
	if netStats, err := net.IOCounters(false); err == nil && len(netStats) > 0 {
		currentNet := NetworkIOState{
			BytesSent:   netStats[0].BytesSent,
			BytesRecv:   netStats[0].BytesRecv,
			PacketsSent: netStats[0].PacketsSent,
			PacketsRecv: netStats[0].PacketsRecv,
			Timestamp:   snapshot.Timestamp,
		}

		// Calculate rates if we have previous data
		if !m.lastNetworkIO.Timestamp.IsZero() {
			duration := currentNet.Timestamp.Sub(m.lastNetworkIO.Timestamp).Seconds()
			if duration > 0 {
				sentDiff := currentNet.BytesSent - m.lastNetworkIO.BytesSent
				recvDiff := currentNet.BytesRecv - m.lastNetworkIO.BytesRecv
				packetSentDiff := currentNet.PacketsSent - m.lastNetworkIO.PacketsSent
				packetRecvDiff := currentNet.PacketsRecv - m.lastNetworkIO.PacketsRecv

				snapshot.NetworkWriteMBps = float64(sentDiff) / (1024 * 1024) / duration
				snapshot.NetworkReadMBps = float64(recvDiff) / (1024 * 1024) / duration
				snapshot.NetworkWriteOps = packetSentDiff
				snapshot.NetworkReadOps = packetRecvDiff
			}
		}

		m.lastNetworkIO = currentNet
	}
}

// collectDiskIOMetrics collects disk I/O metrics and calculates rates
func (m *SystemStatsMonitor) collectDiskIOMetrics(snapshot *SystemStatsSnapshot) {
	if diskIOStats, err := disk.IOCounters(); err == nil {
		var totalRead, totalWrite, totalReadOps, totalWriteOps uint64

		// Sum across all disks
		for _, ioStat := range diskIOStats {
			totalRead += ioStat.ReadBytes
			totalWrite += ioStat.WriteBytes
			totalReadOps += ioStat.ReadCount
			totalWriteOps += ioStat.WriteCount
		}

		currentDisk := DiskIOState{
			ReadBytes:  totalRead,
			WriteBytes: totalWrite,
			ReadOps:    totalReadOps,
			WriteOps:   totalWriteOps,
			Timestamp:  snapshot.Timestamp,
		}

		// Calculate rates if we have previous data
		if !m.lastDiskIO.Timestamp.IsZero() {
			duration := currentDisk.Timestamp.Sub(m.lastDiskIO.Timestamp).Seconds()
			if duration > 0 {
				readDiff := currentDisk.ReadBytes - m.lastDiskIO.ReadBytes
				writeDiff := currentDisk.WriteBytes - m.lastDiskIO.WriteBytes
				readOpsDiff := currentDisk.ReadOps - m.lastDiskIO.ReadOps
				writeOpsDiff := currentDisk.WriteOps - m.lastDiskIO.WriteOps

				snapshot.DiskReadMBps = float64(readDiff) / (1024 * 1024) / duration
				snapshot.DiskWriteMBps = float64(writeDiff) / (1024 * 1024) / duration
				snapshot.DiskReadOps = readOpsDiff
				snapshot.DiskWriteOps = writeOpsDiff
			}
		}

		m.lastDiskIO = currentDisk
	}
}

// collectFileDescriptorMetrics collects file descriptor usage metrics
func (m *SystemStatsMonitor) collectFileDescriptorMetrics(snapshot *SystemStatsSnapshot) {
	pid := os.Getpid()

	// Count open file descriptors by reading /proc/PID/fd directory
	fdDir := fmt.Sprintf("/proc/%d/fd", pid)
	if files, err := os.ReadDir(fdDir); err == nil {
		snapshot.OpenFileDescriptors = int64(len(files))
	}

	// Get file descriptor limits using syscall
	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err == nil {
		snapshot.MaxFileDescriptors = int64(rlimit.Cur)
		if snapshot.MaxFileDescriptors > 0 {
			snapshot.FileDescriptorPercent = float64(snapshot.OpenFileDescriptors) / float64(snapshot.MaxFileDescriptors) * 100
		}
	}
}

// collectProcessMetrics collects current process metrics
func (m *SystemStatsMonitor) collectProcessMetrics(snapshot *SystemStatsSnapshot) {
	if m.currentProcess != nil {
		// Process memory
		if memInfo, err := m.currentProcess.MemoryInfo(); err == nil {
			snapshot.ProcessMemoryMB = memInfo.RSS / (1024 * 1024)
		}

		// Process CPU (this may block briefly)
		if cpuPercent, err := m.currentProcess.CPUPercent(); err == nil {
			snapshot.ProcessCPUPercent = cpuPercent
		}

		// Thread count
		if numThreads, err := m.currentProcess.NumThreads(); err == nil {
			snapshot.ProcessThreads = numThreads
		}
	}
}

// collectGoMetrics collects Go runtime metrics
func (m *SystemStatsMonitor) collectGoMetrics(snapshot *SystemStatsSnapshot) {
	// Goroutine count
	snapshot.GoRoutinesCount = int64(runtime.NumGoroutine())

	// Memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	snapshot.GoMemoryMB = memStats.HeapInuse / (1024 * 1024)
	snapshot.GoGCCount = memStats.NumGC
}

// collectCustomMetrics collects custom application-specific metrics via callback manager
func (m *SystemStatsMonitor) collectCustomMetrics(snapshot *SystemStatsSnapshot) {
	if m.config.CustomCallbackManager != nil {
		if customStats := m.config.CustomCallbackManager.CollectAllMetrics(); customStats != nil {
			// Copy the custom stats into the snapshot
			for key, value := range customStats {
				snapshot.CustomMetrics[key] = value
			}
		}
	}
}

// updateSnapshot updates the current snapshot thread-safely
func (m *SystemStatsMonitor) updateSnapshot(snapshot SystemStatsSnapshot) {
	m.snapshotMutex.Lock()
	defer m.snapshotMutex.Unlock()
	m.currentSnapshot = &snapshot
}

// checkAndLog checks if logging conditions are met and logs if necessary
func (m *SystemStatsMonitor) checkAndLog(snapshot SystemStatsSnapshot) {
	if m.config.Logger == nil {
		return
	}

	shouldLog := false
	reasons := make([]string, 0)

	// Check time-based logging
	if m.config.LogConditions.LogInterval > 0 {
		if time.Since(m.lastLogTime) >= m.config.LogConditions.LogInterval {
			shouldLog = true
			reasons = append(reasons, "interval")
		}
	}

	// Get current averaged values for threshold checking
	avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors := func() (float64, float64, float64, float64, float64, float64) {
		m.averagingMutex.Lock()
		defer m.averagingMutex.Unlock()
		if m.averagingState.sampleCount > 0 {
			return m.averagingState.cpuSum / float64(m.averagingState.sampleCount),
				m.averagingState.networkReadMBpsSum / float64(m.averagingState.sampleCount),
				m.averagingState.networkWriteMBpsSum / float64(m.averagingState.sampleCount),
				m.averagingState.diskReadMBpsSum / float64(m.averagingState.sampleCount),
				m.averagingState.diskWriteMBpsSum / float64(m.averagingState.sampleCount),
				m.averagingState.fileDescriptorSum / float64(m.averagingState.sampleCount)
		}
		return 0, 0, 0, 0, 0, 0
	}()

	// Check threshold-based logging (use averaged values for fluctuating metrics)
	if m.config.LogConditions.CPUThreshold > 0 && avgCPU > m.config.LogConditions.CPUThreshold {
		shouldLog = true
		reasons = append(reasons, fmt.Sprintf("CPU=%.1f%%", avgCPU))
	}

	if m.config.LogConditions.MemoryThreshold > 0 && snapshot.MemoryPercent > m.config.LogConditions.MemoryThreshold {
		shouldLog = true
		reasons = append(reasons, fmt.Sprintf("Memory=%.1f%%", snapshot.MemoryPercent))
	}

	if m.config.LogConditions.LoadThreshold > 0 && snapshot.LoadAverage1Min > m.config.LogConditions.LoadThreshold {
		shouldLog = true
		reasons = append(reasons, fmt.Sprintf("Load=%.2f", snapshot.LoadAverage1Min))
	}

	// Check disk thresholds (using disk space metrics for percentage-based thresholds)
	if m.config.LogConditions.DiskThreshold > 0 {
		if snapshot.DiskSpaceUsedPercent > m.config.LogConditions.DiskThreshold {
			shouldLog = true
			reasons = append(reasons, fmt.Sprintf("Disk=%.1f%%", snapshot.DiskSpaceUsedPercent))
		}
	}

	// Check file descriptor threshold (use averaged values)
	if m.config.LogConditions.FileDescriptorThreshold > 0 {
		// Calculate averaged file descriptor percentage
		if snapshot.MaxFileDescriptors > 0 {
			avgFileDescriptorPercent := avgFileDescriptors / float64(snapshot.MaxFileDescriptors) * 100
			if avgFileDescriptorPercent > m.config.LogConditions.FileDescriptorThreshold {
				shouldLog = true
				reasons = append(reasons, fmt.Sprintf("FD=%.1f%%", avgFileDescriptorPercent))
			}
		}
	}

	// Check network I/O threshold (use averaged values)
	if m.config.LogConditions.NetworkMBpsThreshold > 0 {
		totalNetworkMBps := avgNetRead + avgNetWrite
		if totalNetworkMBps > m.config.LogConditions.NetworkMBpsThreshold {
			shouldLog = true
			reasons = append(reasons, fmt.Sprintf("NetworkIO=%.1fMB/s", totalNetworkMBps))
		}
	}

	// Check disk I/O threshold (use averaged values)
	if m.config.LogConditions.DiskIOThreshold > 0 {
		totalDiskMBps := avgDiskRead + avgDiskWrite
		if totalDiskMBps > m.config.LogConditions.DiskIOThreshold {
			shouldLog = true
			reasons = append(reasons, fmt.Sprintf("DiskIO=%.1fMB/s", totalDiskMBps))
		}
	}

	if shouldLog {
		m.logSnapshot(snapshot, reasons)
		m.lastLogTime = snapshot.Timestamp
	}
}

// logSnapshot logs the snapshot with the given reasons
func (m *SystemStatsMonitor) logSnapshot(snapshot SystemStatsSnapshot, reasons []string) {
	// Create disk metrics summary (folder sizes) - simplified without file/folder counts
	folderSummary := ""
	// Sort the paths for consistent log output
	paths := make([]string, 0, len(snapshot.DiskMetrics))
	for path := range snapshot.DiskMetrics {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		metrics := snapshot.DiskMetrics[path]
		if folderSummary != "" {
			folderSummary += "; "
		}
		folderSummary += fmt.Sprintf("%s:%.0fMB", path, float64(metrics.FolderSizeMB))
	}

	// Get averaged values for fluctuating metrics
	avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors := m.getAveragedMetrics()

	logMsg := fmt.Sprintf(
		"CPU:%.1f%% Mem:%.1f%%(%.0fMB) Load:%.2f Folders:[%s] DiskSpace:%.1f%%(%.0fMB free) Net:%.1f/%.1fMB/s Disk:%.1f/%.1fMB/s FDs:%.0f(%.1f%%) Proc:%.1f%%(%.0fMB,%dth) Go:%d/%.0fMB",
		avgCPU, // Use averaged CPU instead of snapshot.CPUPercent
		snapshot.MemoryPercent,
		float64(snapshot.MemoryUsedMB),
		snapshot.LoadAverage1Min,
		folderSummary,
		snapshot.DiskSpaceUsedPercent,
		float64(snapshot.DiskSpaceFreeMB),
		avgNetRead,         // Use averaged network read
		avgNetWrite,        // Use averaged network write
		avgDiskRead,        // Use averaged disk read
		avgDiskWrite,       // Use averaged disk write
		avgFileDescriptors, // Use averaged file descriptors
		snapshot.FileDescriptorPercent,
		snapshot.ProcessCPUPercent,
		float64(snapshot.ProcessMemoryMB),
		snapshot.ProcessThreads,
		snapshot.GoRoutinesCount,
		float64(snapshot.GoMemoryMB),
	)

	// Add custom metrics if available
	if len(snapshot.CustomMetrics) > 0 {
		customSummary := ""
		// Sort custom metrics by key for consistent log output
		keys := make([]string, 0, len(snapshot.CustomMetrics))
		for key := range snapshot.CustomMetrics {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value := snapshot.CustomMetrics[key]
			if customSummary != "" {
				customSummary += ","
			}
			customSummary += fmt.Sprintf("%s:%s", key, value)
		}
		logMsg += fmt.Sprintf(" Custom:[%s]", customSummary)
	}

	if len(reasons) > 0 {
		logMsg += fmt.Sprintf(" [%s]", fmt.Sprintf("%v", reasons))
	}

	m.config.Logger.Log(LogInfo, logMsg)
}

// GetFormattedSnapshot returns a human-readable string representation of the current snapshot
func (m *SystemStatsMonitor) GetFormattedSnapshot() string {
	snapshot := m.GetSnapshot()

	folderInfo := ""
	for path, metrics := range snapshot.DiskMetrics {
		if folderInfo != "" {
			folderInfo += ", "
		}
		folderInfo += fmt.Sprintf("%s: %.0fMB (%d files, %d dirs)",
			path, float64(metrics.FolderSizeMB), metrics.FileCount, metrics.DirectoryCount)
	}

	customInfo := ""
	for key, value := range snapshot.CustomMetrics {
		if customInfo != "" {
			customInfo += ", "
		}
		customInfo += fmt.Sprintf("%s: %s", key, value)
	}

	result := fmt.Sprintf(
		"[%s] CPU:%.1f%% Load:%.2f Mem:%.1f%%(%.0fMB) Folders:[%s] DiskSpace:%.1f%%(%.0f/%.0fMB) Net:%.1f/%.1fMB/s Disk:%.1f/%.1fMB/s FDs:%d/%d(%.1f%%) Proc:%.1f%%(%.0fMB,%dth) Go:%d/%.0fMB GC:%d",
		snapshot.Timestamp.Format(time.RFC3339),
		snapshot.CPUPercent,
		snapshot.LoadAverage1Min,
		snapshot.MemoryPercent, float64(snapshot.MemoryUsedMB),
		folderInfo,
		snapshot.DiskSpaceUsedPercent,
		float64(snapshot.DiskSpaceFreeMB),
		float64(snapshot.DiskSpaceTotalMB),
		snapshot.NetworkReadMBps, snapshot.NetworkWriteMBps,
		snapshot.DiskReadMBps, snapshot.DiskWriteMBps,
		snapshot.OpenFileDescriptors,
		snapshot.MaxFileDescriptors,
		snapshot.FileDescriptorPercent,
		snapshot.ProcessCPUPercent, float64(snapshot.ProcessMemoryMB), snapshot.ProcessThreads,
		snapshot.GoRoutinesCount, float64(snapshot.GoMemoryMB), snapshot.GoGCCount,
	)

	if customInfo != "" {
		result += fmt.Sprintf(" Custom:[%s]", customInfo)
	}

	return result
}

// RegisterCustomStatsCallback registers a custom stats callback with a unique identifier
// This allows multiple components to register their own callbacks without conflicts
func (m *SystemStatsMonitor) RegisterCustomStatsCallback(id string, callback CustomStatsCallback) {
	if m.config.CustomCallbackManager == nil {
		m.config.CustomCallbackManager = NewCustomCallbackManager()
	}
	m.config.CustomCallbackManager.RegisterCallback(id, callback)
}

// UnregisterCustomStatsCallback removes a specific custom stats callback by ID
func (m *SystemStatsMonitor) UnregisterCustomStatsCallback(id string) {
	if m.config.CustomCallbackManager != nil {
		m.config.CustomCallbackManager.UnregisterCallback(id)
	}
}

// IsCustomStatsCallbackRegistered checks if a custom stats callback with the given ID is registered
func (m *SystemStatsMonitor) IsCustomStatsCallbackRegistered(id string) bool {
	if m.config.CustomCallbackManager != nil {
		return m.config.CustomCallbackManager.IsCallbackRegistered(id)
	}
	return false
}

// UnregisterAllCustomStatsCallbacks removes all custom stats callbacks
func (m *SystemStatsMonitor) UnregisterAllCustomStatsCallbacks() {
	if m.config.CustomCallbackManager != nil {
		m.config.CustomCallbackManager.UnregisterAllCallbacks()
	}
}

// GetRegisteredCallbackIDs returns a list of all registered callback IDs
func (m *SystemStatsMonitor) GetRegisteredCallbackIDs() []string {
	if m.config.CustomCallbackManager != nil {
		return m.config.CustomCallbackManager.GetCallbackIDs()
	}
	return []string{}
}

// IsCallbackRegistered checks if a callback with the given ID is registered
func (m *CustomCallbackManager) IsCallbackRegistered(id string) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	_, exists := m.callbacks[id]
	return exists
}

// SetCustomStatsCallback is deprecated but kept for backward compatibility
// Use RegisterCustomStatsCallback instead for multi-callback support
func (m *SystemStatsMonitor) SetCustomStatsCallback(callback CustomStatsCallback) {
	m.RegisterCustomStatsCallback("default", callback)
}
