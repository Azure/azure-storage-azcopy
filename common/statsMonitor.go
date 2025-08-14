package common

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
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

// Stats collection feature flags - can be modified to enable/disable specific stats
const (
	// Core system metrics
	EnableCPUStats    = true
	EnableMemoryStats = true
	EnableLoadStats   = true

	// Disk and filesystem metrics
	EnableDiskMetrics    = true
	EnableDiskSpaceStats = true
	EnableDiskIOStats    = true

	// Network metrics
	EnableNetworkIOStats = true
	EnableSocketStats    = true

	// Process and runtime metrics
	EnableFileDescriptorStats = true
	EnableProcessStats        = true
	EnableGoRuntimeStats      = true

	// Custom metrics
	EnableCustomStats = true

	// Static system info logging at startup
	EnableStaticSystemInfo = true

	// Detailed methodology logging
	EnableMethodologyLogging = true
)

// CustomStatsID represents a standardized identifier for custom stats callbacks
// Consumers must define a new value here to register custom stats
type CustomStatsID string

const (
	// CustomStatsIDSTE represents Storage Transfer Engine stats
	STEId CustomStatsID = "ste"

	// CustomStatsIDOrchestrator represents sync orchestrator stats
	SyncOrchestratorId CustomStatsID = "so"

	// CustomStatsIDTreeCrawler represents TreeCrawler stats
	TreeCrawlerId CustomStatsID = "tc"
)

// String returns the string representation of the CustomStatsID
func (id CustomStatsID) String() string {
	return string(id)
}

// CustomStatEntry represents a single key-value pair with preserved order
type CustomStatEntry struct {
	Key   string
	Value string
}

// CustomStatsCallback defines a function type for collecting custom application-specific metrics
// with preserved ordering. The callback should return a slice of CustomStatEntry to maintain order.
//
// Example usage:
//
//	monitor.RegisterCustomStatsCallback(CustomStatsIDOrchestrator, func() []CustomStatEntry {
//	    return []CustomStatEntry{
//	        {"Waiting", "42"},      // This will appear first
//	        {"Active", "15"},       // This will appear second
//	        {"Indexer", "3"},       // This will appear third
//	        {"SrcEnum", "1"},       // This will appear fourth
//	        {"ErrorCount", "0"},    // This will appear last
//	    }
//	})
//
// This ensures your metrics appear in exactly the order you specify.
type CustomStatsCallback func() []CustomStatEntry

// CustomCallbackConfig holds configuration for a custom stats callback
type CustomCallbackConfig struct {
	Callback CustomStatsCallback // Callback function returning ordered metrics
	Interval time.Duration       // Collection interval for this callback (0 = use monitor default)
	LastCall time.Time           // Last time this callback was executed
	LastData []CustomStatEntry   // Cache last collected data for consistent logging
}

// CustomCallbackManager manages multiple custom stats callbacks with individual intervals
type CustomCallbackManager struct {
	callbacks       map[CustomStatsID]*CustomCallbackConfig
	mutex           sync.RWMutex
	defaultInterval time.Duration // Default interval from stats monitor
}

// NewCustomCallbackManager creates a new callback manager
func NewCustomCallbackManager() *CustomCallbackManager {
	return &CustomCallbackManager{
		callbacks: make(map[CustomStatsID]*CustomCallbackConfig),
	}
}

// SetDefaultInterval sets the default collection interval for callbacks
func (m *CustomCallbackManager) SetDefaultInterval(interval time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.defaultInterval = interval
}

// RegisterCallback registers a callback with a unique identifier using default interval
func (m *CustomCallbackManager) RegisterCallback(id CustomStatsID, callback CustomStatsCallback) {
	m.RegisterCallbackWithInterval(id, callback, 0) // 0 = use default interval
}

// RegisterCallbackWithInterval registers a callback with a unique identifier and custom interval
func (m *CustomCallbackManager) RegisterCallbackWithInterval(id CustomStatsID, callback CustomStatsCallback, interval time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.callbacks[id] = &CustomCallbackConfig{
		Callback: callback,
		Interval: interval,
		LastCall: time.Time{},                // Zero time will trigger immediate first call
		LastData: make([]CustomStatEntry, 0), // Initialize empty cache
	}
}

// UnregisterCallback removes a callback by its identifier
func (m *CustomCallbackManager) UnregisterCallback(id CustomStatsID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Collect final metrics before removing the callback
	if config, exists := m.callbacks[id]; exists && config != nil && config.Callback != nil {
		if orderedMetrics := config.Callback(); orderedMetrics != nil {
			// Update cached data with final metrics
			config.LastData = make([]CustomStatEntry, len(orderedMetrics))
			copy(config.LastData, orderedMetrics)
			// Update last call time
			config.LastCall = time.Now()
		}
	}

	delete(m.callbacks, id)
}

// UnregisterAllCallbacks removes all registered callbacks
func (m *CustomCallbackManager) UnregisterAllCallbacks() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Collect final metrics for all callbacks before removing them
	now := time.Now()
	for _, config := range m.callbacks {
		if config != nil && config.Callback != nil {
			if orderedMetrics := config.Callback(); orderedMetrics != nil {
				// Update cached data with final metrics
				config.LastData = make([]CustomStatEntry, len(orderedMetrics))
				copy(config.LastData, orderedMetrics)
				// Update last call time
				config.LastCall = now
			}
		}
	}

	m.callbacks = make(map[CustomStatsID]*CustomCallbackConfig)
}

// CollectAllMetrics calls registered callbacks based on their intervals and combines their results
// Always returns the last known data for each callback to ensure consistent logging
func (m *CustomCallbackManager) CollectAllMetrics() map[string]string {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	result := make(map[string]string)
	now := time.Now()

	for callbackID, config := range m.callbacks {
		if config == nil || config.Callback == nil {
			continue
		}

		// Determine the effective interval for this callback
		effectiveInterval := config.Interval
		if effectiveInterval == 0 {
			effectiveInterval = m.defaultInterval
		}

		// Check if it's time to call this callback
		shouldCall := config.LastCall.IsZero() ||
			(effectiveInterval > 0 && now.Sub(config.LastCall) >= effectiveInterval)

		if shouldCall {
			if orderedMetrics := config.Callback(); orderedMetrics != nil {
				// Update cached data with fresh metrics
				config.LastData = make([]CustomStatEntry, len(orderedMetrics))
				copy(config.LastData, orderedMetrics)
				// Update last call time
				config.LastCall = now
			}
		}

		// Always include the last known data (either fresh or cached)
		for _, entry := range config.LastData {
			// Prefix metric names with callback ID to avoid conflicts
			prefixedKey := fmt.Sprintf("%s.%s", callbackID, entry.Key)
			result[prefixedKey] = entry.Value
		}
	}

	return result
}

// CollectAllOrderedMetrics calls registered callbacks and returns ordered results
// This preserves the order for callbacks that specify ordering
func (m *CustomCallbackManager) CollectAllOrderedMetrics() []CustomStatEntry {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	result := make([]CustomStatEntry, 0)
	now := time.Now()

	// Sort callback IDs for consistent ordering across different callbacks
	callbackIDs := make([]CustomStatsID, 0, len(m.callbacks))
	for id := range m.callbacks {
		callbackIDs = append(callbackIDs, id)
	}
	// Sort by string representation
	sort.Slice(callbackIDs, func(i, j int) bool {
		return callbackIDs[i].String() < callbackIDs[j].String()
	})

	for _, callbackID := range callbackIDs {
		config := m.callbacks[callbackID]
		if config == nil || config.Callback == nil {
			continue
		}

		// Determine the effective interval for this callback
		effectiveInterval := config.Interval
		if effectiveInterval == 0 {
			effectiveInterval = m.defaultInterval
		}

		// Check if it's time to call this callback
		shouldCall := config.LastCall.IsZero() ||
			(effectiveInterval > 0 && now.Sub(config.LastCall) >= effectiveInterval)

		if shouldCall {
			if orderedMetrics := config.Callback(); orderedMetrics != nil {
				// Update cached data with fresh metrics
				config.LastData = make([]CustomStatEntry, len(orderedMetrics))
				copy(config.LastData, orderedMetrics)
				// Update last call time
				config.LastCall = now
			}
		}

		// Always include the last known ordered data with prefixed keys
		for _, entry := range config.LastData {
			prefixedKey := fmt.Sprintf("%s.%s", callbackID, entry.Key)
			result = append(result, CustomStatEntry{
				Key:   prefixedKey,
				Value: entry.Value,
			})
		}
	}

	return result
}

// GetCallbackIDs returns a list of all registered callback IDs as strings
func (m *CustomCallbackManager) GetCallbackIDs() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	ids := make([]string, 0, len(m.callbacks))
	for id := range m.callbacks {
		ids = append(ids, id.String())
	}
	return ids
}

// GetCallbackInfo returns information about registered callbacks including their intervals
func (m *CustomCallbackManager) GetCallbackInfo() map[string]map[string]string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	info := make(map[string]map[string]string)
	for id, config := range m.callbacks {
		if config != nil {
			effectiveInterval := config.Interval
			if effectiveInterval == 0 {
				effectiveInterval = m.defaultInterval
			}

			info[id.String()] = map[string]string{
				"interval":         effectiveInterval.String(),
				"custom_interval":  config.Interval.String(),
				"default_interval": m.defaultInterval.String(),
				"last_call":        config.LastCall.Format(time.RFC3339),
			}
		}
	}
	return info
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

	// Network socket metrics
	OpenSockets            int64
	EstablishedConnections int64
	TimeWaitConnections    int64

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

	// Set default interval for custom callback manager if it exists
	if config.CustomCallbackManager != nil {
		config.CustomCallbackManager.SetDefaultInterval(config.Interval)
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
	if EnableStaticSystemInfo {
		m.logStaticSystemInfo()
	}

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

	// Ephemeral port range
	if startPort, endPort, err := m.getEphemeralPortRange(); err == nil {
		staticInfo = append(staticInfo, fmt.Sprintf("Ephemeral Ports: %d-%d [%d]", startPort, endPort, endPort-startPort+1))
	}

	// Go version and architecture
	staticInfo = append(staticInfo, fmt.Sprintf("Go: %s %s/%s", runtime.Version(), runtime.GOOS, runtime.GOARCH))

	// Process ID
	staticInfo = append(staticInfo, fmt.Sprintf("PID: %d", os.Getpid()))

	// Log detailed stats monitoring configuration and methodology
	if EnableMethodologyLogging {
		m.logStatsMonitoringDetails()
	}

	// Log the static information on multiple lines for better readability
	m.config.Logger.Log(LogInfo, "\n")
	m.config.Logger.Log(LogInfo, "=== SYSTEM INFORMATION ===")
	for _, info := range staticInfo {
		m.config.Logger.Log(LogInfo, fmt.Sprintf("• %s", info))
	}

	// Log environment variables after system information and before periodic stats
	m.logEnvironmentVariables()
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
		"=== ENABLED FEATURES ===",
	}

	// Add enabled feature information
	featureStatus := []string{
		fmt.Sprintf("• CPU Stats: %t", EnableCPUStats),
		fmt.Sprintf("• Memory Stats: %t", EnableMemoryStats),
		fmt.Sprintf("• Load Stats: %t", EnableLoadStats),
		fmt.Sprintf("• Disk Metrics: %t", EnableDiskMetrics),
		fmt.Sprintf("• Disk Space Stats: %t", EnableDiskSpaceStats),
		fmt.Sprintf("• Disk I/O Stats: %t", EnableDiskIOStats),
		fmt.Sprintf("• Network I/O Stats: %t", EnableNetworkIOStats),
		fmt.Sprintf("• Socket Stats: %t", EnableSocketStats),
		fmt.Sprintf("• File Descriptor Stats: %t", EnableFileDescriptorStats),
		fmt.Sprintf("• Process Stats: %t", EnableProcessStats),
		fmt.Sprintf("• Go Runtime Stats: %t", EnableGoRuntimeStats),
		fmt.Sprintf("• Custom Stats: %t", EnableCustomStats),
	}
	methodologyLines = append(methodologyLines, featureStatus...)

	methodologyLines = append(methodologyLines, []string{
		"",
		"=== METRIC DETAILS ===",
		"• CPU: System CPU percentage (100ms sample via gopsutil, then averaged)",
		"• Memory: Virtual memory usage percentage and MB used (via /proc/meminfo)",
		"• Load: 1/5/15 minute load averages from OS (already time-averaged by kernel)",
		"• Network I/O: MB/s read/write rates calculated from /proc/net/dev byte counters (averaged)",
		"• Disk I/O: MB/s read/write rates from /sys/block/*/stat across all disks (averaged)",
		"• File Descriptors: Count from /proc/PID/fd directory, limits from getrlimit() (averaged)",
		"• Sockets: Open connections via lsof/ss commands (established, TIME_WAIT states)",
		"• Disk Space: Filesystem usage from statvfs() system call (point-in-time)",
		"• Process: Current process CPU/memory/threads via /proc/PID/stat (point-in-time)",
		"• Go Runtime: Goroutines, heap memory, GC count from runtime package (point-in-time)",
		"• Custom Metrics: Application-specific via registered callback functions (configurable intervals)",
		"",
		"=== LOGGING BEHAVIOR ===",
		"• Automatic logging every " + fmt.Sprintf("%.0f", m.config.LogConditions.LogInterval.Seconds()) + "s (if configured)",
		"• Threshold-based alerts when metrics exceed configured limits",
		"• Averaged metrics prevent false alarms from temporary spikes",
		"• Dynamic log format based on enabled features",
		"• Log format: CPU:X% Mem:X%(XMB) Load:X.X Net:X.X/X.XMB/s Disk:X.X/X.XMB/s FDs:X(X%) Sockets:X(ESTAB:X,TIME-WAIT:X) ...",
	}...)

	for _, line := range methodologyLines {
		m.config.Logger.Log(LogInfo, line)
	}
}

// logEnvironmentVariables logs relevant AzCopy environment variables and their current values
func (m *SystemStatsMonitor) logEnvironmentVariables() {
	if m.config.Logger == nil {
		return
	}

	m.config.Logger.Log(LogInfo, "\n")
	m.config.Logger.Log(LogInfo, "=== ENVIRONMENT VARIABLES ===")

	envVars := []EnvironmentVariable{
		EEnvironmentVariable.LogLocation(),
		EEnvironmentVariable.JobPlanLocation(),
		EEnvironmentVariable.ConcurrencyValue(),
		EEnvironmentVariable.TransferInitiationPoolSize(),
		EEnvironmentVariable.EnumerationPoolSize(),
		EEnvironmentVariable.DisableHierarchicalScanning(),
		EEnvironmentVariable.ParallelStatFiles(),
		EEnvironmentVariable.AutoTuneToCpu(),
		EEnvironmentVariable.UserAgentPrefix(),
	}

	// Collect set environment variables
	setVars := make([]string, 0)

	// Check AzCopy-specific environment variables
	for _, envVar := range envVars {
		if envVar.Hidden {
			continue // Skip hidden/secret variables
		}

		value := GetEnvironmentVariable(envVar)
		if value != "" {
			setVars = append(setVars, fmt.Sprintf("%s=%s", envVar.Name, value))
		}
	}

	// Log the results
	if len(setVars) > 0 {
		m.config.Logger.Log(LogInfo, "• Set Environment Variables:")
		for _, varInfo := range setVars {
			m.config.Logger.Log(LogInfo, fmt.Sprintf("  - %s", varInfo))
		}
	} else {
		m.config.Logger.Log(LogInfo, "• No relevant environment variables are set with non-default values")
	}

	m.config.Logger.Log(LogInfo, "")
}

// monitorLoop is the main monitoring loop
func (m *SystemStatsMonitor) monitorLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.Interval)
	defer ticker.Stop()

	// Take initial snapshot
	snapshot := m.collectSnapshot()
	m.updateSnapshot(snapshot)

	m.config.Logger.Log(LogInfo, "\n")

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
	if EnableCPUStats || EnableLoadStats {
		m.collectCPUMetrics(&snapshot)
	}

	// Collect memory metrics
	if EnableMemoryStats {
		m.collectMemoryMetrics(&snapshot)
	}

	// Collect disk metrics for monitored paths
	if EnableDiskMetrics {
		m.collectDiskMetrics(&snapshot)
	}

	// Collect disk space metrics for filesystems
	if EnableDiskSpaceStats {
		m.collectDiskSpaceMetrics(&snapshot)
	}

	// Collect network I/O metrics
	if EnableNetworkIOStats {
		m.collectNetworkMetrics(&snapshot)
	}

	// Collect disk I/O metrics
	if EnableDiskIOStats {
		m.collectDiskIOMetrics(&snapshot)
	}

	// Collect file descriptor metrics
	if EnableFileDescriptorStats {
		m.collectFileDescriptorMetrics(&snapshot)
	}

	// Collect network socket metrics
	if EnableSocketStats {
		m.collectSocketMetrics(&snapshot)
	}

	// Collect process-specific metrics
	if EnableProcessStats {
		m.collectProcessMetrics(&snapshot)
	}

	// Collect Go runtime metrics
	if EnableGoRuntimeStats {
		m.collectGoMetrics(&snapshot)
	}

	// Collect custom application metrics
	if EnableCustomStats {
		m.collectCustomMetrics(&snapshot)
	}

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

	// Accumulate fluctuating metrics only if enabled
	if EnableCPUStats {
		m.averagingState.cpuSum += snapshot.CPUPercent
	}
	if EnableNetworkIOStats {
		m.averagingState.networkReadMBpsSum += snapshot.NetworkReadMBps
		m.averagingState.networkWriteMBpsSum += snapshot.NetworkWriteMBps
	}
	if EnableDiskIOStats {
		m.averagingState.diskReadMBpsSum += snapshot.DiskReadMBps
		m.averagingState.diskWriteMBpsSum += snapshot.DiskWriteMBps
	}
	if EnableFileDescriptorStats {
		m.averagingState.fileDescriptorSum += float64(snapshot.OpenFileDescriptors)
	}
	m.averagingState.sampleCount++
}

// getAveragedMetrics returns averaged values for fluctuating metrics and resets the accumulator
func (m *SystemStatsMonitor) getAveragedMetrics() (avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors float64) {
	m.averagingMutex.Lock()
	defer m.averagingMutex.Unlock()

	// Calculate averages if we have samples, but only for enabled metrics
	if m.averagingState.sampleCount > 0 {
		if EnableCPUStats {
			avgCPU = m.averagingState.cpuSum / float64(m.averagingState.sampleCount)
		}
		if EnableNetworkIOStats {
			avgNetRead = m.averagingState.networkReadMBpsSum / float64(m.averagingState.sampleCount)
			avgNetWrite = m.averagingState.networkWriteMBpsSum / float64(m.averagingState.sampleCount)
		}
		if EnableDiskIOStats {
			avgDiskRead = m.averagingState.diskReadMBpsSum / float64(m.averagingState.sampleCount)
			avgDiskWrite = m.averagingState.diskWriteMBpsSum / float64(m.averagingState.sampleCount)
		}
		if EnableFileDescriptorStats {
			avgFileDescriptors = m.averagingState.fileDescriptorSum / float64(m.averagingState.sampleCount)
		}
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
	if EnableCPUStats {
		if cpuPercents, err := cpu.Percent(100*time.Millisecond, false); err == nil && len(cpuPercents) > 0 {
			snapshot.CPUPercent = cpuPercents[0]
		}
	}

	// Load averages
	if EnableLoadStats {
		if loadStat, err := load.Avg(); err == nil {
			snapshot.LoadAverage1Min = loadStat.Load1
			snapshot.LoadAverage5Min = loadStat.Load5
			snapshot.LoadAverage15Min = loadStat.Load15
		}
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

// countSocketsByState counts sockets in a specific state using 'ss' command
func (m *SystemStatsMonitor) countSocketsByState(state string) int64 {
	cmd := exec.Command("ss", "-tan")
	out, err := cmd.Output()
	if err != nil {
		return 0
	}

	count := int64(0)
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, state) {
			count++
		}
	}
	return count
}

// countOpenSockets counts total open sockets using 'lsof' command
func (m *SystemStatsMonitor) countOpenSockets() int64 {
	cmd := exec.Command("lsof", "-i", "-nP")
	out, err := cmd.Output()
	if err != nil {
		return 0
	}
	lines := strings.Split(string(out), "\n")
	count := int64(len(lines) - 1) // subtract header
	if count < 0 {
		count = 0
	}
	return count
}

// getEphemeralPortRange reads the ephemeral port range from /proc/sys/net/ipv4/ip_local_port_range
func (m *SystemStatsMonitor) getEphemeralPortRange() (int, int, error) {
	data, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		return 0, 0, err
	}
	parts := strings.Fields(string(data))
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("unexpected format")
	}
	start, err1 := strconv.Atoi(parts[0])
	end, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return 0, 0, fmt.Errorf("failed to parse port range")
	}
	return start, end, nil
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

// collectSocketMetrics collects network socket usage metrics
func (m *SystemStatsMonitor) collectSocketMetrics(snapshot *SystemStatsSnapshot) {
	// Count open sockets (may fail on systems without lsof)
	snapshot.OpenSockets = m.countOpenSockets()

	// Count established connections (may fail on systems without ss)
	snapshot.EstablishedConnections = m.countSocketsByState("ESTAB")

	// Count TIME_WAIT connections (may fail on systems without ss)
	snapshot.TimeWaitConnections = m.countSocketsByState("TIME-WAIT")
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
	if EnableCPUStats && m.config.LogConditions.CPUThreshold > 0 && avgCPU > m.config.LogConditions.CPUThreshold {
		shouldLog = true
		reasons = append(reasons, fmt.Sprintf("CPU=%.1f%%", avgCPU))
	}

	if EnableMemoryStats && m.config.LogConditions.MemoryThreshold > 0 && snapshot.MemoryPercent > m.config.LogConditions.MemoryThreshold {
		shouldLog = true
		reasons = append(reasons, fmt.Sprintf("Memory=%.1f%%", snapshot.MemoryPercent))
	}

	if EnableLoadStats && m.config.LogConditions.LoadThreshold > 0 && snapshot.LoadAverage1Min > m.config.LogConditions.LoadThreshold {
		shouldLog = true
		reasons = append(reasons, fmt.Sprintf("Load=%.2f", snapshot.LoadAverage1Min))
	}

	// Check disk thresholds (using disk space metrics for percentage-based thresholds)
	if EnableDiskSpaceStats && m.config.LogConditions.DiskThreshold > 0 {
		if snapshot.DiskSpaceUsedPercent > m.config.LogConditions.DiskThreshold {
			shouldLog = true
			reasons = append(reasons, fmt.Sprintf("Disk=%.1f%%", snapshot.DiskSpaceUsedPercent))
		}
	}

	// Check file descriptor threshold (use averaged values)
	if EnableFileDescriptorStats && m.config.LogConditions.FileDescriptorThreshold > 0 {
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
	if EnableNetworkIOStats && m.config.LogConditions.NetworkMBpsThreshold > 0 {
		totalNetworkMBps := avgNetRead + avgNetWrite
		if totalNetworkMBps > m.config.LogConditions.NetworkMBpsThreshold {
			shouldLog = true
			reasons = append(reasons, fmt.Sprintf("NetworkIO=%.1fMB/s", totalNetworkMBps))
		}
	}

	// Check disk I/O threshold (use averaged values)
	if EnableDiskIOStats && m.config.LogConditions.DiskIOThreshold > 0 {
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

// getReadableSystemStats returns a multi-line, human-readable system stats summary
func (m *SystemStatsMonitor) getReadableSystemStats(snapshot SystemStatsSnapshot, avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors float64, orderedCustomMetrics []CustomStatEntry) []string {
	stats := make([]string, 0)
	stats = append(stats, "=== SYSTEM PERFORMANCE SNAPSHOT ===")

	// Timestamp and triggers
	triggerInfo := fmt.Sprintf("Timestamp: %s", snapshot.Timestamp.Format("2006-01-02 15:04:05"))
	stats = append(stats, triggerInfo)

	// CPU and Load metrics
	if EnableCPUStats || EnableLoadStats {
		cpuLine := "• CPU Performance:"
		if EnableCPUStats {
			cpuLine += fmt.Sprintf(" Usage=%.1f%% (averaged)", avgCPU)
		}
		if EnableLoadStats {
			cpuLine += fmt.Sprintf(" Load=%.2f/%.2f/%.2f (1m/5m/15m)",
				snapshot.LoadAverage1Min, snapshot.LoadAverage5Min, snapshot.LoadAverage15Min)
		}
		stats = append(stats, cpuLine)
	}

	// Memory metrics
	if EnableMemoryStats {
		memLine := fmt.Sprintf("• Memory Usage: %.1f%% (%.0f MB used, %.0f MB available, %.0f MB total)",
			snapshot.MemoryPercent,
			float64(snapshot.MemoryUsedMB),
			float64(snapshot.MemoryAvailableMB),
			float64(snapshot.MemoryTotalMB))
		if snapshot.SwapPercent > 0 {
			memLine += fmt.Sprintf(" | Swap: %.1f%% (%.0f MB used)",
				snapshot.SwapPercent, float64(snapshot.SwapUsedMB))
		}
		stats = append(stats, memLine)
	}

	// Disk space metrics
	if EnableDiskSpaceStats {
		diskLine := fmt.Sprintf("• Disk Space: %.1f%% used (%.0f MB free of %.0f MB total)",
			snapshot.DiskSpaceUsedPercent,
			float64(snapshot.DiskSpaceFreeMB),
			float64(snapshot.DiskSpaceTotalMB))
		stats = append(stats, diskLine)
	}

	// Folder metrics
	if EnableDiskMetrics && len(snapshot.DiskMetrics) > 0 {
		folderLine := "• Monitored Folders:"
		// Sort paths for consistent output
		paths := make([]string, 0, len(snapshot.DiskMetrics))
		for path := range snapshot.DiskMetrics {
			paths = append(paths, path)
		}
		sort.Strings(paths)

		for i, path := range paths {
			metrics := snapshot.DiskMetrics[path]
			if i == 0 {
				folderLine += fmt.Sprintf(" %s=%.0fMB (%d files, %d dirs)",
					path, float64(metrics.FolderSizeMB), metrics.FileCount, metrics.DirectoryCount)
			} else {
				folderLine += fmt.Sprintf(" | %s=%.0fMB (%d files, %d dirs)",
					path, float64(metrics.FolderSizeMB), metrics.FileCount, metrics.DirectoryCount)
			}
		}
		stats = append(stats, folderLine)
	}

	// I/O metrics
	if EnableNetworkIOStats || EnableDiskIOStats {
		ioLine := "• I/O Performance:"
		if EnableNetworkIOStats {
			ioLine += fmt.Sprintf(" Network=%.1f/%.1f MB/s (read/write, averaged)",
				avgNetRead, avgNetWrite)
		}
		if EnableDiskIOStats {
			if EnableNetworkIOStats {
				ioLine += " |"
			}
			ioLine += fmt.Sprintf(" Disk=%.1f/%.1f MB/s (read/write, averaged)",
				avgDiskRead, avgDiskWrite)
		}
		stats = append(stats, ioLine)
	}

	// System resources
	if EnableFileDescriptorStats || EnableSocketStats {
		resLine := "• System Resources:"
		if EnableFileDescriptorStats {
			resLine += fmt.Sprintf(" File Descriptors=%.0f/%.0f (%.1f%% used, averaged)",
				avgFileDescriptors, float64(snapshot.MaxFileDescriptors), snapshot.FileDescriptorPercent)
		}
		if EnableSocketStats {
			if EnableFileDescriptorStats {
				resLine += " |"
			}
			resLine += fmt.Sprintf(" Sockets=%d total (%d established, %d time-wait)",
				snapshot.OpenSockets, snapshot.EstablishedConnections, snapshot.TimeWaitConnections)
		}
		stats = append(stats, resLine)
	}

	// Process metrics
	if EnableProcessStats || EnableGoRuntimeStats {
		procLine := "• Process Metrics:"
		if EnableProcessStats {
			procLine += fmt.Sprintf(" Current Process=%.1f%% CPU, %.0f MB memory, %d threads",
				snapshot.ProcessCPUPercent, float64(snapshot.ProcessMemoryMB), snapshot.ProcessThreads)
		}
		if EnableGoRuntimeStats {
			if EnableProcessStats {
				procLine += " |"
			}
			procLine += fmt.Sprintf(" Go Runtime=%d goroutines, %.0f MB heap, %d GC cycles",
				snapshot.GoRoutinesCount, float64(snapshot.GoMemoryMB), snapshot.GoGCCount)
		}
		stats = append(stats, procLine)
	}

	// Custom metrics - use ordered metrics if provided, otherwise fall back to map-based approach
	if EnableCustomStats {
		if len(orderedCustomMetrics) > 0 {
			customLine := "• Custom Application Metrics:"
			for i, entry := range orderedCustomMetrics {
				if i == 0 {
					customLine += fmt.Sprintf(" %s=%s", entry.Key, entry.Value)
				} else {
					customLine += fmt.Sprintf(" | %s=%s", entry.Key, entry.Value)
				}
			}
			stats = append(stats, customLine)
		} else if len(snapshot.CustomMetrics) > 0 {
			customLine := "• Custom Application Metrics:"
			// Sort custom metrics by key for consistent output
			keys := make([]string, 0, len(snapshot.CustomMetrics))
			for key := range snapshot.CustomMetrics {
				keys = append(keys, key)
			}
			sort.Strings(keys)

			for i, key := range keys {
				value := snapshot.CustomMetrics[key]
				if i == 0 {
					customLine += fmt.Sprintf(" %s=%s", key, value)
				} else {
					customLine += fmt.Sprintf(" | %s=%s", key, value)
				}
			}
			stats = append(stats, customLine)
		}
	}

	return stats
}

// logSnapshot logs the snapshot with the given reasons
func (m *SystemStatsMonitor) logSnapshot(snapshot SystemStatsSnapshot, reasons []string) {
	// Get averaged values for fluctuating metrics
	avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors := m.getAveragedMetrics()

	// Get ordered custom metrics if available
	var orderedCustomMetrics []CustomStatEntry
	if m.config.CustomCallbackManager != nil {
		orderedCustomMetrics = m.config.CustomCallbackManager.CollectAllOrderedMetrics()
	}

	// Log only the compact stats line (single line as before)
	m.config.Logger.Log(LogInfo, m.buildCompactStatsLine(snapshot, avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors, orderedCustomMetrics, reasons))
}

// buildCompactStatsLine builds the original compact stats line for quick reference
func (m *SystemStatsMonitor) buildCompactStatsLine(snapshot SystemStatsSnapshot, avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors float64, orderedCustomMetrics []CustomStatEntry, reasons []string) string {
	// Create disk metrics summary (folder sizes) - simplified without file/folder counts
	folderSummary := ""
	if EnableDiskMetrics {
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
	}

	// Build log message dynamically based on enabled features
	logParts := make([]string, 0)

	// CPU and Memory (core metrics)
	if EnableCPUStats {
		logParts = append(logParts, fmt.Sprintf("CPU:%.1f%%", avgCPU))
	}
	if EnableMemoryStats {
		logParts = append(logParts, fmt.Sprintf("Mem:%.1f%%(%.0fMB)", snapshot.MemoryPercent, float64(snapshot.MemoryUsedMB)))
	}
	if EnableLoadStats {
		logParts = append(logParts, fmt.Sprintf("Load:%.2f", snapshot.LoadAverage1Min))
	}

	// Disk metrics
	if EnableDiskMetrics && folderSummary != "" {
		logParts = append(logParts, fmt.Sprintf("Dirs:[%s]", folderSummary))
	}
	if EnableDiskSpaceStats {
		logParts = append(logParts, fmt.Sprintf("DiskSpace:%.1f%%(%.0fMB free)", snapshot.DiskSpaceUsedPercent, float64(snapshot.DiskSpaceFreeMB)))
	}

	// Network and I/O metrics
	if EnableNetworkIOStats {
		logParts = append(logParts, fmt.Sprintf("Net:%.1f/%.1fMB/s", avgNetRead, avgNetWrite))
	}
	if EnableDiskIOStats {
		logParts = append(logParts, fmt.Sprintf("Disk:%.1f/%.1fMB/s", avgDiskRead, avgDiskWrite))
	}

	// File descriptors and sockets
	if EnableFileDescriptorStats {
		logParts = append(logParts, fmt.Sprintf("FDs:%.0f(%.1f%%)", avgFileDescriptors, snapshot.FileDescriptorPercent))
	}
	if EnableSocketStats {
		logParts = append(logParts, fmt.Sprintf("Soc:%d(ESTAB:%d,TIME-WAIT:%d)", snapshot.OpenSockets, snapshot.EstablishedConnections, snapshot.TimeWaitConnections))
	}

	// Process and runtime metrics
	if EnableProcessStats {
		logParts = append(logParts, fmt.Sprintf("Proc:%.1f%%(%.0fMB,%dth)", snapshot.ProcessCPUPercent, float64(snapshot.ProcessMemoryMB), snapshot.ProcessThreads))
	}
	if EnableGoRuntimeStats {
		logParts = append(logParts, fmt.Sprintf("Go:%d/%.0fMB", snapshot.GoRoutinesCount, float64(snapshot.GoMemoryMB)))
	}

	// Join all enabled parts
	logMsg := strings.Join(logParts, " ")

	// Add custom metrics if available and enabled
	if EnableCustomStats && len(orderedCustomMetrics) > 0 {
		customSummary := ""
		// Use ordered custom metrics to preserve callback-specified order
		for _, entry := range orderedCustomMetrics {
			if customSummary != "" {
				customSummary += ","
			}
			customSummary += fmt.Sprintf("%s:%s", entry.Key, entry.Value)
		}
		logMsg += fmt.Sprintf(" Custom:[%s]", customSummary)
	}

	if len(reasons) > 0 {
		logMsg += fmt.Sprintf(" [%s]", fmt.Sprintf("%v", reasons))
	}

	return logMsg
}

// GetFormattedSnapshot returns a human-readable string representation of the current snapshot
func (m *SystemStatsMonitor) GetFormattedSnapshot() string {
	snapshot := m.GetSnapshot()

	// Get current values (no averaging needed for on-demand snapshot)
	avgCPU := snapshot.CPUPercent
	avgNetRead := snapshot.NetworkReadMBps
	avgNetWrite := snapshot.NetworkWriteMBps
	avgDiskRead := snapshot.DiskReadMBps
	avgDiskWrite := snapshot.DiskWriteMBps
	avgFileDescriptors := float64(snapshot.OpenFileDescriptors)

	// Get ordered custom metrics if available
	var orderedCustomMetrics []CustomStatEntry
	if m.config.CustomCallbackManager != nil {
		orderedCustomMetrics = m.config.CustomCallbackManager.CollectAllOrderedMetrics()
	}

	// Get readable stats
	readableStats := m.getReadableSystemStats(snapshot, avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors, orderedCustomMetrics)

	// Also include the compact summary for completeness
	compactSummary := m.buildCompactStatsLine(snapshot, avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors, orderedCustomMetrics, nil)

	// Combine readable stats with compact summary
	result := strings.Join(readableStats, "\n")
	result += "\n• " + compactSummary

	return result
}

// RegisterCustomStatsCallback registers a custom stats callback with a unique identifier
// This allows multiple components to register their own callbacks without conflicts
func (m *SystemStatsMonitor) RegisterCustomStatsCallback(id CustomStatsID, callback CustomStatsCallback) {
	if m.config.CustomCallbackManager == nil {
		m.config.CustomCallbackManager = NewCustomCallbackManager()
		m.config.CustomCallbackManager.SetDefaultInterval(m.config.Interval)
	}
	m.config.CustomCallbackManager.RegisterCallback(id, callback)

	// Log the initial data at registration time
	if m.config.Logger != nil {
		if initialData := callback(); len(initialData) > 0 {
			// Get current system snapshot for logging
			snapshot := m.GetSnapshot()

			// Add custom metrics to the snapshot
			for _, entry := range initialData {
				prefixedKey := fmt.Sprintf("%s.%s", id.String(), entry.Key)
				snapshot.CustomMetrics[prefixedKey] = entry.Value
			}

			// Create ordered custom metrics
			orderedCustomMetrics := make([]CustomStatEntry, len(initialData))
			for i, entry := range initialData {
				orderedCustomMetrics[i] = CustomStatEntry{
					Key:   fmt.Sprintf("%s.%s", id.String(), entry.Key),
					Value: entry.Value,
				}
			}

			// Get current averaged values for consistent logging format
			avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors := m.getAveragedMetrics()

			// Log using regular compact stats line with start trigger
			reasons := []string{fmt.Sprintf("start-%s", id.String())}
			logLine := m.buildCompactStatsLine(snapshot, avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors, orderedCustomMetrics, reasons)
			m.config.Logger.Log(LogInfo, logLine)
		}
	}
}

// RegisterCustomStatsCallbackWithInterval registers a custom stats callback with a unique identifier and custom interval
// This allows callbacks to be collected at different frequencies than the main stats monitor
func (m *SystemStatsMonitor) RegisterCustomStatsCallbackWithInterval(id CustomStatsID, callback CustomStatsCallback, interval time.Duration) {
	if m.config.CustomCallbackManager == nil {
		m.config.CustomCallbackManager = NewCustomCallbackManager()
		m.config.CustomCallbackManager.SetDefaultInterval(m.config.Interval)
	}
	m.config.CustomCallbackManager.RegisterCallbackWithInterval(id, callback, interval)

	// Log the initial data at registration time
	if m.config.Logger != nil {
		if initialData := callback(); len(initialData) > 0 {
			// Get current system snapshot for logging
			snapshot := m.GetSnapshot()

			// Add custom metrics to the snapshot
			for _, entry := range initialData {
				prefixedKey := fmt.Sprintf("%s.%s", id.String(), entry.Key)
				snapshot.CustomMetrics[prefixedKey] = entry.Value
			}

			// Create ordered custom metrics
			orderedCustomMetrics := make([]CustomStatEntry, len(initialData))
			for i, entry := range initialData {
				orderedCustomMetrics[i] = CustomStatEntry{
					Key:   fmt.Sprintf("%s.%s", id.String(), entry.Key),
					Value: entry.Value,
				}
			}

			// Get current averaged values for consistent logging format
			avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors := m.getAveragedMetrics()

			// Log using regular compact stats line with start trigger
			reasons := []string{fmt.Sprintf("start-%s", id.String())}
			logLine := m.buildCompactStatsLine(snapshot, avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors, orderedCustomMetrics, reasons)
			m.config.Logger.Log(LogInfo, logLine)
		}
	}
}

// UnregisterCustomStatsCallback removes a specific custom stats callback by ID
func (m *SystemStatsMonitor) UnregisterCustomStatsCallback(id CustomStatsID) {
	if m.config.CustomCallbackManager != nil {
		// Force a final collection and log the stats before unregistering
		if m.config.CustomCallbackManager.ForceCollectCallback(id) && m.config.Logger != nil {
			// Get the current snapshot to include system metrics with the final custom stats
			snapshot := m.GetSnapshot()

			// Get averaged values for consistent logging format
			avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors := m.getAveragedMetrics()

			// Get fresh ordered custom metrics that include the final collection
			orderedCustomMetrics := m.config.CustomCallbackManager.CollectAllOrderedMetrics()

			// Force a log entry with the final stats
			logMsg := m.buildCompactStatsLine(snapshot, avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors, orderedCustomMetrics, []string{fmt.Sprintf("stop-%s", id.String())})
			m.config.Logger.Log(LogInfo, logMsg)
		}

		// Now unregister the callback
		m.config.CustomCallbackManager.UnregisterCallback(id)
	}
}

// IsCustomStatsCallbackRegistered checks if a custom stats callback with the given ID is registered
func (m *SystemStatsMonitor) IsCustomStatsCallbackRegistered(id CustomStatsID) bool {
	if m.config.CustomCallbackManager != nil {
		return m.config.CustomCallbackManager.IsCallbackRegistered(id)
	}
	return false
}

// UnregisterAllCustomStatsCallbacks removes all custom stats callbacks
func (m *SystemStatsMonitor) UnregisterAllCustomStatsCallbacks() {
	if m.config.CustomCallbackManager != nil {
		// Force a final collection of all callbacks and log the stats before unregistering
		collectedCount := m.config.CustomCallbackManager.ForceCollectAllCallbacks()
		if collectedCount > 0 && m.config.Logger != nil {
			// Get the current snapshot to include system metrics with the final custom stats
			snapshot := m.GetSnapshot()

			// Get averaged values for consistent logging format
			avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors := m.getAveragedMetrics()

			// Get fresh ordered custom metrics that include the final collection
			orderedCustomMetrics := m.config.CustomCallbackManager.CollectAllOrderedMetrics()

			// Force a log entry with the final stats
			logMsg := m.buildCompactStatsLine(snapshot, avgCPU, avgNetRead, avgNetWrite, avgDiskRead, avgDiskWrite, avgFileDescriptors, orderedCustomMetrics, []string{fmt.Sprintf("final-all-%d", collectedCount)})
			m.config.Logger.Log(LogInfo, logMsg)
		}

		// Now unregister all callbacks
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

// GetCustomCallbackInfo returns detailed information about registered callbacks including their intervals
func (m *SystemStatsMonitor) GetCustomCallbackInfo() map[string]map[string]string {
	if m.config.CustomCallbackManager != nil {
		return m.config.CustomCallbackManager.GetCallbackInfo()
	}
	return make(map[string]map[string]string)
}

// ForceCollectCustomStats forces immediate data collection for a specific custom stats callback
// This bypasses the normal interval checking and immediately calls the specified callback
// Returns true if the callback was found and called, false otherwise
func (m *SystemStatsMonitor) ForceCollectCustomStats(id CustomStatsID) bool {
	if m.config.CustomCallbackManager != nil {
		return m.config.CustomCallbackManager.ForceCollectCallback(id)
	}
	return false
}

// ForceCollectAllCustomStats forces immediate data collection for all registered custom stats callbacks
// This bypasses the normal interval checking and immediately calls all callbacks
// Returns the number of callbacks that were successfully called
func (m *SystemStatsMonitor) ForceCollectAllCustomStats() int {
	if m.config.CustomCallbackManager != nil {
		return m.config.CustomCallbackManager.ForceCollectAllCallbacks()
	}
	return 0
}

// GetFreshCustomStats forces collection of all custom stats and returns the fresh results
// This is a convenience method that combines ForceCollectAllCustomStats with getting the results
func (m *SystemStatsMonitor) GetFreshCustomStats() map[string]string {
	if m.config.CustomCallbackManager != nil {
		// Force immediate collection of all callbacks
		m.config.CustomCallbackManager.ForceCollectAllCallbacks()
		// Return the fresh results
		return m.config.CustomCallbackManager.CollectAllMetrics()
	}
	return make(map[string]string)
}

// GetFreshOrderedCustomStats forces collection and returns ordered custom stats
// This preserves the order specified by each callback
func (m *SystemStatsMonitor) GetFreshOrderedCustomStats() []CustomStatEntry {
	if m.config.CustomCallbackManager != nil {
		// Force immediate collection of all callbacks
		m.config.CustomCallbackManager.ForceCollectAllCallbacks()
		// Return the fresh ordered results
		return m.config.CustomCallbackManager.CollectAllOrderedMetrics()
	}
	return make([]CustomStatEntry, 0)
}

// LogAdhocCustomStats logs a one-time custom stats message with a tag and newlines before and after
// This is useful for logging important events or state changes outside of regular monitoring intervals
func (m *SystemStatsMonitor) LogAdhocCustomStats(tag string, entries []CustomStatEntry) {
	if m.config.Logger == nil || len(entries) == 0 {
		return
	}

	// Build the custom stats string in the same format as regular stats
	customSummary := ""
	for i, entry := range entries {
		if i > 0 {
			customSummary += ","
		}
		customSummary += fmt.Sprintf("%s:%s", entry.Key, entry.Value)
	}

	// Log with tag at the start and newlines before and after for visibility
	m.config.Logger.Log(LogInfo, "")
	if tag != "" {
		m.config.Logger.Log(LogInfo, fmt.Sprintf("%s: [%s]", tag, customSummary))
	} else {
		m.config.Logger.Log(LogInfo, fmt.Sprintf("ADHOC: [%s]", customSummary))
	}
	m.config.Logger.Log(LogInfo, "")
}

// IsCallbackRegistered checks if a callback with the given ID is registered
func (m *CustomCallbackManager) IsCallbackRegistered(id CustomStatsID) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	_, exists := m.callbacks[id]
	return exists
}

// ForceCollectCallback forces immediate data collection for a specific callback by ID
// This bypasses the normal interval checking and immediately calls the callback
// Returns true if the callback was found and called, false otherwise
func (m *CustomCallbackManager) ForceCollectCallback(id CustomStatsID) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	config, exists := m.callbacks[id]
	if !exists || config == nil || config.Callback == nil {
		return false
	}

	// Force immediate collection regardless of interval
	if orderedMetrics := config.Callback(); orderedMetrics != nil {
		// Update cached data with fresh metrics
		config.LastData = make([]CustomStatEntry, len(orderedMetrics))
		copy(config.LastData, orderedMetrics)
		// Update last call time
		config.LastCall = time.Now()
	}

	return true
}

// ForceCollectAllCallbacks forces immediate data collection for all registered callbacks
// This bypasses the normal interval checking and immediately calls all callbacks
// Returns the number of callbacks that were successfully called
func (m *CustomCallbackManager) ForceCollectAllCallbacks() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	collected := 0
	now := time.Now()

	for _, config := range m.callbacks {
		if config != nil && config.Callback != nil {
			// Force immediate collection regardless of interval
			if orderedMetrics := config.Callback(); orderedMetrics != nil {
				// Update cached data with fresh metrics
				config.LastData = make([]CustomStatEntry, len(orderedMetrics))
				copy(config.LastData, orderedMetrics)
				// Update last call time
				config.LastCall = now
				collected++
			}
		}
	}

	return collected
}
