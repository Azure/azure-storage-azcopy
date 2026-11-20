package ste

import (
	"errors"
	"fmt"
	"math"
)

// MinimumJobExecutionConcurrency is the main-pool floor for claimed-plan workers.
const MinimumJobExecutionConcurrency = 300

// RuntimeSettings resolves process-local execution resources from the normal
// AzCopy environment/profile unless the caller supplies explicit settings.
// Native copy settings still come from the plan. Both paths enforce the same
// main-pool minimum.
func (c JobExecutionSessionConfig) RuntimeSettings() (ConcurrencySettings, int64, error) {
	if c.ChunkChannelSize < 0 || c.ChunkChannelSize > 1000000 {
		return ConcurrencySettings{}, 0, errors.New("claimed chunk channel capacity must be between zero (profile default) and 1000000")
	}
	var s ConcurrencySettings
	if c.Concurrency != nil {
		s = *c.Concurrency
	} else {
		// Use normal AzCopy pools, but retain the session's download-file allowance
		// rather than deriving it from OS limits for this S2S-only host.
		s = NewConcurrencySettings(math.MaxInt32)
		s.MaxOpenDownloadFiles = 16
		// Raise only an implicit fixed-pool default. Explicit settings (including
		// AUTO) must pass native validation rather than be rewritten.
		if !s.MaxMainPoolSize.IsUserSpecified && !s.AutoTuneMainPool() &&
			s.InitialMainPoolSize < MinimumJobExecutionConcurrency {
			s.InitialMainPoolSize = MinimumJobExecutionConcurrency
			s.MaxMainPoolSize.Value = MinimumJobExecutionConcurrency
			s.MaxMainPoolSize.DefaultSourceDesc = "split-worker minimum"
			s.MaxIdleConnections = MinimumJobExecutionConcurrency
		}
	}
	// This bounded adapter supports fixed pools. Do not silently substitute a
	// null tuner for a supplied auto-tuning configuration.
	if s.InitialMainPoolSize <= 0 || s.MaxMainPoolSize == nil ||
		s.MaxMainPoolSize.Value != s.InitialMainPoolSize ||
		s.TransferInitiationPoolSize == nil || s.TransferInitiationPoolSize.Value <= 0 ||
		s.EnumerationPoolSize == nil || s.EnumerationPoolSize.Value <= 0 ||
		s.SchedulerParallelism == nil || s.SchedulerParallelism.Value <= 0 ||
		s.CheckCpuWhenTuning == nil || s.ParallelStatFiles == nil ||
		s.MaxOpenDownloadFiles <= 0 || s.MaxIdleConnections <= 0 {
		return ConcurrencySettings{}, 0, errors.New("claimed runtime requires complete positive fixed-pool concurrency settings; automatic tuning is not supported")
	}
	if s.InitialMainPoolSize < MinimumJobExecutionConcurrency {
		return ConcurrencySettings{}, 0, fmt.Errorf("split-worker main concurrency must be at least %d", MinimumJobExecutionConcurrency)
	}
	bytes := c.BufferBytes
	if bytes == 0 {
		bytes = jobSessionCacheBytes
	}
	if bytes < jobSessionCacheBytes || bytes > 1<<40 {
		return ConcurrencySettings{}, 0, errors.New("claimed buffer budget must be between 64 MiB and 1 TiB")
	}
	if math.IsNaN(c.CapMbps) || math.IsInf(c.CapMbps, 0) || c.CapMbps < 0 || c.CapMbps > 1e6 {
		return ConcurrencySettings{}, 0, errors.New("claimed bandwidth cap must be finite and between zero and 1000000 Mbps")
	}
	if c.CapMbps > 0 && int64(c.CapMbps*1000*1000/8) == 0 {
		return ConcurrencySettings{}, 0, errors.New("positive claimed bandwidth cap must allow at least one byte per second")
	}
	// ConcurrencySettings contains pointers. Freeze them so a caller cannot
	// mutate live pool limits after manager construction.
	main, transfer, enumeration, scheduler := *s.MaxMainPoolSize, *s.TransferInitiationPoolSize, *s.EnumerationPoolSize, *s.SchedulerParallelism
	cpu, stat := *s.CheckCpuWhenTuning, *s.ParallelStatFiles
	s.MaxMainPoolSize, s.TransferInitiationPoolSize = &main, &transfer
	s.EnumerationPoolSize, s.SchedulerParallelism = &enumeration, &scheduler
	s.CheckCpuWhenTuning, s.ParallelStatFiles = &cpu, &stat
	return s, bytes, nil
}

// RuntimeChannelSizes resolves the per-session queue override. RuntimeSettings
// validates the configuration before any manager or channels are constructed.
func (c JobExecutionSessionConfig) RuntimeChannelSizes() ChannelSizeConfig {
	sizes := GetChannelSizeConfig()
	if c.ChunkChannelSize != 0 {
		sizes.ChunkChannelSize = c.ChunkChannelSize
	}
	return sizes
}
