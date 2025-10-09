package common

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type s3StreamingMode int

const (
	s3StreamingModeAuto s3StreamingMode = iota
	s3StreamingModeForceOn
	s3StreamingModeForceOff
)

const (
	defaultS3StreamingThresholdBytes   = 4 * 1024 * 1024 // 4 MiB
	defaultS3StreamingReservationBytes = 1 * 1024 * 1024 // 1 MiB reservation against cache limiter
)

type s3StreamingSettings struct {
	mode        s3StreamingMode
	threshold   int64
	reservation int64
	verbose     bool
	metrics     bool
}

var (
	s3StreamingSettingsOnce sync.Once
	s3StreamingSettingsVal  s3StreamingSettings
)

func initS3StreamingSettings() {
	cfg := s3StreamingSettings{}

	modeEnv := strings.ToLower(strings.TrimSpace(os.Getenv("AZCOPY_S3_STREAMING")))
	switch modeEnv {
	case "0", "off", "false":
		cfg.mode = s3StreamingModeForceOff
	case "1", "on", "true":
		cfg.mode = s3StreamingModeForceOn
	default:
		cfg.mode = s3StreamingModeAuto
	}

	thresholdBytes := int64(defaultS3StreamingThresholdBytes)
	if raw := strings.TrimSpace(os.Getenv("AZCOPY_S3_STREAMING_THRESHOLD_MB")); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > 0 {
			thresholdBytes = v * 1024 * 1024
		}
	}
	cfg.threshold = thresholdBytes

	reservationBytes := int64(defaultS3StreamingReservationBytes)
	if raw := strings.TrimSpace(os.Getenv("AZCOPY_S3_STREAMING_RESERVATION_MB")); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v >= 0 {
			reservationBytes = v * 1024 * 1024
		}
	}
	cfg.reservation = reservationBytes

	cfg.verbose = os.Getenv("AZCOPY_S3_CHUNK_VERBOSE") == "1"
	cfg.metrics = os.Getenv("AZCOPY_S3_STREAM_METRICS") == "1"

	s3StreamingSettingsVal = cfg
}

func streamingCfg() s3StreamingSettings {
	s3StreamingSettingsOnce.Do(initS3StreamingSettings)
	return s3StreamingSettingsVal
}

// S3StreamingVerbose reports whether verbose logging for S3 chunk reader is enabled.
func S3StreamingVerbose() bool {
	return streamingCfg().verbose
}

// S3StreamingMetricsEnabled reports whether instrumentation logs should be emitted for S3 streaming.
func S3StreamingMetricsEnabled() bool {
	return streamingCfg().metrics
}

// shouldUseS3Streaming determines whether streaming is enabled for the given chunk length.
func shouldUseS3Streaming(length int64, allowStreaming bool) bool {
	if !allowStreaming {
		return false
	}

	cfg := streamingCfg()
	switch cfg.mode {
	case s3StreamingModeForceOn:
		return true
	case s3StreamingModeForceOff:
		return false
	default:
		return length >= cfg.threshold
	}
}

// s3StreamingReservation returns the number of bytes that should be reserved in the cache limiter for streaming requests.
func s3StreamingReservation(length int64) int64 {
	cfg := streamingCfg()
	if cfg.reservation <= 0 {
		return 0
	}
	if length < cfg.reservation {
		return length
	}
	return cfg.reservation
}

// RecordS3DownloadMetric emits instrumentation for S3 download timing when enabled.
func RecordS3DownloadMetric(id ChunkID, size int64, duration time.Duration, mode string) {
	if !S3StreamingMetricsEnabled() {
		return
	}
	if duration <= 0 {
		duration = 1 // guard against division by zero
	}
	throughput := float64(size) / duration.Seconds()
	GetLifecycleMgr().Info(fmt.Sprintf("[S3Metrics] download mode=%s chunk=%s bytes=%d duration=%v throughput=%.2f MiB/s", mode, id.Name, size, duration, throughput/(1024*1024)))
}

// RecordS3UploadMetric emits instrumentation for S3 upload timing when enabled.
func RecordS3UploadMetric(id ChunkID, size int64, duration time.Duration) {
	if !S3StreamingMetricsEnabled() {
		return
	}
	if duration <= 0 {
		duration = 1
	}
	throughput := float64(size) / duration.Seconds()
	GetLifecycleMgr().Info(fmt.Sprintf("[S3Metrics] upload chunk=%s bytes=%d duration=%v throughput=%.2f MiB/s", id.Name, size, duration, throughput/(1024*1024)))
}
