package common

import (
	"context"
	"fmt"
	"math"
	"net"
	"strings"
	"time"
)

// Network retry configuration with sensible defaults
type NetworkRetryConfig struct {
	MaxRetries        int           `json:"maxRetries"`        // Maximum number of retry attempts
	InitialDelay      time.Duration `json:"initialDelay"`      // Initial delay before first retry
	MaxDelay          time.Duration `json:"maxDelay"`          // Maximum delay between retries
	BackoffMultiplier float64       `json:"backoffMultiplier"` // Exponential backoff multiplier
	Enabled           bool          `json:"enabled"`           // Whether retry is enabled
}

// Default retry configuration
func DefaultNetworkRetryConfig() NetworkRetryConfig {
	return NetworkRetryConfig{
		MaxRetries:        10,                // Maximum number of retry attempts
		InitialDelay:      2 * time.Second,   // Initial delay before first retry
		MaxDelay:          120 * time.Second, // Maximum delay between retries
		BackoffMultiplier: 2.0,               // Exponential backoff multiplier
		Enabled:           true,              // Retry enabled by default
	}
}

// Helper function to check if an error is a retryable network error
func IsRetryableNetworkError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Check for common network timeout/connection errors
	networkErrors := []string{
		"dial tcp",
		"timeout",
		"connection reset by peer",
		"connection refused",
		"network is unreachable",
		"connection timed out",
		"temporary failure in name resolution",
		"no route to host",
		"context deadline exceeded",
	}

	for _, netErr := range networkErrors {
		if strings.Contains(errStr, netErr) {
			return true
		}
	}

	// Check for net.Error timeout or temporary
	if netErr, ok := err.(net.Error); ok && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}

	return false
}

// Generic retry wrapper that works with any function and accepts custom config
func WithNetworkRetry[T any](ctx context.Context, logger ILoggerResetable, operation string, fn func() (T, error), config ...NetworkRetryConfig) (T, error) {
	// Use default config if none provided
	retryConfig := DefaultNetworkRetryConfig()
	if len(config) > 0 {
		retryConfig = config[0]
	}

	var lastErr error
	var zeroValue T

	// If retry is disabled, just call the function once
	if !retryConfig.Enabled {
		return fn()
	}

	for attempt := 0; attempt <= retryConfig.MaxRetries; attempt++ {
		result, err := fn()

		// If successful, return immediately
		if err == nil {
			if attempt > 0 && logger != nil {
				logger.Log(LogError,
					fmt.Sprintf("Network retry succeeded for %s after %d attempts", operation, attempt))
			}
			return result, nil
		}

		lastErr = err

		// Check if this is a retryable network error
		if !IsRetryableNetworkError(err) {
			logger.Log(LogError,
				fmt.Sprintf("Non-retryable error in %s: %v", operation, err))
			// Not a retryable error, fail immediately
			return zeroValue, err
		}

		// If this was the last attempt, don't wait
		if attempt == retryConfig.MaxRetries {
			break
		}

		// Calculate exponential backoff delay
		delay := time.Duration(float64(retryConfig.InitialDelay) * math.Pow(retryConfig.BackoffMultiplier, float64(attempt)))
		if delay > retryConfig.MaxDelay {
			delay = retryConfig.MaxDelay
		}

		if logger != nil {
			logger.Log(LogError,
				fmt.Sprintf("Network timeout in %s (attempt %d/%d): %v. Retrying in %v...",
					operation, attempt+1, retryConfig.MaxRetries+1, err, delay))
		}

		// Wait before retry, but check if context is cancelled
		select {
		case <-ctx.Done():
			return zeroValue, ctx.Err()
		case <-time.After(delay):
			// Continue to next attempt
		}
	}

	// All retries exhausted
	if logger != nil {
		logger.Log(LogError,
			fmt.Sprintf("Network retry exhausted for %s after %d attempts. Final error: %v",
				operation, retryConfig.MaxRetries+1, lastErr))
	}

	return zeroValue, fmt.Errorf("network operation failed after %d attempts: %w", retryConfig.MaxRetries+1, lastErr)
}

// Convenience functions for common retry scenarios
func WithAggressiveRetry[T any](ctx context.Context, logger ILoggerResetable, operation string, fn func() (T, error)) (T, error) {
	aggressiveConfig := NetworkRetryConfig{
		MaxRetries:        10,
		InitialDelay:      500 * time.Millisecond,
		MaxDelay:          30 * time.Second,
		BackoffMultiplier: 1.5,
		Enabled:           true,
	}
	return WithNetworkRetry(ctx, logger, operation, fn, aggressiveConfig)
}

func WithConservativeRetry[T any](ctx context.Context, logger ILoggerResetable, operation string, fn func() (T, error)) (T, error) {
	conservativeConfig := NetworkRetryConfig{
		MaxRetries:        3,
		InitialDelay:      5 * time.Second,
		MaxDelay:          120 * time.Second,
		BackoffMultiplier: 3.0,
		Enabled:           true,
	}
	return WithNetworkRetry(ctx, logger, operation, fn, conservativeConfig)
}

func WithoutRetry[T any](ctx context.Context, logger ILoggerResetable, operation string, fn func() (T, error)) (T, error) {
	noRetryConfig := NetworkRetryConfig{
		Enabled: false,
	}
	return WithNetworkRetry(ctx, logger, operation, fn, noRetryConfig)
}
