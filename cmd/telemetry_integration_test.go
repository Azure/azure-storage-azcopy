package cmd

import (
	"testing"
	"os"
)

func TestTelemetryIntegration(t *testing.T) {
	// Save original values to restore later
	originalTelemetry := azcopyTelemetryValue
	originalEnv := os.Getenv("AZCOPY_USER_AGENT_PREFIX")
	defer func() {
		azcopyTelemetryValue = originalTelemetry
		if originalEnv != "" {
			os.Setenv("AZCOPY_USER_AGENT_PREFIX", originalEnv)
		} else {
			os.Unsetenv("AZCOPY_USER_AGENT_PREFIX")
		}
	}()

	baseUserAgent := "AzCopy/10.29.1" // Use static version for predictable testing

	// Test case 1: Only CLI telemetry parameter
	azcopyTelemetryValue = "MyCompany/1.0"
	os.Unsetenv("AZCOPY_USER_AGENT_PREFIX")
	result := buildUserAgentWithTelemetry(baseUserAgent)
	expected := "MyCompany/1.0 AzCopy/10.29.1"
	if result != expected {
		t.Errorf("CLI telemetry only: expected %s, got %s", expected, result)
	}

	// Test case 2: Only environment variable
	azcopyTelemetryValue = ""
	os.Setenv("AZCOPY_USER_AGENT_PREFIX", "EnvPrefix/1.0")
	result = buildUserAgentWithTelemetry(baseUserAgent)
	expected = "EnvPrefix/1.0 AzCopy/10.29.1"
	if result != expected {
		t.Errorf("Env prefix only: expected %s, got %s", expected, result)
	}

	// Test case 3: Both CLI telemetry and environment variable
	azcopyTelemetryValue = "MyCompany/1.0"
	os.Setenv("AZCOPY_USER_AGENT_PREFIX", "EnvPrefix/1.0")
	result = buildUserAgentWithTelemetry(baseUserAgent)
	expected = "EnvPrefix/1.0 MyCompany/1.0 AzCopy/10.29.1"
	if result != expected {
		t.Errorf("Both CLI and env: expected %s, got %s", expected, result)
	}

	// Test case 4: Neither CLI nor environment variable
	azcopyTelemetryValue = ""
	os.Unsetenv("AZCOPY_USER_AGENT_PREFIX")
	result = buildUserAgentWithTelemetry(baseUserAgent)
	expected = "AzCopy/10.29.1"
	if result != expected {
		t.Errorf("Neither CLI nor env: expected %s, got %s", expected, result)
	}
}