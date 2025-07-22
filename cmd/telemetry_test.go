package cmd

import (
	"testing"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestBuildUserAgentWithTelemetry(t *testing.T) {
	// Save original value to restore later
	originalTelemetry := azcopyTelemetryValue
	defer func() {
		azcopyTelemetryValue = originalTelemetry
	}()

	baseUserAgent := common.UserAgent

	// Test 1: No telemetry value
	azcopyTelemetryValue = ""
	result := buildUserAgentWithTelemetry(baseUserAgent)
	// Should have only the base user agent (possibly with env prefix)
	if result != common.AddUserAgentPrefix(baseUserAgent) {
		t.Errorf("Expected %s, got %s", common.AddUserAgentPrefix(baseUserAgent), result)
	}

	// Test 2: With telemetry value
	azcopyTelemetryValue = "MyCompany/1.0"
	result = buildUserAgentWithTelemetry(baseUserAgent)
	// Should have telemetry prefix followed by user agent
	expected := common.AddUserAgentPrefix("MyCompany/1.0 " + baseUserAgent)
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}

	// Test 3: Empty telemetry value should be handled gracefully
	azcopyTelemetryValue = ""
	result = buildUserAgentWithTelemetry(baseUserAgent)
	if result != common.AddUserAgentPrefix(baseUserAgent) {
		t.Errorf("Expected %s, got %s", common.AddUserAgentPrefix(baseUserAgent), result)
	}
}