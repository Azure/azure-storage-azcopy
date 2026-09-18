// Copyright © Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"crypto/rand"
	"encoding/hex"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	telemetrySchemaVersion = "1"
)

const (
	// envTelemetryConnectionString overrides the embedded connection string.
	envTelemetryConnectionString = "AZCOPY_TELEMETRY_CONNECTION_STRING"
	// envDisableTelemetry, when set to "true", disables telemetry entirely.
	envDisableTelemetry = "AZCOPY_DISABLE_TELEMETRY"
	// envE2ETelemetryRunID optionally correlates telemetry emitted by one E2E
	// pipeline matrix leg. It is unset in normal AzCopy usage.
	envE2ETelemetryRunID = "AZCOPY_E2E_TELEMETRY_RUN_ID"
	// The shorter shutdown drain can cancel an in-flight send before this deadline.
	telemetrySendTimeout = 5 * time.Second
	// telemetryFlushTimeout is the maximum telemetry may add to process exit.
	telemetryFlushTimeout    = 4 * time.Second
	telemetryMaxPendingSends = 4
	// installationIDFileName stores the anonymous, per-install identifier.
	installationIDFileName         = "installation_id"
	installationIDRenameRetryDelay = 10 * time.Millisecond
	installationIDRenameAttempts   = 100
)

func newTelemetryInvocationID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return ""
	}
	return hex.EncodeToString(buf)
}

func buildResourceAttributes() telemetry.ResourceAttributes {
	hw := probeHostHardware()

	return telemetry.ResourceAttributes{
		AzCopyVersion:      common.AzcopyVersion,
		SchemaVersion:      telemetrySchemaVersion,
		E2ETestRunID:       strings.TrimSpace(os.Getenv(envE2ETelemetryRunID)),
		OSType:             runtime.GOOS,
		OSVersion:          hw.osVersion,
		HostArch:           runtime.GOARCH,
		HostNumCPU:         runtime.NumCPU(),
		HostCPUModel:       hw.cpuModel,
		HostMemoryTotalGB:  hw.memoryTotalGB,
		AzureVMDetected:    probeAzureVM(),
		InstallationID:     installationID(),
		InvocationContext:  detectInvocationContext(os.Getenv),
	}
}

// installationID returns a stable, anonymous per-install identifier. It is a
// random 128-bit value persisted with AzCopy's application data. It is NOT
// derived from any machine identity and contains no PII.
func installationID() string {
	return installationIDInDir(common.GetAzCopyAppPath())
}

func installationIDInDir(appDataDir string) string {
	if appDataDir == "" {
		return ""
	}
	if err := os.MkdirAll(appDataDir, 0700); err != nil {
		return ""
	}

	return createInstallationID(filepath.Join(appDataDir, installationIDFileName), appDataDir)
}

func readInstallationID(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	id := strings.TrimSpace(string(b))
	if len(id) != 32 {
		return ""
	}
	if _, err = hex.DecodeString(id); err != nil {
		return ""
	}
	return id
}

func createInstallationID(path, appDataDir string) string {
	if id := readInstallationID(path); id != "" {
		return id
	}

	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return ""
	}
	id := hex.EncodeToString(buf)

	tempFile, err := os.CreateTemp(appDataDir, "."+installationIDFileName+"-*")
	if err != nil {
		return ""
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if _, err = tempFile.WriteString(id); err != nil {
		_ = tempFile.Close()
		return ""
	}
	if err = tempFile.Close(); err != nil {
		return ""
	}

	for attempt := 0; attempt < installationIDRenameAttempts; attempt++ {
		if err = os.Rename(tempPath, path); err == nil {
			return id
		}
		time.Sleep(installationIDRenameRetryDelay)
	}
	return ""
}

// detectInvocationContext infers how AzCopy was invoked. getenv is injected for
// testability.
func detectInvocationContext(getenv func(string) string) string {
	for _, k := range []string{"TF_BUILD", "GITHUB_ACTIONS", "CI", "JENKINS_URL", "GITLAB_CI", "BUILD_BUILDID"} {
		if getenv(k) != "" {
			return "ci"
		}
	}
	return "interactive"
}
