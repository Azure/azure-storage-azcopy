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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestNICSpeedBucket(t *testing.T) {
	assert.Equal(t, "unknown", nicSpeedBucket(-1))
	assert.Equal(t, "<1gbps", nicSpeedBucket(100))
	assert.Equal(t, "1-<10gbps", nicSpeedBucket(1000))
	assert.Equal(t, "10-<40gbps", nicSpeedBucket(10000))
	assert.Equal(t, ">=40gbps", nicSpeedBucket(40000))
}

func TestDetectInvocationContext(t *testing.T) {
	a := assert.New(t)
	a.Equal("interactive", detectInvocationContext(func(string) string { return "" }))
	a.Equal("ci", detectInvocationContext(func(k string) string {
		if k == "GITHUB_ACTIONS" {
			return "true"
		}
		return ""
	}))
}

func TestInstallationIDPersistsOutsideJobPlanFolder(t *testing.T) {
	rootDir := t.TempDir()
	appDataDir := filepath.Join(rootDir, ".azcopy")
	jobPlanDir := filepath.Join(rootDir, "plans")
	require.NoError(t, os.Mkdir(jobPlanDir, 0700))

	first := installationIDInDir(appDataDir)
	second := installationIDInDir(appDataDir)

	assert.Len(t, first, 32)
	assert.Equal(t, first, second)
	assert.FileExists(t, filepath.Join(appDataDir, installationIDFileName))
	assert.NoFileExists(t, filepath.Join(appDataDir, installationIDFileName+".lock"))

	entries, err := os.ReadDir(jobPlanDir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestInstallationIDIsStableAcrossConcurrentReads(t *testing.T) {
	appDataDir := filepath.Join(t.TempDir(), ".azcopy")
	first := installationIDInDir(appDataDir)
	require.Len(t, first, 32)
	const callCount = 16
	results := make(chan string, callCount)

	for i := 0; i < callCount; i++ {
		go func() {
			results <- installationIDInDir(appDataDir)
		}()
	}

	for i := 0; i < callCount; i++ {
		assert.Equal(t, first, <-results)
	}
}

func TestInstallationIDRecoversMalformedFile(t *testing.T) {
	appDataDir := filepath.Join(t.TempDir(), ".azcopy")
	require.NoError(t, os.Mkdir(appDataDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(appDataDir, installationIDFileName), []byte("partial"), 0600))

	first := installationIDInDir(appDataDir)
	require.Len(t, first, 32)
	assert.Equal(t, first, readInstallationID(filepath.Join(appDataDir, installationIDFileName)))
	assert.Equal(t, first, installationIDInDir(appDataDir))
}

func TestInstallationIDIgnoresLegacyLockPath(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		appDataDir := t.TempDir()
		path := filepath.Join(appDataDir, installationIDFileName)
		require.NoError(t, os.Mkdir(path+".lock", 0700))
		if malformed {
			require.NoError(t, os.WriteFile(path, []byte("partial"), 0600))
		}
		identity := installationIDInDir(appDataDir)
		require.Len(t, identity, 32)
		require.Equal(t, identity, readInstallationID(path))
		require.Equal(t, identity, installationIDInDir(appDataDir))
		entries, err := os.ReadDir(appDataDir)
		require.NoError(t, err)
		require.Len(t, entries, 2)
	}
}

func TestNewTelemetryInvocationID(t *testing.T) {
	first := newTelemetryInvocationID()
	second := newTelemetryInvocationID()
	assert.Len(t, first, 32)
	assert.Len(t, second, 32)
	assert.NotEqual(t, first, second)
}

func TestBuildResourceAttributesSchemaVersion(t *testing.T) {
	resource := buildResourceAttributes()
	assert.Equal(t, "1", resource.SchemaVersion)
}

func TestBuildResourceAttributesE2ETestRunID(t *testing.T) {
	t.Setenv(envE2ETelemetryRunID, "pipeline-run-123")
	assert.Equal(t, "pipeline-run-123", buildResourceAttributes().E2ETestRunID)

	t.Setenv(envE2ETelemetryRunID, "   ")
	assert.Empty(t, buildResourceAttributes().E2ETestRunID)
}
