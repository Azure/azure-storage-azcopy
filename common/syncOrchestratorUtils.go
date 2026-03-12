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

package common

import (
	"os"
	"strings"
)

type SyncOrchTestMode string

const (
	// Disable the test mode for sync orchestrator.
	SyncOrchTestModeNone SyncOrchTestMode = "NONE"

	// SyncOrchTestModeDefault is the default mode for sync orchestrator tests.
	SyncOrchTestModeDefault SyncOrchTestMode = "DEFAULT"

	// SyncOrchTestModeCTimeOpt enables optimization based on ctime for sync orchestrator tests.
	// Last sync time is set to Now()-15m to ensure that ctime optimization is applied.
	SyncOrchTestModeCTimeOpt SyncOrchTestMode = "CTIME_OPT"

	// SyncOrchTestModeMetadataOnly is used to test metadata-only sync operations.
	SyncOrchTestModeMetadataOnly SyncOrchTestMode = "METADATA_ONLY"
)

// GetSyncOrchTestModeFromEnv retrieves the SyncOrchTestMode from the environment variable "SYNC_ORCHESTRATOR_TEST_MODE".
// If the variable is not set or invalid, it returns SyncOrchTestModeNone.
func GetSyncOrchTestModeFromEnv() SyncOrchTestMode {
	val := strings.ToUpper(os.Getenv("SYNC_ORCHESTRATOR_TEST_MODE"))
	switch SyncOrchTestMode(val) {
	case SyncOrchTestModeDefault,
		SyncOrchTestModeCTimeOpt,
		SyncOrchTestModeMetadataOnly:
		return SyncOrchTestMode(val)
	default:
		return SyncOrchTestModeNone
	}
}

func IsSyncOrchTestModeSet() bool {
	mode := GetSyncOrchTestModeFromEnv()
	return mode != SyncOrchTestModeNone
}
