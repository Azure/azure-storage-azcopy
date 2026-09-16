package azcopy

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestTelemetryBudgetBoundsSourceScopes(t *testing.T) {
	tracker := newSourceShapeTracker(common.ELocation.Blob(), common.ESymlinkHandlingType.Skip(), common.EHardlinkHandlingType.Follow())
	tracker.accountScope = true
	for index := 0; index < 10000; index++ {
		object := traverser.StoredObject{ContainerName: fmt.Sprint(index), EntityType: common.EEntityType.File(), Size: 10}
		require.NoError(t, tracker.recordScanned(object))
		tracker.recordScheduled(object)
	}
	assert.Len(t, tracker.scannedScope, maxTrackedSourceScopes)
	assert.Len(t, tracker.touchedScope, maxTrackedSourceScopes)
	summary := tracker.snapshot()
	assert.EqualValues(t, -1, summary.ContainersScanned)
	assert.EqualValues(t, -1, summary.ContainersTouched)
	assert.EqualValues(t, 10000, summary.ObjectsScanned)
	assert.EqualValues(t, 100000, summary.BytesScanned)
	tracker = newSourceShapeTracker(common.ELocation.S3(), common.ESymlinkHandlingType.Skip(), common.EHardlinkHandlingType.Follow())
	tracker.addScannedScope(strings.Repeat("x", maxTrackedSourceScopeBytes+1))
	assert.Empty(t, tracker.scannedScope)
	assert.EqualValues(t, -1, tracker.snapshot().BucketsScanned)
}
