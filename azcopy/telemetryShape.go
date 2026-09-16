// This file collects source-workload statistics for telemetry: object counts,
// sizes, approximate size percentiles, directory depth, and source containers or
// buckets scanned versus touched by scheduled transfers, using bounded memory.
// Numeric overflow is reported as -1, including derived statistics whose inputs
// overflow the signed summary range.

package azcopy

import (
	"math"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

const (
	smallSourceObjectThresholdBytes = int64(1024 * 1024)
	directSourceScopeKey            = "\x00direct"
	maxTrackedSourceScopes          = 128
	maxTrackedSourceScopeBytes      = 256
)

var sourceObjectSizeBucketUpperBounds = [...]int64{
	0,
	1024,
	16 * 1024,
	256 * 1024,
	1024 * 1024,
	16 * 1024 * 1024,
	256 * 1024 * 1024,
	1024 * 1024 * 1024,
	16 * 1024 * 1024 * 1024,
	math.MaxInt64,
}

type sourceScopeKind uint8

const (
	sourceScopeNone sourceScopeKind = iota
	sourceScopeContainer
	sourceScopeBucket
)

type sourceShapeSummary struct {
	ObjectsScanned           int64
	BytesScanned             int64
	AverageObjectSizeBytes   float64
	ObjectSizeP50BytesApprox int64
	ObjectSizeP90BytesApprox int64
	ObjectSizeP95BytesApprox int64
	ObjectsUnder1MiB         int64
	ObjectsUnder1MiBRatioPct float64
	MaxDirectoryDepth        int64
	ContainersScanned        int64
	ContainersTouched        int64
	BucketsScanned           int64
	BucketsTouched           int64
}

type sourceShapeTracker struct {
	mu       sync.Mutex
	isActive func() bool

	scopeKind        sourceScopeKind
	accountScope     bool
	symlinkHandling  common.SymlinkHandlingType
	hardlinkHandling common.HardlinkHandlingType
	scannedScope     map[string]struct{}
	touchedScope     map[string]struct{}
	scannedOverflow  bool
	touchedOverflow  bool

	objectCount   uint64
	bytesScanned  uint64
	smallCount    uint64
	maxDepth      int64
	maxObjectSize int64
	sizeBuckets   [len(sourceObjectSizeBucketUpperBounds)]uint64
}

func newSourceShapeTracker(location common.Location, symlinkHandling common.SymlinkHandlingType, hardlinkHandling common.HardlinkHandlingType) *sourceShapeTracker {
	kind := sourceScopeNone
	switch location {
	case common.ELocation.S3(), common.ELocation.GCP():
		kind = sourceScopeBucket
	default:
		if location.IsAzure() {
			kind = sourceScopeContainer
		}
	}
	return &sourceShapeTracker{
		scopeKind:        kind,
		symlinkHandling:  symlinkHandling,
		hardlinkHandling: hardlinkHandling,
		scannedScope:     make(map[string]struct{}),
		touchedScope:     make(map[string]struct{}),
	}
}

func (t *sourceShapeTracker) initializeScopes(resourceTraverser traverser.ResourceTraverser) error {
	if !t.collectionEnabled() || t.scopeKind == sourceScopeNone {
		return nil
	}

	_, isAccount := resourceTraverser.(traverser.AccountTraverser)
	t.mu.Lock()
	t.accountScope = isAccount
	t.mu.Unlock()
	if !isAccount {
		t.addScannedScope(directSourceScopeKey)
		return nil
	}

	return nil
}

func (t *sourceShapeTracker) recordScanned(object traverser.StoredObject) error {
	if !t.collectionEnabled() || !t.isShapePayloadObject(object.EntityType) {
		return nil
	}

	size := object.Size
	if size < 0 {
		size = 0
	}
	depth := relativePathDepth(object.RelativePath)

	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.collectionEnabled() {
		return nil
	}
	if t.accountScope && object.ContainerName != "" {
		recordSourceScope(t.scannedScope, &t.scannedOverflow, object.ContainerName)
	}
	t.objectCount = addSourceShapeValue(t.objectCount, 1)
	t.bytesScanned = addSourceShapeValue(t.bytesScanned, uint64(size))
	if size < smallSourceObjectThresholdBytes {
		t.smallCount = addSourceShapeValue(t.smallCount, 1)
	}
	if size > t.maxObjectSize {
		t.maxObjectSize = size
	}
	if depth > t.maxDepth {
		t.maxDepth = depth
	}
	for index, upperBound := range sourceObjectSizeBucketUpperBounds {
		if size <= upperBound {
			t.sizeBuckets[index] = addSourceShapeValue(t.sizeBuckets[index], 1)
			break
		}
	}
	return nil
}

func (t *sourceShapeTracker) recordScheduled(object traverser.StoredObject) {
	if !t.collectionEnabled() || !t.isShapePayloadObject(object.EntityType) || t.scopeKind == sourceScopeNone {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.collectionEnabled() {
		return
	}
	key := directSourceScopeKey
	if t.accountScope {
		key = object.ContainerName
		if key == "" {
			return
		}
	}
	recordSourceScope(t.touchedScope, &t.touchedOverflow, key)
}

func (t *sourceShapeTracker) addScannedScope(scope string) {
	if !t.collectionEnabled() || scope == "" {
		return
	}
	t.mu.Lock()
	if t.collectionEnabled() {
		recordSourceScope(t.scannedScope, &t.scannedOverflow, scope)
	}
	t.mu.Unlock()
}

func (t *sourceShapeTracker) collectionEnabled() bool {
	return t != nil && (t.isActive == nil || t.isActive())
}

func recordSourceScope(scopes map[string]struct{}, overflow *bool, scope string) {
	if *overflow {
		return
	}
	if _, exists := scopes[scope]; exists {
		return
	}
	if len(scopes) == maxTrackedSourceScopes || len(scope) > maxTrackedSourceScopeBytes {
		*overflow = true
		return
	}
	scopes[strings.Clone(scope)] = struct{}{}
}

func sourceScopeCount(scopes map[string]struct{}, overflow bool) int64 {
	if overflow {
		return -1
	}
	return int64(len(scopes))
}

func addSourceShapeValue(value, increment uint64) uint64 {
	if value > math.MaxInt64 || increment > math.MaxInt64-value {
		return uint64(math.MaxInt64) + 1
	}
	return value + increment
}

func sourceShapeValue(value uint64) int64 {
	if value > math.MaxInt64 {
		return -1
	}
	return int64(value)
}

func (t *sourceShapeTracker) snapshot() sourceShapeSummary {
	if !t.collectionEnabled() {
		return sourceShapeSummary{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	summary := sourceShapeSummary{
		ObjectsScanned:           sourceShapeValue(t.objectCount),
		BytesScanned:             sourceShapeValue(t.bytesScanned),
		ObjectSizeP50BytesApprox: t.approximatePercentile(50),
		ObjectSizeP90BytesApprox: t.approximatePercentile(90),
		ObjectSizeP95BytesApprox: t.approximatePercentile(95),
		ObjectsUnder1MiB:         sourceShapeValue(t.smallCount),
		MaxDirectoryDepth:        t.maxDepth,
	}
	if summary.ObjectsScanned < 0 || summary.BytesScanned < 0 {
		summary.AverageObjectSizeBytes = -1
	} else if summary.ObjectsScanned > 0 {
		summary.AverageObjectSizeBytes = float64(summary.BytesScanned) / float64(summary.ObjectsScanned)
	}
	if summary.ObjectsScanned < 0 || summary.ObjectsUnder1MiB < 0 {
		summary.ObjectsUnder1MiBRatioPct = -1
	} else if summary.ObjectsScanned > 0 {
		summary.ObjectsUnder1MiBRatioPct = 100 * float64(summary.ObjectsUnder1MiB) / float64(summary.ObjectsScanned)
	}
	switch t.scopeKind {
	case sourceScopeContainer:
		summary.ContainersScanned = sourceScopeCount(t.scannedScope, t.scannedOverflow)
		summary.ContainersTouched = sourceScopeCount(t.touchedScope, t.touchedOverflow)
	case sourceScopeBucket:
		summary.BucketsScanned = sourceScopeCount(t.scannedScope, t.scannedOverflow)
		summary.BucketsTouched = sourceScopeCount(t.touchedScope, t.touchedOverflow)
	}
	return summary
}

func (t *sourceShapeTracker) approximatePercentile(percentile uint64) int64 {
	if t.objectCount > math.MaxInt64 {
		return -1
	}
	if t.objectCount == 0 {
		return 0
	}
	rank := t.objectCount/100*percentile + (t.objectCount%100*percentile+99)/100
	for index, count := range t.sizeBuckets {
		if count >= rank {
			upperBound := sourceObjectSizeBucketUpperBounds[index]
			if upperBound == math.MaxInt64 {
				return t.maxObjectSize
			}
			return upperBound
		}
		rank -= count
	}
	return t.maxObjectSize
}

func (t *sourceShapeTracker) isShapePayloadObject(entityType common.EntityType) bool {
	switch entityType {
	case common.EEntityType.File():
		return true
	case common.EEntityType.Symlink():
		return t.symlinkHandling == common.ESymlinkHandlingType.Preserve()
	case common.EEntityType.Hardlink():
		return t.hardlinkHandling == common.EHardlinkHandlingType.Follow()
	default:
		return false
	}
}

func relativePathDepth(relativePath string) int64 {
	if relativePath == "" || relativePath == "\x00" {
		return 0
	}
	var componentCount int64
	inComponent := false
	for index := 0; index < len(relativePath); index++ {
		if relativePath[index] == '/' || relativePath[index] == '\\' {
			inComponent = false
		} else if !inComponent {
			componentCount++
			inComponent = true
		}
	}
	if componentCount == 0 {
		return 0
	}
	return componentCount - 1
}
