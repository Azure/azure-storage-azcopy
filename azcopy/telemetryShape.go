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
	mu sync.Mutex

	scopeKind        sourceScopeKind
	accountScope     bool
	symlinkHandling  common.SymlinkHandlingType
	hardlinkHandling common.HardlinkHandlingType
	scannedScope     map[string]struct{}
	touchedScope     map[string]struct{}

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
	if t == nil || t.scopeKind == sourceScopeNone {
		return nil
	}

	accountTraverser, isAccount := resourceTraverser.(traverser.AccountTraverser)
	t.mu.Lock()
	t.accountScope = isAccount
	t.mu.Unlock()
	if !isAccount {
		t.addScannedScope(directSourceScopeKey)
		return nil
	}

	scopes, err := accountTraverser.ListContainers()
	if err != nil {
		return err
	}
	for _, scope := range scopes {
		t.addScannedScope(scope)
	}
	return nil
}

func (t *sourceShapeTracker) recordScanned(object traverser.StoredObject) error {
	if t == nil || !t.isShapePayloadObject(object.EntityType) {
		return nil
	}

	size := object.Size
	if size < 0 {
		size = 0
	}
	depth := relativePathDepth(object.RelativePath)

	t.mu.Lock()
	defer t.mu.Unlock()
	t.objectCount++
	t.bytesScanned += uint64(size)
	if size < smallSourceObjectThresholdBytes {
		t.smallCount++
	}
	if size > t.maxObjectSize {
		t.maxObjectSize = size
	}
	if depth > t.maxDepth {
		t.maxDepth = depth
	}
	for index, upperBound := range sourceObjectSizeBucketUpperBounds {
		if size <= upperBound {
			t.sizeBuckets[index]++
			break
		}
	}
	return nil
}

func (t *sourceShapeTracker) recordScheduled(object traverser.StoredObject) {
	if t == nil || !t.isShapePayloadObject(object.EntityType) || t.scopeKind == sourceScopeNone {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	key := directSourceScopeKey
	if t.accountScope {
		key = object.ContainerName
		if key == "" {
			return
		}
	}
	t.touchedScope[key] = struct{}{}
}

func (t *sourceShapeTracker) addScannedScope(scope string) {
	if t == nil || scope == "" {
		return
	}
	t.mu.Lock()
	t.scannedScope[scope] = struct{}{}
	t.mu.Unlock()
}

func (t *sourceShapeTracker) snapshot() sourceShapeSummary {
	if t == nil {
		return sourceShapeSummary{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	summary := sourceShapeSummary{
		ObjectsScanned:           int64(t.objectCount),
		BytesScanned:             int64(t.bytesScanned),
		ObjectSizeP50BytesApprox: t.approximatePercentile(0.50),
		ObjectSizeP90BytesApprox: t.approximatePercentile(0.90),
		ObjectSizeP95BytesApprox: t.approximatePercentile(0.95),
		ObjectsUnder1MiB:         int64(t.smallCount),
		MaxDirectoryDepth:        t.maxDepth,
	}
	if t.objectCount > 0 {
		summary.AverageObjectSizeBytes = float64(t.bytesScanned) / float64(t.objectCount)
		summary.ObjectsUnder1MiBRatioPct = 100 * float64(t.smallCount) / float64(t.objectCount)
	}
	switch t.scopeKind {
	case sourceScopeContainer:
		summary.ContainersScanned = int64(len(t.scannedScope))
		summary.ContainersTouched = int64(len(t.touchedScope))
	case sourceScopeBucket:
		summary.BucketsScanned = int64(len(t.scannedScope))
		summary.BucketsTouched = int64(len(t.touchedScope))
	}
	return summary
}

func (t *sourceShapeTracker) approximatePercentile(percentile float64) int64 {
	if t.objectCount == 0 {
		return 0
	}
	rank := uint64(math.Ceil(percentile * float64(t.objectCount)))
	if rank == 0 {
		rank = 1
	}
	var cumulative uint64
	for index, count := range t.sizeBuckets {
		cumulative += count
		if cumulative >= rank {
			upperBound := sourceObjectSizeBucketUpperBounds[index]
			if upperBound == math.MaxInt64 {
				return t.maxObjectSize
			}
			return upperBound
		}
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
	parts := strings.FieldsFunc(relativePath, func(r rune) bool { return r == '/' || r == '\\' })
	if len(parts) <= 1 {
		return 0
	}
	return int64(len(parts) - 1)
}
