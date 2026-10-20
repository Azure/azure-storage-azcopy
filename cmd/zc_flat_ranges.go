package cmd

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"golang.org/x/sync/errgroup"
)

// logFlatListing follows Blob crawl logging: console-only, independent of sync build tags.
func logFlatListing(level common.LogLevel, message string) {
	prefix := "[INFO] "
	if level == common.LogError {
		prefix = "[ERROR] "
	}
	message = prefix + message
	if level == common.LogError {
		glcm.Warn("[AzCopy] " + message)
	} else {
		glcm.Info("[AzCopy] " + message)
	}
}

// flatListRange owns a half-open interval [start, end) in root-relative blob names.
type flatListRange struct {
	start, end string // Relative to the original transfer root; empty end is unbounded.
}

// flatListRangeErrorInfo identifies a failed enumeration range without inventing a directory path.
// It retains directory-style failure accounting; RangeBounds distinguishes it from a root failure.
type flatListRangeErrorInfo struct {
	ErrorBlobInfo
	bounds      flatListRange
	destination bool
}

// RangeBounds returns the half-open interval relative to the transfer root.
func (e *flatListRangeErrorInfo) RangeBounds() (string, string) { return e.bounds.start, e.bounds.end }

// IsDestinationError distinguishes the endpoints even when both locations are Blob.
func (e *flatListRangeErrorInfo) IsDestinationError() bool { return e.destination }

// Error includes the failed range and endpoint in scanner and copy-log messages.
func (e *flatListRangeErrorInfo) Error() string {
	side := "source"
	if e.destination {
		side = "destination"
	}
	return fmt.Sprintf("%s flat range [%q,%q): %v", side, e.bounds.start, e.bounds.end, e.ErrorBlobInfo.Error)
}

// Unwrap preserves the original listing or merge-join error.
func (e *flatListRangeErrorInfo) Unwrap() error { return e.ErrorBlobInfo.Error }

// ErrorMessage exposes the range-qualified error through the scanner contract.
func (e *flatListRangeErrorInfo) ErrorMessage() error { return e }

// flatRangeError attaches range identity to an error without exposing URL query credentials.
func (t *blobTraverser) flatRangeError(bounds flatListRange, err error) *flatListRangeErrorInfo {
	root, _, _ := strings.Cut(t.rawURL, "?")
	return &flatListRangeErrorInfo{
		ErrorBlobInfo: ErrorBlobInfo{BlobPath: root, BlobName: getObjectNameOnly(root), Error: err, Dir: true},
		bounds:        bounds, destination: t.isSyncDestination,
	}
}

// reportFlatRangeError never blocks enumeration. Undelivered errors remain fatal to the caller.
func (t *blobTraverser) reportFlatRangeError(bounds flatListRange, err error) bool {
	var info *flatListRangeErrorInfo
	if !errors.As(err, &info) {
		return false
	}
	select {
	case t.errorChannel <- info:
		return true
	default:
		WarnStdoutAndScanningLog(fmt.Sprintf("Flat range error channel unavailable or full: %v", info))
		return false
	}
}

// flatBlobObjectPath joins a root and key without normalizing the blob name.
// Leading/repeated separators and dot segments must remain literal.
func flatBlobObjectPath(root, key string) string {
	if key == "" {
		return root
	}
	if root == "" || strings.HasSuffix(root, "/") {
		return root + key
	}
	return root + "/" + key
}

// flatListParallelism reads the shared environment setting and validates its supported limits.
// Unset or empty values use the registered default; an explicit 1 selects serial listing.
func flatListParallelism() (int, error) {
	setting := enum.EEnvironmentVariable.MoverFlatListParallelism()
	value := setting.Get()
	if value == "" {
		value = setting.DefaultValue
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < enum.MinFlatListParallelism || n > enum.MaxFlatListParallelism {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", setting.Name, enum.MinFlatListParallelism, enum.MaxFlatListParallelism)
	}
	return n, nil
}

// flatListRanges returns one unbounded serial range or fixed ranges from the shared boundaries.
// Adjacent boundaries and unbounded tails cover every name, including non-alphabet characters.
func flatListRanges(workers int) []flatListRange {
	if workers <= enum.MinFlatListParallelism {
		return []flatListRange{{}}
	}
	ranges := make([]flatListRange, 0, enum.MaxFlatListParallelism)
	start := ""
	for _, boundary := range enum.FlatListRangeBoundaries {
		end := string(boundary)
		ranges = append(ranges, flatListRange{start: start, end: end})
		start = end
	}
	return append(ranges, flatListRange{start: start})
}

// flatListRangeStats tracks scheduler tasks, not individual requests or processed blobs.
type flatListRangeStats struct {
	mu                                           sync.Mutex
	active, queued, completed, failed, cancelled int
}

// started moves one queued range into the active worker count.
func (s *flatListRangeStats) started() {
	s.mu.Lock()
	s.queued--
	s.active++
	s.mu.Unlock()
}

// finished records the original range outcome even when its error was successfully reported.
func (s *flatListRangeStats) finished(err, parentErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	switch {
	case err == nil:
		s.completed++
	case parentErr != nil && errors.Is(err, parentErr):
		s.cancelled++
	default:
		s.failed++
	}
}

// startReporting emits minute snapshots; stop must be called after all started workers have exited.
func (s *flatListRangeStats) startReporting(ctx context.Context, workers, total int) func() {
	logStats := func(final bool) {
		s.mu.Lock()
		active, queued, completed, failed, cancelled := s.active, s.queued, s.completed, s.failed, s.cancelled
		s.mu.Unlock()
		if final && ctx.Err() != nil {
			cancelled += queued
			queued = 0
		}
		logFlatListing(common.LogInfo, fmt.Sprintf(
			"[FlatListRangeStats] activeWorkers=%d/%d queuedRanges=%d completedRanges=%d failedRanges=%d cancelledRanges=%d totalRanges=%d final=%t",
			active, workers, queued, completed, failed, cancelled, total, final))
	}
	statsCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-statsCtx.Done():
				return
			case <-ticker.C:
				logStats(false)
			}
		}
	}()
	return func() {
		cancel()
		<-done
		logStats(true)
	}
}

// runFlatListRanges bounds active tasks without cancelling healthy ranges on a range failure.
// Reported failures do not abort healthy work; undelivered errors and parent cancellation are returned.
func runFlatListRanges(parent context.Context, workers int, report func(flatListRange, error) bool,
	fn func(context.Context, flatListRange) error) error {
	var group errgroup.Group
	group.SetLimit(workers)
	ranges := flatListRanges(workers)
	rangeErrors := make([]error, len(ranges))
	stats := &flatListRangeStats{queued: len(ranges)}
	stopStats := stats.startReporting(parent, workers, len(ranges))
	defer stopStats()
	logFlatListing(common.LogInfo, fmt.Sprintf("[FlatListRanges] workers=%d ranges=%d strategy=static", workers, len(ranges)))
	for index, bounds := range ranges {
		if parent.Err() != nil {
			break
		}
		index, bounds := index, bounds
		group.Go(func() error {
			if err := parent.Err(); err != nil {
				return err
			}
			stats.started()
			var err error
			defer func() { stats.finished(err, parent.Err()) }()
			err = fn(parent, bounds)
			if err != nil {
				logFlatListing(common.LogError, fmt.Sprintf(
					"[FlatListRangeFailed] start=%q end=%q error=%v", bounds.start, bounds.end, err))
				if parent.Err() == nil && report != nil && report(bounds, err) {
					return nil
				}
				rangeErrors[index] = fmt.Errorf("flat range [%q,%q): %w", bounds.start, bounds.end, err)
				return rangeErrors[index]
			}
			logFlatListing(common.LogInfo, fmt.Sprintf("[FlatListRangeComplete] start=%q end=%q", bounds.start, bounds.end))
			return nil
		})
	}
	waitErr := group.Wait()
	if err := errors.Join(rangeErrors...); err != nil {
		return errors.Join(err, parent.Err())
	}
	if waitErr != nil {
		return waitErr
	}
	return parent.Err()
}

// flatListRequestBounds maps a relative range onto the root and intersects the API prefix.
// Disjoint scopes skip the request; the prefix supplies the upper bound when it is tighter.
func flatListRequestBounds(root, prefix string, bounds flatListRange) (start, end string, empty bool) {
	start = root + bounds.start
	if start < prefix {
		start = prefix
	}
	if !strings.HasPrefix(start, prefix) {
		return "", "", true
	}
	if bounds.end != "" {
		end = root + bounds.end
		if end <= prefix {
			return "", "", true
		}
		if !strings.HasPrefix(end, prefix) {
			end = ""
		}
	}
	return start, end, false
}

// validateFlatRanges rejects traversal modes incompatible with ordered FNS Arrow ranges.
// This prevents unsupported listings from feeding range-scoped copy or mirror decisions.
func (t *blobTraverser) validateFlatRanges() error {
	if !t.flatListingEnabled || t.isDFS || !t.recursive || t.parallelListing ||
		t.include.Deleted() || t.include.Snapshots() || t.include.Versions() || t.include.DirStubs() {
		return fmt.Errorf("static ranges require recursive FNS Arrow listing without deleted blobs, snapshots, versions or directory-stub inclusion")
	}
	return nil
}

// flatRangeTraverser restricts an existing blob traverser to one logical key interval.
type flatRangeTraverser struct {
	blob    *blobTraverser
	bounds  flatListRange
	onError func(error)
}

// IsDirectory exposes the range as a directory-like work item, not an actual blob path.
func (t *flatRangeTraverser) IsDirectory(bool) (bool, error) { return true, nil }

// Traverse lists one bounded interval through the existing flat pager and prefetch path.
// Object names remain relative to the original root, not to the range boundary.
func (t *flatRangeTraverser) Traverse(pre objectMorpher, process objectProcessor, filters []ObjectFilter) (err error) {
	defer func() {
		if err != nil && t.onError != nil {
			t.onError(err)
		}
	}()
	parts, err := blob.ParseURL(t.blob.rawURL)
	if err != nil {
		return err
	}
	prefix := parts.BlobName
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return t.blob.serialListRange(t.blob.serviceClient.NewContainerClient(parts.ContainerName),
		parts.ContainerName, prefix, FilterSet(filters).GetEnumerationPreFilter(true), pre, process, filters, &t.bounds)
}

// rangeClone gives a range its own context while reusing the client and traversal settings.
// Counter callbacks are serialized because all clones update the same job totals.
func (t *blobTraverser) rangeClone(ctx context.Context, mu *sync.Mutex) *blobTraverser {
	clone := *t
	clone.ctx = ctx
	if t.incrementEnumerationCounter != nil {
		clone.incrementEnumerationCounter = func(entity common.EntityType) {
			mu.Lock()
			defer mu.Unlock()
			t.incrementEnumerationCounter(entity)
		}
	}
	return &clone
}
