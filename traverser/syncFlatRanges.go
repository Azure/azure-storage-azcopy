//go:build smslidingwindow

package traverser

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// mergeStaticFlatRanges runs the existing merge join on identically bounded source/target pairs.
// Each pair preserves its own ordering; outputs from different ranges need no global ordering.
func (r *syncRun) mergeStaticFlatRanges(ctx context.Context, workers int, enumerator *SyncEnumerator,
	cca *SyncJob, source, destination ResourceTraverser) error {
	src, srcOK := source.(*BlobTraverser)
	dst, dstOK := destination.(*BlobTraverser)
	if !srcOK || !dstOK {
		return fmt.Errorf("static flat sync requires native Blob traversers")
	}
	for _, traverser := range []*BlobTraverser{src, dst} {
		if err := traverser.validateFlatRanges(); err != nil {
			return err
		}
	}
	// Wrap shared job counters without changing the original enumerator's callbacks.
	var countMu sync.Mutex
	local := *enumerator
	if increment := enumerator.primaryTraverserTemplate.Options.IncrementNotTransferred; increment != nil {
		local.primaryTraverserTemplate.Options.IncrementNotTransferred = func(entity common.EntityType) {
			countMu.Lock()
			defer countMu.Unlock()
			increment(entity)
		}
	}
	return runFlatListRanges(ctx, workers, src.reportFlatRangeError, func(ctx context.Context, bounds flatListRange) error {
		pairCtx, cancelCause := context.WithCancelCause(ctx)
		cancel := func() { cancelCause(nil) }
		defer cancel()
		// Cancel a blocked peer immediately, preserving the first failure rather than its
		// peer's resulting cancellation error. Other range pairs retain their own contexts.
		source := &flatRangeTraverser{
			blob: src.rangeClone(pairCtx, &countMu), bounds: bounds,
			onError: func(err error) {
				cancelCause(src.flatRangeError(bounds, &mergeJoinTraversalError{location: cca.FromTo.From(),
					err: fmt.Errorf("source traversal error during merge-join: %w", err)}))
			},
		}
		destination := &flatRangeTraverser{
			blob: dst.rangeClone(pairCtx, &countMu), bounds: bounds,
			onError: func(err error) {
				if !IsDestinationNotFoundDuringSync(err) {
					cancelCause(dst.flatRangeError(bounds, &mergeJoinTraversalError{location: cca.FromTo.To(),
						err: fmt.Errorf("destination traversal error during merge-join: %w", err)}))
				}
			},
		}
		// The synthetic leading separator is removed by buildChildPath. A real
		// leading separator in a blob name survives; range bounds are never paths.
		dirs, err := r.mergeJoinTwoWaySyncDir(pairCtx, cancel, &local, cca, "/", source, destination, true)
		var mergeErr *mergeJoinTraversalError
		if err != nil && !errors.As(err, &mergeErr) {
			return err
		}
		var traversalErr *flatListRangeErrorInfo
		if errors.As(context.Cause(pairCtx), &traversalErr) {
			return traversalErr
		}
		if err != nil {
			return err
		}
		if len(dirs) != 0 {
			return fmt.Errorf("static flat sync unexpectedly produced child directories")
		}
		return nil
	})
}
