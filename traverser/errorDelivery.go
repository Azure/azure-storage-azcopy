package traverser

import (
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var errErrorChannelClosed = errors.New("error channel is closed")

// sendToErrorChannel waits for the consumer to receive item, or for ctx to end. A nil channel is a no-op.
func sendToErrorChannel(ctx context.Context, ch chan<- TraverserErrorItemInfo, item TraverserErrorItemInfo) (err error) {
	if ch == nil {
		return nil
	}
	// The consumer may close the channel while a sender is blocked on it.
	defer func() {
		if recover() != nil {
			err = errErrorChannelClosed
		}
	}()

	select {
	case ch <- item:
		return nil
	default:
	}

	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	select {
	case ch <- item:
		return nil
	case <-done:
		return ctx.Err()
	}
}

// sendToErrorChannelOrLog sends item and logs when it could not be delivered.
func sendToErrorChannelOrLog(ctx context.Context, ch chan<- TraverserErrorItemInfo, item TraverserErrorItemInfo) {
	if err := sendToErrorChannel(ctx, ch, item); err != nil {
		WarnStdoutAndScanningLog(fmt.Sprintf("Could not deliver error to channel (%v): %v", err, item.ErrorMessage()))
	}
}

// countScanErrorAndSendToErrorChannel counts item as a failed entity, then sends it to the channel so the caller learns which path failed.
// Counting comes first so the totals stay exact even without a channel or when delivery fails.
func countScanErrorAndSendToErrorChannel(ctx context.Context, ch chan<- TraverserErrorItemInfo, countScanError func(common.EntityType), item TraverserErrorItemInfo) {
	if countScanError != nil {
		if item.IsDir() {
			countScanError(common.EEntityType.Folder())
		} else {
			countScanError(common.EEntityType.File())
		}
	}
	sendToErrorChannelOrLog(ctx, ch, item)
}
