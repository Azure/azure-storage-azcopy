package cmd

import "sync"

// Traversers send scan errors to the caller's error channel without
// blocking and drop them (logging only) when it is full, so a slow or absent
// reader never stalls enumeration. A caller that decides from those errors
// whether a job may complete must see every one: it registers its channel
// with RequireErrorDelivery, and sends to that channel then wait for room
// instead. Such a caller must keep draining the channel until enumeration has
// returned.
var errorChannelsRequiringDelivery sync.Map // chan<- TraverserErrorItemInfo -> struct{}

// RequireErrorDelivery makes scan error sends to ch wait for room instead of
// dropping errors, until the returned func is called.
func RequireErrorDelivery(ch chan<- TraverserErrorItemInfo) (release func()) {
	errorChannelsRequiringDelivery.Store(ch, struct{}{})
	return func() { errorChannelsRequiringDelivery.Delete(ch) }
}

// sendScanError sends item on ch, waiting for room if ch requires delivery
// (see RequireErrorDelivery). It reports whether item was sent.
func sendScanError(ch chan<- TraverserErrorItemInfo, item TraverserErrorItemInfo) bool {
	if _, ok := errorChannelsRequiringDelivery.Load(ch); ok {
		ch <- item
		return true
	}
	select {
	case ch <- item:
		return true
	default:
		return false
	}
}
