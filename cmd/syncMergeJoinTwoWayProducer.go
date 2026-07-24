//go:build smslidingwindow
// +build smslidingwindow

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

package cmd

import (
	"container/heap"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// ============================================================================
// TWO-WAY streaming merge-join: producer side (demux + folder reorder + spill)
//
// A single directory listing yields folders (BlobPrefixes) and files (BlobItems).
// The two-way merge-join runs SEPARATE two-pointer merges for folders and files, so we
// demultiplex each traverser's output into two typed channels:
//   - fileCh:   files in raw key order (raw name == merge key on both FNS and HNS); streamed.
//   - folderCh: folders in canonical "<name>/" key order. HNS lists folder prefixes in RAW-name
//               order (e.g. "h2" before "h2-4.2.0.dist-info") which differs from "<name>/" order
//               for prefix-siblings, so folders are reordered by a folders-only min-heap
//               (flush-on-folder-push). Because this stream is folders ONLY, releasing buffered
//               folders on each new folder is safe and keeps the buffer bounded to the active
//               prefix-extension group (no files interleave to invalidate the flush).
//
// LIVENESS: folder emission is made NON-BLOCKING via an in-memory spill queue drained
// opportunistically. Files use normal bounded backpressure. A traverser can therefore only ever
// block on its file channel; it never blocks on folders. That breaks the potential cross-type
// deadlock (a producer stalled on one stream starving the other merge): with folders non-blocking,
// each producer always makes progress to its files, so the file-merge — which is starved only when
// a file channel is EMPTY (i.e. has room) — is always eventually fed. See two-way-merge-join-design.md §4a.
// ============================================================================

// twoWaySideErr is a shared, NON-consuming holder for a traverser's (one side's) traversal error.
// The producer records its error here before it closes its object channels; both merges (file and
// folder) and the deferred cleanup read it without consuming it, so BOTH merges observe a source
// error for mirror-delete protection (a shared error channel would deliver the error to only one).
// Set-before-close makes observation race-free: seeing a side's channels closed => its error, if
// any, is already visible.
type twoWaySideErr struct {
	mu  sync.Mutex
	err error
}

func (s *twoWaySideErr) set(e error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = e
	}
	s.mu.Unlock()
}

func (s *twoWaySideErr) get() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// twoWayFolderEntry is a buffered folder awaiting emission in canonical key order.
type twoWayFolderEntry struct {
	obj StoredObject
	err error
	key string // "<name>/"
}

// twoWayFolderHeap is a min-heap of buffered folders ordered by canonical merge-join key.
type twoWayFolderHeap []twoWayFolderEntry

func (h twoWayFolderHeap) Len() int            { return len(h) }
func (h twoWayFolderHeap) Less(i, j int) bool  { return h[i].key < h[j].key }
func (h twoWayFolderHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *twoWayFolderHeap) Push(x interface{}) { *h = append(*h, x.(twoWayFolderEntry)) }
func (h *twoWayFolderHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}

// twoWayFolderKey returns the canonical folder merge-join key ("<name>/").
func twoWayFolderKey(obj StoredObject) string {
	return strings.TrimSuffix(obj.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING) + common.AZCOPY_PATH_SEPARATOR_STRING
}

// traverserToTypedChannels runs a ResourceTraverser and bridges its push-based output into two
// pull-based channels: folders (reordered to canonical "<name>/" order, non-blocking emission via
// spill) and files (raw key order, bounded backpressure). Any traversal error is recorded into
// sideErr BEFORE the object channels are closed, so observers see it race-free once the side is
// drained. Both channels are closed when the traverser goroutine exits. Cancel via ctx.
func traverserToTypedChannels(
	ctx context.Context,
	t ResourceTraverser,
	filters []ObjectFilter,
	label string,
	sideErr *twoWaySideErr,
) (<-chan StoredObject, <-chan StoredObject) {

	folderCh := make(chan StoredObject, mergeJoinChannelBufferSize)
	fileCh := make(chan StoredObject, mergeJoinChannelBufferSize)

	go func() {
		defer close(folderCh)
		defer close(fileCh)

		var dirHeap twoWayFolderHeap
		var folderSpill []twoWayFolderEntry // ready-to-emit folders that folderCh could not accept yet

		// drainSpill pushes as many spilled folders to folderCh as it can WITHOUT blocking.
		drainSpillNonBlocking := func() {
			for len(folderSpill) > 0 {
				select {
				case folderCh <- folderSpill[0].obj:
					folderSpill = folderSpill[1:]
				default:
					return
				}
			}
		}
		// readyFolder queues a folder for emission (in canonical order) without blocking the producer.
		readyFolder := func(e twoWayFolderEntry) {
			mergeJoinTraceLog("%s FOLDER-EMIT key=%q relPath=%q", label, e.key, e.obj.relativePath)
			folderSpill = append(folderSpill, e)
			drainSpillNonBlocking()
		}
		// flushFoldersBefore releases buffered folders whose key sorts before key (folders-only stream,
		// so this is safe and bounds the buffer to the active prefix-extension group).
		flushFoldersBefore := func(key string) {
			for dirHeap.Len() > 0 && dirHeap[0].key < key {
				readyFolder(heap.Pop(&dirHeap).(twoWayFolderEntry))
			}
		}

		var prevFileKey string
		fileCount := 0

		err := t.Traverse(noPreProccessor, func(obj StoredObject) error {
			if obj.entityType == common.EEntityType.Folder() {
				key := twoWayFolderKey(obj)
				mergeJoinTraceLog("%s DEMUX folder buffered key=%q relPath=%q", label, key, obj.relativePath)
				flushFoldersBefore(key)
				heap.Push(&dirHeap, twoWayFolderEntry{obj: obj, key: key})
				drainSpillNonBlocking()
				return nil
			}
			// File: order-invariant guard (files must be service-sorted by raw key). A violation is a
			// HARD FAILURE — the two-pointer merge would otherwise mis-classify (missed transfers or,
			// for mirror, extra deletes).
			fileKey := obj.relativePath
			fileCount++
			if fileCount > 1 && fileKey < prevFileKey {
				return fmt.Errorf(
					"ORDER VIOLATION in %s files: key=%q < previous key=%q — listing not lexicographically sorted; aborting merge-join",
					label, fileKey, prevFileKey)
			}
			prevFileKey = fileKey
			mergeJoinTraceLog("%s DEMUX+FILE-EMIT key=%q", label, fileKey)
			// Opportunistically keep folders flowing even while we (maybe) block on the file channel.
			drainSpillNonBlocking()
			select {
			case fileCh <- obj:
				return nil
			default:
				mergeJoinChanFullEvents.Add(1)
			}
			for {
				select {
				case fileCh <- obj:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				default:
					// While blocked on the file channel, keep draining folders so the folder-merge is
					// not starved by this producer's file backpressure.
					drainSpillNonBlocking()
					select {
					case fileCh <- obj:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		}, filters)

		// Traversal finished (or errored). Release all remaining buffered folders, then flush the
		// spill to folderCh with BLOCKING sends (no more production, so this cannot deadlock).
		for dirHeap.Len() > 0 {
			readyFolder(heap.Pop(&dirHeap).(twoWayFolderEntry))
		}
		for len(folderSpill) > 0 {
			select {
			case folderCh <- folderSpill[0].obj:
				folderSpill = folderSpill[1:]
			case <-ctx.Done():
				folderSpill = nil
			}
		}

		if err != nil {
			mergeJoinSyncOneDirLog(common.LogError,
				fmt.Sprintf("%s traversal error: %v", label, err), true)
			sideErr.set(err)
		}
	}()

	return folderCh, fileCh
}

// ---------------------------------------------------------------------------
// Shared merge-join channel plumbing + logging/tracing infrastructure.
// The producer owns the channel sizing and back-pressure counter; the logging
// helpers are cross-cutting (the consumer calls them too) and live here with the
// plumbing so there is a single, findable home for merge-join instrumentation.
// ---------------------------------------------------------------------------

// mergeJoinChannelBufferSize controls the buffer size of channels used to bridge
// push-based traversers into pull-based iteration for the channel-based merge-join.
// Each slot holds one StoredObject (~430 bytes), so 5K slots ≈ 2.1MB per channel.
// Sized to one full ListBlobs page (max 5000 items) so the producer can stage an entire
// page and immediately fetch the next over the network while the consumer keeps draining.
const mergeJoinChannelBufferSize = 5_000

// mergeJoinChanFullEvents increments each time a producer's send finds the channel
// full (back-pressure: the merge/consumer is slower than the listing/producer).
var mergeJoinChanFullEvents atomic.Int64

// mergeJoinTraceEnabled turns on verbose per-object, per-comparison tracing of the
// streaming merge-join sync path. Enable by setting AZCOPY_MERGEJOIN_TRACE to a
// truthy value (1/true/yes). Off by default so production logs stay quiet.
var mergeJoinTraceEnabled = func() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("AZCOPY_MERGEJOIN_TRACE"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}()

// mergeJoinTraceLog emits a verbose merge-join trace line (log file + console, so it
// is visible in Log Analytics container logs) only when AZCOPY_MERGEJOIN_TRACE is on.
func mergeJoinTraceLog(format string, args ...interface{}) {
	if !mergeJoinTraceEnabled {
		return
	}
	mergeJoinSyncOneDirLog(common.LogInfo, "[TRACE] "+fmt.Sprintf(format, args...), true)
}

// mergeJoinSyncOneDirLog logs messages specific to the merge-join sync path.
// Set toConsole=true to also emit to stdout (visible in LAW container logs).
func mergeJoinSyncOneDirLog(level common.LogLevel, msg string, toConsole ...bool) {
	console := false
	if len(toConsole) > 0 {
		console = toConsole[0]
	}
	syncOrchestratorLog(level, fmt.Sprintf("[MergeJoin] %s", msg), console)
}
