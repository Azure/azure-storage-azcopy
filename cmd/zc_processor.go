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

package cmd

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/pkg/errors"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

// Shuffle/backpressure tuning. The scanner buffers transfers, shuffles a window across the key range,
// then dispatches plan parts asynchronously (plan files on the SMB share are the durable store).
//
// inMemoryWindowMultiplier: park watermark = this * shuffle window. Parking at 2x keeps the park point
// above the flush point (so a full window can fill without deadlock) with a double-buffer of runway.
const inMemoryWindowMultiplier = 2

// scannerBackpressureWindowParts: floor (in plan-parts) for the park watermark, which is
// max(getShuffleThresholdParts()*inMemoryWindowMultiplier, scannerBackpressureWindowParts). At 100
// parts (* 10k transfers/part) the scanner may run ~1M transfers ahead of async dispatch before it
// parks — a wide hysteresis band that avoids the per-part parking sawtooth. Baked constant (no env).
const scannerBackpressureWindowParts = 100

var shuffleThresholdLogOnce sync.Once

// getShuffleThresholdParts is the shuffle window in plan-parts. Only reached on the high-perf mover
// path, where the buildmode profile default is overridable by MOVER_SHUFFLE_PARTS (>=1).
func getShuffleThresholdParts() int {
	effective := buildmode.ShuffleThresholdParts()
	rawMover := strings.TrimSpace(os.Getenv("MOVER_SHUFFLE_PARTS"))
	if rawMover != "" {
		if n, err := strconv.Atoi(rawMover); err == nil && n >= 1 {
			effective = n
		}
	}
	shuffleThresholdLogOnce.Do(func() {
		fmt.Printf("[ShuffleConfig] MOVER_SHUFFLE_PARTS raw=%q effective=%d\n", rawMover, effective)
	})
	return effective
}

var shuffleEnabledLogOnce sync.Once

// isShuffleEnabled reports whether transfer shuffling is on. Only reached on the high-perf mover path,
// where the buildmode profile default is overridable by AZCOPY_SHUFFLE_TRANSFERS.
func isShuffleEnabled() bool {
	enabled := buildmode.ShuffleEnabled()
	if raw := enum.EEnvironmentVariable.ShuffleTransfers().Get(); raw != "" {
		enabled = strings.EqualFold(raw, "true") || raw == "1"
	}
	shuffleEnabledLogOnce.Do(func() {
		fmt.Printf("[ShuffleConfig] shuffle enabled=%v\n", enabled)
	})
	return enabled
}

// dispatchItem is a snapshot of a single part ready for async dispatch to STE.
// It captures all state needed so the dispatch goroutine can call sendPartToSte
// without holding any locks on the processor.
type dispatchItem struct {
	transfers common.Transfers
	partNum   common.PartNumber
}

type copyTransferProcessor struct {
	numOfTransfersPerPart int
	copyJobTemplate       *common.CopyJobPartOrderRequest
	source                common.ResourceString
	destination           common.ResourceString

	// handles for progress tracking
	reportFirstPartDispatched func(jobStarted bool)
	reportFinalPartDispatched func()

	preserveAccessTier     bool
	folderPropertiesOption common.FolderPropertyOption
	symlinkHandlingType    common.SymlinkHandlingType
	dryrunMode             bool
	hardlinkHandlingType   common.HardlinkHandlingType

	//XDM: This is only essential when sync is through syncOrchestrator
	syncTransferMutex sync.Mutex // mutex to synchronize access to the shuffle buffer
	flushMutex        sync.Mutex // mutex to serialize flush operations (sendPartToSte uses shared copyJobTemplate)

	// shuffleBuffer accumulates transfers across multiple plan parts before shuffling and flushing.
	// This ensures each plan part contains transfers from diverse key-space prefixes rather than
	// consecutive ranges, improving storage partition utilization at high throughput.
	shuffleBuffer            []common.CopyTransfer
	shuffleBufferSizeInBytes uint64
	shuffleBufferFileCounts  common.Transfers // tracks entity type counts for the buffer

	// flushWindowCounter is a monotonically increasing flush window ID used for diagnostics.
	flushWindowCounter uint32

	// High-perf mover path: assembled plan parts are pushed into dispatchCh and a pool
	// of background goroutines calls sendPartToSte asynchronously, allowing the buffer
	// to refill concurrently. Non-high-perf stays on synchronous dispatch for mover-default
	// behavior parity.
	dispatchCh   chan dispatchItem
	dispatchOnce sync.Once
	dispatchErr  error          // first error from dispatch goroutine
	dispatchWg   sync.WaitGroup // tracks all dispatch workers
	dispatchDone chan struct{}  // closed when all dispatch workers exit

	// Backpressure signaling
	bufferDrainCond *sync.Cond // condition variable signaled when the shuffle buffer drains below the high watermark
}

func newCopyTransferProcessor(copyJobTemplate *common.CopyJobPartOrderRequest, numOfTransfersPerPart int, source, destination common.ResourceString, reportFirstPartDispatched func(bool), reportFinalPartDispatched func(), preserveAccessTier, dryrunMode bool) *copyTransferProcessor {
	p := &copyTransferProcessor{
		numOfTransfersPerPart:     numOfTransfersPerPart,
		copyJobTemplate:           copyJobTemplate,
		source:                    source,
		destination:               destination,
		reportFirstPartDispatched: reportFirstPartDispatched,
		reportFinalPartDispatched: reportFinalPartDispatched,
		preserveAccessTier:        preserveAccessTier,
		folderPropertiesOption:    copyJobTemplate.Fpo,
		symlinkHandlingType:       copyJobTemplate.SymlinkHandlingType,
		dryrunMode:                dryrunMode,
	}
	// High-perf mover only: allocate the async dispatch pipeline + backpressure resources.
	// Mover-default keeps the original synchronous path with no extra allocation.
	if useAsyncDispatchPipeline() {
		p.dispatchCh = make(chan dispatchItem, dispatchChParts())
		p.dispatchDone = make(chan struct{})
		p.bufferDrainCond = sync.NewCond(&sync.Mutex{})
	}
	return p
}

// dispatchChParts returns the async-dispatch channel depth in plan-parts (AZCOPY_DISPATCH_CH_PARTS).
// Each buffered item is a full part (numOfTransfersPerPart CopyTransfers with source+dest URLs), so
// this directly caps how many transfers sit between flush and the STE when the engine is backpressured.
// The old hardcoded 5000 (=50M transfers => tens of GB of live CopyTransfers) was the dominant heap holder.
func dispatchChParts() int {
	return 128
}

func useAsyncDispatchPipeline() bool {
	return buildmode.IsMover && buildmode.HighPerf()
}

func useHighPerfSyncPath() bool {
	return buildmode.IsMover && buildmode.HighPerf()
}

// startDispatchPipeline ensures the background dispatch worker pool is running.
// Called lazily on first use via sync.Once.
func (s *copyTransferProcessor) startDispatchPipeline() {
	if !useAsyncDispatchPipeline() {
		return
	}

	s.dispatchOnce.Do(func() {
		const numDispatchWorkers = 32 // parallelize fsync-heavy plan file creation
		s.dispatchWg.Add(numDispatchWorkers)
		for i := 0; i < numDispatchWorkers; i++ {
			go s.dispatchWorker()
		}
		// Close dispatchDone when all workers finish
		go func() {
			s.dispatchWg.Wait()
			close(s.dispatchDone)
		}()
	})
}

// dispatchWorker is a background goroutine that reads dispatchItems and sends them to STE.
// Multiple workers run concurrently to parallelize plan file creation (fsync).
func (s *copyTransferProcessor) dispatchWorker() {
	defer s.dispatchWg.Done()
	for item := range s.dispatchCh {
		if s.dispatchErr != nil {
			continue // drain channel after first error
		}
		if err := s.dispatchPartNow(item); err != nil {
			s.dispatchErr = err
		}
	}
}

func (s *copyTransferProcessor) dispatchPartNow(item dispatchItem) error {
	// Build a local copy of the template for this part
	template := *s.copyJobTemplate
	template.Transfers = item.transfers
	template.PartNum = item.partNum

	resp := jobsAdmin.ExecuteNewCopyJobPartOrder(template)

	// Report first part dispatched if this is part 0
	if item.partNum == 0 && s.reportFirstPartDispatched != nil {
		s.reportFirstPartDispatched(resp.JobStarted)
	}

	if resp.ErrorMsg != "" {
		return errors.New(string(resp.ErrorMsg))
	}

	return nil
}

func (s *copyTransferProcessor) dispatchPart(item dispatchItem) error {
	if useAsyncDispatchPipeline() {
		s.startDispatchPipeline()
		s.dispatchCh <- item
		if s.dispatchErr != nil {
			return s.dispatchErr
		}
		return nil
	}

	return s.dispatchPartNow(item)
}

// waitForDispatchPipeline closes the dispatch channel and waits for all workers, returning the first
// dispatch error. startDispatchPipeline is called first (idempotent) so small jobs that never flushed
// a full part don't deadlock on an unclosed dispatchDone.
func (s *copyTransferProcessor) waitForDispatchPipeline() error {
	if !useAsyncDispatchPipeline() {
		return nil
	}

	s.startDispatchPipeline()
	close(s.dispatchCh)
	<-s.dispatchDone
	return s.dispatchErr
}

type DryrunTransfer struct {
	EntityType   common.EntityType
	BlobType     common.BlobType
	FromTo       common.FromTo
	Source       string
	Destination  string
	SourceSize   *int64
	HttpHeaders  blob.HTTPHeaders
	Metadata     common.Metadata
	BlobTier     *blob.AccessTier
	BlobVersion  *string
	BlobTags     common.BlobTags
	BlobSnapshot *string
}

type dryrunTransferSurrogate struct {
	EntityType         string
	BlobType           string
	FromTo             string
	Source             string
	Destination        string
	SourceSize         int64           `json:"SourceSize,omitempty"`
	ContentType        string          `json:"ContentType,omitempty"`
	ContentEncoding    string          `json:"ContentEncoding,omitempty"`
	ContentDisposition string          `json:"ContentDisposition,omitempty"`
	ContentLanguage    string          `json:"ContentLanguage,omitempty"`
	CacheControl       string          `json:"CacheControl,omitempty"`
	ContentMD5         []byte          `json:"ContentMD5,omitempty"`
	BlobTags           common.BlobTags `json:"BlobTags,omitempty"`
	Metadata           common.Metadata `json:"Metadata,omitempty"`
	BlobTier           blob.AccessTier `json:"BlobTier,omitempty"`
	BlobVersion        string          `json:"BlobVersion,omitempty"`
	BlobSnapshotID     string          `json:"BlobSnapshotID,omitempty"`
}

func (d *DryrunTransfer) UnmarshalJSON(bytes []byte) error {
	var surrogate dryrunTransferSurrogate

	err := json.Unmarshal(bytes, &surrogate)
	if err != nil {
		return fmt.Errorf("failed to parse dryrun transfer: %w", err)
	}

	err = d.FromTo.Parse(surrogate.FromTo)
	if err != nil {
		return fmt.Errorf("failed to parse fromto: %w", err)
	}

	err = d.EntityType.Parse(surrogate.EntityType)
	if err != nil {
		return fmt.Errorf("failed to parse entity type: %w", err)
	}

	err = d.BlobType.Parse(surrogate.BlobType)
	if err != nil {
		return fmt.Errorf("failed to parse entity type: %w", err)
	}

	d.Source = surrogate.Source
	d.Destination = surrogate.Destination

	d.SourceSize = &surrogate.SourceSize
	d.HttpHeaders.BlobContentType = &surrogate.ContentType
	d.HttpHeaders.BlobContentEncoding = &surrogate.ContentEncoding
	d.HttpHeaders.BlobCacheControl = &surrogate.CacheControl
	d.HttpHeaders.BlobContentDisposition = &surrogate.ContentDisposition
	d.HttpHeaders.BlobContentLanguage = &surrogate.ContentLanguage
	d.HttpHeaders.BlobContentMD5 = surrogate.ContentMD5
	d.BlobTags = surrogate.BlobTags
	d.Metadata = surrogate.Metadata
	d.BlobTier = &surrogate.BlobTier
	d.BlobVersion = &surrogate.BlobVersion
	d.BlobSnapshot = &surrogate.BlobSnapshotID

	return nil
}

func (d DryrunTransfer) MarshalJSON() ([]byte, error) {
	surrogate := dryrunTransferSurrogate{
		d.EntityType.String(),
		d.BlobType.String(),
		d.FromTo.String(),
		d.Source,
		d.Destination,
		ternary.IffNotNil(d.SourceSize, 0),
		ternary.IffNotNil(d.HttpHeaders.BlobContentType, ""),
		ternary.IffNotNil(d.HttpHeaders.BlobContentEncoding, ""),
		ternary.IffNotNil(d.HttpHeaders.BlobContentDisposition, ""),
		ternary.IffNotNil(d.HttpHeaders.BlobContentLanguage, ""),
		ternary.IffNotNil(d.HttpHeaders.BlobCacheControl, ""),
		d.HttpHeaders.BlobContentMD5,
		d.BlobTags,
		d.Metadata,
		ternary.IffNotNil(d.BlobTier, ""),
		ternary.IffNotNil(d.BlobVersion, ""),
		ternary.IffNotNil(d.BlobSnapshot, ""),
	}

	return json.Marshal(surrogate)
}

func (s *copyTransferProcessor) scheduleCopyTransfer(storedObject StoredObject) (err error) {

	// Escape paths on destinations where the characters are invalid
	// And re-encode them where the characters are valid.
	var srcRelativePath, dstRelativePath string
	if storedObject.relativePath == "\x00" { // Short circuit when we're talking about root/, because the STE is funky about this.
		srcRelativePath, dstRelativePath = storedObject.relativePath, storedObject.relativePath
	} else {
		srcRelativePath = pathEncodeRules(storedObject.relativePath, s.copyJobTemplate.FromTo, false, true)
		dstRelativePath = pathEncodeRules(storedObject.relativePath, s.copyJobTemplate.FromTo, false, false)
		if srcRelativePath != "" {
			srcRelativePath = "/" + srcRelativePath
		}
		if dstRelativePath != "" {
			dstRelativePath = "/" + dstRelativePath
		}
	}

	// In order to fix nameless dir case, we had to store directories in the stored object index with a trailing slash
	// When we go to actually transfer a folder, we need to remove the trailing slash because it's not supported by azure apis
	if s.folderPropertiesOption != common.EFolderPropertiesOption.NoFolders() && storedObject.entityType == common.EEntityType.Folder() {
		srcRelativePath = strings.TrimSuffix(srcRelativePath, common.AZCOPY_PATH_SEPARATOR_STRING)
		dstRelativePath = strings.TrimSuffix(dstRelativePath, common.AZCOPY_PATH_SEPARATOR_STRING)
	}

	copyTransfer, shouldSendToSte := storedObject.ToNewCopyTransfer(false, srcRelativePath, dstRelativePath, s.preserveAccessTier, s.folderPropertiesOption, s.symlinkHandlingType, s.hardlinkHandlingType)

	if s.copyJobTemplate.FromTo.To() == common.ELocation.None() {
		copyTransfer.BlobTier = s.copyJobTemplate.BlobAttributes.BlockBlobTier.ToAccessTierType()

		metadataString := s.copyJobTemplate.BlobAttributes.Metadata
		metadataMap := common.Metadata{}
		if len(metadataString) > 0 {
			for _, keyAndValue := range strings.Split(metadataString, ";") { // key/value pairs are separated by ';'
				kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
				metadataMap[kv[0]] = &kv[1]
			}
		}
		copyTransfer.Metadata = metadataMap

		copyTransfer.BlobTags = common.ToCommonBlobTagsMap(s.copyJobTemplate.BlobAttributes.BlobTagsString)
	}

	if !shouldSendToSte {
		return nil // skip this one
	}

	if s.dryrunMode {
		glcm.Dryrun(func(format common.OutputFormat) string {
			prettySrcRelativePath, prettyDstRelativePath := srcRelativePath, dstRelativePath

			fromTo := s.copyJobTemplate.FromTo
			if fromTo.From().IsRemote() {
				prettySrcRelativePath, err = url.PathUnescape(prettySrcRelativePath)
				if err != nil {
					prettySrcRelativePath = srcRelativePath // Fall back, because it's better than failing.
				}
			}

			if fromTo.To().IsRemote() {
				prettyDstRelativePath, err = url.PathUnescape(prettyDstRelativePath)
				if err != nil {
					prettyDstRelativePath = dstRelativePath // Fall back, because it's better than failing.
				}
			}

			if format == common.EOutputFormat.Json() {
				tx := DryrunTransfer{
					EntityType:  storedObject.entityType,
					BlobType:    common.FromBlobType(storedObject.blobType),
					FromTo:      s.copyJobTemplate.FromTo,
					Source:      common.GenerateFullPath(s.copyJobTemplate.SourceRoot.Value, prettySrcRelativePath),
					Destination: "",
					SourceSize:  &storedObject.size,
					HttpHeaders: blob.HTTPHeaders{
						BlobCacheControl:       &storedObject.cacheControl,
						BlobContentDisposition: &storedObject.contentDisposition,
						BlobContentEncoding:    &storedObject.contentEncoding,
						BlobContentLanguage:    &storedObject.contentLanguage,
						BlobContentMD5:         storedObject.md5,
						BlobContentType:        &storedObject.contentType,
					},
					Metadata:     storedObject.Metadata,
					BlobTier:     &storedObject.blobAccessTier,
					BlobVersion:  &storedObject.blobVersionID,
					BlobTags:     storedObject.blobTags,
					BlobSnapshot: &storedObject.blobSnapshotID,
				}

				if fromTo.To() != common.ELocation.None() && fromTo.To() != common.ELocation.Unknown() {
					tx.Destination = common.GenerateFullPath(s.copyJobTemplate.DestinationRoot.Value, prettyDstRelativePath)
				}

				jsonOutput, err := json.Marshal(tx)
				common.PanicIfErr(err)
				return string(jsonOutput)
			} else {
				// if remove then To() will equal to common.ELocation.Unknown()
				if s.copyJobTemplate.FromTo.To() == common.ELocation.Unknown() { // remove
					return fmt.Sprintf("DRYRUN: remove %v",
						common.GenerateFullPath(s.copyJobTemplate.SourceRoot.Value, prettySrcRelativePath))
				}
				if s.copyJobTemplate.FromTo.To() == common.ELocation.None() { // set-properties
					return fmt.Sprintf("DRYRUN: set-properties %v",
						common.GenerateFullPath(s.copyJobTemplate.SourceRoot.Value, prettySrcRelativePath))
				} else { // copy for sync
					return fmt.Sprintf("DRYRUN: copy %v to %v",
						common.GenerateFullPath(s.copyJobTemplate.SourceRoot.Value, prettySrcRelativePath),
						common.GenerateFullPath(s.copyJobTemplate.DestinationRoot.Value, prettyDstRelativePath))
				}
			}
		})
		return nil
	}

	if UseSyncOrchestrator && useHighPerfSyncPath() {
		if isShuffleEnabled() {
			shuffleThreshold := getShuffleThresholdParts()

			// Backpressure: park the scanner only when the buffer runs far ahead of async dispatch.
			// Park point sits above the flush point (double-buffer) to avoid deadlock, with a floor for
			// wide hysteresis (no per-part parking -> no sawtooth).
			parkParts := shuffleThreshold * inMemoryWindowMultiplier
			if parkParts < scannerBackpressureWindowParts {
				parkParts = scannerBackpressureWindowParts
			}
			highWatermark := s.numOfTransfersPerPart * parkParts
			s.bufferDrainCond.L.Lock()
			for len(s.shuffleBuffer) >= highWatermark {
				s.bufferDrainCond.Wait()
			}
			s.bufferDrainCond.L.Unlock()

			var needsFlush bool

			s.syncTransferMutex.Lock()
			s.shuffleBuffer = append(s.shuffleBuffer, copyTransfer)
			s.shuffleBufferSizeInBytes += uint64(copyTransfer.SourceSize)
			switch copyTransfer.EntityType {
			case common.EEntityType.File():
				s.shuffleBufferFileCounts.FileTransferCount++
			case common.EEntityType.Folder():
				s.shuffleBufferFileCounts.FolderTransferCount++
			case common.EEntityType.Symlink():
				s.shuffleBufferFileCounts.SymlinkTransferCount++
			case common.EEntityType.Hardlink():
				s.shuffleBufferFileCounts.HardlinksConvertedCount++
			case common.EEntityType.FileProperties():
				s.shuffleBufferFileCounts.FilePropertyTransferCount++
			}
			needsFlush = len(s.shuffleBuffer) >= s.numOfTransfersPerPart*shuffleThreshold
			s.syncTransferMutex.Unlock()

			if needsFlush {
				if err := s.flushShuffleBuffer(); err != nil {
					return err
				}
			}
		} else {
			// Direct dispatch: accumulate transfers, dispatch immediately when a full part is ready.
			// No shuffle and no part reordering — bounded O(numOfTransfersPerPart) memory.
			s.syncTransferMutex.Lock()
			s.shuffleBuffer = append(s.shuffleBuffer, copyTransfer)
			needsFlush := len(s.shuffleBuffer) >= s.numOfTransfersPerPart
			s.syncTransferMutex.Unlock()

			if needsFlush {
				if err := s.flushDirectBuffer(); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if UseSyncOrchestrator {
		s.syncTransferMutex.Lock()
		defer s.syncTransferMutex.Unlock()
	}

	if len(s.copyJobTemplate.Transfers.List) == s.numOfTransfersPerPart {
		resp := s.sendPartToSte()

		// TODO: If we ever do launch errors outside of the final "no transfers" error, make them output nicer things here.
		if resp.ErrorMsg != "" {
			return errors.New(string(resp.ErrorMsg))
		}

		// reset the transfers buffer
		s.copyJobTemplate.Transfers = common.Transfers{}
		s.copyJobTemplate.PartNum++
	}

	// only append the transfer after we've checked and dispatched a part
	// so that there is at least one transfer for the final part
	s.copyJobTemplate.Transfers.List = append(s.copyJobTemplate.Transfers.List, copyTransfer)
	s.copyJobTemplate.Transfers.TotalSizeInBytes += uint64(copyTransfer.SourceSize)

	switch copyTransfer.EntityType {
	case common.EEntityType.File():
		s.copyJobTemplate.Transfers.FileTransferCount++
	case common.EEntityType.Folder():
		s.copyJobTemplate.Transfers.FolderTransferCount++
	case common.EEntityType.Symlink():
		s.copyJobTemplate.Transfers.SymlinkTransferCount++
	case common.EEntityType.Hardlink():
		s.copyJobTemplate.Transfers.HardlinksConvertedCount++
	case common.EEntityType.FileProperties():
		s.copyJobTemplate.Transfers.FilePropertyTransferCount++
	}

	return nil
}

// flushDirectBuffer dispatches accumulated transfers directly to STE without shuffling or
// part reordering. Used when AZCOPY_SHUFFLE_TRANSFERS is disabled. Memory usage is bounded
// at O(numOfTransfersPerPart).
func (s *copyTransferProcessor) flushDirectBuffer() error {
	s.flushMutex.Lock()
	defer s.flushMutex.Unlock()

	s.syncTransferMutex.Lock()
	if len(s.shuffleBuffer) < s.numOfTransfersPerPart {
		s.syncTransferMutex.Unlock()
		return nil
	}
	toFlush := s.shuffleBuffer
	s.shuffleBuffer = make([]common.CopyTransfer, 0, s.numOfTransfersPerPart)
	s.syncTransferMutex.Unlock()

	for len(toFlush) >= s.numOfTransfersPerPart {
		// Copy batch to a new right-sized slice to avoid retaining the entire toFlush array
		batch := make([]common.CopyTransfer, s.numOfTransfersPerPart)
		copy(batch, toFlush[:s.numOfTransfersPerPart])
		toFlush = toFlush[s.numOfTransfersPerPart:]

		transfers := common.Transfers{List: batch}
		for _, t := range batch {
			transfers.TotalSizeInBytes += uint64(t.SourceSize)
			switch t.EntityType {
			case common.EEntityType.File():
				transfers.FileTransferCount++
			case common.EEntityType.Folder():
				transfers.FolderTransferCount++
			case common.EEntityType.Symlink():
				transfers.SymlinkTransferCount++
			case common.EEntityType.Hardlink():
				transfers.HardlinksConvertedCount++
			case common.EEntityType.FileProperties():
				transfers.FilePropertyTransferCount++
			}
		}

		if err := s.dispatchPart(dispatchItem{
			transfers: transfers,
			partNum:   s.copyJobTemplate.PartNum,
		}); err != nil {
			return err
		}
		s.copyJobTemplate.PartNum++
	}

	// Put remainder back
	if len(toFlush) > 0 {
		s.syncTransferMutex.Lock()
		s.shuffleBuffer = append(toFlush, s.shuffleBuffer...)
		s.syncTransferMutex.Unlock()
	}
	return nil
}

// flushShuffleBuffer swaps out the accumulated buffer, shuffles it lock-free, and dispatches it as
// plan-part-sized batches so transfers from different key-space prefixes are mixed across parts.
func (s *copyTransferProcessor) flushShuffleBuffer() error {
	// Phase 1: swap out the buffer under syncTransferMutex so enumeration can refill immediately.
	s.syncTransferMutex.Lock()
	if len(s.shuffleBuffer) < s.numOfTransfersPerPart {
		// Another goroutine already flushed, nothing to do
		s.syncTransferMutex.Unlock()
		return nil
	}
	// Swap out the buffer — take ownership of the current slice, give the struct a fresh one
	toFlush := s.shuffleBuffer
	threshold := getShuffleThresholdParts()
	newCap := s.numOfTransfersPerPart * threshold
	if newCap < s.numOfTransfersPerPart*2 {
		newCap = s.numOfTransfersPerPart * 2
	}
	s.shuffleBuffer = make([]common.CopyTransfer, 0, newCap)
	s.shuffleBufferSizeInBytes = 0
	s.shuffleBufferFileCounts = common.Transfers{}
	s.syncTransferMutex.Unlock()

	// Phase 2: shuffle lock-free (we exclusively own toFlush); does not block enumeration.
	rand.Shuffle(len(toFlush), func(i, j int) {
		toFlush[i], toFlush[j] = toFlush[j], toFlush[i]
	})

	// Phase 3: dispatch under flushMutex (serializes copyJobTemplate/PartNum/flushWindowCounter).
	s.flushMutex.Lock()
	defer s.flushMutex.Unlock()

	// Track which flush window these batches belong to
	s.flushWindowCounter++
	currentWindow := s.flushWindowCounter

	// Log transfer-level shuffle diagnostics
	if jobsAdmin.JobsAdmin != nil {
		nBatches := len(toFlush) / s.numOfTransfersPerPart
		samples := make([]string, 0, 5)
		step := len(toFlush) / 5
		if step == 0 {
			step = 1
		}
		for i := 0; i < len(toFlush) && len(samples) < 5; i += step {
			src := toFlush[i].Source
			if len(src) > 5 {
				samples = append(samples, src[:5])
			}
		}
		common.LogToJobLogWithPrefix(
			fmt.Sprintf("[ShuffleDiag] Transfer-level flush window #%d: shuffled %d transfers -> %d batches, sample prefixes: %v",
				currentWindow, len(toFlush), nBatches, samples),
			common.LogInfo)
	}

	// Dispatch part-sized batches; high-perf uses async pipeline, non-high-perf stays synchronous.
	for len(toFlush) >= s.numOfTransfersPerPart {
		// Capped 3-index slice: hand out a view (no per-part malloc/copy); backing array frees when
		// the last part drains from dispatchCh.
		n := s.numOfTransfersPerPart
		batch := toFlush[:n:n]
		toFlush = toFlush[n:]

		transfers := common.Transfers{List: batch}
		// Calculate size for this batch
		for _, t := range batch {
			transfers.TotalSizeInBytes += uint64(t.SourceSize)
			switch t.EntityType {
			case common.EEntityType.File():
				transfers.FileTransferCount++
			case common.EEntityType.Folder():
				transfers.FolderTransferCount++
			case common.EEntityType.Symlink():
				transfers.SymlinkTransferCount++
			case common.EEntityType.Hardlink():
				transfers.HardlinksConvertedCount++
			case common.EEntityType.FileProperties():
				transfers.FilePropertyTransferCount++
			}
		}

		if err := s.dispatchPart(dispatchItem{
			transfers: transfers,
			partNum:   s.copyJobTemplate.PartNum,
		}); err != nil {
			return err
		}
		s.copyJobTemplate.PartNum++
	}

	// Wake scanners parked on backpressure; they re-check the watermark (wide hysteresis preserved).
	s.bufferDrainCond.L.Lock()
	s.bufferDrainCond.Broadcast()
	s.bufferDrainCond.L.Unlock()

	// Put any remainder (< numOfTransfersPerPart) back into the buffer
	if len(toFlush) > 0 {
		s.syncTransferMutex.Lock()
		// Prepend remainder to whatever new transfers accumulated while we were flushing
		s.shuffleBuffer = append(toFlush, s.shuffleBuffer...)
		for _, t := range toFlush {
			s.shuffleBufferSizeInBytes += uint64(t.SourceSize)
			switch t.EntityType {
			case common.EEntityType.File():
				s.shuffleBufferFileCounts.FileTransferCount++
			case common.EEntityType.Folder():
				s.shuffleBufferFileCounts.FolderTransferCount++
			case common.EEntityType.Symlink():
				s.shuffleBufferFileCounts.SymlinkTransferCount++
			case common.EEntityType.Hardlink():
				s.shuffleBufferFileCounts.HardlinksConvertedCount++
			case common.EEntityType.FileProperties():
				s.shuffleBufferFileCounts.FilePropertyTransferCount++
			}
		}
		s.syncTransferMutex.Unlock()
	}

	return nil
}

var NothingScheduledError = errors.New("no transfers were scheduled because no files matched the specified criteria")
var FinalPartCreatedMessage = "Final job part has been created"

func (s *copyTransferProcessor) dispatchFinalPart() (copyJobInitiated bool, err error) {
	// Flush any remaining transfers before dispatching the final part
	if UseSyncOrchestrator && useHighPerfSyncPath() && len(s.shuffleBuffer) > 0 {
		s.flushMutex.Lock()

		// Dispatch any remaining full plan parts.
		// The transfer-level shuffle already ran when these transfers were enqueued and the
		// part-buffering/reorder stage has been removed, so the shuffle and non-shuffle paths
		// are identical here — send each assembled part directly. Any dispatch
		// error is surfaced by waitForDispatchPipeline() below.
		for len(s.shuffleBuffer) > s.numOfTransfersPerPart {
			batch := make([]common.CopyTransfer, s.numOfTransfersPerPart)
			copy(batch, s.shuffleBuffer[:s.numOfTransfersPerPart])
			s.shuffleBuffer = s.shuffleBuffer[s.numOfTransfersPerPart:]

			transfers := common.Transfers{List: batch}
			for _, t := range batch {
				transfers.TotalSizeInBytes += uint64(t.SourceSize)
				switch t.EntityType {
				case common.EEntityType.File():
					transfers.FileTransferCount++
				case common.EEntityType.Folder():
					transfers.FolderTransferCount++
				case common.EEntityType.Symlink():
					transfers.SymlinkTransferCount++
				case common.EEntityType.Hardlink():
					transfers.HardlinksConvertedCount++
				case common.EEntityType.FileProperties():
					transfers.FilePropertyTransferCount++
				}
			}
			if err := s.dispatchPart(dispatchItem{
				transfers: transfers,
				partNum:   s.copyJobTemplate.PartNum,
			}); err != nil {
				s.flushMutex.Unlock()
				return false, err
			}
			s.copyJobTemplate.PartNum++
		}

		// Place the last remaining transfers (< numOfTransfersPerPart) into the template for the final part
		s.copyJobTemplate.Transfers = common.Transfers{List: s.shuffleBuffer}
		for _, t := range s.shuffleBuffer {
			s.copyJobTemplate.Transfers.TotalSizeInBytes += uint64(t.SourceSize)
			switch t.EntityType {
			case common.EEntityType.File():
				s.copyJobTemplate.Transfers.FileTransferCount++
			case common.EEntityType.Folder():
				s.copyJobTemplate.Transfers.FolderTransferCount++
			case common.EEntityType.Symlink():
				s.copyJobTemplate.Transfers.SymlinkTransferCount++
			case common.EEntityType.Hardlink():
				s.copyJobTemplate.Transfers.HardlinksConvertedCount++
			case common.EEntityType.FileProperties():
				s.copyJobTemplate.Transfers.FilePropertyTransferCount++
			}
		}
		s.shuffleBuffer = nil
		s.flushMutex.Unlock()
	}

	// Wait for all pipelined parts to finish before sending the final part.
	// The final part must be the last one sent to STE to signal job completion.
	if err := s.waitForDispatchPipeline(); err != nil {
		return false, err
	}

	var resp common.CopyJobPartOrderResponse
	s.copyJobTemplate.IsFinalPart = true
	resp = s.sendPartToSte()

	if !resp.JobStarted {
		if resp.ErrorMsg == common.ECopyJobPartOrderErrorType.NoTransfersScheduledErr() {
			return false, NothingScheduledError
		}

		return false, fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s",
			s.copyJobTemplate.JobID, s.copyJobTemplate.PartNum, resp.ErrorMsg)
	}

	common.LogToJobLogWithPrefix(FinalPartCreatedMessage, common.LogInfo)

	if s.reportFinalPartDispatched != nil {
		s.reportFinalPartDispatched()
	}
	return true, nil
}

// only test the response on the final dispatch to help diagnose root cause of test failures from 0 transfers
func (s *copyTransferProcessor) sendPartToSte() common.CopyJobPartOrderResponse {
	resp := jobsAdmin.ExecuteNewCopyJobPartOrder(*s.copyJobTemplate)

	// if the current part order sent to ste is 0, then alert the progress reporting routine
	if s.copyJobTemplate.PartNum == 0 && s.reportFirstPartDispatched != nil {
		s.reportFirstPartDispatched(resp.JobStarted)
	}

	return resp
}
