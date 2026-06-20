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
	"strconv"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/pkg/errors"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// Backpressure limits to prevent unbounded memory growth.
//
// inMemoryWindowMultiplier sets the high watermark for the scanner backpressure as a
// multiple of the shuffle window (numOfTransfersPerPart * shuffleThreshold). The shuffle
// buffer is the true in-memory accumulator: every flush swaps it out, shuffles it lock-free,
// and dispatches the resulting plan parts asynchronously to STE (the plan files written to
// the SMB share are the durable store, so dispatched parts no longer cost heap). The scanner
// therefore runs unblocked in the common case. It is only parked when dispatch genuinely
// cannot keep up and the buffer grows past this watermark — i.e. a double-buffer worth of
// transfers (one window filling + one window mid-flush). A wide hysteresis band (release once
// the buffer drains below a single window) prevents the per-part on/off thrashing that caused
// the throughput sawtooth.
const inMemoryWindowMultiplier = 2

// getShuffleThresholdParts returns the number of plan parts worth of transfers to buffer
// before performing a shuffle/flush. Default 0 (no shuffle); overridable via AZCOPY_SHUFFLE_THRESHOLD_PARTS.
// Set to a value larger than the expected total plan parts in the job to defer all dispatch
// until enumeration completes (single global shuffle of all transfers in dispatchFinalPart).
var shuffleThresholdLogOnce sync.Once

func getShuffleThresholdParts() int {
	const defaultThreshold = 30
	effective := defaultThreshold
	rawValue := common.GetEnvironmentVariable(common.EEnvironmentVariable.ShuffleThresholdParts())
	if rawValue != "" {
		if n, err := strconv.Atoi(rawValue); err == nil && n >= 0 {
			effective = n
		}
	}
	shuffleThresholdLogOnce.Do(func() {
		fmt.Printf("[ShuffleConfig] AZCOPY_SHUFFLE_THRESHOLD_PARTS raw=%q effective=%d\n", rawValue, effective)
	})
	return effective
}

var shuffleEnabledLogOnce sync.Once

func isShuffleEnabled() bool {
	enabled := false
	rawValue := common.GetEnvironmentVariable(common.EEnvironmentVariable.ShuffleTransfers())
	if rawValue != "" {
		switch strings.ToLower(rawValue) {
		case "true", "1":
			enabled = true
		}
	}
	shuffleEnabledLogOnce.Do(func() {
		fmt.Printf("[ShuffleConfig] AZCOPY_SHUFFLE_TRANSFERS raw=%q enabled=%v\n", rawValue, enabled)
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

	// pendingParts buffers assembled plan parts before sending to STE.
	// Parts from different shuffle-buffer flush windows are interleaved (shuffled)
	// so the STE processes diverse prefix ranges concurrently.
	pendingParts       []pendingPart
	flushWindowCounter uint32 // monotonically increasing flush window ID for diagnostics

	// Pipelined dispatch: parts are pushed into dispatchCh and a pool of background goroutines
	// calls sendPartToSte asynchronously, allowing the shuffle buffer to refill concurrently.
	dispatchCh   chan dispatchItem
	dispatchOnce sync.Once
	dispatchErr  error         // first error from dispatch goroutine
	dispatchWg   sync.WaitGroup // tracks all dispatch workers
	dispatchDone chan struct{}  // closed when all dispatch workers exit

	// Backpressure signaling
	bufferDrainCond *sync.Cond // condition variable signaled when pendingParts or shuffleBuffer drain below limits
}

// pendingPart wraps a plan part's transfers with metadata about its origin,
// used to verify that part-level shuffling interleaves different flush windows.
type pendingPart struct {
	transfers   common.Transfers
	flushWindow uint32 // which shuffle-buffer flush produced this part
	batchIndex  int    // original position within that flush
}

func newCopyTransferProcessor(copyJobTemplate *common.CopyJobPartOrderRequest, numOfTransfersPerPart int, source, destination common.ResourceString, reportFirstPartDispatched func(bool), reportFinalPartDispatched func(), preserveAccessTier, dryrunMode bool) *copyTransferProcessor {
	m := &sync.Mutex{}
	return &copyTransferProcessor{
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
		dispatchCh:                make(chan dispatchItem, 1000), // buffer 1000 parts for async dispatch
		dispatchDone:              make(chan struct{}),
		bufferDrainCond:           sync.NewCond(m),
	}
}

// startDispatchPipeline ensures the background dispatch worker pool is running.
// Called lazily on first use via sync.Once.
func (s *copyTransferProcessor) startDispatchPipeline() {
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
		// Build a local copy of the template for this part
		template := *s.copyJobTemplate
		template.Transfers = item.transfers
		template.PartNum = item.partNum

		var resp common.CopyJobPartOrderResponse
		Rpc(common.ERpcCmd.CopyJobPartOrder(), &template, &resp)

		// Report first part dispatched if this is part 0
		if item.partNum == 0 && s.reportFirstPartDispatched != nil {
			s.reportFirstPartDispatched(resp.JobStarted)
		}

		if resp.ErrorMsg != "" {
			s.dispatchErr = errors.New(string(resp.ErrorMsg))
		}
	}
}

// waitForDispatchPipeline closes the dispatch channel and waits for all workers to finish.
// Returns the first error encountered during async dispatch.
func (s *copyTransferProcessor) waitForDispatchPipeline() error {
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
		common.IffNotNil(d.SourceSize, 0),
		common.IffNotNil(d.HttpHeaders.BlobContentType, ""),
		common.IffNotNil(d.HttpHeaders.BlobContentEncoding, ""),
		common.IffNotNil(d.HttpHeaders.BlobContentDisposition, ""),
		common.IffNotNil(d.HttpHeaders.BlobContentLanguage, ""),
		common.IffNotNil(d.HttpHeaders.BlobCacheControl, ""),
		d.HttpHeaders.BlobContentMD5,
		d.BlobTags,
		d.Metadata,
		common.IffNotNil(d.BlobTier, ""),
		common.IffNotNil(d.BlobVersion, ""),
		common.IffNotNil(d.BlobSnapshot, ""),
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

	if UseSyncOrchestrator {
		if isShuffleEnabled() {
			// Buffer transfers and shuffle across multiple plan parts to spread key-space prefixes.
			shuffleThreshold := getShuffleThresholdParts()

			// Last-resort backpressure (anti-sawtooth): do NOT block the scanner per-part.
			// Enumerated transfers stream into the shuffle buffer, get chopped into plan parts,
			// and are dispatched asynchronously — the plan files on the SMB share are the durable
			// store, so the scanner does not need to wait on dispatch in the common case. We only
			// park the scanner when dispatch genuinely cannot keep up and the in-memory buffer
			// exceeds a double-buffer worth of transfers. Woken scanners re-check this loop
			// condition, so the wide hysteresis band (drain below one window before refilling past
			// two) is preserved and the boundary thrashing that caused the sawtooth is eliminated.
			highWatermark := s.numOfTransfersPerPart * shuffleThreshold * inMemoryWindowMultiplier
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
			// No shuffle, no pendingParts buffering — bounded O(numOfTransfersPerPart) memory.
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

		// Pipeline: async dispatch
		s.startDispatchPipeline()
		s.dispatchCh <- dispatchItem{
			transfers: transfers,
			partNum:   s.copyJobTemplate.PartNum,
		}
		s.copyJobTemplate.PartNum++

		if s.dispatchErr != nil {
			return s.dispatchErr
		}
	}

	// Put remainder back
	if len(toFlush) > 0 {
		s.syncTransferMutex.Lock()
		s.shuffleBuffer = append(toFlush, s.shuffleBuffer...)
		s.syncTransferMutex.Unlock()
	}
	return nil
}

// flushShuffleBuffer shuffles the accumulated transfer buffer and dispatches it as multiple plan parts.
// This ensures transfers from different key-space prefixes are mixed across plan parts,
// preventing the append-only partition access pattern that limits storage throughput.
//
// Thread safety: The buffer swap is protected by syncTransferMutex (microseconds).
// The Fisher-Yates shuffle runs lock-free on the exclusively-owned slice so enumeration
// goroutines can continue filling the next buffer concurrently.
// Only the dispatch phase acquires flushMutex to serialize sendPartToSte calls.
func (s *copyTransferProcessor) flushShuffleBuffer() error {
	// Phase 1: Fast buffer swap (microseconds, protected by syncTransferMutex only).
	// This allows enumeration to immediately start filling the next buffer.
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

	// Phase 2: Shuffle without any lock — we exclusively own toFlush.
	// This is the expensive part (~seconds for millions of items) but it no longer
	// blocks enumeration since syncTransferMutex was already released above.
	rand.Shuffle(len(toFlush), func(i, j int) {
		toFlush[i], toFlush[j] = toFlush[j], toFlush[i]
	})

	// Phase 3: Dispatch under flushMutex — serializes access to shared state
	// (pendingParts, copyJobTemplate, PartNum, flushWindowCounter).
	s.flushMutex.Lock()
	defer s.flushMutex.Unlock()

	// Track which flush window these batches belong to
	s.flushWindowCounter++
	currentWindow := s.flushWindowCounter
	batchIdx := 0

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
		jobsAdmin.JobsAdmin.LogToJobLog(
			fmt.Sprintf("[ShuffleDiag] Transfer-level flush window #%d: shuffled %d transfers -> %d batches, pending total will be %d, sample prefixes: %v",
				currentWindow, len(toFlush), nBatches, len(s.pendingParts)+nBatches, samples),
			common.LogInfo)
	}

	// Dispatch in plan-part-sized batches
	for len(toFlush) >= s.numOfTransfersPerPart {
		// Copy batch to a new right-sized slice to avoid retaining the entire toFlush array
		batch := make([]common.CopyTransfer, s.numOfTransfersPerPart)
		copy(batch, toFlush[:s.numOfTransfersPerPart])
		toFlush = toFlush[s.numOfTransfersPerPart:]

		s.copyJobTemplate.Transfers = common.Transfers{List: batch}
		// Calculate size for this batch
		for _, t := range batch {
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

		// Buffer the part instead of sending immediately; parts will be
		// shuffled across multiple flush windows in flushPendingParts.
		s.pendingParts = append(s.pendingParts, pendingPart{
			transfers:   s.copyJobTemplate.Transfers,
			flushWindow: currentWindow,
			batchIndex:  batchIdx,
		})
		batchIdx++
		s.copyJobTemplate.Transfers = common.Transfers{}
	}

	// Flush pending parts if we've accumulated enough to interleave different prefix ranges
	const partReorderThreshold = 100
	if len(s.pendingParts) >= partReorderThreshold {
		if err := s.flushPendingParts(); err != nil {
			return err
		}
	}

	// Wake any scanner goroutines parked on the high-watermark backpressure: this flush
	// swapped out a full window and dispatched it, so the in-memory shuffle buffer has drained.
	// Woken scanners re-check the high-watermark loop condition, so the wide hysteresis band is
	// preserved — they only re-park if the buffer is still above the double-buffer cap.
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

// flushPendingParts shuffles the order of buffered plan parts and sends them to the STE.
// By interleaving parts from different shuffle-buffer flush windows, the STE processes
// diverse prefix ranges concurrently rather than sweeping through them sequentially.
// Must be called while holding flushMutex.
func (s *copyTransferProcessor) flushPendingParts() error {
	if len(s.pendingParts) == 0 {
		return nil
	}

	// Log pre-shuffle state: how many parts from which flush windows
	if jobsAdmin.JobsAdmin != nil {
		windowCounts := make(map[uint32]int)
		for _, p := range s.pendingParts {
			windowCounts[p.flushWindow]++
		}
		jobsAdmin.JobsAdmin.LogToJobLog(
			fmt.Sprintf("[ShuffleDiag] Part-level flush: shuffling %d pending parts from %d flush windows: %v",
				len(s.pendingParts), len(windowCounts), windowCounts),
			common.LogInfo)
	}

	// Part-level shuffle disabled: transfer-level shuffle in flushShuffleBuffer already
	// ensures each batch has diverse prefixes, so reordering batches adds no value.

	// Log dispatch order (first 10 + last 5)
	if jobsAdmin.JobsAdmin != nil {
		var order strings.Builder
		for i, p := range s.pendingParts {
			if i >= 10 && i < len(s.pendingParts)-5 {
				if i == 10 {
					order.WriteString("...")
				}
				continue
			}
			if order.Len() > 0 {
				order.WriteString(", ")
			}
			firstSrc := ""
			if len(p.transfers.List) > 0 {
				src := p.transfers.List[0].Source
				if len(src) > 5 {
					firstSrc = src[:5]
				}
			}
			fmt.Fprintf(&order, "fw%d/b%d(%s)", p.flushWindow, p.batchIndex, firstSrc)
		}
		jobsAdmin.JobsAdmin.LogToJobLog(
			fmt.Sprintf("[ShuffleDiag] Part dispatch order (PartNum %d+): [%s]",
				s.copyJobTemplate.PartNum, order.String()),
			common.LogInfo)
	}

	for _, p := range s.pendingParts {
		// Pipeline: push to dispatch channel for async processing.
		// The dispatch goroutine handles sendPartToSte (with fsync) in the background,
		// allowing the next shuffle buffer to accumulate concurrently.
		s.startDispatchPipeline()
		s.dispatchCh <- dispatchItem{
			transfers: p.transfers,
			partNum:   s.copyJobTemplate.PartNum,
		}
		s.copyJobTemplate.PartNum++
	}

	// Check if dispatch goroutine hit an error
	if s.dispatchErr != nil {
		s.pendingParts = s.pendingParts[:0]
		return s.dispatchErr
	}

	s.pendingParts = s.pendingParts[:0]
	return nil
}

var NothingScheduledError = errors.New("no transfers were scheduled because no files matched the specified criteria")
var FinalPartCreatedMessage = "Final job part has been created"

func (s *copyTransferProcessor) dispatchFinalPart() (copyJobInitiated bool, err error) {
	fmt.Printf("[ShuffleConfig] dispatchFinalPart entered: UseSyncOrchestrator=%v, shuffleBufferLen=%d, pendingPartsLen=%d\n", UseSyncOrchestrator, len(s.shuffleBuffer), len(s.pendingParts))
	// Flush any remaining transfers before dispatching the final part
	if UseSyncOrchestrator && len(s.shuffleBuffer) > 0 {
		s.flushMutex.Lock()

		if isShuffleEnabled() {
			// Add full plan parts to pendingParts for interleaved dispatch
			s.flushWindowCounter++
			finalWindow := s.flushWindowCounter
			finalBatchIdx := 0
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
				s.pendingParts = append(s.pendingParts, pendingPart{
					transfers:   transfers,
					flushWindow: finalWindow,
					batchIndex:  finalBatchIdx,
				})
				finalBatchIdx++
			}

			// Flush all pending parts (shuffled order) before dispatching the final part
			if err := s.flushPendingParts(); err != nil {
				s.flushMutex.Unlock()
				return false, err
			}
		} else {
			// Direct dispatch: send remaining full parts via pipeline
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
				s.startDispatchPipeline()
				s.dispatchCh <- dispatchItem{
					transfers: transfers,
					partNum:   s.copyJobTemplate.PartNum,
				}
				s.copyJobTemplate.PartNum++
			}
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

	if jobsAdmin.JobsAdmin != nil {
		jobsAdmin.JobsAdmin.LogToJobLog(FinalPartCreatedMessage, common.LogInfo)
	}

	if s.reportFinalPartDispatched != nil {
		s.reportFinalPartDispatched()
	}
	return true, nil
}

// only test the response on the final dispatch to help diagnose root cause of test failures from 0 transfers
func (s *copyTransferProcessor) sendPartToSte() common.CopyJobPartOrderResponse {
	var resp common.CopyJobPartOrderResponse
	Rpc(common.ERpcCmd.CopyJobPartOrder(), s.copyJobTemplate, &resp)

	// if the current part order sent to ste is 0, then alert the progress reporting routine
	if s.copyJobTemplate.PartNum == 0 && s.reportFirstPartDispatched != nil {
		s.reportFirstPartDispatched(resp.JobStarted)
	}

	return resp
}
