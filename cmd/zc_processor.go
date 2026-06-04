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

// getShuffleThresholdParts returns the number of plan parts worth of transfers to buffer
// before performing a shuffle/flush. Default 0 (no shuffle); overridable via AZCOPY_SHUFFLE_THRESHOLD_PARTS.
// Set to a value larger than the expected total plan parts in the job to defer all dispatch
// until enumeration completes (single global shuffle of all transfers in dispatchFinalPart).
var shuffleThresholdLogOnce sync.Once

func getShuffleThresholdParts() int {
	const defaultThreshold = 0
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
}

// pendingPart wraps a plan part's transfers with metadata about its origin,
// used to verify that part-level shuffling interleaves different flush windows.
type pendingPart struct {
	transfers   common.Transfers
	flushWindow uint32 // which shuffle-buffer flush produced this part
	batchIndex  int    // original position within that flush
}

func newCopyTransferProcessor(copyJobTemplate *common.CopyJobPartOrderRequest, numOfTransfersPerPart int, source, destination common.ResourceString, reportFirstPartDispatched func(bool), reportFinalPartDispatched func(), preserveAccessTier, dryrunMode bool) *copyTransferProcessor {
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
	}
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
		// Buffer transfers and shuffle across multiple plan parts to spread key-space prefixes.
		// This prevents the append-only partition access pattern where consecutive plan parts
		// contain consecutive blob prefixes, which concentrates storage partition load.
		// Threshold (in plan parts) is overridable via AZCOPY_SHUFFLE_THRESHOLD_PARTS — set
		// to a value larger than the expected total parts to defer all dispatch until the
		// final part (single global shuffle of all transfers).
		shuffleThreshold := getShuffleThresholdParts()

		var needsFlush bool

		// Hold the mutex only for the append operation, not for the flush
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
			fmt.Printf("[ShuffleConfig] Intermediate flush triggered (bufferLen >= %d * %d)\n", s.numOfTransfersPerPart, shuffleThreshold)
			if err := s.flushShuffleBuffer(); err != nil {
				return err
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

// flushShuffleBuffer shuffles the accumulated transfer buffer and dispatches it as multiple plan parts.
// This ensures transfers from different key-space prefixes are mixed across plan parts,
// preventing the append-only partition access pattern that limits storage throughput.
//
// Thread safety: Holds syncTransferMutex only to swap out the buffer (fast), then releases it
// before the expensive sendPartToSte calls so enumeration goroutines can keep appending.
func (s *copyTransferProcessor) flushShuffleBuffer() error {
	// Serialize flushes — only one goroutine can flush at a time since sendPartToSte
	// uses shared copyJobTemplate state (PartNum, Transfers, etc.)
	s.flushMutex.Lock()
	defer s.flushMutex.Unlock()

	// Take the buffer lock to atomically swap out the buffer, then release immediately
	s.syncTransferMutex.Lock()
	if len(s.shuffleBuffer) < s.numOfTransfersPerPart {
		// Another goroutine already flushed, nothing to do
		s.syncTransferMutex.Unlock()
		return nil
	}
	// Swap out the buffer — take ownership of the current slice, give the struct a fresh one
	toFlush := s.shuffleBuffer
	s.shuffleBuffer = make([]common.CopyTransfer, 0, s.numOfTransfersPerPart*30)
	s.shuffleBufferSizeInBytes = 0
	s.shuffleBufferFileCounts = common.Transfers{}
	s.syncTransferMutex.Unlock()

	// From here on, we own toFlush exclusively — no lock needed for shuffle/dispatch

	// Fisher-Yates shuffle to randomize transfer order across all buffered transfers
	rand.Shuffle(len(toFlush), func(i, j int) {
		toFlush[i], toFlush[j] = toFlush[j], toFlush[i]
	})

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
		batch := toFlush[:s.numOfTransfersPerPart]
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

	// Shuffle part order to interleave different prefix ranges
	rand.Shuffle(len(s.pendingParts), func(i, j int) {
		s.pendingParts[i], s.pendingParts[j] = s.pendingParts[j], s.pendingParts[i]
	})

	// Log post-shuffle dispatch order (first 10 + last 5)
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
		s.copyJobTemplate.Transfers = p.transfers
		resp := s.sendPartToSte()
		if resp.ErrorMsg != "" {
			return errors.New(string(resp.ErrorMsg))
		}

		s.copyJobTemplate.Transfers = common.Transfers{}
		s.copyJobTemplate.PartNum++
	}

	s.pendingParts = s.pendingParts[:0]
	return nil
}

var NothingScheduledError = errors.New("no transfers were scheduled because no files matched the specified criteria")
var FinalPartCreatedMessage = "Final job part has been created"

func (s *copyTransferProcessor) dispatchFinalPart() (copyJobInitiated bool, err error) {
	fmt.Printf("[ShuffleConfig] dispatchFinalPart entered: UseSyncOrchestrator=%v, shuffleBufferLen=%d, pendingPartsLen=%d\n", UseSyncOrchestrator, len(s.shuffleBuffer), len(s.pendingParts))
	// Flush any remaining shuffled transfers before dispatching the final part
	if UseSyncOrchestrator && len(s.shuffleBuffer) > 0 {
		// Wait for any in-progress flush to finish before touching the buffer
		s.flushMutex.Lock()

		// Shuffle the remaining transfers
		rand.Shuffle(len(s.shuffleBuffer), func(i, j int) {
			s.shuffleBuffer[i], s.shuffleBuffer[j] = s.shuffleBuffer[j], s.shuffleBuffer[i]
		})

		// Add full plan parts to pendingParts for interleaved dispatch
		s.flushWindowCounter++
		finalWindow := s.flushWindowCounter
		finalBatchIdx := 0
		for len(s.shuffleBuffer) > s.numOfTransfersPerPart {
			batch := s.shuffleBuffer[:s.numOfTransfersPerPart]
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
			return false, err
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
