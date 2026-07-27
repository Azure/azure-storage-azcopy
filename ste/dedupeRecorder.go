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

package ste

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// This file owns the per-job state shared by the block-level dedupe prototype. Observe mode
// records and measures potential hits without changing transfers; act mode records only committed
// destination blocks and uses them to make safe staging decisions.

// dedupeJobState holds a single job's dedupe tables plus cumulative would-be-hit counters.
type dedupeJobState struct {
	smallProgressMu sync.Mutex

	// table is the Phase 1 measurement table: blocks are recorded pre-write purely to count the
	// would-be-hit rate, so it must NOT be used to decide a real reference (the content may not yet
	// exist at the destination).
	table *common.DedupeHashTable

	// committed is the Phase 2 table: it holds only blocks confirmed committed at the destination
	// (recorded in the sender Epilogue after a successful CommitBlockList). A reference is only ever
	// served from an entry in this table, so the target content is guaranteed to exist.
	committed *common.DedupeHashTable

	// Counters are cumulative across every blob processed for the job, and are updated
	// atomically because transfers (and therefore observeSourceGrid calls) run concurrently.
	hashedBlocks   int64 // blocks that carried crc64+sha256 (i.e. were eligible for dedupe)
	wouldBeHits    int64 // eligible blocks whose content was already recorded by an earlier block
	dedupableBytes int64 // total size of would-be-hit blocks: bytes that need not be re-transferred

	// Phase 2 "act" counters (also cumulative + atomic). Together they quantify how many source
	// reads dedupe actually avoided (enforce) or would avoid (shadow).
	referencedBlocks        int64 // enforce: blocks staged from the destination instead of the source
	referencedBytes         int64 // enforce: source-read bytes avoided
	wouldReferenceBlocks    int64 // shadow: blocks that would be referenced under enforce
	wouldReferenceBytes     int64 // shadow: source-read bytes that would be avoided under enforce
	sourceStagedBlocks      int64 // blocks staged from the source (real source reads)
	sourceStagedBytes       int64 // bytes staged from the source
	fallbackBlocks          int64 // enforce: hits whose target reference failed and fell back to source
	filesStarted            int64 // files armed for source-grid dedupe
	filesCommitted          int64 // files whose destination block list was committed
	smallFilesStarted       int64 // Blob-to-Blob files smaller than 4 MiB that entered transfer handling
	smallFilesCompleted     int64 // small files that completed successfully
	smallFilesFailed        int64 // small files that completed with a failure status
	smallFilesSkipped       int64 // small files skipped by transfer policy
	smallFilesCanceled      int64 // small files canceled before completion
	smallFileBytesStarted   int64
	smallFileBytesCompleted int64
	smallFileBytesFailed    int64
	smallFileBytesSkipped   int64
	smallFileBytesCanceled  int64
	progressEventsDropped   uint64

	actMode int32 // active dedupeActMode for this job
}

// addReferenced records a block that enforce mode staged from the destination (a source read avoided).
func (s *dedupeJobState) addReferenced(size int64) {
	atomic.AddInt64(&s.referencedBlocks, 1)
	atomic.AddInt64(&s.referencedBytes, size)
}

// addWouldReference records a block that shadow mode would have referenced under enforce.
func (s *dedupeJobState) addWouldReference(size int64) {
	atomic.AddInt64(&s.wouldReferenceBlocks, 1)
	atomic.AddInt64(&s.wouldReferenceBytes, size)
}

// addSourceStaged records a block that was actually staged from the source (a real source read).
func (s *dedupeJobState) addSourceStaged(size int64) {
	atomic.AddInt64(&s.sourceStagedBlocks, 1)
	atomic.AddInt64(&s.sourceStagedBytes, size)
}

// addFallback records an enforce hit whose target reference failed and fell back to the source.
func (s *dedupeJobState) addFallback() {
	atomic.AddInt64(&s.fallbackBlocks, 1)
}

func (s *dedupeJobState) addFileStarted() {
	atomic.AddInt64(&s.filesStarted, 1)
}

func (s *dedupeJobState) addFileCommitted() {
	atomic.AddInt64(&s.filesCommitted, 1)
}

func (s *dedupeJobState) addSmallFileStarted(size int64) {
	s.smallProgressMu.Lock()
	defer s.smallProgressMu.Unlock()
	s.addSmallFileStartedLocked(size)
}

func (s *dedupeJobState) addSmallFileResult(status common.TransferStatus, size int64) string {
	s.smallProgressMu.Lock()
	defer s.smallProgressMu.Unlock()
	return s.addSmallFileResultLocked(status, size)
}

func (s *dedupeJobState) addSmallFileStartedLocked(size int64) {
	s.smallFilesStarted++
	s.smallFileBytesStarted += size
}

func (s *dedupeJobState) addSmallFileResultLocked(status common.TransferStatus, size int64) string {
	switch status {
	case common.ETransferStatus.Success():
		s.smallFilesCompleted++
		s.smallFileBytesCompleted += size
		return "small_file_transfer_complete"
	case common.ETransferStatus.Failed(),
		common.ETransferStatus.BlobTierFailure(),
		common.ETransferStatus.TierAvailabilityCheckFailure():
		s.smallFilesFailed++
		s.smallFileBytesFailed += size
		return "small_file_transfer_failed"
	case common.ETransferStatus.SkippedEntityAlreadyExists(),
		common.ETransferStatus.SkippedBlobHasSnapshots(),
		common.ETransferStatus.SkippedArchiveNotRestored():
		s.smallFilesSkipped++
		s.smallFileBytesSkipped += size
		return "small_file_transfer_skipped"
	case common.ETransferStatus.Cancelled():
		s.smallFilesCanceled++
		s.smallFileBytesCanceled += size
		return "small_file_transfer_canceled"
	default:
		return ""
	}
}

type dedupeProgressSnapshot struct {
	referencedBlocks        int64
	referencedBytes         int64
	wouldReferenceBlocks    int64
	wouldReferenceBytes     int64
	sourceStagedBlocks      int64
	sourceStagedBytes       int64
	fallbackBlocks          int64
	filesStarted            int64
	filesCommitted          int64
	smallFilesStarted       int64
	smallFilesCompleted     int64
	smallFilesFailed        int64
	smallFilesSkipped       int64
	smallFilesCanceled      int64
	smallFileBytesStarted   int64
	smallFileBytesCompleted int64
	smallFileBytesFailed    int64
	smallFileBytesSkipped   int64
	smallFileBytesCanceled  int64
}

func (s *dedupeJobState) progressSnapshot() dedupeProgressSnapshot {
	s.smallProgressMu.Lock()
	defer s.smallProgressMu.Unlock()
	return s.progressSnapshotLocked()
}

func (s *dedupeJobState) progressSnapshotLocked() dedupeProgressSnapshot {
	return dedupeProgressSnapshot{
		referencedBlocks:        atomic.LoadInt64(&s.referencedBlocks),
		referencedBytes:         atomic.LoadInt64(&s.referencedBytes),
		wouldReferenceBlocks:    atomic.LoadInt64(&s.wouldReferenceBlocks),
		wouldReferenceBytes:     atomic.LoadInt64(&s.wouldReferenceBytes),
		sourceStagedBlocks:      atomic.LoadInt64(&s.sourceStagedBlocks),
		sourceStagedBytes:       atomic.LoadInt64(&s.sourceStagedBytes),
		fallbackBlocks:          atomic.LoadInt64(&s.fallbackBlocks),
		filesStarted:            atomic.LoadInt64(&s.filesStarted),
		filesCommitted:          atomic.LoadInt64(&s.filesCommitted),
		smallFilesStarted:       s.smallFilesStarted,
		smallFilesCompleted:     s.smallFilesCompleted,
		smallFilesFailed:        s.smallFilesFailed,
		smallFilesSkipped:       s.smallFilesSkipped,
		smallFilesCanceled:      s.smallFilesCanceled,
		smallFileBytesStarted:   s.smallFileBytesStarted,
		smallFileBytesCompleted: s.smallFileBytesCompleted,
		smallFileBytesFailed:    s.smallFileBytesFailed,
		smallFileBytesSkipped:   s.smallFileBytesSkipped,
		smallFileBytesCanceled:  s.smallFileBytesCanceled,
	}
}

func (s dedupeProgressSnapshot) fields(mode dedupeActMode) string {
	targetBlocks := s.referencedBlocks
	targetBytes := s.referencedBytes
	transferredBlocks := s.referencedBlocks + s.sourceStagedBlocks
	transferredBytes := s.referencedBytes + s.sourceStagedBytes
	if mode == dedupeActShadow {
		targetBlocks = s.wouldReferenceBlocks
		targetBytes = s.wouldReferenceBytes
		transferredBlocks = s.sourceStagedBlocks
		transferredBytes = s.sourceStagedBytes
	}
	smallFilesInProgress := s.smallFilesStarted -
		s.smallFilesCompleted -
		s.smallFilesFailed -
		s.smallFilesSkipped -
		s.smallFilesCanceled
	if smallFilesInProgress < 0 {
		smallFilesInProgress = 0
	}
	smallFileBytesInProgress := s.smallFileBytesStarted -
		s.smallFileBytesCompleted -
		s.smallFileBytesFailed -
		s.smallFileBytesSkipped -
		s.smallFileBytesCanceled
	if smallFileBytesInProgress < 0 {
		smallFileBytesInProgress = 0
	}

	return fmt.Sprintf(
		"filesStarted=%d filesCommitted=%d targetURIBlocks=%d targetURIBytes=%d "+
			"sourceURIBlocks=%d sourceURIBytes=%d fallbackBlocks=%d transferredBlocks=%d "+
			"transferredBytes=%d wanSavingsPercent=%.1f smallFilesStarted=%d "+
			"smallFilesCompleted=%d smallFilesFailed=%d smallFilesSkipped=%d "+
			"smallFilesCanceled=%d smallFilesInProgress=%d smallFileBytesStarted=%d "+
			"smallFileBytesCompleted=%d smallFileBytesFailed=%d smallFileBytesSkipped=%d "+
			"smallFileBytesCanceled=%d smallFileBytesInProgress=%d",
		s.filesStarted, s.filesCommitted, targetBlocks, targetBytes,
		s.sourceStagedBlocks, s.sourceStagedBytes, s.fallbackBlocks, transferredBlocks,
		transferredBytes, dedupePercent(targetBytes, transferredBytes),
		s.smallFilesStarted, s.smallFilesCompleted, s.smallFilesFailed,
		s.smallFilesSkipped, s.smallFilesCanceled, smallFilesInProgress,
		s.smallFileBytesStarted, s.smallFileBytesCompleted, s.smallFileBytesFailed,
		s.smallFileBytesSkipped, s.smallFileBytesCanceled, smallFileBytesInProgress)
}

const dedupeProgressPrefix = "DEDUPE_PROGRESS"
const smallFileProgressThresholdBytes = int64(4 * 1024 * 1024)
const dedupeProgressQueueCapacity = 4096
const dedupeProgressFinalWriteTimeout = 5 * time.Second
const dedupeLogDirectoryName = "dedupe-logs"
const dedupeLogMaxSize = 500 * 1024 * 1024

type dedupeProgressEntry struct {
	jobID           common.JobID
	message         string
	closeAfterWrite bool
	done            chan error
}

type dedupeProgressFileSink struct {
	rootFolder func() string
	writers    map[common.JobID]io.WriteCloser
}

func newDedupeProgressFileSink(rootFolder func() string) *dedupeProgressFileSink {
	return &dedupeProgressFileSink{
		rootFolder: rootFolder,
		writers:    make(map[common.JobID]io.WriteCloser),
	}
}

func (s *dedupeProgressFileSink) logPath(jobID common.JobID) (string, error) {
	if s == nil || s.rootFolder == nil {
		return "", fmt.Errorf("dedupe log root is not configured")
	}
	root := s.rootFolder()
	if root == "" {
		return "", fmt.Errorf("AzCopy log folder is not configured")
	}
	return filepath.Join(root, dedupeLogDirectoryName, jobID.String()+".log"), nil
}

func (s *dedupeProgressFileSink) writer(jobID common.JobID) (io.WriteCloser, error) {
	if writer, ok := s.writers[jobID]; ok {
		return writer, nil
	}

	logPath, err := s.logPath(jobID)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(logPath), os.ModeDir|os.ModePerm); err != nil {
		return nil, fmt.Errorf("create dedupe log directory: %w", err)
	}

	writer, err := common.NewRotatingWriter(logPath, dedupeLogMaxSize)
	if err != nil {
		return nil, fmt.Errorf("open dedupe log: %w", err)
	}
	s.writers[jobID] = writer
	return writer, nil
}

func (s *dedupeProgressFileSink) write(jobID common.JobID, message string) error {
	writer, err := s.writer(jobID)
	if err != nil {
		return err
	}
	return writeDedupeProgressTo(writer, message)
}

func (s *dedupeProgressFileSink) close(jobID common.JobID) error {
	writer, ok := s.writers[jobID]
	if !ok {
		return nil
	}
	delete(s.writers, jobID)
	return writer.Close()
}

var (
	dedupeJobsMu              sync.Mutex
	dedupeJobs                          = make(map[common.JobID]*dedupeJobState)
	dedupeProgressErrorOutput io.Writer = os.Stderr
	dedupeProgressSanitizer             = common.NewAzCopyLogSanitizer()
	dedupeProgressQueue                 = make(chan dedupeProgressEntry, dedupeProgressQueueCapacity)
	dedupeProgressSink                  = newDedupeProgressFileSink(func() string {
		return common.AzcopyLogFolder
	})
)

type dedupeJobPartCompletionTracker map[PartNumber]struct{}

func (t dedupeJobPartCompletionTracker) record(partNum *PartNumber) bool {
	if partNum == nil {
		return false
	}
	if _, exists := t[*partNum]; exists {
		return false
	}
	t[*partNum] = struct{}{}
	return true
}

func (t dedupeJobPartCompletionTracker) allKnownPartsReported(knownParts uint32) bool {
	return knownParts > 0 && uint32(len(t)) == knownParts
}

func (t dedupeJobPartCompletionTracker) reset() {
	clear(t)
}

func init() {
	go drainDedupeProgress()
}

func writeDedupeProgressTo(output io.Writer, message string) error {
	_, err := fmt.Fprintln(output, dedupeProgressSanitizer.SanitizeLogMessage(message))
	return err
}

func tryEnqueueDedupeProgress(queue chan<- dedupeProgressEntry, entry dedupeProgressEntry) bool {
	select {
	case queue <- entry:
		return true
	default:
		return false
	}
}

func enqueueDedupeProgress(jobID common.JobID, message string) {
	entry := dedupeProgressEntry{jobID: jobID, message: message}
	if !tryEnqueueDedupeProgress(dedupeProgressQueue, entry) {
		atomic.AddUint64(&dedupeStateForJob(jobID).progressEventsDropped, 1)
	}
}

func drainDedupeProgress() {
	for entry := range dedupeProgressQueue {
		if st, ok := dedupeStateForJobIfExists(entry.jobID); ok {
			if dropped := atomic.SwapUint64(&st.progressEventsDropped, 0); dropped > 0 {
				droppedMessage := fmt.Sprintf(
					"%s event=output_dropped jobId=%s count=%d",
					dedupeProgressPrefix, entry.jobID.String(), dropped)
				if err := dedupeProgressSink.write(entry.jobID, droppedMessage); err != nil {
					_, _ = fmt.Fprintf(dedupeProgressErrorOutput,
						"%s event=output_error jobId=%s reason=%q\n",
						dedupeProgressPrefix, entry.jobID.String(), err.Error())
				}
			}
		}

		err := dedupeProgressSink.write(entry.jobID, entry.message)
		if err != nil {
			_, _ = fmt.Fprintf(dedupeProgressErrorOutput,
				"%s event=output_error jobId=%s reason=%q\n",
				dedupeProgressPrefix, entry.jobID.String(), err.Error())
		}
		if entry.closeAfterWrite {
			if closeErr := dedupeProgressSink.close(entry.jobID); closeErr != nil {
				_, _ = fmt.Fprintf(dedupeProgressErrorOutput,
					"%s event=output_error jobId=%s reason=%q\n",
					dedupeProgressPrefix, entry.jobID.String(), closeErr.Error())
				if err == nil {
					err = closeErr
				}
			}
		}
		if entry.done != nil {
			entry.done <- err
		}
	}
}

func writeFinalDedupeProgress(jobID common.JobID, message string) error {
	done := make(chan error, 1)
	entry := dedupeProgressEntry{
		jobID:           jobID,
		message:         message,
		closeAfterWrite: true,
		done:            done,
	}
	timer := time.NewTimer(dedupeProgressFinalWriteTimeout)
	defer timer.Stop()

	select {
	case dedupeProgressQueue <- entry:
	case <-timer.C:
		return fmt.Errorf("timed out queueing final dedupe progress")
	}

	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("timed out writing final dedupe progress")
	}
}

func emitDedupeProgress(jptm IJobPartTransferMgr, message string) {
	jptm.LogAtLevelForCurrentTransfer(common.LogDebug, message)
	enqueueDedupeProgress(jptm.Info().JobID, message)
}

func isSmallFileProgressTransfer(jptm IJobPartTransferMgr) bool {
	if jptm == nil || jptm.Info() == nil {
		return false
	}
	fromTo := jptm.FromTo()
	info := jptm.Info()
	sourceSize := info.SourceSize
	return fromTo.From() == common.ELocation.Blob() &&
		fromTo.To() == common.ELocation.Blob() &&
		(info.EntityType == common.EEntityType.File() ||
			info.EntityType == common.EEntityType.Hardlink()) &&
		sourceSize >= 0 &&
		sourceSize < smallFileProgressThresholdBytes
}

type smallFileProgressTracker interface {
	markSmallFileProgressStarted() bool
	smallFileProgressStarted() bool
}

func (jptm *jobPartTransferMgr) markSmallFileProgressStarted() bool {
	return atomic.CompareAndSwapUint32(&jptm.atomicSmallFileProgressStarted, 0, 1)
}

func (jptm *jobPartTransferMgr) smallFileProgressStarted() bool {
	return atomic.LoadUint32(&jptm.atomicSmallFileProgressStarted) == 1
}

func emitSmallFileTransferStart(jptm IJobPartTransferMgr) {
	if !isSmallFileProgressTransfer(jptm) {
		return
	}
	st, ok := dedupeStateForJobIfExists(jptm.Info().JobID)
	if !ok {
		return
	}
	mode := dedupeActMode(atomic.LoadInt32(&st.actMode))
	if mode == dedupeActOff {
		return
	}
	tracker, ok := jptm.(smallFileProgressTracker)
	if !ok || !tracker.markSmallFileProgressStarted() {
		return
	}

	st.smallProgressMu.Lock()
	defer st.smallProgressMu.Unlock()
	st.addSmallFileStartedLocked(jptm.Info().SourceSize)
	emitDedupeProgress(jptm, dedupeProgressMessage(
		"small_file_transfer_start",
		mode,
		jptm.Info(),
		fmt.Sprintf(
			"status=%q fileBytes=%d thresholdBytes=%d",
			"InProgress",
			jptm.Info().SourceSize,
			smallFileProgressThresholdBytes,
		),
		st.progressSnapshotLocked()))
}

func emitSmallFileTransferResult(
	jptm IJobPartTransferMgr,
	status common.TransferStatus,
) {
	if !isSmallFileProgressTransfer(jptm) {
		return
	}
	tracker, ok := jptm.(smallFileProgressTracker)
	if !ok || !tracker.smallFileProgressStarted() {
		return
	}
	st, ok := dedupeStateForJobIfExists(jptm.Info().JobID)
	if !ok {
		return
	}
	mode := dedupeActMode(atomic.LoadInt32(&st.actMode))
	if mode == dedupeActOff {
		return
	}

	status = normalizeSmallFileResultStatus(status, jptm.WasCanceled())

	st.smallProgressMu.Lock()
	defer st.smallProgressMu.Unlock()
	event := st.addSmallFileResultLocked(status, jptm.Info().SourceSize)
	if event == "" {
		return
	}
	emitDedupeProgress(jptm, dedupeProgressMessage(
		event,
		mode,
		jptm.Info(),
		fmt.Sprintf(
			"status=%q fileBytes=%d thresholdBytes=%d",
			status.String(),
			jptm.Info().SourceSize,
			smallFileProgressThresholdBytes,
		),
		st.progressSnapshotLocked()))
}

func normalizeSmallFileResultStatus(
	status common.TransferStatus,
	wasCanceled bool,
) common.TransferStatus {
	if wasCanceled &&
		(status == common.ETransferStatus.NotStarted() ||
			status == common.ETransferStatus.Started() ||
			status == common.ETransferStatus.Restarted()) {
		return common.ETransferStatus.Cancelled()
	}
	return status
}

func emitDedupeTransferFailure(
	jptm IJobPartTransferMgr,
	event string,
	operation string,
	status int,
	requestID string,
	reason string,
	partNumber PartNumber,
	transferIndex uint32,
) {
	st, ok := dedupeStateForJobIfExists(jptm.Info().JobID)
	if !ok {
		return
	}
	mode := dedupeActMode(atomic.LoadInt32(&st.actMode))
	if mode == dedupeActOff {
		return
	}
	details := fmt.Sprintf(
		"operation=%q httpStatus=%d requestId=%q reason=%q partNumber=%d transferIndex=%d",
		operation,
		status,
		requestID,
		reason,
		partNumber,
		transferIndex,
	)
	emitDedupeProgress(jptm, dedupeProgressMessage(
		event,
		mode,
		jptm.Info(),
		details,
		st.progressSnapshot()))
}

func dedupeProgressMessage(event string, mode dedupeActMode, info *TransferInfo, details string, snapshot dedupeProgressSnapshot) string {
	return fmt.Sprintf(
		"%s event=%s mode=%s jobId=%s file=%q destination=%q %s %s",
		dedupeProgressPrefix, event, mode, info.JobID.String(), info.DstFilePath,
		sanitizedDestForDedupe(info.Destination), details, snapshot.fields(mode))
}

// dedupeStateForJob returns the dedupe state for a job, creating it on first use. Callers are
// limited to the observe and act paths, so the default transfer path allocates no dedupe state.
func dedupeStateForJob(jobID common.JobID) *dedupeJobState {
	dedupeJobsMu.Lock()
	defer dedupeJobsMu.Unlock()

	st, ok := dedupeJobs[jobID]
	if !ok {
		st = &dedupeJobState{
			table:     common.NewDedupeHashTable(),
			committed: common.NewDedupeHashTable(),
		}
		dedupeJobs[jobID] = st
	}
	return st
}

func dedupeStateForJobIfExists(jobID common.JobID) (*dedupeJobState, bool) {
	dedupeJobsMu.Lock()
	defer dedupeJobsMu.Unlock()

	st, ok := dedupeJobs[jobID]
	return st, ok
}

func setDedupeActModeForJob(jobID common.JobID, mode dedupeActMode) {
	if mode == dedupeActOff {
		return
	}
	atomic.StoreInt32(&dedupeStateForJob(jobID).actMode, int32(mode))
}

// clearDedupeStateForJob drops a job's dedupe state to release its memory. It is safe to call
// when no state exists and is invoked after a job reaches a terminal status.
func clearDedupeStateForJob(jobID common.JobID) {
	dedupeJobsMu.Lock()
	defer dedupeJobsMu.Unlock()

	if st, ok := dedupeJobs[jobID]; ok {
		st.table.Clear()
		st.committed.Clear()
		delete(dedupeJobs, jobID)
	}
}

// recordCommittedBlocks records a freshly-committed destination blob's hashed blocks into the job's
// committed table, so that later blocks with identical content can be served from this blob. It is
// called from the sender Epilogue after CommitBlockList succeeds, with the destination's real ETag.
// Because the destination is a byte-identical copy of the source, each block lives at the same
// offset/length in the target blob as in the source.
func recordCommittedBlocks(jobID common.JobID, destURI, destinationSAS string, etag azcore.ETag, plan *SourceGridPlan) (recorded int) {
	return recordCommittedBlocksWithObserver(jobID, destURI, destinationSAS, etag, plan, nil)
}

type dedupeTableRecordEvent struct {
	Block       PlannedBlock
	Stored      common.BlockEntry
	Inserted    bool
	PreHit      bool
	TableStats  common.DedupeHashTableStats
	RecordIndex int
}

func recordCommittedBlocksWithObserver(jobID common.JobID, destURI, destinationSAS string, etag azcore.ETag, plan *SourceGridPlan, observer func(dedupeTableRecordEvent)) (recorded int) {
	if plan == nil || etag == "" {
		return 0
	}
	st := dedupeStateForJob(jobID)
	target := destinationWithSASForDedupe(destURI, destinationSAS)
	for _, b := range plan.Blocks {
		if !blockHasHashes(b) {
			continue
		}
		stored, inserted := st.committed.Insert(common.BlockEntry{
			JobID:        jobID,
			CRC64:        b.CRC64,
			SHA256:       b.SHA256,
			TargetURI:    target,
			TargetOffset: b.Offset,
			TargetLength: b.Size,
			ETag:         etag,
		})
		recorded++
		if observer != nil {
			observer(dedupeTableRecordEvent{
				Block:       b,
				Stored:      stored,
				Inserted:    inserted,
				TableStats:  st.committed.StatsForCRC64(b.CRC64),
				RecordIndex: recorded,
			})
		}
	}
	return recorded
}

// destinationWithSASForDedupe returns an executable destination URL for later use as
// x-ms-copy-source. The destination client URL commonly already contains the SAS; merge any
// missing SAS fields without duplicating query parameters.
func destinationWithSASForDedupe(destURI, destinationSAS string) string {
	if destinationSAS == "" {
		return destURI
	}
	u, err := url.Parse(destURI)
	if err != nil {
		return destURI
	}
	sas := destinationSAS
	if sas[0] == '?' {
		sas = sas[1:]
	}
	if sas == "" {
		return destURI
	}

	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return destURI
	}
	sasQuery, err := url.ParseQuery(sas)
	if err != nil {
		return destURI
	}

	for key, values := range sasQuery {
		query[key] = append([]string(nil), values...)
	}
	u.RawQuery = query.Encode()
	return u.String()
}

// logDedupeActSummary logs a running view of the job's cumulative Phase 2 savings. The terminal
// job summary is emitted separately when the job completes.
func logDedupeActSummary(jptm IJobPartTransferMgr, mode dedupeActMode) {
	st := dedupeStateForJob(jptm.Info().JobID)

	referencedBlocks := atomic.LoadInt64(&st.referencedBlocks)
	referencedBytes := atomic.LoadInt64(&st.referencedBytes)
	wouldRefBlocks := atomic.LoadInt64(&st.wouldReferenceBlocks)
	wouldRefBytes := atomic.LoadInt64(&st.wouldReferenceBytes)
	sourceBlocks := atomic.LoadInt64(&st.sourceStagedBlocks)
	sourceBytes := atomic.LoadInt64(&st.sourceStagedBytes)
	fallbacks := atomic.LoadInt64(&st.fallbackBlocks)

	switch mode {
	case dedupeActEnforce:
		// Referenced blocks were staged from the destination; source-staged blocks were read from the
		// source. Total = referenced + source; avoided source-read bytes = referencedBytes.
		totalStagedBytes := referencedBytes + sourceBytes
		jptm.LogAtLevelForCurrentTransfer(common.LogInfo, fmt.Sprintf(
			"dedupe-summary(enforce): dedupe-target blocks=%d bytes=%d; sourceURI blocks=%d bytes=%d; "+
				"WAN savings=%d bytes (%.1f%% of %d staged bytes); fallback blocks=%d",
			referencedBlocks, referencedBytes, sourceBlocks, sourceBytes,
			referencedBytes, dedupePercent(referencedBytes, totalStagedBytes), totalStagedBytes,
			fallbacks))
	case dedupeActShadow:
		// Every block is staged from the source in shadow mode, so sourceBytes is the total; the
		// would-reference blocks are the subset that enforce would have avoided.
		jptm.LogAtLevelForCurrentTransfer(common.LogInfo, fmt.Sprintf(
			"dedupe-summary(shadow): would-dedupe-target blocks=%d bytes=%d; sourceURI blocks=%d bytes=%d; "+
				"potential WAN savings=%d bytes (%.1f%% of %d staged bytes); all bytes currently staged from source",
			wouldRefBlocks, wouldRefBytes, sourceBlocks, sourceBytes,
			wouldRefBytes, dedupePercent(wouldRefBytes, sourceBytes), sourceBytes))
	}
}

func logDedupeJobSummary(jobID common.JobID, log func(common.LogLevel, string)) {
	st, ok := dedupeStateForJobIfExists(jobID)
	if !ok {
		return
	}
	mode := dedupeActMode(atomic.LoadInt32(&st.actMode))
	if mode == dedupeActOff {
		return
	}
	log(common.LogInfo, dedupeJobSummaryMessage(mode, st))
}

func finalizeDedupeJob(
	jobID common.JobID,
	status common.JobStatus,
	transfersCompleted int,
	transfersSkipped int,
	transfersFailed int,
	log func(common.LogLevel, string),
) {
	st, ok := dedupeStateForJobIfExists(jobID)
	if !ok {
		return
	}
	mode := dedupeActMode(atomic.LoadInt32(&st.actMode))
	if mode != dedupeActOff {
		log(common.LogInfo, dedupeJobSummaryMessage(mode, st))
		message := fmt.Sprintf(
			"%s event=job_complete mode=%s jobId=%s status=%q transfersCompleted=%d "+
				"transfersSkipped=%d transfersFailed=%d %s",
			dedupeProgressPrefix,
			mode,
			jobID.String(),
			status.String(),
			transfersCompleted,
			transfersSkipped,
			transfersFailed,
			st.progressSnapshot().fields(mode))
		if err := writeFinalDedupeProgress(jobID, message); err != nil {
			log(common.LogError,
				dedupeProgressPrefix+" event=output_error reason="+fmt.Sprintf("%q", err.Error()))
		}
	}
	clearDedupeStateForJob(jobID)
}

func dedupeJobSummaryMessage(mode dedupeActMode, st *dedupeJobState) string {
	referencedBlocks := atomic.LoadInt64(&st.referencedBlocks)
	referencedBytes := atomic.LoadInt64(&st.referencedBytes)
	wouldRefBlocks := atomic.LoadInt64(&st.wouldReferenceBlocks)
	wouldRefBytes := atomic.LoadInt64(&st.wouldReferenceBytes)
	sourceBlocks := atomic.LoadInt64(&st.sourceStagedBlocks)
	sourceBytes := atomic.LoadInt64(&st.sourceStagedBytes)
	fallbacks := atomic.LoadInt64(&st.fallbackBlocks)

	switch mode {
	case dedupeActShadow:
		totalStagedBytes := sourceBytes
		return fmt.Sprintf(
			"dedupe-job-summary(shadow): totalBlocks=%d wouldTargetURIBlocks=%d sourceURIBlocks=%d fallbackBlocks=%d potentialAvoidedSourceReadBytes=%d totalStagedBytes=%d potentialWanSavingsPercent=%.1f",
			sourceBlocks, wouldRefBlocks, sourceBlocks, fallbacks, wouldRefBytes, totalStagedBytes,
			dedupePercent(wouldRefBytes, totalStagedBytes))
	case dedupeActEnforce:
		totalStagedBytes := referencedBytes + sourceBytes
		return fmt.Sprintf(
			"dedupe-job-summary(enforce): totalBlocks=%d targetURIBlocks=%d sourceURIBlocks=%d fallbackBlocks=%d avoidedSourceReadBytes=%d totalStagedBytes=%d wanSavingsPercent=%.1f",
			referencedBlocks+sourceBlocks, referencedBlocks, sourceBlocks, fallbacks, referencedBytes, totalStagedBytes,
			dedupePercent(referencedBytes, totalStagedBytes))
	default:
		return "dedupe-job-summary(off): disabled"
	}
}

// blockHasHashes reports whether a planned block carried both content hashes in the extended
// GetBlockList response. Presence is tracked separately because an all-zero hash is valid.
func blockHasHashes(b PlannedBlock) bool {
	return b.HasHashes
}

// measureAndRecord performs the Phase 1 lookup-then-record over a set of planned blocks against
// the given table. For each block that carries hashes it (1) looks the content up, counting a
// would-be hit when identical content was already recorded, then (2) records the block keyed to
// where it is being migrated (targetURI). It returns the number of eligible (hashed) blocks, the
// number of would-be hits, and the total size of those hit blocks.
//
// It is deliberately free of any jptm/logging dependency so it can be unit tested directly.
//
// Note: between the Lookup and Insert of a given block another goroutine may insert identical
// content, so concurrent identical blocks can be under-counted as misses. That is acceptable for a
// measurement phase — the reported hit rate is conservative (never over-counted).
func measureAndRecord(table *common.DedupeHashTable, jobID common.JobID, targetURI string, blocks []PlannedBlock) (hashed, hits, dedupableBytes int64) {
	return measureAndRecordWithObserver(table, jobID, targetURI, blocks, nil)
}

func measureAndRecordWithObserver(table *common.DedupeHashTable, jobID common.JobID, targetURI string, blocks []PlannedBlock, observer func(dedupeTableRecordEvent)) (hashed, hits, dedupableBytes int64) {
	for _, b := range blocks {
		if !blockHasHashes(b) {
			continue
		}
		hashed++

		_, hit := table.Lookup(b.CRC64, b.SHA256)
		if hit {
			hits++
			dedupableBytes += b.Size
		}

		stored, inserted := table.Insert(common.BlockEntry{
			JobID:     jobID,
			CRC64:     b.CRC64,
			SHA256:    b.SHA256,
			TargetURI: targetURI,
			// The destination is a byte-identical copy of the source, so the block lives
			// at the same offset/length in the target blob as in the source.
			TargetOffset: b.Offset,
			TargetLength: b.Size,
			// ETag is populated in Phase 2, where recording happens after a successful
			// destination write; Phase 1 records pre-write, for measurement only.
		})
		if observer != nil {
			observer(dedupeTableRecordEvent{
				Block:       b,
				Stored:      stored,
				Inserted:    inserted,
				PreHit:      hit,
				TableStats:  table.StatsForCRC64(b.CRC64),
				RecordIndex: int(hashed),
			})
		}
	}
	return hashed, hits, dedupableBytes
}

// sanitizedDestForDedupe strips the query string from a destination URL before logging or
// comparing it. The committed table intentionally retains an authenticated URL for reuse.
func sanitizedDestForDedupe(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		if index := strings.IndexAny(raw, "?#"); index >= 0 {
			return raw[:index]
		}
		return raw
	}
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	return u.String()
}

// dedupePercent returns hits/total as a percentage, treating total==0 as 0%.
func dedupePercent(hits, total int64) float64 {
	if total == 0 {
		return 0
	}
	if hits >= total {
		return 100
	}
	return 100 * float64(hits) / float64(total)
}

// recordSourceGridForDedupe implements Phase 1 for a single transfer (blob). It records the
// source's committed blocks into the per-job table, accumulates the would-be-hit counters, and
// logs both this blob's contribution and the running job-wide hit rate. It changes no transfer
// behavior.
func recordSourceGridForDedupe(jptm IJobPartTransferMgr, plan *SourceGridPlan) {
	info := jptm.Info()
	st := dedupeStateForJob(info.JobID)
	targetURI := sanitizedDestForDedupe(info.Destination)

	hashed, hits, dedupableBytes := measureAndRecordWithObserver(st.table, info.JobID, targetURI, plan.Blocks, func(event dedupeTableRecordEvent) {
		jptm.LogAtLevelForCurrentTransfer(common.LogDebug, fmt.Sprintf(
			"dedupe-table(observe): record=%d inserted=%t wouldBeHit=%t entries=%d buckets=%d bucketEntries=%d refCount=%d "+
				"crc64=%016x sha256=%x offset=%d size=%d target=%s",
			event.RecordIndex, event.Inserted, event.PreHit, event.TableStats.Entries, event.TableStats.Buckets,
			event.TableStats.BucketEntries, event.Stored.RefCount, event.Block.CRC64, event.Block.SHA256,
			event.Block.Offset, event.Block.Size, sanitizedDestForDedupe(event.Stored.TargetURI)))
	})
	if hashed == 0 {
		return // nothing eligible (service GetHash feature off, or include not honored)
	}

	totalHashed := atomic.AddInt64(&st.hashedBlocks, hashed)
	totalHits := atomic.AddInt64(&st.wouldBeHits, hits)
	totalBytes := atomic.AddInt64(&st.dedupableBytes, dedupableBytes)

	jptm.LogAtLevelForCurrentTransfer(common.LogInfo, fmt.Sprintf(
		"dedupe-phase1: blob %q would-be-hits=%d/%d blocks; job cumulative would-be-hits=%d/%d blocks (%.1f%%), dedupable bytes=%d, table entries=%d",
		info.SrcFilePath, hits, hashed,
		totalHits, totalHashed, dedupePercent(totalHits, totalHashed),
		totalBytes, st.table.Len()))
}

// --- Phase 2 core: staging-time hit decision ---

// srcBlockKey identifies a source committed block by its position and length. A uniform AzCopy
// chunk can be matched to a source block (and so to its content hashes) only when the two share the
// same offset and size — until source-grid chunking (Phase 3) makes every chunk a source block, the
// index therefore only resolves chunks that already align with the source's committed boundaries.
type srcBlockKey struct {
	offset int64
	size   int64
}

// srcBlockHashes are the content hashes of a single source committed block.
type srcBlockHashes struct {
	crc64  uint64
	sha256 [32]byte
}

// buildSourceBlockHashIndex turns a source-grid plan into a lookup from a block's (offset,size) to
// its content hashes, including only blocks that actually carry hashes. The staging path consults it
// to discover whether the chunk it is about to send has a known hash to look up.
func buildSourceBlockHashIndex(plan *SourceGridPlan) map[srcBlockKey]srcBlockHashes {
	idx := make(map[srcBlockKey]srcBlockHashes, len(plan.Blocks))
	for _, b := range plan.Blocks {
		if !blockHasHashes(b) {
			continue
		}
		idx[srcBlockKey{offset: b.Offset, size: b.Size}] = srcBlockHashes{crc64: b.CRC64, sha256: b.SHA256}
	}
	return idx
}

// decideStaging is the core Phase 2 "act on a hit" decision for a single block about to be staged at
// [offset, offset+size) of the source. It returns the matching target entry with reference=true when
// (a) the chunk exactly matches a hashed source block, and (b) that content is already recorded in the
// destination hash index as committed destination content — meaning the block can be staged from the
// target blob (Put Block From URL over the target's sub-range) instead of re-read from the source.
// Otherwise it returns reference=false and the caller stages from the source as normal.
//
// currentTargetURI is the blob currently being written. A matching entry for that same target is not
// a cross-blob reuse candidate and must be ignored; same-blob hits are intentionally observe-only in
// this targetURI design because the current blob's destination ranges are not committed/readable yet.
func decideStaging(index map[srcBlockKey]srcBlockHashes, committed *common.DedupeHashTable, offset, size int64, currentTargetURI string) (target common.BlockEntry, reference bool) {
	h, ok := index[srcBlockKey{offset: offset, size: size}]
	if !ok {
		return common.BlockEntry{}, false // no known hash for this chunk (not aligned to a source block)
	}
	entry, hit := committed.Lookup(h.crc64, h.sha256)
	if !hit {
		return common.BlockEntry{}, false // identical content not yet migrated to the destination
	}
	if entry.TargetURI == "" || entry.TargetOffset < 0 || entry.ETag == "" || entry.TargetLength != size {
		return common.BlockEntry{}, false // never reuse an unversioned or differently-sized target range
	}
	if sameDedupeTarget(entry.TargetURI, currentTargetURI) {
		return common.BlockEntry{}, false // avoid unsafe same-blob/self reuse; wait for a committed target blob
	}
	return entry, true
}

func sameDedupeTarget(a, b string) bool {
	return sanitizedDestForDedupe(a) == sanitizedDestForDedupe(b)
}
