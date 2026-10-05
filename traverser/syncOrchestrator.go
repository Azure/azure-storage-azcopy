//go:build smslidingwindow
// +build smslidingwindow

// // Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package traverser

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
)

// CustomSyncHandlerFunc defines the signature for custom sync handlers that process
// synchronization operations between source and destination locations.
const UseSyncOrchestrator = true

var (
	// notFoundErrors contains error messages that are considered normal during sync operations.
	// These errors don't cause the sync to fail (e.g., 404 responses from target locations).
	notFoundErrors []string = []string{
		"ParentNotFound",
		"BlobNotFound",
		"PathNotFound",
		"ResourceNotFound",
		"ShareNotFound",
		"ShareBeingDeleted",
	}
)

type minimalStoredObject struct {
	RelativePath string // Relative path of the object within the directory

	// Originally, we only needed to store relativePath for the purpose of enqueueing subdirectories.
	// We are adding changeTime to this struct to provide us parent directory change time while we
	// process subdirectories. This is for enabling the optimization of skipping target traversal.
	// This has an implication of increased memory usage for all scenarios to provide optimization for
	// just NFS sources (as of 06/01/2025). Ideally, we can decide to not store changeTime here
	// at the time of initialization of the SyncTraverser. This is an optimization that we will consider
	// later.
	ChangeTime time.Time // Change time of the object

	isPresentAtDestination bool // Indicates if the object is present at the secondary location
	IsVirtualPrefix        bool // Virtual directory prefix, not a real dir stub or HNS directory
}

// GetCustomSyncHandlerInfo returns a description of the current sync handler implementation.
// Used for logging and debugging purposes to identify which sync strategy is active.
func GetCustomSyncHandlerInfo() string {
	return "Sync Handler: Sliding Window"
}

// SyncTraverser manages the traversal of a single directory during sync operations.
// It processes files and subdirectories, storing them in the indexer and scheduling transfers.
type SyncTraverser struct {
	run        *syncRun
	enumerator *SyncEnumerator // Main sync enumerator that coordinates the overall sync operation
	comparator ObjectProcessor // Processes objects from the destination for comparison
	dir        string          // Current directory being processed (relative path)

	// There is a risk here to use pointers for sub directories because by the time we dereference
	// this storedObject pointer and enqueue the directory, it is removed from the indexer by
	// either comparator or finalize. Using paths here just to be safe.
	sub_dirs []minimalStoredObject // Subdirectories discovered during traversal (queued for enqueueing post processing)
}

// SyncOrchErrorInfo holds information about files and folders that failed enumeration.
// Implements TraverserErrorItemInfo interface to provide consistent error reporting.
type SyncOrchErrorInfo struct {
	DirPath           string          // Full path to the directory or file that failed
	DirName           string          // Name of the directory or file that failed
	ErrorMsg          error           // The actual error that occurred during processing
	TraverserLocation common.Location // The location of the error (source or destination)
}

// Compile-time check to ensure ErrorFileInfo implements TraverserErrorItemInfo
var _ TraverserErrorItemInfo = (*SyncOrchErrorInfo)(nil)

///////////////////////////////////////////////////////////////////////////
// START - Implementing methods defined in TraverserErrorItemInfo

func (e SyncOrchErrorInfo) FullPath() string {
	return e.DirPath
}

func (e SyncOrchErrorInfo) Name() string {
	return e.DirName
}

func (e SyncOrchErrorInfo) Size() int64 {
	return 0 // Size is not applicable for directories, so we return 0.
}

func (e SyncOrchErrorInfo) LastModifiedTime() time.Time {
	return time.Time{} // Last modified time is not applicable for directories, so we return zero time.
}

func (e SyncOrchErrorInfo) IsDir() bool {
	return true // This struct is used for directories, so we return true.
}

func (e SyncOrchErrorInfo) ErrorMessage() error {
	return e.ErrorMsg
}

func (e SyncOrchErrorInfo) Location() common.Location {
	return e.TraverserLocation
}

// END - Implementing methods defined in TraverserErrorItemInfo
// /////////////////////////////////////////////////////////////////////////

func IsDestinationNotFoundDuringSync(err error) bool {
	isNotFoundError := false
	for _, notFoundErr := range notFoundErrors {
		if strings.Contains(err.Error(), notFoundErr) {
			isNotFoundError = true
			break
		}
	}

	return isNotFoundError
}

func (r *syncRun) writeSyncErrToChannel(errorChannel chan<- TraverserErrorItemInfo, err SyncOrchErrorInfo) {
	if errorChannel != nil {
		// Use defer/recover to handle the case where the channel is closed during cancellation.
		// This prevents "panic: send on closed channel" when the job is being cancelled.
		// [TO DO] - BugFix - Identify the root cause for cases when the error channel is closed prematurely and fix it.
		defer func() {
			if recovered := recover(); recovered != nil {
				// Channel was closed, log the error instead of panicking
				r.syncOrchestratorLog(
					common.LogError,
					fmt.Sprintf("Error channel closed, could not send error: %v", err.ErrorMessage()))
			}
		}()

		select {
		case errorChannel <- err:
		default:
			// Channel might be full, log the error instead
			r.syncOrchestratorLog(
				common.LogError,
				fmt.Sprintf("Failed to send error to channel: %v", err.ErrorMessage()))
		}
	}
}

func (r *syncRun) validateLocalRoot(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		return err
	}

	return nil
}

// validateFileShareRoot validates an Azure Files share root URL
func (r *syncRun) validateFileShareRoot(sourcePath string) error {
	parsedURL, err := url.Parse(sourcePath)
	if err != nil {
		return err
	}

	// Require an absolute URL (scheme + host) and at least a share name in the path.
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		return fmt.Errorf("invalid Azure Files URL: must include scheme and host")
	}

	trimmedPath := strings.Trim(parsedURL.Path, "/")
	shareName := strings.SplitN(trimmedPath, "/", 2)[0]
	if shareName == "" {
		return fmt.Errorf("invalid Azure Files URL: missing share name")
	}

	return nil
}

// validateS3Root returns the root object for the sync orchestrator based on the S3 source path.
// It parses the S3 URL and determines the entity type (file or folder) based on the URL structure.
//
// Parameters:
// - sourcePath: The S3 source path as a string.
//
// Returns:
// - StoredObject: The root StoredObject for the given S3 source path.
// - error: An error if parsing the URL or creating the StoredObject fails.
func (r *syncRun) validateS3Root(sourcePath string) error {

	parsedURL, err := url.Parse(sourcePath)
	if err != nil {
		return err
	}

	_, err = common.NewS3URLParts(*parsedURL)
	if err != nil {
		return err
	}

	return nil
}

func (r *syncRun) validateBlobRoot(sourcePath string) error {
	_, err := blob.ParseURL(sourcePath)
	if err != nil {
		return err
	}
	return nil
}

// validateBlobFSRoot validates a BlobFS root URL by converting the DFS endpoint to Blob
// and then parsing it using the Blob URL parser. This mirrors how BlobFS traversers are initialized.
func (r *syncRun) validateBlobFSRoot(sourcePath string) error {
	blobPath := strings.Replace(sourcePath, ".dfs", ".blob", 1)
	_, err := blob.ParseURL(blobPath)
	if err != nil {
		return err
	}
	return nil
}

// validateAndGetRootObject returns the root object for the sync orchestrator
// based on the source path and fromTo configuration. This determines the starting
// point for sync enumeration operations.
//
// Parameters:
// - path: The source path to create a root object for
// - fromTo: Specifies the source and destination location types
//
// Returns:
// - error: Error if the source type is unsupported or path processing fails
func (r *syncRun) validateAndGetRootObject(path string, fromTo common.FromTo) (minimalStoredObject, error) {

	r.syncOrchestratorLog(
		common.LogInfo,
		fmt.Sprintf("Getting root object for path = %s\n", path))
	var err error

	switch fromTo.From() {
	case common.ELocation.Local():
		err = r.validateLocalRoot(path)
	case common.ELocation.S3():
		err = r.validateS3Root(path)
	case common.ELocation.Blob():
		err = r.validateBlobRoot(path)
	case common.ELocation.BlobFS():
		err = r.validateBlobFSRoot(path)
	case common.ELocation.File():
		err = r.validateFileShareRoot(path)
	default:
		err = fmt.Errorf("sync orchestrator is not supported for %s source", fromTo.From().String())
	}

	if err == nil {
		return minimalStoredObject{
			RelativePath:           common.AZCOPY_PATH_SEPARATOR_STRING, // we want enumerator to always operate with a leading slash to handle nameless dirs case (ex: ///blob.txt)
			ChangeTime:             time.Time{},
			isPresentAtDestination: true,
		}, nil
	} else {
		return minimalStoredObject{}, err
	}
}

// buildChildPath constructs the full child path by joining the base directory
// with the relative path, and handles path separator normalization.
// Ensures consistent path formatting across different operating systems.
func buildChildPath(baseDir, relativePath string, isDirectory bool) string {
	if isDirectory && relativePath == "" {
		// A directory's self-entry represents its current job-relative path, not another child.
		return strings.TrimPrefix(baseDir, common.AZCOPY_PATH_SEPARATOR_STRING)
	}
	if isDirectory {
		// S3 traverser will return relative paths with trailing '/' and blob traverser will not
		// this line normalizes relative paths such that they will not end with '/' regardless of the traverser implementation
		relativePath = strings.TrimSuffix(relativePath, common.AZCOPY_PATH_SEPARATOR_STRING)
	}

	childPath := relativePath
	if baseDir != "" {
		childPath = baseDir + relativePath
	}

	if isDirectory {
		childPath += common.AZCOPY_PATH_SEPARATOR_STRING
	}

	childPath = strings.TrimPrefix(childPath, common.AZCOPY_PATH_SEPARATOR_STRING) // we want to store paths in object indexer without a slash to be in correct format for scheduleTransfer

	return childPath
}

// processor handles StoredObjects from the source location during traversal.
// It builds the full path, categorizes objects as files or directories,
// and stores them in the indexer for later comparison and transfer.
func (st *SyncTraverser) processor(so StoredObject) error {
	r := st.run
	// Debug: Log original path before transformation
	originalPath := so.RelativePath
	r.syncOrchestratorLog(common.LogDebug, fmt.Sprintf("[PROCESSOR] Before buildChildPath - dir='%s', originalPath='%s'", st.dir, originalPath))

	// Skip directory placeholder objects that represent the current directory itself
	// These have empty relativePath after prefix trimming and would cause re-enqueueing of the same directory
	// This issue specifically occurs with GCP S3-compatible storage where directory placeholders are returned
	if r.isGCPSource && originalPath == "" && so.EntityType == common.EEntityType.Folder() {
		r.syncOrchestratorLog(common.LogDebug, fmt.Sprintf("[PROCESSOR] Skipping self-referential directory placeholder for dir='%s' (GCP S3-compatible source)", st.dir))
		return nil
	}

	isDirectory := so.EntityType == common.EEntityType.Folder()
	isSelfEntry := isDirectory && originalPath == ""
	so.RelativePath = buildChildPath(st.dir, so.RelativePath, isDirectory)

	// Thread-safe storage in the indexer first
	st.enumerator.ObjectIndexer.rwMutex.Lock()
	err := st.enumerator.ObjectIndexer.Store(so)
	st.enumerator.ObjectIndexer.rwMutex.Unlock()

	if err != nil {
		return err
	}

	// Update throttling counters if enabled
	if r.enableThrottling {
		r.totalFilesInIndexer.Add(1) // Increment the count of files in the indexer
	}

	if isDirectory && !isSelfEntry {
		st.sub_dirs = append(st.sub_dirs, minimalStoredObject{
			RelativePath:    common.AZCOPY_PATH_SEPARATOR_STRING + so.RelativePath, // we want enumerator to always operate with a leading slash to handle nameless dirs case (ex: ///blob.txt)
			ChangeTime:      so.ChangeTime,
			IsVirtualPrefix: so.IsVirtualPrefix,
		})
	}

	return nil
}

// customComparator processes StoredObjects from the destination location during traversal.
// It builds the full path and passes the object to the main comparator for sync decision making.
func (st *SyncTraverser) customComparator(so StoredObject) error {
	// Build full path for destination object

	isDirectory := so.EntityType == common.EEntityType.Folder()
	so.RelativePath = buildChildPath(st.dir, so.RelativePath, isDirectory)

	// comparison and deletion from indexer will happen under the lock

	return st.comparator(so)
}

// finalize completes the processing of the current directory by scheduling
// transfers for all discovered files and cleaning up the indexer.
// This method is called after both source and destination traversals are complete.
func (st *SyncTraverser) finalize(scheduleTransfer bool) error {
	r := st.run

	// st.dir will have leading slash but the paths in the indexer do not
	dirPrefix := strings.TrimPrefix(st.dir, common.AZCOPY_PATH_SEPARATOR_STRING)

	// Use exclusive lock for the entire operation to prevent concurrent iteration and modification
	st.enumerator.ObjectIndexer.rwMutex.RLock()

	if r.enableThrottling {
		r.totalFilesInIndexer.Store(int64(len(st.enumerator.ObjectIndexer.IndexMap))) // Set accurate count
	}

	// Collect items to process (we need to collect first to avoid modifying map while iterating)
	var itemsToProcess []string
	for path := range st.enumerator.ObjectIndexer.IndexMap {
		if st.belongsToCurrentDirectory(path, dirPrefix) {
			itemsToProcess = append(itemsToProcess, path)
		}
	}
	st.enumerator.ObjectIndexer.rwMutex.RUnlock()

	// Process collected items while still holding the lock to prevent concurrent access
	for _, path := range itemsToProcess {
		err := st.finalizeChild(path, scheduleTransfer)
		if err != nil {
			return err
		}
	}

	return nil
}

// belongsToCurrentDirectory determines if a given path belongs to the current directory
// being processed by this SyncTraverser instance.
func (st *SyncTraverser) belongsToCurrentDirectory(path, dirPrefix string) bool {
	if !strings.HasPrefix(path, dirPrefix) {
		return false
	}

	remainder := path[len(dirPrefix):]

	if remainder == "" {
		return true
	}

	// Strip trailing "/" so direct child dirs (remainder="dir1/") aren't rejected
	trimmed := strings.TrimSuffix(remainder, common.AZCOPY_PATH_SEPARATOR_STRING)
	return !strings.Contains(trimmed, common.AZCOPY_PATH_SEPARATOR_STRING)
}

// hasAnyChildChangedSinceLastSync checks if at least 1 child object changed in the current directory
// since the last successful sync job start time.
func (st *SyncTraverser) hasAnyChildChangedSinceLastSync() (bool, uint32) {
	r := st.run
	// st.dir will have leading slash but the paths in the indexer do not
	dirPrefix := strings.TrimPrefix(st.dir, common.AZCOPY_PATH_SEPARATOR_STRING)

	foundOneChanged := false

	// This is purely for incrementing the metrics with a computation cost
	childCount := uint32(0)

	st.enumerator.ObjectIndexer.rwMutex.RLock()
	// Collect items to process (we need to collect first to avoid modifying map while iterating)
	for path := range st.enumerator.ObjectIndexer.IndexMap {
		if st.belongsToCurrentDirectory(path, dirPrefix) {
			// Increment child count for each item
			// This will be the total number of children in the directory only if there are
			// no changes in any file.
			childCount++

			if st.enumerator.ObjectIndexer.IndexMap[path].ChangeTime.IsZero() {
				// If change time is zero, we cannot determine if it changed since last sync
				// so we assume it has changed
				foundOneChanged = true
				break
			} else if st.enumerator.ObjectIndexer.IndexMap[path].ChangeTime.After(r.orchestratorOptions.lastSuccessfulSyncJobStartTime) {
				foundOneChanged = true
				break
			}
		}
	}
	st.enumerator.ObjectIndexer.rwMutex.RUnlock()
	return foundOneChanged, childCount - uint32(len(st.sub_dirs))
}

// finalizeChild processes a single child object (file or directory) by scheduling it for transfer.
// It retrieves the stored object from the indexer and schedules it for transfer.
// If the object is a directory, it will be processed after all files in that directory are finalized.
// This method is called after the traversal is complete for each child object.
func (st *SyncTraverser) finalizeChild(child string, scheduleTransfer bool) error {
	r := st.run
	st.enumerator.ObjectIndexer.rwMutex.RLock()
	// Get pointer to the stored object from indexer
	storedObject, exists := st.enumerator.ObjectIndexer.IndexMap[child]
	st.enumerator.ObjectIndexer.rwMutex.RUnlock()

	if exists {
		// Schedule the file/directory for transfer using the pointer
		if scheduleTransfer {
			err := st.enumerator.scheduleTransfer(storedObject)
			if err != nil {
				return err
			}
		}

		// Remove from indexer to free memory
		st.enumerator.ObjectIndexer.rwMutex.Lock()
		delete(st.enumerator.ObjectIndexer.IndexMap, child)
		st.enumerator.ObjectIndexer.rwMutex.Unlock()

		if r.enableThrottling {
			r.totalFilesInIndexer.Add(-1) // Decrement the count after processing
		}
	}

	return nil
}

// shouldTrySkippingTargetTraversal checks if we should even try skipping the target traversal
func (st *SyncTraverser) shouldTrySkippingTargetTraversal(parentDirCTime time.Time, deleteDestination common.DeleteDestination) bool {
	r := st.run

	// Check 1: valid
	// This flag indicates whether the sync orchestrator options are valid.
	//
	// Check 2: optimizeEnumerationByCTime
	// This flag indicates whether we can optimize enumeration by using ctime values. Usually this is set to true
	// when the sync orchestrator is used with XDM Mover and only for source objects that have reliable ctime values.
	// As of the wrting of this comment [06/01/2025], this was true for NFS sources that have ctime posix properties.
	//
	// Check 3: deleteDestination
	// Skipping target traversal is only safe if we are not deleting any destination objects. If we are deleting destination objects,
	// we need to enumerate the destination objects to ensure that we do not miss any objects that need to be deleted.
	// If we are not deleting destination objects, we can use ctime optimization to skip enumeration of
	// destination objects that have not changed since the last successful sync job.
	//
	// Check 4: parentDirCTime
	// We can only use ctime optimization if the parent directory has a valid ctime value
	//
	// Check 5: lastSuccessfulSyncJobStartTime
	// We can only use ctime optimization if the parent directory ctime is before the last successful sync job start time.
	// This ensures that we do not miss any objects that were added after the last successful sync job.
	// If the parent directory ctime is after the last successful sync job start time,
	// we need to enumerate the destination objects to ensure that we do not miss any objects that need to be deleted.

	return r.orchestratorOptions != nil &&
		r.orchestratorOptions.valid &&
		r.orchestratorOptions.optimizeEnumerationByCTime &&
		deleteDestination == common.EDeleteDestination.False() &&
		!parentDirCTime.IsZero() &&
		!r.orchestratorOptions.lastSuccessfulSyncJobStartTime.IsZero() &&
		parentDirCTime.Before(r.orchestratorOptions.lastSuccessfulSyncJobStartTime)
}

// newSyncTraverser creates a new SyncTraverser instance for processing a specific directory.
// Pre-allocates slices with reasonable capacity to reduce memory allocations.
func (r *syncRun) newSyncTraverser(enumerator *SyncEnumerator, dir string, comparator ObjectProcessor) *SyncTraverser {
	return &SyncTraverser{
		run:        r,
		enumerator: enumerator,
		dir:        dir,
		sub_dirs:   make([]minimalStoredObject, 0, directorySizeBuffer),
		comparator: comparator,
	}
}

func (r *syncRun) validate(cca *SyncJob, orchestratorOptions *SyncOrchestratorOptions) error {
	switch cca.FromTo {
	case common.EFromTo.LocalBlob(), common.EFromTo.LocalBlobFS():
		// sync orchestrator is supported for these types
	case common.EFromTo.LocalFile(), common.EFromTo.LocalFileSMB(), common.EFromTo.LocalFileNFS():
		// sync orchestrator is supported for these types
	case common.EFromTo.S3Blob():
		// sync orchestrator is supported for these types
	case common.EFromTo.BlobBlob(), common.EFromTo.BlobBlobFS(), common.EFromTo.BlobFSBlob(), common.EFromTo.BlobFSBlobFS(), common.EFromTo.FileFile():
		// sync orchestrator is supported for these types
	default:
		return fmt.Errorf(
			"sync orchestrator is only supported for the following source and destination types:\n" +
				"\t- Local->Blob\n" +
				"\t- Local->BlobFS\n" +
				"\t- Local->File\n" +
				"\t- Local->FileSMB\n" +
				"\t- Local->FileNFS\n" +
				"\t- S3->Blob\n" +
				"\t- Blob->Blob\n" +
				"\t- Blob->BlobFS\n" +
				"\t- BlobFS->Blob\n" +
				"\t- BlobFS->BlobFS\n" +
				"\t- File->File",
		)
	}

	if cca.Recursive {
		return errors.New("sync orchestrator does not support recursive traversal. Use --recursive=false.")
	}

	if r.orchestratorOptions == nil {
		return errors.New("orchestrator options are required for sync orchestrator")
	}

	if r.orchestratorOptions != nil && r.orchestratorOptions.valid {
		return r.orchestratorOptions.validate(cca.FromTo.From())
	}

	return nil
}

// syncOrchestratorHandler validates the job and initializes its traversal limits.
func (r *syncRun) syncOrchestratorHandler(cca *SyncJob, enumerator *SyncEnumerator, ctx context.Context) error {
	err := r.validate(cca, enumerator.orchestratorOptions) // Validate the command arguments for sync orchestrator
	if err != nil {
		r.syncOrchestratorLog(common.LogPanic, err.Error())
		return err
	}

	r.orchestratorOptions = enumerator.orchestratorOptions

	// Detect if source is Google Cloud Storage via S3-compatible API
	r.isGCPSource = false
	if r.orchestratorOptions.fromTo == common.EFromTo.S3Blob() {
		if parsedURL, err := url.Parse(cca.Source.Value); err == nil {
			if s3Parts, err := common.NewS3URLParts(*parsedURL); err == nil {
				r.isGCPSource = s3Parts.IsGoogleCloudStorage()
			}
		}
	}

	// Log (once per job) whether the streaming merge-join is used. Enablement is decided in the
	// mover (subscription allowlist via featureConfig OR the MOVER_SYNC_MJ env var) and
	// passed to azcopy as the single cca.useStreamingMergeJoin flag. This makes it easy to confirm
	// the gating in production logs.
	if useStreamingMergeJoin(cca) {
		r.syncOrchestratorLog(common.LogInfo, fmt.Sprintf(
			"Streaming merge-join ENABLED (mover flag) for %s->%s", cca.FromTo.From(), cca.FromTo.To()), true)

		// The streaming merge-join uses its own directory-crawl parallelism
		// (MOVER_SYNC_MJ_TRAV, default 32)
		// — separate from the indexMap path, which keeps
		// orchestratorOptions.parallelTraversers unchanged — because the merge-join also lists
		// source and destination concurrently within each directory.
		if r.orchestratorOptions != nil {
			mjTrav := resolveMergeJoinParallelTraversers()
			r.orchestratorOptions.parallelTraversers = mjTrav
			r.syncOrchestratorLog(common.LogInfo, fmt.Sprintf(
				"Streaming merge-join directory-crawl parallelism set to %d (%s)", mjTrav, enum.EEnvironmentVariable.MoverSyncMergeJoinTraversers().Name), true)
		}
	} else if cca.UseStreamingMergeJoin {
		r.syncOrchestratorLog(common.LogInfo, fmt.Sprintf(
			"Streaming merge-join requested (mover flag) but NOT used for %s->%s (pair not eligible); using indexMap sync path",
			cca.FromTo.From(), cca.FromTo.To()), true)
	}

	// Initialize resource limits based on source/destination types
	r.initializeLimits(r.orchestratorOptions)
	return r.runSyncOrchestrator(cca, enumerator, ctx)
}

// runSyncOrchestrator coordinates the entire sync operation using a sliding window approach.
// It processes directories in parallel while respecting resource limits and handles graceful shutdown.
//
// The algorithm works as follows:
// 1. Create traversers for source and destination
// 2. Process files in current directory
// 3. Discover subdirectories and queue them for processing
// 4. Use semaphores to limit concurrent directory processing
// 5. Schedule transfers after comparison is complete
func (r *syncRun) runSyncOrchestrator(cca *SyncJob, enumerator *SyncEnumerator, ctx context.Context) error {
	startTime := time.Now()
	mainCtx, cancel := context.WithCancel(ctx) // Use mainCtx for operations, cancel to signal shutdown
	defer cancel()                             // Ensure cancellation happens on exit

	if cca.SetCancel != nil {
		cca.SetCancel(cancel)
	}

	// Initialize semaphore for directory concurrency control
	if r.enableThrottling {
		r.semaphore = r.NewThrottleSemaphore(mainCtx, cca.JobID)
		defer r.semaphore.Close()
	}

	// Log the orchestrator start with key configuration values
	r.syncOrchestratorLog(
		common.LogInfo,
		fmt.Sprintf("Starting sync orchestrator - Source: %s, Destination: %s, options: %v",
			cca.Source.Value,
			cca.Destination.Value,
			r.orchestratorOptions.ToStringMap()))

	var crawlWg sync.WaitGroup // WaitGroup for all directory processing tasks

	// syncOneDir processes a single directory by creating source and destination traversers,
	// enumerating files, comparing them, and scheduling transfers. It also discovers
	// subdirectories and enqueues them for further processing.
	syncOneDir := func(
		dir parallel.Directory,
		enqueueDir func(parallel.Directory),
		enqueueOutput func(parallel.DirectoryEntry, error)) error {
		defer crawlWg.Done() // Signal this task is done when it finishes

		// Track that this directory entered the processing queue
		defer r.totalDirectoriesProcessed.Add(1)

		var err error

		// Acquire semaphore slot to limit concurrent directory processing
		if r.enableThrottling {
			err = r.semaphore.AcquireSourceSlot(mainCtx)
			if err != nil {
				r.syncOrchestratorLog(
					common.LogError,
					fmt.Sprintf("Failed to acquire source slot for dir '%s': %s", dir.(minimalStoredObject).RelativePath, err))
				return err
			}
		}

		r.srcDirEnumerating.Add(1) // Increment active directory count

		// func pathEncodeRules(path string, fromTo common.FromTo, disableAutoDecoding bool, source bool) string
		// srcRelativePath = pathEncodeRules(dir.(minimalStoredObject).relativePath, cca.fromTo, false, true)
		dstRelativePath := cca.EncodeDestinationPath(dir.(minimalStoredObject).RelativePath)

		// Build source and destination paths for current directory
		sync_src := []string{cca.Source.Value, dir.(minimalStoredObject).RelativePath}
		sync_dst := []string{cca.Destination.Value, dstRelativePath}

		pt_src := cca.Source
		st_src := cca.Destination

		pt_src.Value = strings.Join(sync_src, "")
		st_src.Value = strings.Join(sync_dst, "")

		// Handle Windows path separators
		if runtime.GOOS == "windows" {
			pt_src.Value = strings.ReplaceAll(pt_src.Value, "/", "\\")
			st_src.Value = strings.ReplaceAll(st_src.Value, "\\", "/")
		}

		// Get traverser templates from enumerator
		ptt := enumerator.primaryTraverserTemplate
		stt := enumerator.secondaryTraverserTemplate

		var errMsg string

		isDestinationPresent := dir.(minimalStoredObject).isPresentAtDestination

		// Fork: use the streaming (channel-based) merge-join for remote sources whose
		// listings are lexicographically sorted; keep the existing indexMap-based flow for
		// local filesystem sources (which do not guarantee sorted listing order).
		if useStreamingMergeJoin(cca) {
			r.mergeJoinSyncOneDirLog(common.LogDebug,
				fmt.Sprintf("Processing dir '%s'", dir.(minimalStoredObject).RelativePath))

			var subDirs []minimalStoredObject
			var mergeErr error

			// Per-directory cancellable context derived from mainCtx. mergeJoinTwoWaySyncDir cancels it
			// and drains/awaits all producer goroutines on EVERY return path, so no detached traverser
			// goroutine outlives this directory's processing and later writes to the shared sync error
			// channel after it is closed ("send on closed channel"). Creating the traversers with dirCtx
			// (not mainCtx) is what lets that cancel abort their in-flight listing promptly.
			dirCtx, dirCancel := context.WithCancel(mainCtx)

			// Channel-based merge-join: bridge source + destination traversers into
			// back-pressured channels and merge their sorted streams.
			pt, ptErr := cca.NewTraverser(
				pt_src,
				ptt.Location,
				dirCtx,
				ptt.Options)
			if ptErr != nil {
				dirCancel()
				errMsg = fmt.Sprintf("Creating source traverser failed for dir %s: %s", pt_src.Value, ptErr)
				r.syncOrchestratorLog(common.LogError, errMsg)
				r.writeSyncErrToChannel(ptt.Options.ErrorChannel, SyncOrchErrorInfo{
					DirPath:           pt_src.Value,
					DirName:           dir.(minimalStoredObject).RelativePath,
					ErrorMsg:          errors.New(errMsg),
					TraverserLocation: cca.FromTo.From(),
				})
				r.srcDirEnumerating.Add(-1)
				if r.enableThrottling {
					r.semaphore.ReleaseSourceSlot()
				}
				return ptErr
			}

			st, stErr := cca.NewTraverser(
				st_src,
				stt.Location,
				dirCtx,
				stt.Options)
			if stErr != nil {
				dirCancel()
				errMsg = fmt.Sprintf("Creating target traverser failed for dir %s: %s\n", st_src.Value, stErr)
				r.syncOrchestratorLog(common.LogError, errMsg)
				r.writeSyncErrToChannel(stt.Options.ErrorChannel, SyncOrchErrorInfo{
					DirPath:           st_src.Value,
					DirName:           dir.(minimalStoredObject).RelativePath,
					ErrorMsg:          errors.New(errMsg),
					TraverserLocation: cca.FromTo.To(),
				})
				r.srcDirEnumerating.Add(-1)
				if r.enableThrottling {
					r.semaphore.ReleaseSourceSlot()
				}
				return stErr
			}

			subDirs, mergeErr = r.mergeJoinTwoWaySyncDir(
				dirCtx,
				dirCancel,
				enumerator,
				cca,
				dir.(minimalStoredObject).RelativePath,
				pt,
				st,
				isDestinationPresent,
			)

			r.srcDirEnumerating.Add(-1) // Decrement active directory count after merge-join completes

			// Release source slot after merge-join completes (source was actively listed during merge-join)
			if r.enableThrottling {
				r.semaphore.ReleaseSourceSlot()
			}

			if mergeErr != nil {
				// Attribute the failure to the correct side (source vs destination) so the error
				// channel reports the same TraverserLocation/path as the indexMap path. Defaults
				// to source; a tagged mergeJoinTraversalError overrides it (e.g. dest listing failed).
				errLocation := cca.FromTo.From()
				errPath := pt_src.Value
				var mjErr *mergeJoinTraversalError
				if errors.As(mergeErr, &mjErr) && mjErr.location == cca.FromTo.To() {
					errLocation = cca.FromTo.To()
					errPath = st_src.Value
				}
				errMsg = fmt.Sprintf("Merge-join sync failed for dir %s: %s", errPath, mergeErr)
				r.syncOrchestratorLog(common.LogError, errMsg, true)
				r.writeSyncErrToChannel(ptt.Options.ErrorChannel, SyncOrchErrorInfo{
					DirPath:           errPath,
					DirName:           dir.(minimalStoredObject).RelativePath,
					ErrorMsg:          errors.New(errMsg),
					TraverserLocation: errLocation,
				})
				return mergeErr
			}

			// Enqueue discovered subdirectories for processing
			for _, sub_dir := range subDirs {
				crawlWg.Add(1)
				enqueueDir(minimalStoredObject{
					RelativePath:           sub_dir.RelativePath,
					ChangeTime:             sub_dir.ChangeTime,
					isPresentAtDestination: sub_dir.isPresentAtDestination,
				})
			}
			return nil
		}

		// ── Existing indexMap-based path (local filesystem sources) ──
		// Create source traverser for current directory
		pt, err := cca.NewTraverser(
			pt_src,
			ptt.Location,
			mainCtx,
			ptt.Options)
		if err != nil {
			errMsg = fmt.Sprintf("SyncOrchestrator: Creating source traverser failed for dir %s: %s", pt_src.Value, err)
			r.syncOrchestratorLog(common.LogError, errMsg)
			r.writeSyncErrToChannel(ptt.Options.ErrorChannel, SyncOrchErrorInfo{
				DirPath:           pt_src.Value,
				DirName:           dir.(minimalStoredObject).RelativePath,
				ErrorMsg:          errors.New(errMsg),
				TraverserLocation: cca.FromTo.From(),
			})
			r.reportSourceFailure()
			return err
		}

		// Create destination traverser for current directory
		st, err := cca.NewTraverser(
			st_src,
			stt.Location,
			mainCtx,
			stt.Options)
		if err != nil {
			errMsg = fmt.Sprintf("SyncOrchestrator: Creating target traverser failed for dir %s: %s\n", st_src.Value, err)
			r.syncOrchestratorLog(common.LogError, errMsg)
			r.writeSyncErrToChannel(stt.Options.ErrorChannel, SyncOrchErrorInfo{
				DirPath:           st_src.Value,
				DirName:           dir.(minimalStoredObject).RelativePath,
				ErrorMsg:          errors.New(errMsg),
				TraverserLocation: cca.FromTo.To(),
			})
			r.reportDestinationFailure()
			return err
		}

		// Create sync traverser for this directory
		stra := r.newSyncTraverser(enumerator, dir.(minimalStoredObject).RelativePath, enumerator.objectComparator)

		err = pt.Traverse(NoPreProccessor, stra.processor, enumerator.filters)
		r.srcDirEnumerating.Add(-1) // Decrement active directory count

		// Release source slot after source traversal is complete
		if r.enableThrottling {
			r.semaphore.ReleaseSourceSlot()
		}

		if err != nil {
			errMsg = fmt.Sprintf("SyncOrchestrator: primary traversal failed for dir %s : %s\n", pt_src.Value, err)
			r.syncOrchestratorLog(common.LogError, errMsg)
			r.writeSyncErrToChannel(ptt.Options.ErrorChannel, SyncOrchErrorInfo{
				DirPath:           pt_src.Value,
				DirName:           dir.(minimalStoredObject).RelativePath,
				ErrorMsg:          errors.New(errMsg),
				TraverserLocation: cca.FromTo.From(),
			})
			r.reportSourceFailure()
			return err
		}

		// Flag to control whether we traverse the destination
		traverseDestination := true

		finalize := true // Flag to control whether we finalize
		// Before proceeding, check if we need to enumerate the destination
		if isDestinationPresent &&
			stra.shouldTrySkippingTargetTraversal(dir.(minimalStoredObject).ChangeTime, cca.DeleteDestination) {
			// It is safe to use change time comparison to determine if the destination needs enumeration,
			// Enumerate all child objects of this directory in the indexer and check all of their change times.
			// If any of them is after the last successful sync, we need to enumerate the destination.
			// Otherwise, we can skip the destination enumeration and proceed with scheduling transfers.

			// For debugging:
			// fmt.Printf("Checking if destination enumeration for dir %s can be skipped.\n", st_src.Value)

			if changed, fileCount := stra.hasAnyChildChangedSinceLastSync(); !changed {
				err = stra.finalize(false) // false indicates we do not want to schedule transfers yet
				if err != nil {
					errMsg = fmt.Sprintf("SyncOrchestrator: Sync finalize to skip target enumeration failed for source dir %s.\n", pt_src.Value)
					r.syncOrchestratorLog(common.LogError, errMsg)
					r.writeSyncErrToChannel(ptt.Options.ErrorChannel, SyncOrchErrorInfo{
						DirPath:           pt_src.Value,
						DirName:           dir.(minimalStoredObject).RelativePath,
						ErrorMsg:          errors.New(errMsg),
						TraverserLocation: cca.FromTo.From(),
					})
					return err
				}

				// For debugging:
				// fmt.Printf("Skipping destination enumeration for dir %s.\n", st_src.Value)
				traverseDestination = false // No need to traverse destination if we are skipping it
				finalize = false            // No need to finalize as we are not scheduling transfers

				for range fileCount {
					// We can increment the count of not transferred files as well
					ptt.Options.IncrementNotTransferred(common.EEntityType.File())
				}

				for _, subDir := range stra.sub_dirs {
					if !subDir.IsVirtualPrefix {
						ptt.Options.IncrementNotTransferred(common.EEntityType.Folder())
					}
				}

				r.dstDirEnumerationSkippedBasedOnCTime.Add(1) // Increment skipped count based on ctime optimization
			}
		}

		if isDestinationPresent && traverseDestination {
			// Acquire target slot for target traversal
			if r.enableThrottling {
				err = r.semaphore.AcquireTargetSlot(mainCtx)
				if err != nil {
					errMsg = fmt.Sprintf("Failed to acquire target slot for dir %s: %s", st_src.Value, err)
					r.syncOrchestratorLog(common.LogError, errMsg)
					// Release destination directory count since we're bailing out
					r.dstDirEnumerating.Add(-1)
					return err
				}
			}

			r.dstDirEnumerating.Add(1) // Increment active destination directory count

			err = st.Traverse(NoPreProccessor, stra.customComparator, enumerator.filters)

			r.dstDirEnumerating.Add(-1) // Decrement active destination directory count

			// Release target slot after target traversal is complete
			if r.enableThrottling {
				r.semaphore.ReleaseTargetSlot()
			}

			if err != nil {
				errMsg = fmt.Sprintf("SyncOrchestrator: Secondary traversal failed for dir %s = %s\n", st_src.Value, err)
				r.syncOrchestratorLog(common.LogError, errMsg)
				// Only report unexpected errors (404s are normal for new files)
				if IsDestinationNotFoundDuringSync(err) {
					isDestinationPresent = false // Destination not found
				} else {
					r.writeSyncErrToChannel(stt.Options.ErrorChannel, SyncOrchErrorInfo{
						DirPath:           st_src.Value,
						DirName:           dir.(minimalStoredObject).RelativePath,
						ErrorMsg:          errors.New(errMsg),
						TraverserLocation: cca.FromTo.To(),
					})

					r.reportDestinationFailure()

					err = stra.finalize(false) // false indicates we do not want to schedule transfers yet
					if err != nil {
						errMsg = fmt.Sprintf("SyncOrchestrator: Failed to cleanup indexer object due to target traversal failure - %s. There may be unintended transfers.\n", pt_src.Value)
						r.syncOrchestratorLog(common.LogError, errMsg)
						r.writeSyncErrToChannel(ptt.Options.ErrorChannel, SyncOrchErrorInfo{
							DirPath:           pt_src.Value,
							DirName:           dir.(minimalStoredObject).RelativePath,
							ErrorMsg:          errors.New(errMsg),
							TraverserLocation: cca.FromTo.From(),
						})
						return err
					}

					return err
				}
			}
		} else {
			r.reportDestinationSkipped()
		}

		if finalize {

			// Complete processing for this directory and schedule transfers
			err = stra.finalize(true) // true indicates we want to schedule transfers

			if err != nil {
				errMsg = fmt.Sprintf("SyncOrchestrator: Sync finalize failed for source dir %s.\n", pt_src.Value)
				r.syncOrchestratorLog(common.LogError, errMsg)
				r.writeSyncErrToChannel(ptt.Options.ErrorChannel, SyncOrchErrorInfo{
					DirPath:           pt_src.Value,
					DirName:           dir.(minimalStoredObject).RelativePath,
					ErrorMsg:          errors.New(errMsg),
					TraverserLocation: cca.FromTo.From(),
				})
				return err
			}
		}

		// Enqueue discovered subdirectories for processing
		for _, sub_dir := range stra.sub_dirs {
			crawlWg.Add(1) // IMPORTANT: Add to WaitGroup *before* enqueuing
			enqueueDir(minimalStoredObject{
				RelativePath:           sub_dir.RelativePath,
				ChangeTime:             sub_dir.ChangeTime,
				isPresentAtDestination: isDestinationPresent,
			})
		}
		return nil
	}

	srcIsDir := false
	var err error

	// verify that the traversers are targeting the same type of resources
	// Sync orchestrator supports only directory to directory sync. The similarity has
	// already been checked in InitEnumerator. Here we check if it is directory or not.
	if cca.FromTo.From() != common.ELocation.S3() {
		srcIsDir, err = enumerator.primaryTraverser.IsDirectory(true)

		if err != nil {
			r.syncOrchestratorLog(
				common.LogError,
				fmt.Sprintf("Failed to check if source is a directory. Err: %s", err))
			return err
		}
	} else {
		// XDM: s3Traverser.IsDirectory is failing for valid directories, skipping the check for S3
		srcIsDir = true
		r.syncOrchestratorLog(
			common.LogWarning,
			fmt.Sprintf("Assuming source - %s is a directory for S3", cca.Source.Value), true)
	}

	if err != nil {
		r.syncOrchestratorLog(
			common.LogPanic,
			fmt.Sprintf("Failed to check if source is a directory. Err: %s", err))
		return err
	}

	if !srcIsDir {
		err = fmt.Errorf("source is not recognized as a directory")
		r.syncOrchestratorLog(common.LogPanic, fmt.Sprintf("Source is not recognized as a directory. Err: %s", err))
		return err
	}

	// Get the root object to start synchronization
	root, err := r.validateAndGetRootObject(cca.Source.Value, cca.FromTo)
	if err != nil {
		r.syncOrchestratorLog(common.LogPanic, fmt.Sprintf("Root object creation failed: %s", err))
		return err
	}

	// Ensure proper cleanup in ALL scenarios (success, failure, cancellation)
	cleanupFunc := func() {
		// Always shutdown monitoring goroutines
		r.syncOrchestratorLog(common.LogInfo, fmt.Sprintf("Orchestrator exiting. Execution time: %v.", time.Since(startTime)), true)
	}
	defer cleanupFunc()

	crawlWg.Add(1) // Add the root directory to the WaitGroup

	// Random dequeue is only used for mover-high-perf Blob/BlobFS<->Blob/BlobFS transfers: it helps
	// avoid partition hotspotting when writing to Azure Blob Storage. Fall back to the original
	// BFS/DFS-hybrid dequeue for everything else (default azcopy CLI, mover-default builds, and
	// non-Blob/BlobFS pairs even in mover-high-perf, e.g. S3 -> Blob).
	isAzureBlobLocation := func(loc common.Location) bool {
		return loc == common.ELocation.Blob() || loc == common.ELocation.BlobFS()
	}
	randomDequeue := buildmode.HighPerf() && isAzureBlobLocation(cca.FromTo.From()) && isAzureBlobLocation(cca.FromTo.To())

	// crawlOutput closes only after every crawler worker (and thus every in-flight syncOneDir + its
	// merge-join producers) has returned — the drain signal we use on cancellation below.
	crawlOutput, crawlStats := parallel.CrawlWithStats(mainCtx, root, syncOneDir, int(r.crawlParallelism), parallel.CrawlOptions{RandomDequeue: randomDequeue})

	// Periodically log crawl/merge-join concurrency stats (mover-high-perf only): active crawl
	// workers, queued directories, in-flight merge-join directory syncs, and goroutine count. This
	// is diagnostic-only output to help investigate concurrency/throughput issues in high-perf runs;
	// it is not needed for the default azcopy CLI or mover-default builds.
	if buildmode.HighPerf() {
		statsCtx, stopStats := context.WithCancel(mainCtx)
		defer stopStats()
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-statsCtx.Done():
					return
				case <-ticker.C:
					active := atomic.LoadInt64(&crawlStats.ActiveWorkers)
					queued := atomic.LoadInt64(&crawlStats.QueuedDirs)
					activeMJ := r.activeMergeJoinDirs.Load()
					goroutines := runtime.NumGoroutine()
					r.syncOrchestratorLog(common.LogInfo, fmt.Sprintf(
						"[CrawlStats] activeWorkers=%d/%d, queuedDirs=%d, activeMergeJoin=%d, goroutines=%d",
						active, int(r.crawlParallelism), queued, activeMJ, goroutines), true)
				}
			}
		}()
	}

	// Cancellation-aware wait
	done := make(chan struct{})
	go func() {
		defer close(done)
		crawlWg.Wait() // Wait for all goroutines in background
	}()

	select {
	case <-done:
		// All goroutines completed normally
		r.syncOrchestratorLog(common.LogInfo, "All sync traversers exited.")

	case <-mainCtx.Done():
		// On cancel, drain crawlOutput until it closes so no producer is still alive to write to the
		// caller-owned sync ErrorChannel after it is closed. (crawlWg can't be used here: the crawler
		// abandons queued dirs on cancel, so it would never reach zero.)
		r.syncOrchestratorLog(common.LogInfo, "Orchestrator cancellation detected; waiting for in-flight traversers to drain.")
		for range crawlOutput {
		}
		r.syncOrchestratorLog(common.LogInfo, "All in-flight traversers drained after cancellation.")
		return nil
	}

	// Always try to finalize the enumerator. This will set cancellation complete and dispatch final part
	r.syncOrchestratorLog(common.LogInfo, "Finalizing enumerator.")
	finalizeErr := enumerator.finalize()
	if finalizeErr != nil {
		r.syncOrchestratorLog(common.LogPanic, fmt.Sprintf("Enumerator finalize failed: %v", finalizeErr))
		// If no previous error, use the finalize error
		if err == nil {
			err = finalizeErr
		}
	}

	return err
}

// custom logging function for the sync orchestrator
func (r *syncRun) syncOrchestratorLog(level common.LogLevel, toLog string, logToConsole ...bool) {
	var prefix string
	switch level {
	case common.LogError:
		prefix = "[ERROR] "
	case common.LogPanic:
		prefix = "[PANIC] "
	case common.LogInfo:
		prefix = "[INFO] "
	case common.LogDebug:
		prefix = "[DEBUG] "
	case common.LogWarning:
		prefix = "[WARNING] "
	default:
		prefix = "[INFO] "
	}
	toLog = prefix + toLog

	shouldLogToConsole := false
	if len(logToConsole) > 0 {
		shouldLogToConsole = logToConsole[0]
	}

	if r.job.Log != nil {
		r.job.Log(level, toLog, shouldLogToConsole)
		return
	}

	if common.AzcopyScanningLogger != nil {
		// XDM: Log all messages at the error level as that is the log level set by Mover
		common.AzcopyScanningLogger.Log(common.LogError, toLog)
	}

	if common.AzcopyScanningLogger == nil || shouldLogToConsole {
		toLog = "[AzCopy] " + toLog
		switch level {
		case common.LogError, common.LogPanic, common.LogWarning:
			common.GetLifecycleMgr().Warn(toLog)
		case common.LogInfo:
			common.GetLifecycleMgr().Info(toLog)
		default:
			common.GetLifecycleMgr().Info(toLog)
		}
	}
}
