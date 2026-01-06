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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
)

// CustomSyncHandlerFunc defines the signature for custom sync handlers that process
// synchronization operations between source and destination locations.
type CustomSyncHandlerFunc func(cca *cookedSyncCmdArgs, enumerator *syncEnumerator, ctx context.Context) error

// CustomCounterIncrementer defines the signature for functions that increment
// counters during file system traversal operations.
type CustomCounterIncrementer func(entry fs.DirEntry, t *localTraverser) error

var (
	// UseSyncOrchestrator controls whether the sync orchestrator functionality is enabled.
	// When true, uses the sliding window approach for directory synchronization.
	UseSyncOrchestrator bool = true

	// semaphore controls the maximum number of directories processed concurrently
	// to prevent resource exhaustion during large-scale sync operations.
	semaphore *ThrottleSemaphore

	// CustomSyncHandler holds the current sync handler implementation.
	// Defaults to syncOrchestratorHandler but can be customized for different strategies.
	CustomSyncHandler CustomSyncHandlerFunc = syncOrchestratorHandler
	// notFoundErrors contains error messages that are considered normal during sync operations.
	// These errors don't cause the sync to fail (e.g., 404 responses from target locations).
	notFoundErrors []string = []string{
		"ParentNotFound",
		"BlobNotFound",
		"PathNotFound",
		"ResourceNotFound",
	}

	orchestratorOptions *SyncOrchestratorOptions
)

type minimalStoredObject struct {
	relativePath string // Relative path of the object within the directory

	// Originally, we only needed to store relativePath for the purpose of enqueueing subdirectories.
	// We are adding changeTime to this struct to provide us parent directory change time while we
	// process subdirectories. This is for enabling the optimization of skipping target traversal.
	// This has an implication of increased memory usage for all scenarios to provide optimization for
	// just NFS sources (as of 06/01/2025). Ideally, we can decide to not store changeTime here
	// at the time of initialization of the SyncTraverser. This is an optimization that we will consider
	// later.
	changeTime time.Time // Change time of the object

	isPresentAtDestination bool // Indicates if the object is present at the secondary location
}

// GetCustomSyncHandlerInfo returns a description of the current sync handler implementation.
// Used for logging and debugging purposes to identify which sync strategy is active.
func GetCustomSyncHandlerInfo() string {
	return "Sync Handler: Sliding Window"
}

// SyncTraverser manages the traversal of a single directory during sync operations.
// It processes files and subdirectories, storing them in the indexer and scheduling transfers.
type SyncTraverser struct {
	enumerator *syncEnumerator // Main sync enumerator that coordinates the overall sync operation
	comparator objectProcessor // Processes objects from the destination for comparison
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

func writeSyncErrToChannel(errorChannel chan<- TraverserErrorItemInfo, err SyncOrchErrorInfo) {
	if errorChannel != nil {
		select {
		case errorChannel <- err:
		default:
			// Channel might be full, log the error instead
			syncOrchestratorLog(
				common.LogError,
				fmt.Sprintf("Failed to send error to channel: %v", err.ErrorMessage()))
		}
	}
}

func validateLocalRoot(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		return err
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
func validateS3Root(sourcePath string) error {

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

func validateBlobRoot(sourcePath string) error {
	_, err := blob.ParseURL(sourcePath)
	if err != nil {
		return err
	}
	return nil
}

// validateBlobFSRoot validates a BlobFS root URL by converting the DFS endpoint to Blob
// and then parsing it using the Blob URL parser. This mirrors how BlobFS traversers are initialized.
func validateBlobFSRoot(sourcePath string) error {
	r := strings.Replace(sourcePath, ".dfs", ".blob", 1)
	_, err := blob.ParseURL(r)
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
func validateAndGetRootObject(path string, fromTo common.FromTo) (minimalStoredObject, error) {

	syncOrchestratorLog(
		common.LogInfo,
		fmt.Sprintf("Getting root object for path = %s\n", path))
	var err error

	switch fromTo.From() {
	case common.ELocation.Local():
		err = validateLocalRoot(path)
	case common.ELocation.S3():
		err = validateS3Root(path)
	case common.ELocation.Blob():
		err = validateBlobRoot(path)
	case common.ELocation.BlobFS():
		err = validateBlobFSRoot(path)
	default:
		err = fmt.Errorf("sync orchestrator is not supported for %s source", fromTo.From().String())
	}

	if err == nil {
		return minimalStoredObject{
			relativePath:           "",
			changeTime:             time.Time{},
			isPresentAtDestination: true,
		}, nil
	} else {
		return minimalStoredObject{}, err
	}
}

// buildChildPath constructs the full child path by joining the base directory
// with the relative path, and handles path separator normalization.
// Ensures consistent path formatting across different operating systems.
func buildChildPath(baseDir, relativePath string) string {
	var strs []string

	if baseDir != "" {
		strs = []string{baseDir, relativePath}
	} else {
		strs = []string{relativePath}
	}

	childPath := strings.TrimSuffix(
		strings.Join(strs, common.AZCOPY_PATH_SEPARATOR_STRING),
		common.AZCOPY_PATH_SEPARATOR_STRING)

	return childPath
}

// processor handles StoredObjects from the source location during traversal.
// It builds the full path, categorizes objects as files or directories,
// and stores them in the indexer for later comparison and transfer.
func (st *SyncTraverser) processor(so StoredObject) error {
	// Debug: Log original path before transformation
	originalPath := so.relativePath
	syncOrchestratorLog(common.LogDebug, fmt.Sprintf("[PROCESSOR] Before buildChildPath - dir='%s', originalPath='%s'", st.dir, originalPath))

	// Skip directory placeholder objects that represent the current directory itself
	// These have empty relativePath after prefix trimming and would cause re-enqueueing of the same directory
	if originalPath == "" && so.entityType == common.EEntityType.Folder() {
		syncOrchestratorLog(common.LogDebug, fmt.Sprintf("[PROCESSOR] Skipping self-referential directory placeholder for dir='%s'", st.dir))
		return nil
	}

	// Build full path for the object relative to current directory
	so.relativePath = buildChildPath(st.dir, so.relativePath)

	// Debug: Log every object being processed
	syncOrchestratorLog(common.LogDebug, fmt.Sprintf("[PROCESSOR] After buildChildPath - finalPath='%s' (type: %s)", so.relativePath, so.entityType))

	// Thread-safe storage in the indexer first
	st.enumerator.objectIndexer.rwMutex.Lock()
	err := st.enumerator.objectIndexer.store(so)
	st.enumerator.objectIndexer.rwMutex.Unlock()

	if err != nil {
		return err
	}

	// Update throttling counters if enabled
	if enableThrottling {
		totalFilesInIndexer.Add(1) // Increment the count of files in the indexer
	}

	if so.entityType == common.EEntityType.Folder() {
		st.sub_dirs = append(st.sub_dirs, minimalStoredObject{
			relativePath: so.relativePath,
			changeTime:   so.changeTime,
		})
		// Debug logging to track subdirectory discovery
		syncOrchestratorLog(common.LogDebug, fmt.Sprintf("Discovered subdirectory: %s (total subdirs now: %d)", so.relativePath, len(st.sub_dirs)))
	}

	return nil
}

// customComparator processes StoredObjects from the destination location during traversal.
// It builds the full path and passes the object to the main comparator for sync decision making.
func (st *SyncTraverser) customComparator(so StoredObject) error {
	// Build full path for destination object
	so.relativePath = buildChildPath(st.dir, so.relativePath)

	// comparison and deletion from indexer will happen under the lock
	return st.comparator(so)
}

// finalize completes the processing of the current directory by scheduling
// transfers for all discovered files and cleaning up the indexer.
// This method is called after both source and destination traversals are complete.
func (st *SyncTraverser) finalize(scheduleTransfer bool) error {

	// Build the directory prefix for matching child objects
	var dirPrefix string
	if st.dir == "" {
		// Root directory - we need to match items that don't have a parent directory
		// or items that are direct children of root
		dirPrefix = ""
	} else {
		// Non-root directory - match items that start with "dir/"
		dirPrefix = st.dir + common.AZCOPY_PATH_SEPARATOR_STRING
	}

	// Use exclusive lock for the entire operation to prevent concurrent iteration and modification
	st.enumerator.objectIndexer.rwMutex.RLock()

	if enableThrottling {
		totalFilesInIndexer.Store(int64(len(st.enumerator.objectIndexer.indexMap))) // Set accurate count
	}

	// Collect items to process (we need to collect first to avoid modifying map while iterating)
	var itemsToProcess []string
	for path := range st.enumerator.objectIndexer.indexMap {
		if st.belongsToCurrentDirectory(path, dirPrefix) {
			itemsToProcess = append(itemsToProcess, path)
		}
	}
	st.enumerator.objectIndexer.rwMutex.RUnlock()

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
	if st.dir == "" {
		// Root directory case:
		// - Accept paths that don't contain any separators (direct children)
		// - Or paths that are exactly what we're looking for at root level
		if !strings.Contains(path, common.AZCOPY_PATH_SEPARATOR_STRING) {
			return true
		}
		// For root, we might also want to include direct children
		// Count separators to determine if it's a direct child
		separatorCount := strings.Count(path, common.AZCOPY_PATH_SEPARATOR_STRING)
		return separatorCount <= 1 // Direct child or root item
	} else {
		// Non-root directory case:
		// Must start with our directory prefix and be a direct child
		if !strings.HasPrefix(path, dirPrefix) {
			return false
		}

		// Get the remainder after our prefix
		remainder := path[len(dirPrefix):]

		// If remainder is empty, this is the directory itself
		if remainder == "" {
			return true
		}

		// If remainder contains separators, it's a grandchild, not direct child
		// We only want direct children
		return !strings.Contains(remainder, common.AZCOPY_PATH_SEPARATOR_STRING)
	}
}

// hasAnyChildChangedSinceLastSync checks if at least 1 child object changed in the current directory
// since the last successful sync job start time.
func (st *SyncTraverser) hasAnyChildChangedSinceLastSync() (bool, uint32) {
	// Build the directory prefix for matching child objects
	var dirPrefix string
	if st.dir == "" {
		// Root directory - we need to match items that don't have a parent directory
		// or items that are direct children of root
		dirPrefix = ""
	} else {
		// Non-root directory - match items that start with "dir/"
		dirPrefix = st.dir + common.AZCOPY_PATH_SEPARATOR_STRING
	}

	foundOneChanged := false

	// This is purely for incrementing the metrics with a computation cost
	childCount := uint32(0)

	st.enumerator.objectIndexer.rwMutex.RLock()
	// Collect items to process (we need to collect first to avoid modifying map while iterating)
	for path := range st.enumerator.objectIndexer.indexMap {
		if st.belongsToCurrentDirectory(path, dirPrefix) {
			// Increment child count for each item
			// This will be the total number of children in the directory only if there are
			// no changes in any file.
			childCount++

			if st.enumerator.objectIndexer.indexMap[path].changeTime.IsZero() {
				// If change time is zero, we cannot determine if it changed since last sync
				// so we assume it has changed
				foundOneChanged = true
				break
			} else if st.enumerator.objectIndexer.indexMap[path].changeTime.After(orchestratorOptions.lastSuccessfulSyncJobStartTime) {
				foundOneChanged = true
				break
			}
		}
	}
	st.enumerator.objectIndexer.rwMutex.RUnlock()
	return foundOneChanged, childCount - uint32(len(st.sub_dirs))
}

// finalizeChild processes a single child object (file or directory) by scheduling it for transfer.
// It retrieves the stored object from the indexer and schedules it for transfer.
// If the object is a directory, it will be processed after all files in that directory are finalized.
// This method is called after the traversal is complete for each child object.
func (st *SyncTraverser) finalizeChild(child string, scheduleTransfer bool) error {
	st.enumerator.objectIndexer.rwMutex.RLock()
	// Get pointer to the stored object from indexer
	storedObject, exists := st.enumerator.objectIndexer.indexMap[child]
	st.enumerator.objectIndexer.rwMutex.RUnlock()

	if exists {
		// Schedule the file/directory for transfer using the pointer
		if scheduleTransfer {
			err := st.enumerator.ctp.scheduleCopyTransfer(storedObject)
			if err != nil {
				return err
			}
		}

		// Remove from indexer to free memory
		st.enumerator.objectIndexer.rwMutex.Lock()
		delete(st.enumerator.objectIndexer.indexMap, child)
		st.enumerator.objectIndexer.rwMutex.Unlock()

		if enableThrottling {
			totalFilesInIndexer.Add(-1) // Decrement the count after processing
		}
	}

	return nil
}

// shouldTrySkippingTargetTraversal checks if we should even try skipping the target traversal
func (st *SyncTraverser) shouldTrySkippingTargetTraversal(parentDirCTime time.Time, deleteDestination common.DeleteDestination) bool {

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

	return orchestratorOptions != nil &&
		orchestratorOptions.valid &&
		orchestratorOptions.optimizeEnumerationByCTime &&
		deleteDestination == common.EDeleteDestination.False() &&
		!parentDirCTime.IsZero() &&
		!orchestratorOptions.lastSuccessfulSyncJobStartTime.IsZero() &&
		parentDirCTime.Before(orchestratorOptions.lastSuccessfulSyncJobStartTime)
}

// newSyncTraverser creates a new SyncTraverser instance for processing a specific directory.
// Pre-allocates slices with reasonable capacity to reduce memory allocations.
func newSyncTraverser(enumerator *syncEnumerator, dir string, comparator objectProcessor) *SyncTraverser {
	return &SyncTraverser{
		enumerator: enumerator,
		dir:        dir,
		sub_dirs:   make([]minimalStoredObject, 0, directorySizeBuffer),
		comparator: comparator,
	}
}

func validate(cca *cookedSyncCmdArgs, orchestratorOptions *SyncOrchestratorOptions) error {
	switch cca.fromTo {
	case common.EFromTo.LocalBlob(), common.EFromTo.LocalBlobFS(), common.EFromTo.LocalFile(), common.EFromTo.S3Blob(), common.EFromTo.BlobBlob(), common.EFromTo.BlobBlobFS(), common.EFromTo.BlobFSBlob(), common.EFromTo.BlobFSBlobFS():
		// sync orchestrator is supported for these types
	default:
		return fmt.Errorf(
			"sync orchestrator is only supported for the following source and destination types:\n" +
				"\t- Local->Blob\n" +
				"\t- Local->BlobFS\n" +
				"\t- Local->File\n" +
				"\t- S3->Blob\n" +
				"\t- Blob->Blob\n" +
				"\t- Blob->BlobFS\n" +
				"\t- BlobFS->Blob\n" +
				"\t- BlobFS->BlobFS",
		)
	}

	if cca.recursive {
		return errors.New("sync orchestrator does not support recursive traversal. Use --recursive=false.")
	}

	if orchestratorOptions == nil {
		return errors.New("orchestrator options are required for sync orchestrator")
	}

	if orchestratorOptions != nil && orchestratorOptions.valid {
		return orchestratorOptions.validate(cca.fromTo.From())
	}

	return nil
}

// syncOrchestratorHandler is the main entry point for the sync orchestrator.
// It initializes profiling, sets up resource limits, and delegates to runSyncOrchestrator.
func syncOrchestratorHandler(cca *cookedSyncCmdArgs, enumerator *syncEnumerator, ctx context.Context) error {
	if startGoProfiling {
		// Start the profiling server for performance monitoring
		go func() {
			syncOrchestratorLog(common.LogInfo, "Listening to port 6060..\n")
			http.ListenAndServe("localhost:6060", nil)
		}()
	}

	err := validate(cca, enumerator.orchestratorOptions) // Validate the command arguments for sync orchestrator
	if err != nil {
		syncOrchestratorLog(common.LogPanic, err.Error())
		return err
	}

	orchestratorOptions = enumerator.orchestratorOptions

	// Initialize resource limits based on source/destination types
	initializeLimits(orchestratorOptions)
	return cca.runSyncOrchestrator(enumerator, ctx)
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
func (cca *cookedSyncCmdArgs) runSyncOrchestrator(enumerator *syncEnumerator, ctx context.Context) error {
	startTime := time.Now()
	mainCtx, cancel := context.WithCancel(ctx) // Use mainCtx for operations, cancel to signal shutdown
	defer cancel()                             // Ensure cancellation happens on exit

	cca.orchestratorCancel = cancel // Store cancel function for later use

	// Initialize semaphore for directory concurrency control
	if enableThrottling {
		semaphore = NewThrottleSemaphore(mainCtx, cca.jobID)
		defer semaphore.Close()
	}

	// Log the orchestrator start with key configuration values
	syncOrchestratorLog(
		common.LogInfo,
		fmt.Sprintf("Starting sync orchestrator - Source: %s, Destination: %s, options: %v",
			cca.source.Value,
			cca.destination.Value,
			orchestratorOptions.ToStringMap()))

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
		defer totalDirectoriesProcessed.Add(1)

		var err error

		// Acquire semaphore slot to limit concurrent directory processing
		if enableThrottling {
			err = semaphore.AcquireSourceSlot(mainCtx)
			if err != nil {
				syncOrchestratorLog(
					common.LogError,
					fmt.Sprintf("Failed to acquire source slot for dir '%s': %s", dir.(minimalStoredObject).relativePath, err))
				return err
			}
		}

		srcDirEnumerating.Add(1) // Increment active directory count

		// Debug logging to track directory processing
		syncOrchestratorLog(common.LogDebug, fmt.Sprintf("START processing directory: '%s' (active dirs: src=%d dst=%d)",
			dir.(minimalStoredObject).relativePath, srcDirEnumerating.Load(), dstDirEnumerating.Load()))

		// Build source and destination paths for current directory
		sync_src := []string{cca.source.Value, dir.(minimalStoredObject).relativePath}
		sync_dst := []string{cca.destination.Value, dir.(minimalStoredObject).relativePath}

		pt_src := cca.source
		st_src := cca.destination

		pt_src.Value = strings.Join(sync_src, common.AZCOPY_PATH_SEPARATOR_STRING)
		st_src.Value = strings.Join(sync_dst, common.AZCOPY_PATH_SEPARATOR_STRING)

		// Debug: Log the full source URL being used for traverser creation
		syncOrchestratorLog(common.LogDebug, fmt.Sprintf("[TRAVERSER_CREATE] Creating traverser for dir='%s', full source URL='%s'", dir.(minimalStoredObject).relativePath, pt_src.Value))

		// Handle Windows path separators
		if runtime.GOOS == "windows" {
			pt_src.Value = strings.ReplaceAll(pt_src.Value, "/", "\\")
			st_src.Value = strings.ReplaceAll(st_src.Value, "\\", "/")
		}

		// Get traverser templates from enumerator
		ptt := enumerator.primaryTraverserTemplate
		stt := enumerator.secondaryTraverserTemplate

		var errMsg string

		// Create source traverser for current directory
		pt, err := InitResourceTraverser(
			pt_src,
			ptt.location,
			mainCtx,
			ptt.options)
		if err != nil {
			errMsg = fmt.Sprintf("Creating source traverser failed for dir %s: %s", pt_src.Value, err)
			syncOrchestratorLog(common.LogError, errMsg)
			writeSyncErrToChannel(ptt.options.ErrorChannel, SyncOrchErrorInfo{
				DirPath:           pt_src.Value,
				DirName:           dir.(minimalStoredObject).relativePath,
				ErrorMsg:          errors.New(errMsg),
				TraverserLocation: cca.fromTo.From(),
			})
			return err
		}

		// Create destination traverser for current directory
		st, err := InitResourceTraverser(
			st_src,
			stt.location,
			mainCtx,
			stt.options)
		if err != nil {
			errMsg = fmt.Sprintf("Creating target traverser failed for dir %s: %s\n", st_src.Value, err)
			syncOrchestratorLog(common.LogError, errMsg)
			writeSyncErrToChannel(stt.options.ErrorChannel, SyncOrchErrorInfo{
				DirPath:           st_src.Value,
				DirName:           dir.(minimalStoredObject).relativePath,
				ErrorMsg:          errors.New(errMsg),
				TraverserLocation: cca.fromTo.To(),
			})
			return err
		}

		// Create sync traverser for this directory
		stra := newSyncTraverser(enumerator, dir.(minimalStoredObject).relativePath, enumerator.objectComparator)

		err = pt.Traverse(noPreProccessor, stra.processor, enumerator.filters)
		srcDirEnumerating.Add(-1) // Decrement active directory count

		// Release source slot after source traversal is complete
		if enableThrottling {
			semaphore.ReleaseSourceSlot()
		}

		if err != nil {
			errMsg = fmt.Sprintf("primary traversal failed for dir %s : %s\n", pt_src.Value, err)
			syncOrchestratorLog(common.LogError, errMsg)
			writeSyncErrToChannel(ptt.options.ErrorChannel, SyncOrchErrorInfo{
				DirPath:           pt_src.Value,
				DirName:           dir.(minimalStoredObject).relativePath,
				ErrorMsg:          errors.New(errMsg),
				TraverserLocation: cca.fromTo.From(),
			})
			cca.IncrementSourceFolderEnumerationFailed()
			return err
		}

		// Flag to control whether we traverse the destination
		traverseDestination := true

		// Flag to check if destination exists
		// We will use the parent directory flag as the seed value to avoid redundant checks
		isDestinationPresent := dir.(minimalStoredObject).isPresentAtDestination
		finalize := true // Flag to control whether we finalize
		// Before proceeding, check if we need to enumerate the destination
		if isDestinationPresent &&
			stra.shouldTrySkippingTargetTraversal(dir.(minimalStoredObject).changeTime, cca.deleteDestination) {
			// It is safe to use change time comparison to determine if the destination needs enumeration,
			// Enumerate all child objects of this directory in the indexer and check all of their change times.
			// If any of them is after the last successful sync, we need to enumerate the destination.
			// Otherwise, we can skip the destination enumeration and proceed with scheduling transfers.

			// For debugging:
			// fmt.Printf("Checking if destination enumeration for dir %s can be skipped.\n", st_src.Value)

			if changed, fileCount := stra.hasAnyChildChangedSinceLastSync(); !changed {
				err = stra.finalize(false) // false indicates we do not want to schedule transfers yet
				if err != nil {
					errMsg = fmt.Sprintf("Sync finalize to skip target enumeration failed for source dir %s.\n", pt_src.Value)
					syncOrchestratorLog(common.LogError, errMsg)
					writeSyncErrToChannel(ptt.options.ErrorChannel, SyncOrchErrorInfo{
						DirPath:           pt_src.Value,
						DirName:           dir.(minimalStoredObject).relativePath,
						ErrorMsg:          errors.New(errMsg),
						TraverserLocation: cca.fromTo.From(),
					})
					return err
				}

				// For debugging:
				// fmt.Printf("Skipping destination enumeration for dir %s.\n", st_src.Value)
				traverseDestination = false // No need to traverse destination if we are skipping it
				finalize = false            // No need to finalize as we are not scheduling transfers

				for range fileCount {
					// We can increment the count of not transferred files as well
					ptt.options.IncrementNotTransferred(common.EEntityType.File())
				}

				for range stra.sub_dirs {
					ptt.options.IncrementNotTransferred(common.EEntityType.Folder())
				}

				dstDirEnumerationSkippedBasedOnCTime.Add(1) // Increment skipped count based on ctime optimization
			}
		}

		if isDestinationPresent && traverseDestination {
			// Acquire target slot for target traversal
			if enableThrottling {
				err = semaphore.AcquireTargetSlot(mainCtx)
				if err != nil {
					errMsg = fmt.Sprintf("Failed to acquire target slot for dir %s: %s", st_src.Value, err)
					syncOrchestratorLog(common.LogError, errMsg)
					// Release destination directory count since we're bailing out
					dstDirEnumerating.Add(-1)
					return err
				}
			}

			dstDirEnumerating.Add(1) // Increment active destination directory count

			err = st.Traverse(noPreProccessor, stra.customComparator, enumerator.filters)

			dstDirEnumerating.Add(-1) // Decrement active destination directory count

			// Release target slot after target traversal is complete
			if enableThrottling {
				semaphore.ReleaseTargetSlot()
			}

			if err != nil {
				errMsg = fmt.Sprintf("Secondary traversal failed for dir %s = %s\n", st_src.Value, err)
				syncOrchestratorLog(common.LogError, errMsg)
				// Only report unexpected errors (404s are normal for new files)
				if IsDestinationNotFoundDuringSync(err) {
					isDestinationPresent = false // Destination not found
				} else {
					writeSyncErrToChannel(stt.options.ErrorChannel, SyncOrchErrorInfo{
						DirPath:           st_src.Value,
						DirName:           dir.(minimalStoredObject).relativePath,
						ErrorMsg:          errors.New(errMsg),
						TraverserLocation: cca.fromTo.To(),
					})

					cca.IncrementDestinationFolderEnumerationFailed()

					err = stra.finalize(false) // false indicates we do not want to schedule transfers yet
					if err != nil {
						errMsg = fmt.Sprintf("Failed to cleanup indexer object due to target traversal failure - %s. There may be unintended transfers.\n", pt_src.Value)
						syncOrchestratorLog(common.LogError, errMsg)
						writeSyncErrToChannel(ptt.options.ErrorChannel, SyncOrchErrorInfo{
							DirPath:           pt_src.Value,
							DirName:           dir.(minimalStoredObject).relativePath,
							ErrorMsg:          errors.New(errMsg),
							TraverserLocation: cca.fromTo.From(),
						})
						return err
					}

					return err
				}
			}
		} else {
			cca.IncrementDestinationFolderEnumerationSkipped()
		}

		if finalize {

			// Complete processing for this directory and schedule transfers
			err = stra.finalize(true) // true indicates we want to schedule transfers

			if err != nil {
				errMsg = fmt.Sprintf("Sync finalize failed for source dir %s.\n", pt_src.Value)
				syncOrchestratorLog(common.LogError, errMsg)
				writeSyncErrToChannel(ptt.options.ErrorChannel, SyncOrchErrorInfo{
					DirPath:           pt_src.Value,
					DirName:           dir.(minimalStoredObject).relativePath,
					ErrorMsg:          errors.New(errMsg),
					TraverserLocation: cca.fromTo.From(),
				})
				return err
			}
		}

		// Enqueue discovered subdirectories for processing
		syncOrchestratorLog(common.LogDebug, fmt.Sprintf("[ENQUEUE_START] Parent dir '%s': enqueueing %d subdirectories", dir.(minimalStoredObject).relativePath, len(stra.sub_dirs)))
		for idx, sub_dir := range stra.sub_dirs {
			crawlWg.Add(1) // IMPORTANT: Add to WaitGroup *before* enqueuing
			syncOrchestratorLog(common.LogDebug, fmt.Sprintf("[ENQUEUE_%d] Subdirectory: '%s' (parent: '%s')", idx, sub_dir.relativePath, dir.(minimalStoredObject).relativePath))
			enqueueDir(minimalStoredObject{
				relativePath:           sub_dir.relativePath,
				changeTime:             sub_dir.changeTime,
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
	if cca.fromTo.From() != common.ELocation.S3() {
		srcIsDir, err = enumerator.primaryTraverser.IsDirectory(true)

		if err != nil {
			syncOrchestratorLog(
				common.LogError,
				fmt.Sprintf("Failed to check if source is a directory. Err: %s", err))
			return err
		}
	} else {
		// XDM: s3Traverser.IsDirectory is failing for valid directories, skipping the check for S3
		srcIsDir = true
		syncOrchestratorLog(
			common.LogWarning,
			fmt.Sprintf("Assuming source - %s is a directory for S3", cca.source.Value), true)
	}

	if err != nil {
		syncOrchestratorLog(
			common.LogPanic,
			fmt.Sprintf("Failed to check if source is a directory. Err: %s", err))
		return err
	}

	if !srcIsDir {
		err = fmt.Errorf("source is not recognized as a directory")
		syncOrchestratorLog(common.LogPanic, fmt.Sprintf("Source is not recognized as a directory. Err: %s", err))
		return err
	}

	// Get the root object to start synchronization
	root, err := validateAndGetRootObject(cca.source.Value, cca.fromTo)
	if err != nil {
		syncOrchestratorLog(common.LogPanic, fmt.Sprintf("Root object creation failed: %s", err))
		return err
	}

	// Ensure proper cleanup in ALL scenarios (success, failure, cancellation)
	cleanupFunc := func() {
		// Always shutdown monitoring goroutines
		syncOrchestratorLog(common.LogInfo, fmt.Sprintf("Orchestrator exiting. Execution time: %v.", time.Since(startTime)), true)
	}
	defer cleanupFunc()

	crawlWg.Add(1) // Add the root directory to the WaitGroup

	// Start parallel crawling with specified concurrency
	parallel.Crawl(mainCtx, root, syncOneDir, int(crawlParallelism))

	// Cancellation-aware wait
	done := make(chan struct{})
	go func() {
		defer close(done)
		crawlWg.Wait() // Wait for all goroutines in background
	}()

	select {
	case <-done:
		// All goroutines completed normally
		syncOrchestratorLog(common.LogInfo, "All sync traversers exited.")

	case <-mainCtx.Done():
		// Cancellation occurred
		syncOrchestratorLog(common.LogInfo, "Orchestrator cancellation detected.")
		return nil
	}

	// Always try to finalize the enumerator. This will set cancellation complete and dispatch final part
	syncOrchestratorLog(common.LogInfo, "Finalizing enumerator.")
	finalizeErr := enumerator.finalize()
	if finalizeErr != nil {
		syncOrchestratorLog(common.LogPanic, fmt.Sprintf("Enumerator finalize failed: %v", finalizeErr))
		// If no previous error, use the finalize error
		if err == nil {
			err = finalizeErr
		}
	}

	return err
}

// custom logging function for the sync orchestrator
func syncOrchestratorLog(level common.LogLevel, toLog string, logToConsole ...bool) {
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

	if azcopyScanningLogger != nil {
		// XDM: Log all messages at the error level as that is the log level set by Mover
		azcopyScanningLogger.Log(common.LogError, toLog)
	}

	if azcopyScanningLogger == nil || shouldLogToConsole {
		toLog = "[AzCopy] " + toLog
		switch level {
		case common.LogError, common.LogPanic, common.LogWarning:
			glcm.Warn(toLog)
		case common.LogInfo:
			glcm.Info(toLog)
		default:
			glcm.Info(toLog)
		}
	}
}
