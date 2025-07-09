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

	// syncMutex provides thread-safe access to shared resources during sync operations.
	// Protects concurrent access to indexer operations and file counting.
	syncMutex sync.Mutex

	// dirSemaphore controls the maximum number of directories processed concurrently
	// to prevent resource exhaustion during large-scale sync operations.
	dirSemaphore *DirSemaphore

	// CustomSyncHandler holds the current sync handler implementation.
	// Defaults to syncOrchestratorHandler but can be customized for different strategies.
	CustomSyncHandler CustomSyncHandlerFunc = syncOrchestratorHandler
	// expectedErrors contains error messages that are considered normal during sync operations.
	// These errors don't cause the sync to fail (e.g., 404 responses from target locations).
	expectedErrors []string = []string{
		"RESPONSE 404",
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

func IsExpectedErrorForTargetDuringSync(err error) bool {
	isExpectedError := false
	for _, expectedErr := range expectedErrors {
		if strings.Contains(err.Error(), expectedErr) {
			isExpectedError = true
			break
		}
	}

	return isExpectedError
}

func writeSyncErrToChannel(errorChannel chan<- TraverserErrorItemInfo, err SyncOrchErrorInfo) {
	if errorChannel != nil {
		select {
		case errorChannel <- err:
		default:
			// Channel might be full, log the error instead
			WarnStdoutAndScanningLog(fmt.Sprintf("Failed to send error to channel: %v", err.ErrorMessage()))
		}
	}
}

func getRootStoredObjectLocal(path string) (StoredObject, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return StoredObject{}, err
	}

	root := newStoredObject(
		nil,
		fi.Name(),
		"",
		common.EEntityType.Folder(),
		time.Time{},
		0,
		noContentProps,
		noBlobProps,
		noMetadata,
		"")

	return root, nil
}

// getRootStoredObjectS3 returns the root object for the sync orchestrator based on the S3 source path.
// It parses the S3 URL and determines the entity type (file or folder) based on the URL structure.
//
// Parameters:
// - sourcePath: The S3 source path as a string.
//
// Returns:
// - StoredObject: The root StoredObject for the given S3 source path.
// - error: An error if parsing the URL or creating the StoredObject fails.
func getRootStoredObjectS3(sourcePath string) (StoredObject, error) {

	parsedURL, err := url.Parse(sourcePath)
	if err != nil {
		return StoredObject{}, err
	}

	s3UrlParts, err := common.NewS3URLParts(*parsedURL)
	if err != nil {
		return StoredObject{}, err
	}

	var searchPrefix string = strings.Join([]string{s3UrlParts.BucketName, s3UrlParts.ObjectKey}, common.AZCOPY_PATH_SEPARATOR_STRING)

	root := newStoredObject(
		nil,
		searchPrefix,
		"",
		common.EEntityType.Folder(),
		time.Time{},
		0,
		noContentProps,
		noBlobProps,
		nil,
		s3UrlParts.BucketName)

	return root, nil
}

// GetRootStoredObject returns the root object for the sync orchestrator
// based on the source path and fromTo configuration. This determines the starting
// point for sync enumeration operations.
//
// Parameters:
// - path: The source path to create a root object for
// - fromTo: Specifies the source and destination location types
//
// Returns:
// - StoredObject: Root object containing path and entity type information
// - error: Error if the source type is unsupported or path processing fails
func GetRootStoredObject(path string, fromTo common.FromTo) (StoredObject, error) {

	glcm.Info(fmt.Sprintf("Getting root object for path = %s\n", path))

	switch fromTo.From() {
	case common.ELocation.Local():
		return getRootStoredObjectLocal(path)
	case common.ELocation.S3():
		return getRootStoredObjectS3(path)
	default:
		return StoredObject{}, fmt.Errorf("sync orchestrator is not supported for %s source", fromTo.From().String())
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
	// Build full path for the object relative to current directory
	so.relativePath = buildChildPath(st.dir, so.relativePath)

	// Thread-safe storage in the indexer first
	syncMutex.Lock()
	err := st.enumerator.objectIndexer.store(so)
	if err != nil {
		syncMutex.Unlock()
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
	}
	syncMutex.Unlock()

	return nil
}

// customComparator processes StoredObjects from the destination location during traversal.
// It builds the full path and passes the object to the main comparator for sync decision making.
func (st *SyncTraverser) customComparator(so StoredObject) error {
	// Build full path for destination object
	so.relativePath = buildChildPath(st.dir, so.relativePath)

	// Thread-safe comparison processing
	syncMutex.Lock()
	err := st.comparator(so)
	syncMutex.Unlock()

	return err
}

// Finalize completes the processing of the current directory by scheduling
// transfers for all discovered files and cleaning up the indexer.
// This method is called after both source and destination traversals are complete.
func (st *SyncTraverser) Finalize(scheduleTransfer bool) error {

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

	// Update final file count for throttling
	syncMutex.Lock()
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
	syncMutex.Unlock()

	// Process collected items
	for _, path := range itemsToProcess {
		err := st.FinalizeChild(path, scheduleTransfer)
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

	syncMutex.Lock()
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
	syncMutex.Unlock()
	return foundOneChanged, childCount - uint32(len(st.sub_dirs))
}

// FinalizeChild processes a single child object (file or directory) by scheduling it for transfer.
// It retrieves the stored object from the indexer and schedules it for transfer.
// If the object is a directory, it will be processed after all files in that directory are finalized.
// This method is called after the traversal is complete for each child object.
func (st *SyncTraverser) FinalizeChild(child string, scheduleTransfer bool) error {
	syncMutex.Lock()
	// Get pointer to the stored object from indexer
	storedObject, exists := st.enumerator.objectIndexer.indexMap[child]
	syncMutex.Unlock()

	if exists {
		// Schedule the file/directory for transfer using the pointer
		if scheduleTransfer {
			err := st.enumerator.ctp.scheduleCopyTransfer(storedObject)
			if err != nil {
				return err
			}
		}

		// Remove from indexer to free memory
		syncMutex.Lock()
		delete(st.enumerator.objectIndexer.indexMap, child)

		if enableThrottling {
			totalFilesInIndexer.Add(-1) // Decrement the count after processing
		}
		syncMutex.Unlock()
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

// syncOrchestratorHandler is the main entry point for the sync orchestrator.
// It initializes profiling, sets up resource limits, and delegates to runSyncOrchestrator.
func syncOrchestratorHandler(cca *cookedSyncCmdArgs, enumerator *syncEnumerator, ctx context.Context) error {
	if startGoProfiling {
		// Start the profiling server for performance monitoring
		go func() {
			WarnStdoutAndScanningLog("Listening to port 6060..\n")
			http.ListenAndServe("localhost:6060", nil)
		}()
	}

	orchestratorOptions = enumerator.orchestratorOptions

	// Initialize resource limits based on source/destination types
	initializeLimits(cca.fromTo, orchestratorOptions)
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
func (cca *cookedSyncCmdArgs) runSyncOrchestrator(enumerator *syncEnumerator, ctx context.Context) (err error) {
	startTime := time.Now()
	mainCtx, cancel := context.WithCancel(ctx) // Use mainCtx for operations, cancel to signal shutdown
	defer cancel()                             // Ensure cancellation happens on exit

	// Initialize semaphore for directory concurrency control
	if enableThrottling {
		dirSemaphore = NewDirSemaphore(ctx)
		defer dirSemaphore.Close()
	}

	// Start dedicated semaphore monitor for resource tracking
	semaphoreMonitorWg := sync.WaitGroup{}
	if enableThrottling {
		semaphoreMonitorWg.Add(1)
		go func() {
			defer semaphoreMonitorWg.Done()
			dirSemaphore.semaphoreMonitor(mainCtx)
		}()
	}

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

		// Acquire semaphore slot to limit concurrent directory processing
		if enableThrottling {
			dirSemaphore.AcquireSlot(mainCtx)
			defer dirSemaphore.ReleaseSlot()
		}

		activeDirectoriesEnumerating.Add(1) // Increment active directory count

		// Build source and destination paths for current directory
		sync_src := []string{cca.source.Value, dir.(StoredObject).relativePath}
		sync_dst := []string{cca.destination.Value, dir.(StoredObject).relativePath}

		pt_src := cca.source
		st_src := cca.destination

		pt_src.Value = strings.Join(sync_src, common.AZCOPY_PATH_SEPARATOR_STRING)
		st_src.Value = strings.Join(sync_dst, common.AZCOPY_PATH_SEPARATOR_STRING)

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
			WarnStdoutAndScanningLog(errMsg)
			writeSyncErrToChannel(ptt.options.ErrorChannel, SyncOrchErrorInfo{
				DirPath:           pt_src.Value,
				DirName:           dir.(StoredObject).relativePath,
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
			WarnStdoutAndScanningLog(errMsg)
			writeSyncErrToChannel(stt.options.ErrorChannel, SyncOrchErrorInfo{
				DirPath:           st_src.Value,
				DirName:           dir.(StoredObject).relativePath,
				ErrorMsg:          errors.New(errMsg),
				TraverserLocation: cca.fromTo.To(),
			})
			return err
		}

		// Create sync traverser for this directory
		stra := newSyncTraverser(enumerator, dir.(StoredObject).relativePath, enumerator.objectComparator)

		// Traverse source location and collect files/directories
		err = pt.Traverse(noPreProccessor, stra.processor, enumerator.filters)
		if err != nil {
			errMsg = fmt.Sprintf("primary traversal failed for dir %s : %s\n", pt_src.Value, err)
			WarnStdoutAndScanningLog(errMsg)
			writeSyncErrToChannel(ptt.options.ErrorChannel, SyncOrchErrorInfo{
				DirPath:           pt_src.Value,
				DirName:           dir.(StoredObject).relativePath,
				ErrorMsg:          errors.New(errMsg),
				TraverserLocation: cca.fromTo.From(),
			})
			cca.IncrementSourceFolderEnumerationFailed()
			return err
		}
		activeDirectoriesEnumerating.Add(-1) // Decrement active directory count

		traverseDestination := true // Flag to control whether we traverse the destination

		// Before proceeding, check if we need to enumerate the destination
		if stra.shouldTrySkippingTargetTraversal(dir.(StoredObject).changeTime, cca.deleteDestination) {
			// It is safe to use change time comparison to determine if the destination needs enumeration,
			// Enumerate all child objects of this directory in the indexer and check all of their change times.
			// If any of them is after the last successful sync, we need to enumerate the destination.
			// Otherwise, we can skip the destination enumeration and proceed with scheduling transfers.
			if changed, fileCount := stra.hasAnyChildChangedSinceLastSync(); !changed {
				err = stra.Finalize(false) // false indicates we do not want to schedule transfers yet
				if err != nil {
					errMsg = fmt.Sprintf("Sync finalize to skip target enumeration failed for source dir %s.\n", pt_src.Value)
					WarnStdoutAndScanningLog(errMsg)
					writeSyncErrToChannel(ptt.options.ErrorChannel, SyncOrchErrorInfo{
						DirPath:           pt_src.Value,
						DirName:           dir.(StoredObject).relativePath,
						ErrorMsg:          errors.New(errMsg),
						TraverserLocation: cca.fromTo.From(),
					})
					return err
				}
				traverseDestination = false // No need to traverse destination if we are skipping it

				for range fileCount {
					// We can increment the count of not transferred files as well
					ptt.options.IncrementNotTransferred(common.EEntityType.File())
				}

				for range stra.sub_dirs {
					ptt.options.IncrementNotTransferred(common.EEntityType.Folder())
				}
			}
		}

		if traverseDestination {
			// Traverse destination location for comparison
			err = st.Traverse(noPreProccessor, stra.customComparator, enumerator.filters)
			if err != nil {
				// Only report unexpected errors (404s are normal for new files)
				if !IsExpectedErrorForTargetDuringSync(err) {
					errMsg = fmt.Sprintf("Secondary traversal failed for dir %s = %s\n", st_src.Value, err)
					WarnStdoutAndScanningLog(errMsg)
					writeSyncErrToChannel(stt.options.ErrorChannel, SyncOrchErrorInfo{
						DirPath:           st_src.Value,
						DirName:           dir.(StoredObject).relativePath,
						ErrorMsg:          errors.New(errMsg),
						TraverserLocation: cca.fromTo.To(),
					})

					cca.IncrementDestinationFolderEnumerationFailed()
					return err
				}
			}

			// Complete processing for this directory and schedule transfers
			err = stra.Finalize(true) // true indicates we want to schedule transfers
			if err != nil {
				errMsg = fmt.Sprintf("Sync finalize failed for source dir %s.\n", pt_src.Value)
				WarnStdoutAndScanningLog(errMsg)
				writeSyncErrToChannel(ptt.options.ErrorChannel, SyncOrchErrorInfo{
					DirPath:           pt_src.Value,
					DirName:           dir.(StoredObject).relativePath,
					ErrorMsg:          errors.New(errMsg),
					TraverserLocation: cca.fromTo.From(),
				})
				return err
			}
		}

		// Enqueue discovered subdirectories for processing
		for _, sub_dir := range stra.sub_dirs {
			crawlWg.Add(1) // IMPORTANT: Add to WaitGroup *before* enqueuing
			enqueueDir(StoredObject{
				relativePath: sub_dir.relativePath,
				entityType:   common.EEntityType.Folder(),
				changeTime:   sub_dir.changeTime,
			})
		}

		return nil
	}

	if cca.recursive {
		return errors.New("Sync orchestrator does not support recursive traversal. Use --recursive=false.")
	}

	// verify that the traversers are targeting the same type of resources
	// Sync orchestrator supports only directory to directory sync. The similarity has
	// already been checked in InitEnumerator. Here we check if it is directory or not.
	srcIsDir, _ := enumerator.primaryTraverser.IsDirectory(true)

	if !srcIsDir {
		WarnStdoutAndScanningLog(fmt.Sprintf("Source is not recognized as a directory. Err: %s", err))
		return err
	}

	// Get the root object to start synchronization
	root, err := GetRootStoredObject(cca.source.Value, cca.fromTo)
	if err != nil {
		WarnStdoutAndScanningLog(fmt.Sprintf("Root object creation failed: %s", err))
		return err
	}

	crawlWg.Add(1) // Add the root directory to the WaitGroup

	// Start parallel crawling with specified concurrency
	parallel.Crawl(mainCtx, root, syncOneDir, int(crawlParallelism))

	if enableDebugLogs {
		WarnStdoutAndScanningLog("Crawl completed. Waiting for all directory processors to finish...")
	}
	crawlWg.Wait() // Wait for all tasks (root + enqueued via syncOneDir) to complete
	WarnStdoutAndScanningLog("All sync traversers exited.")

	// Finalize the enumerator to complete any remaining operations
	WarnStdoutAndScanningLog("Finalizing enumerator...")
	err = enumerator.finalize()
	if err != nil {
		WarnStdoutAndScanningLog(fmt.Sprintf("Enumerator finalize failed: %v", err))
		return err
	}

	// Shutdown all monitoring goroutines
	WarnStdoutAndScanningLog("All operations complete. Shutting down monitors...")
	cancel() // Signal all goroutines using mainCtx to stop

	if enableThrottling {
		semaphoreMonitorWg.Wait()
	}
	WarnStdoutAndScanningLog(fmt.Sprintf("Orchestrator exiting. Execution time: %v.", time.Since(startTime)))

	return err
}
