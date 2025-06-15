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

var (
	UseSyncOrchestrator bool = true
)

var syncMutex sync.Mutex

type CustomSyncHandlerFunc func(cca *cookedSyncCmdArgs, enumerator *syncEnumerator, ctx context.Context) error

var CustomSyncHandler CustomSyncHandlerFunc = syncOrchestratorHandler

type CustomCounterIncrementer func(entry fs.DirEntry, t *localTraverser) error

var counterIncrementer CustomCounterIncrementer = IncrementCounter

func IncrementCounter(entry fs.DirEntry, t *localTraverser) error {
	if entry.IsDir() {
		t.incrementEnumerationCounter(common.EEntityType.Folder())
	} else {
		t.incrementEnumerationCounter(common.EEntityType.File())
	}
	return nil
}

func GetCustomSyncHandlerInfo() string {
	return "Sync Handler: Sliding Window"
}

type SyncTraverser struct {
	enumerator *syncEnumerator
	comparator objectProcessor
	dir        string
	sub_dirs   []StoredObject
	children   []StoredObject
}

// global runtime constants for sync orchestrator
var (
	isDestinationFolderAware bool = false
)

func getRootStoredObjectLocal(path string) (StoredObject, error) {
	glcm.Info(fmt.Sprintf("OS Stat on source = %s \n", path))
	fi, err := os.Stat(path)
	if err != nil {
		return StoredObject{}, err
	}

	var entityType common.EntityType = common.EEntityType.File()
	if fi.IsDir() {
		entityType = common.EEntityType.Folder()
	}

	root := newStoredObject(
		nil,
		fi.Name(),
		"",
		entityType,
		time.Time{},
		0,
		noContentProps,
		noBlobProps,
		noMetadata,
		"")

	glcm.Info(fmt.Sprintf("Root object created: %s, Entity type: %s", root.relativePath, entityType.String()))

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

	var entityType common.EntityType = common.EEntityType.Folder()
	if s3UrlParts.IsObjectSyntactically() && !s3UrlParts.IsDirectorySyntactically() && !s3UrlParts.IsBucketSyntactically() {
		// this is not working. It does not impact negatively if a valid prefix is passed.
		entityType = common.EEntityType.File()
	}

	var searchPrefix string = strings.Join([]string{s3UrlParts.BucketName, s3UrlParts.ObjectKey}, common.AZCOPY_PATH_SEPARATOR_STRING)

	root := newStoredObject(
		nil,
		searchPrefix,
		"",
		entityType,
		time.Time{},
		0,
		noContentProps,
		noBlobProps,
		nil,
		s3UrlParts.BucketName)

	if enableDebugLogs {
		glcm.Info(fmt.Sprintf("S3 Root: %s, Entity type: %s", searchPrefix, entityType.String()))
	}

	return root, nil
}

// GetRootStoredObject returns the root object for the sync orchestrator
// based on the source path and fromTo
// We don't really the StoredObject but just the relative path and the entityType
// The rest of the fields are not used at the time of creation
func GetRootStoredObject(path string, fromTo common.FromTo) (StoredObject, error) {

	glcm.Info(fmt.Sprintf("Getting root object for path = %s\n", path))

	switch fromTo.From() {
	case common.ELocation.Local():
		return getRootStoredObjectLocal(path)
	case common.ELocation.S3():
		return getRootStoredObjectS3(path)
	default:
		return StoredObject{}, fmt.Errorf("Sync orchestrator is not supported for %s source.", fromTo.From().String())
	}
}

func (st *SyncTraverser) processor(so StoredObject) error {
	var child_path string
	var strs []string
	if st.dir != "" {
		strs = []string{st.dir, so.relativePath}
	} else {
		strs = []string{so.relativePath}
	}
	child_path = strings.Join(strs, common.AZCOPY_PATH_SEPARATOR_STRING)
	so.relativePath = strings.Trim(child_path, common.AZCOPY_PATH_SEPARATOR_STRING)

	if so.entityType == common.EEntityType.Folder() {
		st.sub_dirs = append(st.sub_dirs, so)
	}

	st.children = append(st.children, so)

	syncMutex.Lock()
	err := st.enumerator.objectIndexer.store(so)
	syncMutex.Unlock()

	return err
}

func (st *SyncTraverser) customComparator(so StoredObject) error {
	var child_path string

	if so.relativePath == "" {
		child_path = st.dir
	} else {
		if st.dir != "" {
			strs := []string{st.dir, so.relativePath}
			child_path = strings.Join(strs, common.AZCOPY_PATH_SEPARATOR_STRING)
		} else {
			child_path = so.relativePath
		}
	}

	so.relativePath = strings.Trim(child_path, common.AZCOPY_PATH_SEPARATOR_STRING)

	syncMutex.Lock()
	err := st.comparator(so)
	syncMutex.Unlock()

	return err
}

func (st *SyncTraverser) Finalize() error {
	for _, child := range st.children {

		syncMutex.Lock()
		so, present := st.enumerator.objectIndexer.indexMap[child.relativePath]
		syncMutex.Unlock()
		if present {
			err := st.enumerator.ctp.scheduleCopyTransfer(so)
			if err != nil {
				return err
			}

			syncMutex.Lock()
			delete(st.enumerator.objectIndexer.indexMap, so.relativePath)
			syncMutex.Unlock()
		}
	}

	return nil
}

func newSyncTraverser(enumerator *syncEnumerator, dir string, comparator objectProcessor) *SyncTraverser {
	return &SyncTraverser{
		enumerator: enumerator,
		dir:        dir,
		sub_dirs:   make([]StoredObject, 0, 1024),
		children:   make([]StoredObject, 0, 1024),
		comparator: comparator,
	}
}

func syncOrchestratorHandler(cca *cookedSyncCmdArgs, enumerator *syncEnumerator, ctx context.Context) error {
	// Start the profiling
	go func() {
		WarnStdoutAndScanningLog("Listening to port 6060..\n")
		http.ListenAndServe("localhost:6060", nil)
	}()

	return cca.runSyncOrchestrator(enumerator, ctx)
}

func (cca *cookedSyncCmdArgs) runSyncOrchestrator(enumerator *syncEnumerator, ctx context.Context) (err error) {
	mainCtx, cancel := context.WithCancel(ctx) // Use mainCtx for operations, cancel to signal shutdown
	defer cancel()                             // Ensure cancellation happens on exit

	StartMetricsCollection(mainCtx)

	// Initialize semaphore for directory concurrency control
	if EnableDirectoryThrottling {
		dirSemaphore = make(chan struct{}, MaxConcurrentDirectories)
	}

	// Initialize adaptive throttle controller
	throttleController := NewAdaptiveThrottleController()

	// Start enhanced system monitor
	monitorWg := sync.WaitGroup{}
	monitorWg.Add(1)
	go func() {
		defer monitorWg.Done()
		enhancedSyncMonitor(mainCtx)
	}()

	// Start dedicated semaphore monitor
	semaphoreMonitorWg := sync.WaitGroup{}
	if EnableDirectoryThrottling {
		semaphoreMonitorWg.Add(1)
		go func() {
			defer semaphoreMonitorWg.Done()
			semaphoreMonitor(mainCtx)
		}()
	}

	// Start monitoring goroutines
	monitorGoroutinesWg := sync.WaitGroup{}
	monitorGoroutinesWg.Add(1)
	go func() {
		defer monitorGoroutinesWg.Done()
		monitorGoroutines(mainCtx)
	}()

	isDestinationFolderAware = cca.fromTo.To().IsFolderAware()

	var crawlWg sync.WaitGroup // WaitGroup for all directory processing tasks

	syncOneDir := func(
		dir parallel.Directory,
		enqueueDir func(parallel.Directory),
		enqueueOutput func(parallel.DirectoryEntry, error)) error {

		defer crawlWg.Done() // Signal this task is done when it finishes

		dirPath := dir.(StoredObject).relativePath

		// Track that this directory entered the processing queue
		totalDirectoriesQueued.Add(1)

		// 1. Semaphore-based directory concurrency control
		if EnableDirectoryThrottling {
			// Increment waiting counter before attempting to acquire semaphore
			waitingForSemaphore.Add(1)

			// Log when directory starts waiting (optional - for detailed debugging)
			if enableDebugLogs {
				WarnStdoutAndScanningLog(fmt.Sprintf(
					"[SEMAPHORE-WAIT] Dir: %s | Waiting: %d | Active: %d | Total: %d",
					dirPath,
					waitingForSemaphore.Load(),
					activeDirProcessors.Load(),
					totalDirectoriesQueued.Load()))
			}

			select {
			case dirSemaphore <- struct{}{}:
				// Successfully acquired semaphore slot
				waitingForSemaphore.Add(-1) // No longer waiting
				defer func() { <-dirSemaphore }()

			case <-mainCtx.Done():
				// Context cancelled while waiting
				waitingForSemaphore.Add(-1) // No longer waiting

				return mainCtx.Err()
			}
		}

		// Track active directory processors
		activeDirProcessors.Add(1)
		defer activeDirProcessors.Add(-1)

		// 2. Multi-metric adaptive throttling
		if shouldThrottle, reason, metrics := throttleController.ShouldThrottle(); shouldThrottle {

			if enableDebugLogs {
				WarnStdoutAndScanningLog(fmt.Sprintf(
					"[THROTTLE-START] Dir: %s | Reason: %s | Goroutines: %d | Memory: %dMB | ActiveDirs: %d | WaitingDirs: %d",
					dirPath, reason, metrics.Goroutines, metrics.ResidentMemoryMB,
					metrics.ActiveDirectories, metrics.WaitingDirectories))
			}

			if err := throttleController.PerformThrottling(mainCtx, dirPath); err != nil {
				return err
			}
		}

		sync_src := []string{cca.Source.Value, dir.(StoredObject).relativePath}
		sync_dst := []string{cca.Destination.Value, dir.(StoredObject).relativePath}

		pt_src := cca.Source
		st_src := cca.Destination

		pt_src.Value = strings.Join(sync_src, common.AZCOPY_PATH_SEPARATOR_STRING)
		st_src.Value = strings.Join(sync_dst, common.AZCOPY_PATH_SEPARATOR_STRING)

		if runtime.GOOS == "windows" {
			pt_src.Value = strings.ReplaceAll(pt_src.Value, "/", "\\")
			st_src.Value = strings.ReplaceAll(st_src.Value, "\\", "/")
		}

		ptt := enumerator.primaryTraverserTemplate
		stt := enumerator.secondaryTraverserTemplate

		syncMutex.Lock()
		err := enumerator.objectIndexer.store(dir.(StoredObject))
		syncMutex.Unlock()

		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("Storing root object failed: %s\n", err))
			return err
		}

		pt, err := InitResourceTraverser(
			pt_src,
			ptt.location,
			&mainCtx,
			ptt.credential,
			ptt.symlinkHandling,
			ptt.listOfFilesChannel,
			ptt.recursive,
			ptt.getProperties,
			ptt.includeDirectoryStubs,
			ptt.permanentDeleteOption,
			ptt.incrementEnumerationCounter,
			ptt.listOfVersionIds,
			ptt.s2sPreserveBlobTags,
			ptt.syncHashType,
			ptt.preservePermissions,
			ptt.logLevel,
			ptt.cpkOptions,
			ptt.errorChannel,
			ptt.stripTopDir,
			ptt.trailingDot,
			ptt.destination,
			ptt.excludeContainerNames,
			ptt.includeVersionsList,
			NewDefaultSyncTraverserOptions())
		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("Creating source traverser failed : %s\n", err))
			return err
		}

		st, err := InitResourceTraverser(
			st_src,
			stt.location,
			&mainCtx,
			stt.credential,
			stt.symlinkHandling,
			stt.listOfFilesChannel,
			stt.recursive,
			stt.getProperties,
			stt.includeDirectoryStubs,
			stt.permanentDeleteOption,
			stt.incrementEnumerationCounter,
			stt.listOfVersionIds,
			stt.s2sPreserveBlobTags,
			stt.syncHashType,
			stt.preservePermissions,
			stt.logLevel,
			stt.cpkOptions,
			stt.errorChannel,
			stt.stripTopDir,
			stt.trailingDot,
			stt.destination,
			stt.excludeContainerNames,
			stt.includeVersionsList,
			NewDefaultSyncTraverserOptions())

		stra := newSyncTraverser(enumerator, dir.(StoredObject).relativePath, enumerator.objectComparator)

		err = pt.Traverse(noPreProccessor, stra.processor, enumerator.filters)
		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("Creating target traverser failed : %s\n", err))
			return err
		}
		err = st.Traverse(noPreProccessor, stra.customComparator, enumerator.filters)
		if err != nil {
			if !strings.Contains(err.Error(), "RESPONSE 404") {
				WarnStdoutAndScanningLog(fmt.Sprintf("Sync traversal failed type = %s \n", err))
				return err
			}
		}

		err = stra.Finalize()
		if err != nil {
			WarnStdoutAndScanningLog("Sync finalize failed!!\n")
			return err
		}

		// If we reach here, it means the directory has been processed successfully
		syncMutex.Lock()
		delete(stra.enumerator.objectIndexer.indexMap, dir.(StoredObject).relativePath)
		syncMutex.Unlock()

		for _, sub_dir := range stra.sub_dirs {
			crawlWg.Add(1) // IMPORTANT: Add to WaitGroup *before* enqueuing
			enqueueDir(sub_dir)
		}

		return nil
	}

	root, err := GetRootStoredObject(cca.Source.Value, cca.fromTo)
	if err != nil {
		WarnStdoutAndScanningLog(fmt.Sprintf("Root object creation failed: %s", err))
		return err
	}

	crawlWg.Add(1) // Add the root directory to the WaitGroup

	if enableDebugLogs {
		WarnStdoutAndScanningLog(fmt.Sprintf(
			"Starting enhanced crawl: Parallelism=%d | MaxDirs=%d | GoroutineLimit=%d | MemoryLimit=%dMB | Root=%s",
			CrawlParallelism, MaxConcurrentDirectories, GoroutineThreshold, MemoryCriticalThresholdMB, root.relativePath))
	}

	parallel.Crawl(mainCtx, root, syncOneDir, CrawlParallelism)

	if enableDebugLogs {
		WarnStdoutAndScanningLog("Crawl completed. Waiting for all directory processors to finish...")
	}
	crawlWg.Wait() // Wait for all tasks (root + enqueued via syncOneDir) to complete
	WarnStdoutAndScanningLog("All sync traversers exited.")

	WarnStdoutAndScanningLog("Finalizing enumerator...")
	err = enumerator.finalize()
	if err != nil {
		WarnStdoutAndScanningLog(fmt.Sprintf("Enumerator finalize failed: %v", err))
		return err
	}

	WarnStdoutAndScanningLog("All operations complete. Shutting down monitors...")
	cancel() // Signal all goroutines using mainCtx to stop

	monitorWg.Wait()           // Wait for syncMonitor to finish
	monitorGoroutinesWg.Wait() // Wait for monitorGoroutines to finish
	if EnableDirectoryThrottling {
		semaphoreMonitorWg.Wait()
	}
	WarnStdoutAndScanningLog("Monitors exited. Orchestrator quitting.")

	return err
}
