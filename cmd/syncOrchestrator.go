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
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
)

var UseSyncOrchestrator = true

// This is temporary flag and logs while we stabilize the sync orchestrator.
// This will be removed once we are confident about the orchestrator.
var enableDebugLogs bool = true

type CustomSyncHandler func(cca *cookedSyncCmdArgs, ctx context.Context) error

var customSyncHandler CustomSyncHandler = syncOrchestratorHandler

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
	return "Sync Handler: Sync Orchestrator"
}

type SyncTraverser struct {
	enumerator *syncEnumerator
	comparator objectProcessor
	dir        string
	sub_dirs   []StoredObject
	children   []StoredObject
}

// AddToObjectIndexer adds a StoredObject to the object indexer of the SyncTraverser.
// It locks the syncMutex before storing the object to ensure thread safety.
// The function returns an error if storing the object fails.
//
// Parameters:
// - so: The StoredObject to be added to the object indexer.
//
// Returns:
// - error: An error if storing the object fails, otherwise nil.
func (st *SyncTraverser) AddToObjectIndexer(so StoredObject) error {

	if enableDebugLogs {
		glcm.Info(fmt.Sprintf("%s: Adding %s to object indexer", st.dir, so.relativePath))
	}
	syncMutex.Lock()
	err := st.enumerator.objectIndexer.store(so)
	syncMutex.Unlock()

	return err
}

// RemoveFromObjectIndexer removes a StoredObject from the object indexer of the SyncTraverser.
// It locks the syncMutex before deleting the object to ensure thread safety.
//
// Parameters:
// - so: The StoredObject to be removed from the object indexer.
func (st *SyncTraverser) RemoveFromObjectIndexer(so StoredObject) {

	if enableDebugLogs {
		glcm.Info(fmt.Sprintf("%s: Removing %s from object indexer", st.dir, so.relativePath))
	}
	syncMutex.Lock()
	delete(st.enumerator.objectIndexer.indexMap, so.relativePath)
	syncMutex.Unlock()
}

// GetFromObjectIndexer retrieves a StoredObject from the object indexer using the provided key.
// It returns the StoredObject and a boolean indicating whether the object was found.
//
// Parameters:
//   - key: A string representing the key to look up in the object indexer.
//
// Returns:
//   - StoredObject: The object associated with the provided key.
//   - bool: A boolean indicating whether the object was found (true) or not (false).
func (st *SyncTraverser) GetFromObjectIndexer(key string) (StoredObject, bool) {
	syncMutex.Lock()
	so, present := st.enumerator.objectIndexer.indexMap[key]
	syncMutex.Unlock()

	return so, present
}

// processor processes a StoredObject by determining its path and type, and then adds it to the appropriate list
// (sub_dirs or children) or marks it as the root object. It also adds the object to the object indexer.
//
// All primary traversals will send the files and directories to the processor.
// NOTE: Blob and file traversals may also send the root directory to the processor.
//
// Parameters:
// - so: The StoredObject to be processed.
//
// Returns:
// - error: An error if adding the object to the indexer fails, otherwise nil.
func (st *SyncTraverser) processor(so StoredObject) error {

	if (so.relativePath == "") && (st.dir == "") {
		// It is the first root. Ignore it.
		if enableDebugLogs {
			glcm.Info(fmt.Sprintf("Processor: Ignoring the absolute root"))
		}
		return nil
	}

	so.relativePath = strings.Trim(so.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING)

	var child_path string
	var strs []string
	if st.dir != "" {
		strs = []string{st.dir, so.relativePath}
	} else {
		strs = []string{so.relativePath}
	}
	child_path = strings.Join(strs, common.AZCOPY_PATH_SEPARATOR_STRING)
	so.relativePath = child_path

	if so.relativePath != st.dir {
		// It is a child object. Ignore the root object.
		if so.entityType == common.EEntityType.Folder() {
			st.sub_dirs = append(st.sub_dirs, so)
		}

		st.children = append(st.children, so)
	} else {
		// It is the root object
		if enableDebugLogs {
			glcm.Info(fmt.Sprintf("%s: Processor: Root %s", st.dir, so.relativePath))
		}
		return nil
	}

	return st.AddToObjectIndexer(so)
}

// comparator compares a StoredObject with the objects in the SyncTraverser's indexer.
//
// All secondary traversals will send the files and directories to the comparator.
// NOTE: Blob and file traversals may also send the root directory to the comparator
// which we want to ignore.
//
// Parameters:
// - so: The StoredObject to be compared.
//
// Returns:
// - error: An error if the comparison fails, otherwise nil.
func (st *SyncTraverser) customComparator(so StoredObject) error {

	so.relativePath = strings.Trim(so.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING)

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

	so.relativePath = child_path

	if so.relativePath == st.dir {
		// Skip the root object if it is present in the indexer. We have already processed it.
		if enableDebugLogs {
			glcm.Info(fmt.Sprintf("%s: Comparator: Skip root %s", st.dir, so.relativePath))
		}
		return nil
	}

	if enableDebugLogs {
		glcm.Info(fmt.Sprintf("%s: Comparator: Compare %s", st.dir, so.relativePath))
	}
	syncMutex.Lock()
	err := st.comparator(so)
	syncMutex.Unlock()

	return err
}

// finalize finalizes the sync process for the SyncTraverser by scheduling copy transfers for objects in indexer
// that need to be transferred. It ensures that each object is only transferred once and removes it from the object indexer
// after scheduling the transfer.
//
// Returns:
// - error: An error if scheduling a copy transfer fails, otherwise nil.
func (st *SyncTraverser) finalize() error {

	for _, child := range st.children {
		so, present := st.GetFromObjectIndexer(child.relativePath)
		if present {

			if enableDebugLogs {
				glcm.Info(fmt.Sprintf("%s: Finalizer: Transfer %s", st.dir, so.relativePath))
			}
			err := st.enumerator.ctp.scheduleCopyTransfer(so)
			if err != nil {
				return err
			}
			st.RemoveFromObjectIndexer(so)
		}
	}

	return nil
}

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

	glcm.Info(fmt.Sprintf("S3 Root: %s, Entity type: %s", searchPrefix, entityType.String()))

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

func newSyncTraverser(enumerator *syncEnumerator, dir string, comparator objectProcessor) *SyncTraverser {
	return &SyncTraverser{
		enumerator: enumerator,
		dir:        dir,
		sub_dirs:   make([]StoredObject, 0, 1024),
		children:   make([]StoredObject, 0, 1024),
		comparator: comparator,
	}
}

var syncQDepth int64
var syncMonitorRun int32
var syncMonitorExited int32
var syncMutex sync.Mutex
var totalGoroutines int32
var goroutineThreshold int32

func monitorGoroutines() {
	for {
		current := runtime.NumGoroutine()
		atomic.StoreInt32(&totalGoroutines, int32(current))
		time.Sleep(5 * time.Second) // Sample at a reasonable interval
	}
}

func shouldThrottle() bool {
	return atomic.LoadInt32(&totalGoroutines) > goroutineThreshold
}

func continueThrottle() bool {
	return atomic.LoadInt32(&totalGoroutines) > int32((goroutineThreshold*80)/100)
}

func getTotalVirtualMemory() (uint64, error) {
	// Open /proc/self/statm
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, err
	}

	// Parse the first field (total virtual memory pages)
	fields := strings.Fields(string(data))
	if len(fields) < 1 {
		return 0, fmt.Errorf("unexpected format in /proc/self/statm")
	}

	// Convert pages to bytes (assuming 4KB pages)
	pages, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0, err
	}

	pageSize := uint64(os.Getpagesize()) // Get system page size
	return uint64((pages * pageSize) / 1024 / 1024), nil
}

func getRSSMemory() (uint64, error) {
	// Open the /proc/<PID>/status file
	pid := os.Getpid()
	file, err := os.Open(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// Scan the file line by line to find VmRSS
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				return 0, fmt.Errorf("unexpected VmRSS line format")
			}
			// Parse the value (in kB) and convert to bytes
			rssKB, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, err
			}
			return uint64(rssKB / 1024), nil
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("VmRSS not found in /proc/%d/status", pid)
}

func syncMonitor() {
	syncMonitorRun = 1
	syncMonitorExited = 0

	WarnStdoutAndScanningLog("Starting SyncMonitor...\n")
	var run int32

	run = 1

	for run == 1 {
		t := time.Now()
		ts := string(t.Format("2006-01-02 15:04:05"))

		grs := atomic.LoadInt32(&totalGoroutines)
		qd := atomic.AddInt64(&syncQDepth, 0)
		vm, _ := getTotalVirtualMemory()
		rss, _ := getRSSMemory()
		WarnStdoutAndScanningLog(fmt.Sprintf("\n%s: SyncMonitor: QDepth = %v, GoRoutines = %v, VirtualMemory = %v, Resident = %v\n", ts, qd, grs, vm, rss))
		time.Sleep(30 * time.Second)
		run = atomic.AddInt32(&syncMonitorRun, 0)
	}

	if enableDebugLogs {
		WarnStdoutAndScanningLog("Exiting SyncMonitor...\n")
	}
	atomic.AddInt32(&syncMonitorExited, 1)
}

func syncOrchestratorHandler(cca *cookedSyncCmdArgs, ctx context.Context) error {
	// Start the profiling
	go func() {
		WarnStdoutAndScanningLog("Listening to port 6060..\n")
		http.ListenAndServe("localhost:6060", nil)
	}()

	return cca.runSyncOrchestrator(ctx)
}

func (cca *cookedSyncCmdArgs) runSyncOrchestrator(ctx context.Context) (err error) {
	go syncMonitor()
	go monitorGoroutines()

	enumerator, err := cca.InitEnumerator(ctx, nil)
	if err != nil {
		return err
	}

	goroutineThreshold = 30000

	syncOneDir := func(dir parallel.Directory, enqueueDir func(parallel.Directory), enqueueOutput func(parallel.DirectoryEntry, error)) error {

		var waits int64
		waits = 0
		if shouldThrottle() {
			for continueThrottle() {
				if (waits % 1800) == 0 {
					WarnStdoutAndScanningLog("Too many go routines, slowing down...\n")
				}
				time.Sleep(100 * time.Millisecond) // Simulate throttling
				waits++
			}
			WarnStdoutAndScanningLog("Continuing sync traversal...\n")
		}

		var sync_src, sync_dst []string
		var pt_src, st_src common.ResourceString

		switch cca.fromTo {
		// in case of S2S, enumerator would have source and destination swapped
		case common.EFromTo.LocalBlob(), common.EFromTo.LocalFile():

			sync_src = []string{cca.Source.Value, dir.(StoredObject).relativePath}
			sync_dst = []string{cca.Destination.Value, dir.(StoredObject).relativePath}

			pt_src = cca.Source
			st_src = cca.Destination

		default:
			// in case of S2S, the destination is scanned/indexed first
			// then the source is scanned and filtered based on what the destination contains

			sync_dst = []string{cca.Source.Value, dir.(StoredObject).relativePath}
			sync_src = []string{cca.Destination.Value, dir.(StoredObject).relativePath}

			st_src = cca.Source
			pt_src = cca.Destination
		}

		pt_src.Value = strings.Join(sync_src, common.AZCOPY_PATH_SEPARATOR_STRING)
		st_src.Value = strings.Join(sync_dst, common.AZCOPY_PATH_SEPARATOR_STRING)

		ptt := enumerator.primaryTraverserTemplate
		stt := enumerator.secondaryTraverserTemplate

		pt, err := InitResourceTraverser(
			pt_src,
			ptt.location,
			&ctx,
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
			WarnStdoutAndScanningLog(fmt.Sprintf("Creating primary traverser failed : %s\n", err))
			return err
		}

		st, err := InitResourceTraverser(
			st_src,
			stt.location,
			&ctx,
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
		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("Creating secondary traverser failed : %s", err))
			return err
		}

		if enableDebugLogs {
			glcm.Info(fmt.Sprintf("Crawl %s", dir.(StoredObject).relativePath))
		}
		stra := newSyncTraverser(enumerator, dir.(StoredObject).relativePath, enumerator.objectComparator)

		err = pt.Traverse(noPreProccessor, stra.processor, enumerator.filters)
		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("Primary traversal failed : %s", err))
			return err
		}

		err = st.Traverse(noPreProccessor, stra.customComparator, enumerator.filters)
		if err != nil {
			if !strings.Contains(err.Error(), "RESPONSE 404") {
				WarnStdoutAndScanningLog(fmt.Sprintf("Sync traversal failed type = %s", err))
				return err
			}
		}

		err = stra.finalize()
		if err != nil {
			WarnStdoutAndScanningLog("Sync finalize failed!!\n")
			return err
		}

		atomic.AddInt64(&syncQDepth, int64(len(stra.sub_dirs)))
		for _, sub_dir := range stra.sub_dirs {
			enqueueDir(sub_dir)
		}

		atomic.AddInt64(&syncQDepth, -1)
		return nil
	}

	if enableDebugLogs {
		glcm.Info(fmt.Sprintf("Syncing %s to %s", cca.Source.Value, cca.Destination.Value))
	}

	root, err := GetRootStoredObject(cca.Source.Value, cca.fromTo)

	if err != nil {
		WarnStdoutAndScanningLog(fmt.Sprintf("Root object creation failed: %s", err))
		return err
	}

	if root.entityType == common.EEntityType.File() {
		WarnStdoutAndScanningLog("Root object is a file, exiting sync orchestrator...")
		return err
	}

	parallelism := 4
	atomic.AddInt64(&syncQDepth, 1)
	var _ = parallel.Crawl(ctx, root, syncOneDir, parallelism)

	cca.waitUntilJobCompletion(false)

	// XXX consider using wg
	for {
		qd := atomic.AddInt64(&syncQDepth, 0)
		if qd == 0 {
			if enableDebugLogs {
				WarnStdoutAndScanningLog("Sync traversers exited..\n")
			}
			break
		}
		time.Sleep(1 * time.Second)
	}

	atomic.AddInt32(&syncMonitorRun, -1)

	for {
		exited := atomic.AddInt32(&syncMonitorExited, 0)
		if exited == 1 {
			if enableDebugLogs {
				WarnStdoutAndScanningLog("Sync monitor exited, quitting..\n")
			}
			break
		}
		time.Sleep(1 * time.Second)
	}

	// This should be a no-op
	err = enumerator.finalize()
	if err != nil {
		WarnStdoutAndScanningLog("Sync finalize failed!!\n")
		return err
	}

	return nil
}
