//go:build smslidingwindow
// +build smslidingwindow

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
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"net/http"
	_ "net/http/pprof"
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

type CustomSyncHandler func(cca *cookedSyncCmdArgs, ctx context.Context) error

var syncHandler CustomSyncHandler = moverSyncHandler

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

type SyncTraverser struct {
	enumerator *syncEnumerator
	comparator objectProcessor
	dir        string
	// sub_dirs []string
	// children []string
	sub_dirs []StoredObject
	children []StoredObject
}

func (st *SyncTraverser) processor(so StoredObject) error {
	var child_path string
	var strs []string
	//print st.dir and so.relativePath

	if st.dir != "" {
		strs = []string{st.dir, so.relativePath}
	} else {
		strs = []string{so.relativePath}
	}
	child_path = strings.Join(strs, common.AZCOPY_PATH_SEPARATOR_STRING)
	so.relativePath = child_path

	if so.entityType == common.EEntityType.Folder() {
		st.sub_dirs = append(st.sub_dirs, so)
	}

	st.children = append(st.children, so)

	syncMutex.Lock()
	err := st.enumerator.objectIndexer.store(so)
	syncMutex.Unlock()

	return err
}

func (st *SyncTraverser) my_comparator(so StoredObject) error {
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

	//fmt.Printf("Starting SyncMonitor...\n")
	var run int32

	run = 1

	for run == 1 {
		t := time.Now()
		ts := string(t.Format("2006-01-02 15:04:05"))

		grs := atomic.LoadInt32(&totalGoroutines)
		qd := atomic.AddInt64(&syncQDepth, 0)
		vm, _ := getTotalVirtualMemory()
		rss, _ := getRSSMemory()
		WarnStdoutAndScanningLog(fmt.Sprintf("%s: SyncMonitor: QDepth = %v, GoRoutines = %v, VirtualMemory = %v, Resident = %v\n", ts, qd, grs, vm, rss))

		time.Sleep(30 * time.Second)
		run = atomic.AddInt32(&syncMonitorRun, 0)
	}
	WarnStdoutAndScanningLog("Exiting SyncMonitor...")

	atomic.AddInt32(&syncMonitorExited, 1)
}

func moverSyncHandler(cca *cookedSyncCmdArgs, ctx context.Context) error {
	// Start the profiling
	go func() {
		WarnStdoutAndScanningLog("Listening to port 6060..")
		http.ListenAndServe("localhost:6060", nil)
	}()

	return cca.runSyncOrchestrator(ctx)
}

func (cca *cookedSyncCmdArgs) runSyncOrchestrator(ctx context.Context) (err error) {
	go syncMonitor()
	go monitorGoroutines()

	enumerator, err := cca.initEnumerator(ctx)
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
					WarnStdoutAndScanningLog("Too many go routines, slowing down...")

				}
				time.Sleep(100 * time.Millisecond) // Simulate throttling
				waits++
			}
			WarnStdoutAndScanningLog("Continuing sync traversal...")
		}

		//sync_src := []string{cca.source.Value, dir.(StoredObject).relativePath}
		//sync_dst := []string{cca.destination.Value, dir.(StoredObject).relativePath}

		pt_src := cca.source
		st_src := cca.destination

		//pt_src.Value = strings.Join(sync_src, common.AZCOPY_PATH_SEPARATOR_STRING)
		//st_src.Value = strings.Join(sync_dst, common.AZCOPY_PATH_SEPARATOR_STRING)

		ptt := enumerator.primaryTraverserTemplate
		stt := enumerator.secondaryTraverserTemplate

		syncMutex.Lock()
		err := enumerator.objectIndexer.store(dir.(StoredObject))
		syncMutex.Unlock()
		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("Storing root object failed: %s", err))
			return err
		}

		pt, err := InitResourceTraverser(pt_src,
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
			ptt.includeVersionsList)
		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("Creating source traverser failed : %s", err))
			return err
		}

		st, err := InitResourceTraverser(st_src,
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
			stt.includeVersionsList)

		stra := newSyncTraverser(enumerator, dir.(StoredObject).relativePath, enumerator.objectComparator)
		fpo, _ := NewFolderPropertyOption(cca.fromTo, cca.recursive, !cca.includeRoot, enumerator.filters, cca.preserveSMBInfo, cca.preservePermissions.IsTruthy(), false, strings.EqualFold(cca.destination.Value, common.Dev_Null), cca.includeDirectoryStubs)

		copyJobTemplate := &common.CopyJobPartOrderRequest{
			JobID:               cca.jobID,
			CommandString:       cca.commandString,
			FromTo:              cca.fromTo,
			Fpo:                 fpo,
			SymlinkHandlingType: cca.symlinkHandling,
			SourceRoot:          cca.source.CloneWithConsolidatedSeparators(),
			DestinationRoot:     cca.destination.CloneWithConsolidatedSeparators(),
			CredentialInfo:      cca.credentialInfo,

			// flags
			BlobAttributes: common.BlobTransferAttributes{
				PreserveLastModifiedTime:         cca.preserveSMBInfo, // true by default for sync so that future syncs have this information available
				PutMd5:                           cca.putMd5,
				MD5ValidationOption:              cca.md5ValidationOption,
				BlockSizeInBytes:                 cca.blockSize,
				PutBlobSizeInBytes:               cca.putBlobSize,
				DeleteDestinationFileIfNecessary: cca.deleteDestinationFileIfNecessary,
			},
			ForceWrite:                     common.EOverwriteOption.True(), // once we decide to transfer for a sync operation, we overwrite the destination regardless
			ForceIfReadOnly:                cca.forceIfReadOnly,
			LogLevel:                       azcopyLogVerbosity,
			PreserveSMBPermissions:         cca.preservePermissions,
			PreserveSMBInfo:                cca.preserveSMBInfo,
			PreservePOSIXProperties:        cca.preservePOSIXProperties,
			S2SSourceChangeValidation:      true,
			DestLengthValidation:           true,
			S2SGetPropertiesInBackend:      true,
			S2SInvalidMetadataHandleOption: common.EInvalidMetadataHandleOption.RenameIfInvalid(),
			CpkOptions:                     cca.cpkOptions,
			S2SPreserveBlobTags:            cca.s2sPreserveBlobTags,

			S2SSourceCredentialType: cca.s2sSourceCredentialType,
			FileAttributes: common.FileTransferAttributes{
				TrailingDot: cca.trailingDot,
			},
		}
		srcCredInfo, _, err := GetCredentialInfoForLocation(ctx, cca.fromTo.From(), cca.source, true, cca.cpkOptions)
		dstCredInfo, _, err := GetCredentialInfoForLocation(ctx, cca.fromTo.To(), cca.destination, false, cca.cpkOptions)

		var srcReauthTok *common.ScopedAuthenticator
		if at, ok := srcCredInfo.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok {
			// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
			srcReauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
		}

		options := createClientOptions(common.AzcopyCurrentJobLogger, nil, srcReauthTok)

		// Create Source Client.
		var azureFileSpecificOptions any
		if cca.fromTo.From() == common.ELocation.File() {
			azureFileSpecificOptions = &common.FileClientOptions{
				AllowTrailingDot: cca.trailingDot == common.ETrailingDotOption.Enable(),
			}
		}

		copyJobTemplate.SrcServiceClient, err = common.GetServiceClientForLocation(
			cca.fromTo.From(),
			cca.source,
			srcCredInfo.CredentialType,
			srcCredInfo.OAuthTokenInfo.TokenCredential,
			&options,
			azureFileSpecificOptions,
		)

		// Create Destination client
		if cca.fromTo.To() == common.ELocation.File() {
			azureFileSpecificOptions = &common.FileClientOptions{
				AllowTrailingDot:       cca.trailingDot == common.ETrailingDotOption.Enable(),
				AllowSourceTrailingDot: (cca.trailingDot == common.ETrailingDotOption.Enable() && cca.fromTo.To() == common.ELocation.File()),
			}
		}

		var dstReauthTok *common.ScopedAuthenticator
		if at, ok := srcCredInfo.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok {
			// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
			dstReauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
		}

		var srcTokenCred *common.ScopedToken
		if cca.fromTo.IsS2S() && srcCredInfo.CredentialType.IsAzureOAuth() {
			srcTokenCred = common.NewScopedCredential(srcCredInfo.OAuthTokenInfo.TokenCredential, srcCredInfo.CredentialType)
		}

		options = createClientOptions(common.AzcopyCurrentJobLogger, srcTokenCred, dstReauthTok)
		copyJobTemplate.DstServiceClient, err = common.GetServiceClientForLocation(
			cca.fromTo.To(),
			cca.destination,
			dstCredInfo.CredentialType,
			dstCredInfo.OAuthTokenInfo.TokenCredential,
			&options,
			azureFileSpecificOptions,
		)

		indexer := newObjectIndexer()
		var comparator objectProcessor
		transferScheduler := newSyncTransferProcessor(cca, NumOfFilesPerDispatchJobPart, fpo, copyJobTemplate)
		destinationCleaner, err := newSyncDeleteProcessor(cca, fpo, copyJobTemplate.DstServiceClient)
		if err != nil {
			return fmt.Errorf("unable to instantiate destination cleaner due to: %s", err.Error())
		}
		destCleanerFunc := newFpoAwareProcessor(fpo, destinationCleaner.removeImmediately)

		comparator = newSyncDestinationComparator(indexer, transferScheduler.scheduleCopyTransfer, destCleanerFunc, cca.compareHash, cca.preserveSMBInfo, cca.mirrorMode).processIfNecessary
		err = pt.Traverse(noPreProccessor, stra.processor, enumerator.filters)
		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("Sync traversal failed type = %s, %s", err, comparator))
			return err
		}

		err = st.Traverse(noPreProccessor, stra.my_comparator, enumerator.filters)
		if err != nil {
			if !strings.Contains(err.Error(), "RESPONSE 404") {
				WarnStdoutAndScanningLog(fmt.Sprintf("Sync traversal failed type = %s", err))
				return err
			}
		}

		err = stra.Finalize()
		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("Sync finalize failed!! %s", err))
			return err
		}

		// XXX should we worry about case??
		syncMutex.Lock()
		delete(stra.enumerator.objectIndexer.indexMap, dir.(StoredObject).relativePath)
		syncMutex.Unlock()

		atomic.AddInt64(&syncQDepth, int64(len(stra.sub_dirs)))
		for _, sub_dir := range stra.sub_dirs {
			enqueueDir(sub_dir)
		}

		atomic.AddInt64(&syncQDepth, -1)

		return nil
	}

	fi, err := os.Stat(cca.source.Value)
	if err != nil {
		return err
	}

	root := newStoredObject(nil, fi.Name(), "", common.EEntityType.Folder(),
		fi.ModTime(), fi.Size(), noContentProps, noBlobProps, noMetadata, "")

	parallelism := 4
	//atomic.AddInt64(&syncQDepth, 1)
	var _ = parallel.Crawl(ctx, root, syncOneDir, parallelism)

	cca.waitUntilJobCompletion(false)

	// XXX consider using wg
	for {
		qd := atomic.AddInt64(&syncQDepth, 0)
		if qd == 0 {
			WarnStdoutAndScanningLog("Sync traversers exited..")
			break
		}
		WarnStdoutAndScanningLog("Waiting for sync traversers to exit..")
		// fmt.Printf("Waiting for sync traversers to exit..\n")
		time.Sleep(1 * time.Second)
	}

	atomic.AddInt32(&syncMonitorRun, -1)

	for {
		exited := atomic.AddInt32(&syncMonitorExited, 0)
		if exited == 1 {
			WarnStdoutAndScanningLog("Sync monitor exited, quitting..")
			break
		}
		WarnStdoutAndScanningLog("Waiting for sync monitor to exit...")
		time.Sleep(1 * time.Second)
	}

	WarnStdoutAndScanningLog("Sync operation completed successfully.")
	err = enumerator.finalize()
	if err != nil {
		WarnStdoutAndScanningLog(fmt.Sprintf("Sync finalize failed!! %s", err))
		return err
	}
	return nil
}
