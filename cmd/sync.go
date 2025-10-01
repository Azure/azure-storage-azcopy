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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"

	"github.com/spf13/cobra"
)

var LocalToFileShareWarnMsg = "AzCopy sync is supported but not fully recommended for Azure Files. AzCopy sync doesn't support differential copies at scale, and some file fidelity might be lost."

type rawSyncCmdArgs struct {
	src       string
	dst       string
	recursive bool
	fromTo    string

	// options from flags
	blockSizeMB           float64
	putBlobSizeMB         float64
	include               string
	exclude               string
	excludePath           string
	includeFileAttributes string
	excludeFileAttributes string
	legacyInclude         string // for warning messages only
	legacyExclude         string // for warning messages only
	includeRegex          string
	excludeRegex          string
	compareHash           string
	localHashStorageMode  string

	includeDirectoryStubs   bool // Includes hdi_isfolder objects in the sync even w/o preservePermissions.
	preservePermissions     bool
	preserveSMBPermissions  bool // deprecated and synonymous with preservePermissions
	preserveOwner           bool
	preserveSMBInfo         bool
	preservePOSIXProperties bool
	followSymlinks          bool
	preserveSymlinks        bool
	backupMode              bool
	putMd5                  bool
	md5ValidationOption     string
	includeRoot             bool
	// this flag indicates the user agreement with respect to deleting the extra files at the destination
	// which do not exists at source. With this flag turned on/off, users will not be asked for permission.
	// otherwise the user is prompted to make a decision
	deleteDestination string

	// this flag is to disable comparator and overwrite files at destination irrespective
	mirrorMode bool

	s2sPreserveAccessTier bool
	// Opt-in flag to preserve the blob index tags during service to service transfer.
	s2sPreserveBlobTags bool

	forceIfReadOnly bool

	// Optional flag to encrypt user data with user provided key.
	// Key is provide in the REST request itself
	// Provided key (EncryptionKey and EncryptionKeySHA256) and its hash will be fetched from environment variables
	// Set EncryptionAlgorithm = "AES256" by default.
	cpkInfo bool
	// Key is present in AzureKeyVault and Azure KeyVault is linked with storage account.
	// Provided key name will be fetched from Azure Key Vault and will be used to encrypt the data
	cpkScopeInfo string
	// dry run mode bool
	dryrun      bool
	trailingDot string

	// when specified, AzCopy deletes the destination blob that has uncommitted blocks, not just the uncommitted blocks
	deleteDestinationFileIfNecessary bool
	// Opt-in flag to persist additional properties to Azure Files
	preserveInfo bool
	hardlinks    string
}

// it is assume that the given url has the SAS stripped, and safe to print
func validateURLIsNotServiceLevel(url string, location common.Location) error {
	srcLevel, err := DetermineLocationLevel(url, location, true)
	if err != nil {
		return err
	}

	if srcLevel == ELocationLevel.Service() {
		return fmt.Errorf("service level URLs (%s) are not supported in sync: ", url)
	}

	return nil
}

func (raw rawSyncCmdArgs) toOptions() (cooked cookedSyncCmdArgs, err error) {
	cooked = cookedSyncCmdArgs{
		dryrunMode:                       raw.dryrun,
		blockSizeMB:                      raw.blockSizeMB,
		putBlobSizeMB:                    raw.putBlobSizeMB,
		recursive:                        raw.recursive,
		forceIfReadOnly:                  raw.forceIfReadOnly,
		backupMode:                       raw.backupMode,
		putMd5:                           raw.putMd5,
		s2sPreserveBlobTags:              raw.s2sPreserveBlobTags,
		cpkByName:                        raw.cpkScopeInfo,
		cpkByValue:                       raw.cpkInfo,
		mirrorMode:                       raw.mirrorMode,
		deleteDestinationFileIfNecessary: raw.deleteDestinationFileIfNecessary,
		includeDirectoryStubs:            raw.includeDirectoryStubs,
		includeRoot:                      raw.includeRoot,
	}
	err = cooked.trailingDot.Parse(raw.trailingDot)
	if err != nil {
		return cooked, err
	}
	cooked.fromTo, err = ValidateFromTo(raw.src, raw.dst, raw.fromTo)
	if err != nil {
		return cooked, err
	}

	switch cooked.fromTo {
	case common.EFromTo.Unknown():
		return cooked, fmt.Errorf("unable to infer the source '%s' / destination '%s'. ", raw.src, raw.dst)
	case common.EFromTo.LocalBlob(), common.EFromTo.LocalFile(), common.EFromTo.LocalBlobFS(), common.EFromTo.LocalFileNFS():
		cooked.destination, err = SplitResourceString(raw.dst, cooked.fromTo.To())
		common.PanicIfErr(err)
	case common.EFromTo.BlobLocal(), common.EFromTo.FileLocal(), common.EFromTo.BlobFSLocal(), common.EFromTo.FileNFSLocal():
		cooked.source, err = SplitResourceString(raw.src, cooked.fromTo.From())
		common.PanicIfErr(err)
	case common.EFromTo.BlobBlob(), common.EFromTo.FileFile(), common.EFromTo.FileNFSFileNFS(), common.EFromTo.BlobFile(), common.EFromTo.FileBlob(), common.EFromTo.BlobFSBlobFS(), common.EFromTo.BlobFSBlob(), common.EFromTo.BlobFSFile(), common.EFromTo.BlobBlobFS(), common.EFromTo.FileBlobFS():
		cooked.destination, err = SplitResourceString(raw.dst, cooked.fromTo.To())
		common.PanicIfErr(err)
		cooked.source, err = SplitResourceString(raw.src, cooked.fromTo.From())
		common.PanicIfErr(err)
	default:
		return cooked, fmt.Errorf("source '%s' / destination '%s' combination '%s' not supported for sync command ", raw.src, raw.dst, cooked.fromTo)
	}

	// Do this check separately so we don't end up with a bunch of code duplication when new src/dstn are added
	if cooked.fromTo.From() == common.ELocation.Local() {
		cooked.source = common.ResourceString{Value: common.ToExtendedPath(cleanLocalPath(raw.src))}
	} else if cooked.fromTo.To() == common.ELocation.Local() {
		cooked.destination = common.ResourceString{Value: common.ToExtendedPath(cleanLocalPath(raw.dst))}
	}

	if err = cooked.symlinkHandling.Determine(raw.followSymlinks, raw.preserveSymlinks); err != nil {
		return cooked, err
	}

	// determine whether we should prompt the user to delete extra files
	err = cooked.deleteDestination.Parse(raw.deleteDestination)
	if err != nil {
		return cooked, err
	}

	// warn on legacy filters
	if raw.legacyInclude != "" || raw.legacyExclude != "" {
		return cooked, fmt.Errorf("the include and exclude parameters have been replaced by include-pattern and exclude-pattern. They work on filenames only (not paths)")
	}

	// parse the filter patterns
	cooked.includePatterns = parsePatterns(raw.include)
	cooked.excludePatterns = parsePatterns(raw.exclude)
	cooked.excludePaths = parsePatterns(raw.excludePath)

	// parse the attribute filter patterns
	cooked.includeFileAttributes = parsePatterns(raw.includeFileAttributes)
	cooked.excludeFileAttributes = parsePatterns(raw.excludeFileAttributes)

	// NFS/SMB arg processing
	if common.IsNFSCopy() {
		cooked.preserveInfo = raw.preserveInfo && areBothLocationsNFSAware(cooked.fromTo)
		//TBD: We will be preserving ACLs and ownership info in case of NFS. (UserID,GroupID and FileMode)
		// Using the same EPreservePermissionsOption that we have today for NFS as well
		// Please provide the feedback if we should introduce new EPreservePermissionsOption instead.
		cooked.preservePermissions = common.NewPreservePermissionsOption(raw.preservePermissions,
			true,
			cooked.fromTo)
		if err = cooked.hardlinks.Parse(raw.hardlinks); err != nil {
			return cooked, err
		}
	} else {
		cooked.preserveInfo = raw.preserveInfo && areBothLocationsSMBAware(cooked.fromTo)
		cooked.preservePOSIXProperties = raw.preservePOSIXProperties
		cooked.preservePermissions = common.NewPreservePermissionsOption(raw.preservePermissions,
			raw.preserveOwner,
			cooked.fromTo)
	}

	if err = cooked.compareHash.Parse(raw.compareHash); err != nil {
		return cooked, err
	}

	switch cooked.compareHash {
	case common.ESyncHashType.MD5():
		// Save any new MD5s on files we download.
		cooked.putMd5 = true
	default: // no need to put a hash of any kind.
	}

	if err = common.LocalHashStorageMode.Parse(raw.localHashStorageMode); err != nil {
		return cooked, err
	}

	err = cooked.md5ValidationOption.Parse(raw.md5ValidationOption)
	if err != nil {
		return cooked, err
	}

	if cooked.fromTo.IsS2S() {
		cooked.preserveAccessTier = raw.s2sPreserveAccessTier
	}

	cooked.includeRegex = parsePatterns(raw.includeRegex)
	cooked.excludeRegex = parsePatterns(raw.excludeRegex)

	return cooked, nil
}

// validates and transform raw input into cooked input
func (raw *rawSyncCmdArgs) cook() (cooked cookedSyncCmdArgs, err error) {
	if cooked, err = raw.toOptions(); err != nil {
		return cooked, err
	}
	if err = cooked.validate(); err != nil {
		return cooked, err
	}
	if err = cooked.processArgs(); err != nil {
		return cooked, err
	}
	return cooked, nil
}

func (cooked *cookedSyncCmdArgs) validate() (err error) {
	// we do not support service level sync yet
	if cooked.fromTo.From().IsRemote() {
		err = validateURLIsNotServiceLevel(cooked.source.Value, cooked.fromTo.From())
		if err != nil {
			return err
		}
	}

	if cooked.fromTo.To().IsRemote() {
		err = validateURLIsNotServiceLevel(cooked.destination.Value, cooked.fromTo.To())
		if err != nil {
			return err
		}
	}

	if err = validateForceIfReadOnly(cooked.forceIfReadOnly, cooked.fromTo); err != nil {
		return err
	}

	if err = validateBackupMode(cooked.backupMode, cooked.fromTo); err != nil {
		return err
	}

	// NFS/SMB validation
	if common.IsNFSCopy() {
		if err := performNFSSpecificValidation(
			cooked.fromTo, cooked.preservePermissions, cooked.preserveInfo,
			cooked.symlinkHandling, cooked.hardlinks); err != nil {
			return err
		}
	} else {
		if err := performSMBSpecificValidation(
			cooked.fromTo, cooked.preservePermissions, cooked.preserveInfo,
			cooked.preservePOSIXProperties, cooked.hardlinks); err != nil {
			return err
		}
	}

	if err = validatePutMd5(cooked.putMd5, cooked.fromTo); err != nil {
		return err
	}

	if err = validateMd5Option(cooked.md5ValidationOption, cooked.fromTo); err != nil {
		return err
	}

	// Check if user has provided `s2s-preserve-blob-tags` flag.
	// If yes, we have to ensure that both source and destination must be blob storage.
	if cooked.s2sPreserveBlobTags && (cooked.fromTo.From() != common.ELocation.Blob() || cooked.fromTo.To() != common.ELocation.Blob()) {
		return fmt.Errorf("either source or destination is not a blob storage. " +
			"blob index tags is a property of blobs only therefore both source and destination must be blob storage")
	}

	if cooked.cpkByName != "" && cooked.cpkByValue {
		return errors.New("cannot use both cpk-by-name and cpk-by-value at the same time")
	}

	if OutputLevel == common.EOutputVerbosity.Quiet() || OutputLevel == common.EOutputVerbosity.Essential() {
		if cooked.deleteDestination == common.EDeleteDestination.Prompt() {
			err = fmt.Errorf("cannot set output level '%s' with delete-destination option '%s'", OutputLevel.String(), cooked.deleteDestination.String())
		} else if cooked.dryrunMode {
			err = fmt.Errorf("cannot set output level '%s' with dry-run mode", OutputLevel.String())
		}
	}
	if err != nil {
		return err
	}

	return nil
}

func (cooked *cookedSyncCmdArgs) processArgs() (err error) {
	// set up the front end scanning logger
	azcopyScanningLogger = common.NewJobLogger(Client.CurrentJobID, LogLevel, common.LogPathFolder, "-scanning")
	azcopyScanningLogger.OpenLog()
	glcm.RegisterCloseFunc(func() {
		azcopyScanningLogger.CloseLog()
	})

	// if no logging, set this empty so that we don't display the log location
	if LogLevel == common.LogNone {
		common.LogPathFolder = ""
	}

	// display a warning message to console and job log file if there is a sync operation being performed from local to file share.
	// Reference : https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-files#synchronize-files
	if cooked.fromTo == common.EFromTo.LocalFile() {

		glcm.Warn(LocalToFileShareWarnMsg)
		common.LogToJobLogWithPrefix(LocalToFileShareWarnMsg, common.LogWarning)

		if cooked.dryrunMode {
			glcm.Dryrun(func(of common.OutputFormat) string {
				if of == common.EOutputFormat.Json() {
					var out struct {
						Warn string `json:"warn"`
					}

					out.Warn = LocalToFileShareWarnMsg
					buf, _ := json.Marshal(out)
					return string(buf)
				}

				return fmt.Sprintf("DRYRUN: warn %s", LocalToFileShareWarnMsg)
			})
		}
	}

	// use the globally generated JobID
	cooked.jobID = Client.CurrentJobID

	cooked.blockSize, err = blockSizeInBytes(cooked.blockSizeMB)
	if err != nil {
		return err
	}
	cooked.putBlobSize, err = blockSizeInBytes(cooked.putBlobSizeMB)
	if err != nil {
		return err
	}

	cooked.cpkOptions = common.CpkOptions{
		CpkScopeInfo: cooked.cpkByName,  // Setting CPK-N
		CpkInfo:      cooked.cpkByValue, // Setting CPK-V
		// Get the key (EncryptionKey and EncryptionKeySHA256) value from environment variables when required.
	}
	// We only support transfer from source encrypted by user key when user wishes to download.
	// Due to service limitation, S2S transfer is not supported for source encrypted by user key.
	if cooked.fromTo.IsDownload() && (cooked.cpkOptions.CpkScopeInfo != "" || cooked.cpkOptions.CpkInfo) {
		glcm.Info("Client Provided Key for encryption/decryption is provided for download scenario. " +
			"Assuming source is encrypted.")
		cooked.cpkOptions.IsSourceEncrypted = true
	}

	return nil
}

type cookedSyncCmdArgs struct {
	// NOTE: for the 64 bit atomic functions to work on a 32 bit system, we have to guarantee the right 64-bit alignment
	// so the 64 bit integers are placed first in the struct to avoid future breaks
	// refer to: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	// defines the number of files listed at the source and compared.
	atomicSourceFilesScanned uint64
	// defines the number of files listed at the destination and compared.
	atomicDestinationFilesScanned uint64
	// defines the scanning status of the sync operation.
	// 0 means scanning is in progress and 1 means scanning is complete.
	atomicScanningStatus uint32
	// defines whether first part has been ordered or not.
	// 0 means first part is not ordered and 1 means first part is ordered.
	atomicFirstPartOrdered uint32

	// deletion count keeps track of how many extra files from the destination were removed
	atomicDeletionCount uint32

	source                  common.ResourceString
	destination             common.ResourceString
	fromTo                  common.FromTo
	credentialInfo          common.CredentialInfo
	s2sSourceCredentialType common.CredentialType

	// filters
	recursive             bool
	symlinkHandling       common.SymlinkHandlingType
	includePatterns       []string
	excludePatterns       []string
	excludePaths          []string
	includeFileAttributes []string
	excludeFileAttributes []string
	includeRegex          []string
	excludeRegex          []string

	// options
	compareHash             common.SyncHashType
	preservePermissions     common.PreservePermissionsOption
	preserveInfo            bool
	preservePOSIXProperties bool
	putMd5                  bool
	md5ValidationOption     common.HashValidationOption
	blockSize               int64
	putBlobSize             int64
	forceIfReadOnly         bool
	backupMode              bool
	includeDirectoryStubs   bool
	includeRoot             bool

	// commandString hold the user given command which is logged to the Job log file
	commandString string

	// generated
	jobID common.JobID

	// variables used to calculate progress
	// intervalStartTime holds the last time value when the progress summary was fetched
	// the value of this variable is used to calculate the throughput
	// it gets updated every time the progress summary is fetched
	intervalStartTime        time.Time
	intervalBytesTransferred uint64

	// used to calculate job summary
	jobStartTime time.Time

	// this flag is set by the enumerator
	// it is useful to indicate whether we are simply waiting for the purpose of cancelling
	// this is set to true once the final part has been dispatched
	isEnumerationComplete bool

	// this flag indicates the user agreement with respect to deleting the extra files at the destination
	// which do not exists at source. With this flag turned on/off, users will not be asked for permission.
	// otherwise the user is prompted to make a decision
	deleteDestination common.DeleteDestination

	preserveAccessTier bool
	// To specify whether user wants to preserve the blob index tags during service to service transfer.
	s2sPreserveBlobTags bool

	cpkOptions common.CpkOptions

	mirrorMode bool

	dryrunMode  bool
	trailingDot common.TrailingDotOption

	deleteDestinationFileIfNecessary bool
	hardlinks                        common.HardlinkHandlingType
	atomicSkippedSymlinkCount        uint32
	atomicSkippedSpecialFileCount    uint32

	blockSizeMB   float64
	putBlobSizeMB float64
	cpkByName     string
	cpkByValue    bool
}

func (cca *cookedSyncCmdArgs) incrementDeletionCount() {
	atomic.AddUint32(&cca.atomicDeletionCount, 1)
}

func (cca *cookedSyncCmdArgs) getDeletionCount() uint32 {
	return atomic.LoadUint32(&cca.atomicDeletionCount)
}

// setFirstPartOrdered sets the value of atomicFirstPartOrdered to 1
func (cca *cookedSyncCmdArgs) setFirstPartOrdered() {
	atomic.StoreUint32(&cca.atomicFirstPartOrdered, 1)
}

// firstPartOrdered returns the value of atomicFirstPartOrdered.
func (cca *cookedSyncCmdArgs) firstPartOrdered() bool {
	return atomic.LoadUint32(&cca.atomicFirstPartOrdered) > 0
}

// setScanningComplete sets the value of atomicScanningStatus to 1.
func (cca *cookedSyncCmdArgs) setScanningComplete() {
	atomic.StoreUint32(&cca.atomicScanningStatus, 1)
}

// scanningComplete returns the value of atomicScanningStatus.
func (cca *cookedSyncCmdArgs) scanningComplete() bool {
	return atomic.LoadUint32(&cca.atomicScanningStatus) > 0
}

// wraps call to lifecycle manager to wait for the job to complete
// if blocking is specified to true, then this method will never return
// if blocking is specified to false, then another goroutine spawns and wait out the job
func (cca *cookedSyncCmdArgs) waitUntilJobCompletion(blocking bool) {
	// print initial message to indicate that the job is starting
	// Output the log location if log-level is set to other then NONE
	var logPathFolder string
	if common.LogPathFolder != "" {
		logPathFolder = fmt.Sprintf("%s%s%s.log", common.LogPathFolder, common.OS_PATH_SEPARATOR, cca.jobID)
	}
	glcm.Init(common.GetStandardInitOutputBuilder(cca.jobID.String(), logPathFolder, false, ""))

	// initialize the times necessary to track progress
	cca.jobStartTime = time.Now()
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = 0

	// hand over control to the lifecycle manager if blocking
	if blocking {
		glcm.InitiateProgressReporting(cca)
		glcm.SurrenderControl()
	} else {
		// non-blocking, return after spawning a go routine to watch the job
		glcm.InitiateProgressReporting(cca)
	}
}

func (cca *cookedSyncCmdArgs) Cancel(lcm common.LifecycleMgr) {
	// prompt for confirmation, except when enumeration is complete
	if !cca.isEnumerationComplete {
		answer := lcm.Prompt("The enumeration (source/destination comparison) is not complete, "+
			"cancelling the job at this point means it cannot be resumed.",
			common.PromptDetails{
				PromptType: common.EPromptType.Cancel(),
				ResponseOptions: []common.ResponseOption{
					common.EResponseOption.Yes(),
					common.EResponseOption.No(),
				},
			})

		if answer != common.EResponseOption.Yes() {
			// user aborted cancel
			return
		}
	}

	err := cookedCancelCmdArgs{jobID: cca.jobID}.process()
	if err != nil {
		lcm.Error("error occurred while cancelling the job " + cca.jobID.String() + ". Failed with error " + err.Error())
	}
}

type scanningProgressJsonTemplate struct {
	FilesScannedAtSource      uint64
	FilesScannedAtDestination uint64
}

func (cca *cookedSyncCmdArgs) reportScanningProgress(lcm common.LifecycleMgr, throughput float64) {

	lcm.Progress(func(format common.OutputFormat) string {
		srcScanned := atomic.LoadUint64(&cca.atomicSourceFilesScanned)
		dstScanned := atomic.LoadUint64(&cca.atomicDestinationFilesScanned)

		if format == common.EOutputFormat.Json() {
			jsonOutputTemplate := scanningProgressJsonTemplate{
				FilesScannedAtSource:      srcScanned,
				FilesScannedAtDestination: dstScanned,
			}
			outputString, err := json.Marshal(jsonOutputTemplate)
			common.PanicIfErr(err)
			return string(outputString)
		}

		// text output
		throughputString := ""
		if cca.firstPartOrdered() {
			throughputString = fmt.Sprintf(", 2-sec Throughput (Mb/s): %v", jobsAdmin.ToFixed(throughput, 4))
		}
		return fmt.Sprintf("%v Files Scanned at Source, %v Files Scanned at Destination%s",
			srcScanned, dstScanned, throughputString)
	})
}

func (cca *cookedSyncCmdArgs) getJsonOfSyncJobSummary(summary common.ListJobSummaryResponse) string {
	wrapped := common.ListSyncJobSummaryResponse{ListJobSummaryResponse: summary}
	wrapped.DeleteTotalTransfers = cca.getDeletionCount()
	wrapped.DeleteTransfersCompleted = cca.getDeletionCount()
	jsonOutput, err := json.Marshal(wrapped)
	common.PanicIfErr(err)
	return string(jsonOutput)
}

func (cca *cookedSyncCmdArgs) ReportProgressOrExit(lcm common.LifecycleMgr) (totalKnownCount uint32) {
	duration := time.Since(cca.jobStartTime) // report the total run time of the job
	var summary common.ListJobSummaryResponse
	var throughput float64
	var jobDone bool

	// fetch a job status and compute throughput if the first part was dispatched
	if cca.firstPartOrdered() {
		summary = jobsAdmin.GetJobSummary(cca.jobID)
		lcm = jobsAdmin.GetJobLCMWrapper(cca.jobID)
		jobDone = summary.JobStatus.IsJobDone()
		totalKnownCount = summary.TotalTransfers

		// compute the average throughput for the last time interval
		bytesInMb := float64(float64(summary.BytesOverWire-cca.intervalBytesTransferred) * 8 / float64(base10Mega))
		timeElapsed := time.Since(cca.intervalStartTime).Seconds()
		throughput = common.Iff(timeElapsed != 0, bytesInMb/timeElapsed, 0)

		// reset the interval timer and byte count
		cca.intervalStartTime = time.Now()
		cca.intervalBytesTransferred = summary.BytesOverWire
	}

	// first part not dispatched, and we are still scanning
	// so a special message is outputted to notice the user that we are not stalling
	if !cca.scanningComplete() {
		cca.reportScanningProgress(lcm, throughput)
		return
	}

	lcm.Progress(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			return cca.getJsonOfSyncJobSummary(summary)
		}

		// indicate whether constrained by disk or not
		perfString, diskString := getPerfDisplayText(summary.PerfStrings, summary.PerfConstraint, duration, false)

		return fmt.Sprintf("%.1f %%, %v Done, %v Failed, %v Pending, %v Total%s, 2-sec Throughput (Mb/s): %v%s",
			summary.PercentComplete,
			summary.TransfersCompleted,
			summary.TransfersFailed,
			summary.TotalTransfers-summary.TransfersCompleted-summary.TransfersFailed,
			summary.TotalTransfers, perfString, jobsAdmin.ToFixed(throughput, 4), diskString)
	})

	if jobDone {
		exitCode := common.EExitCode.Success()
		if summary.TransfersFailed > 0 || summary.JobStatus == common.EJobStatus.Cancelled() || summary.JobStatus == common.EJobStatus.Cancelling() {
			exitCode = common.EExitCode.Error()
		}

		summary.SkippedSymlinkCount = atomic.LoadUint32(&cca.atomicSkippedSymlinkCount)
		summary.SkippedSpecialFileCount = atomic.LoadUint32(&cca.atomicSkippedSpecialFileCount)

		lcm.Exit(func(format common.OutputFormat) string {
			if format == common.EOutputFormat.Json() {
				return cca.getJsonOfSyncJobSummary(summary)
			}
			screenStats, logStats := formatExtraStats(cca.fromTo, summary.AverageIOPS, summary.AverageE2EMilliseconds, summary.NetworkErrorPercentage, summary.ServerBusyPercentage)

			output := fmt.Sprintf(
				`
Job %s Summary
Files Scanned at Source: %v
Files Scanned at Destination: %v
Elapsed Time (Minutes): %v
Number of Copy Transfers for Files: %v
Number of Copy Transfers for Folder Properties: %v 
Total Number of Copy Transfers: %v
Number of Copy Transfers Completed: %v
Number of Copy Transfers Failed: %v
Number of Deletions at Destination: %v
Number of Symbolic Links Skipped: %v
Number of Special Files Skipped: %v
Number of Hardlinks Converted: %v
Total Number of Bytes Transferred: %v
Total Number of Bytes Skipped: %v
Total Number of Bytes Enumerated: %v
Final Job Status: %v%s%s
`,
				summary.JobID.String(),
				atomic.LoadUint64(&cca.atomicSourceFilesScanned),
				atomic.LoadUint64(&cca.atomicDestinationFilesScanned),
				jobsAdmin.ToFixed(duration.Minutes(), 4),
				summary.FileTransfers,
				summary.FolderPropertyTransfers,
				summary.TotalTransfers,
				summary.TransfersCompleted,
				summary.TransfersFailed,
				cca.atomicDeletionCount,
				summary.SkippedSymlinkCount,
				summary.SkippedSpecialFileCount,
				summary.HardlinksConvertedCount,
				summary.TotalBytesTransferred,
				summary.TotalBytesSkipped,
				summary.TotalBytesEnumerated,
				summary.JobStatus,
				screenStats,
				formatPerfAdvice(summary.PerformanceAdvice))

			jobMan, exists := jobsAdmin.JobsAdmin.JobMgr(summary.JobID)
			if exists {
				jobMan.Log(common.LogInfo, logStats+"\n"+output)
			}

			return output
		}, exitCode)
	}

	return
}

func (cca *cookedSyncCmdArgs) process() (err error) {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	err = common.SetBackupMode(cca.backupMode, cca.fromTo)
	if err != nil {
		return err
	}

	if err := common.VerifyIsURLResolvable(cca.source.Value); cca.fromTo.From().IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve source: %w", err)
	}

	if err := common.VerifyIsURLResolvable(cca.destination.Value); cca.fromTo.To().IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve destination: %w", err)
	}

	// Verifies credential type and initializes credential info.
	// Note that this is for the destination.
	cca.credentialInfo, _, err = GetCredentialInfoForLocation(ctx, cca.fromTo.To(), cca.destination, false, cca.cpkOptions)
	if err != nil {
		return err
	}

	srcCredInfo, _, err := GetCredentialInfoForLocation(ctx, cca.fromTo.From(), cca.source, true, cca.cpkOptions)
	if err != nil {
		return err
	}
	cca.s2sSourceCredentialType = srcCredInfo.CredentialType
	// Download is the only time our primary credential type will be based on source
	if cca.fromTo.IsDownload() {
		cca.credentialInfo = srcCredInfo
	} else if cca.fromTo.IsS2S() {
		cca.s2sSourceCredentialType = srcCredInfo.CredentialType // Assign the source credential type in S2S
	}

	// For OAuthToken credential, assign OAuthTokenInfo to CopyJobPartOrderRequest properly,
	// the info will be transferred to STE.
	if cca.credentialInfo.CredentialType.IsAzureOAuth() || srcCredInfo.CredentialType.IsAzureOAuth() {
		uotm := GetUserOAuthTokenManagerInstance()
		// Get token from env var or cache.
		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return err
		} else if _, err := tokenInfo.GetTokenCredential(); err != nil {
			return err
		}
	}

	// Check if destination is system container
	if cca.fromTo.IsS2S() || cca.fromTo.IsUpload() {
		dstContainerName, err := GetContainerName(cca.destination.Value, cca.fromTo.To())
		if err != nil {
			return fmt.Errorf("failed to get container name from destination (is it formatted correctly?)")
		}
		if common.IsSystemContainer(dstContainerName) {
			return fmt.Errorf("cannot copy to system container '%s'", dstContainerName)
		}
	}

	enumerator, err := cca.initEnumerator(ctx)
	if err != nil {
		return err
	}

	// trigger the progress reporting
	if !cca.dryrunMode {
		cca.waitUntilJobCompletion(false)
	}

	// trigger the enumeration
	err = enumerator.enumerate()
	if err != nil {
		return err
	}
	return nil
}

func init() {
	raw := rawSyncCmdArgs{}
	// syncCmd represents the sync command
	var syncCmd = &cobra.Command{
		Use:     "sync",
		Aliases: []string{"sc", "s"},
		Short:   syncCmdShortDescription,
		Long:    syncCmdLongDescription,
		Example: syncCmdExample,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("2 arguments source and destination are required for this command. Number of commands passed %d", len(args))
			}
			raw.src = args[0]
			raw.dst = args[1]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			glcm.EnableInputWatcher()
			if cancelFromStdin {
				glcm.EnableCancelFromStdIn()
			}
			// We infer FromTo and validate it here since it is critical to a lot of other options parsing below.
			userFromTo, err := ValidateFromTo(raw.src, raw.dst, raw.fromTo)
			if err != nil {
				glcm.Error("failed to parse --from-to user input due to error: " + err.Error())
			}

			raw.preserveInfo, raw.preservePermissions = ComputePreserveFlags(cmd, userFromTo,
				raw.preserveInfo, raw.preserveSMBInfo, raw.preservePermissions, raw.preserveSMBPermissions)

			cooked, err := raw.cook()
			if err != nil {
				glcm.Error("error parsing the input given by the user. Failed with error " + err.Error() + getErrorCodeUrl(err))
			}

			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				glcm.Error("Cannot perform sync due to error: " + err.Error() + getErrorCodeUrl(err))
			}
			if cooked.dryrunMode {
				glcm.Exit(nil, common.EExitCode.Success())
			}

			glcm.SurrenderControl()
		},
	}

	rootCmd.AddCommand(syncCmd)
	syncCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", true,
		"True by default, look into sub-directories recursively when syncing between directories. (default true).")

	syncCmd.PersistentFlags().StringVar(&raw.fromTo, "from-to", "",
		"Source-to-destination combination. Required for NFS transfers; optional for SMB."+
			"Examples: LocalBlob, BlobLocal, LocalFileSMB, FileSMBLocal, BlobFile, FileBlob, LocaFileNFS, "+
			"FileNFSLocal, FileNFSFileNFS, etc.")

	syncCmd.PersistentFlags().BoolVar(&raw.includeDirectoryStubs, "include-directory-stub", false,
		"False by default, includes blobs with the hdi_isfolder metadata in the transfer.")

	// TODO: enable for copy with IfSourceNewer
	// smb info/permissions can be persisted in the scenario of File -> File
	syncCmd.PersistentFlags().BoolVar(&raw.preserveSMBPermissions, "preserve-smb-permissions", false,
		"False by default. "+
			"\n Preserves SMB ACLs between aware resources (Azure Files). "+
			"\n This flag applies to both files and folders, unless a file-only filter is specified (e.g. include-pattern).")

	syncCmd.PersistentFlags().BoolVar(&raw.preserveSMBInfo, "preserve-smb-info", (runtime.GOOS == "windows"),
		"Preserves SMB property info (last write time, creation time, attribute bits)"+
			" between SMB-aware resources (Windows and Azure Files SMB). "+
			"On windows, this flag will be set to true by default. \n If the source or destination is a "+
			"\n volume mounted on Linux using SMB protocol, this flag will have to be explicitly set to true."+
			"\n  Only the attribute bits supported by Azure Files will be transferred; any others will be ignored. "+
			"\n This flag applies to both files and folders, unless a file-only filter is specified "+
			"(e.g. include-pattern). \n The info transferred for folders is the same as that for files, "+
			"except for Last Write Time which is never preserved for folders.")

	//Marking this flag as hidden as we might not support it in the future
	_ = syncCmd.PersistentFlags().MarkHidden("preserve-smb-info")
	syncCmd.PersistentFlags().BoolVar(&raw.preserveInfo, PreserveInfoFlag, false,
		"Specify this flag if you want to preserve properties during the transfer operation."+
			"The previously available flag for SMB (--preserve-smb-info) is now redirected to --preserve-info "+
			"flag for both SMB and NFS operations. The default value is true for Windows when copying to Azure Files SMB"+
			"share and for Linux when copying to Azure Files NFS share. ")

	syncCmd.PersistentFlags().BoolVar(&raw.preservePOSIXProperties, "preserve-posix-properties", false,
		"False by default. 'Preserves' property info gleaned from stat or statx into object metadata.")

	// TODO: enable when we support local <-> File
	syncCmd.PersistentFlags().BoolVar(&raw.forceIfReadOnly, "force-if-read-only", false, "False by default. "+
		"\n When overwriting an existing file on Windows or Azure Files, force the overwrite to work even if the"+
		"existing file has its read-only attribute set.")
	// syncCmd.PersistentFlags().BoolVar(&raw.preserveOwner, common.PreserveOwnerFlagName, common.PreserveOwnerDefault, "Only has an effect in downloads, and only when --preserve-smb-permissions is used. If true (the default), the file Owner and Group are preserved in downloads. If set to false, --preserve-smb-permissions will still preserve ACLs but Owner and Group will be based on the user running AzCopy")
	// syncCmd.PersistentFlags().BoolVar(&raw.backupMode, common.BackupModeFlagName, false, "Activates Windows' SeBackupPrivilege for uploads, or SeRestorePrivilege for downloads, to allow AzCopy to see read all files, regardless of their file system permissions, and to restore all permissions. Requires that the account running AzCopy already has these permissions (e.g. has Administrator rights or is a member of the 'Backup Operators' group). All this flag does is activate privileges that the account already has")

	syncCmd.PersistentFlags().Float64Var(&raw.blockSizeMB, "block-size-mb", 0,
		"Use this block size (specified in MiB) when uploading to Azure Storage or downloading from Azure Storage. "+
			"\n Default is automatically calculated based on file size. Decimal fractions are allowed (For example: 0.25).")

	syncCmd.PersistentFlags().Float64Var(&raw.putBlobSizeMB, "put-blob-size-mb", 0,
		"Use this size (specified in MiB) as a threshold to determine whether to upload a blob as a single PUT request"+
			"when uploading to Azure Storage. \n The default value is automatically calculated based on file size."+
			"Decimal fractions are allowed (For example: 0.25).")

	syncCmd.PersistentFlags().StringVar(&raw.include, "include-pattern", "",
		"Include only files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")

	syncCmd.PersistentFlags().StringVar(&raw.exclude, "exclude-pattern", "",
		"Exclude files where the name matches the pattern list.\n For example: *.jpg;*.pdf;exactName")

	syncCmd.PersistentFlags().StringVar(&raw.excludePath, "exclude-path", "",
		"Exclude these paths when comparing the source against the destination. "+
			"\n This option does not support wildcard characters (*). "+
			"\n Checks relative path prefix(For example: myFolder;myFolder/subDirName/file.pdf).")

	syncCmd.PersistentFlags().StringVar(&raw.includeFileAttributes, "include-attributes", "",
		"(Windows only) Include only files whose attributes match the attribute list.\n For example: A;S;R")

	syncCmd.PersistentFlags().StringVar(&raw.excludeFileAttributes, "exclude-attributes", "",
		"(Windows only) Exclude files whose attributes match the attribute list.\n For example: A;S;R")

	syncCmd.PersistentFlags().StringVar(&raw.includeRegex, "include-regex", "",
		"Include the relative path of the files that match with the regular expressions. "+
			"\n Separate regular expressions with ';'.")

	syncCmd.PersistentFlags().StringVar(&raw.excludeRegex, "exclude-regex", "",
		"Exclude the relative path of the files that match with the regular expressions. "+
			"\n Separate regular expressions with ';'.")

	syncCmd.PersistentFlags().StringVar(&raw.deleteDestination, "delete-destination", "false",
		"Defines whether to delete extra files from the destination that are not present at the source. "+
			"\n Could be set to true, false, or prompt. "+
			"\n If set to prompt, the user will be asked a question before scheduling files and blobs for deletion. (default 'false').")

	syncCmd.PersistentFlags().BoolVar(&raw.putMd5, "put-md5", false,
		"Create an MD5 hash of each file, and save the hash as the Content-MD5 property of the destination blob or file. "+
			"\n (By default the hash is NOT created.) Only available when uploading.")

	syncCmd.PersistentFlags().StringVar(&raw.md5ValidationOption, "check-md5", common.DefaultHashValidationOption.String(),
		"Specifies how strictly MD5 hashes should be validated when downloading. "+
			"\n This option is only available when downloading. "+
			"\n Available values include: NoCheck, LogOnly, FailIfDifferent, FailIfDifferentOrMissing. (default 'FailIfDifferent').")

	syncCmd.PersistentFlags().BoolVar(&raw.s2sPreserveAccessTier, "s2s-preserve-access-tier", true,
		"Preserve access tier during service to service copy. "+
			"\n Please refer to [Azure Blob storage: hot, cool, and archive access tiers](https://docs.microsoft.com/azure/storage/blobs/storage-blob-storage-tiers) to ensure destination storage account supports setting access tier. "+
			"\n In the cases that setting access tier is not supported, please use s2sPreserveAccessTier=false to bypass copying access tier (default true). ")

	syncCmd.PersistentFlags().BoolVar(&raw.s2sPreserveBlobTags, "s2s-preserve-blob-tags", false,
		"False by default. "+
			"\n Preserve index tags during service to service sync from one blob storage to another.")

	// Public Documentation: https://docs.microsoft.com/en-us/azure/storage/blobs/encryption-customer-provided-keys
	// Clients making requests against Azure Blob storage have the option to provide an encryption key on a per-request basis.
	// Including the encryption key on the request provides granular control over encryption settings for Blob storage operations.
	// Customer-provided keys can be stored in Azure Key Vault or in another key store linked to storage account.
	syncCmd.PersistentFlags().StringVar(&raw.cpkScopeInfo, "cpk-by-name", "",
		"Client provided key by name let clients making requests against Azure Blob storage an option "+
			"\n to provide an encryption key on a per-request basis. "+
			"\n Provided key name will be fetched from Azure Key Vault and will be used to encrypt the data")

	syncCmd.PersistentFlags().BoolVar(&raw.cpkInfo, "cpk-by-value", false,
		"False by default. Client provided key by name let clients making requests against Azure Blob storage an option "+
			"\n to provide an encryption key on a per-request basis. "+
			"\n Provided key and its hash will be fetched from environment variables (CPK_ENCRYPTION_KEY and CPK_ENCRYPTION_KEY_SHA256 must be set).")

	syncCmd.PersistentFlags().BoolVar(&raw.mirrorMode, "mirror-mode", false,
		"Disable last-modified-time based comparison and "+
			"\n overwrites the conflicting files and blobs at the destination if this flag is set to true. "+
			"\n Default is false.")

	syncCmd.PersistentFlags().BoolVar(&raw.dryrun, "dry-run", false,
		"False by default. Prints the path of files that would be copied or removed by the sync command. "+
			"\n This flag does not copy or remove the actual files.")

	syncCmd.PersistentFlags().StringVar(&raw.trailingDot, "trailing-dot", "",
		"'Enable' by default to treat file share related operations in a safe manner."+
			"\n  Available options: "+strings.Join(common.ValidTrailingDotOptions(), ", ")+". "+
			"\n Choose 'Disable' to go back to legacy (potentially unsafe) treatment of trailing dot files where the file service will trim any trailing dots in paths. "+
			"\n This can result in potential data corruption if the transfer contains two paths that differ only by a trailing dot (ex: mypath and mypath.). "+
			"\n If this flag is set to 'Disable' and AzCopy encounters a trailing dot file, it will warn customers in the scanning log but will not attempt to abort the operation."+
			"\n If the destination does not support trailing dot files (Windows or Blob Storage), "+
			"\n AzCopy will fail if the trailing dot file is the root of the transfer and skip any trailing dot paths encountered during enumeration.")

	syncCmd.PersistentFlags().BoolVar(&raw.includeRoot, "include-root", false, "Disabled by default. "+
		"\n Enable to include the root directory's properties when persisting properties such as SMB or HNS ACLs")

	syncCmd.PersistentFlags().StringVar(&raw.compareHash, "compare-hash", "None",
		"Inform sync to rely on hashes as an alternative to LMT. "+
			"\n Missing hashes at a remote source will throw an error. (None, MD5) Default: None")

	syncCmd.PersistentFlags().StringVar(&common.LocalHashDir, "hash-meta-dir", "",
		"When using `--local-hash-storage-mode=HiddenFiles` "+
			"\n you can specify an alternate directory to store hash metadata files in (as opposed to next to the related files in the source)")

	syncCmd.PersistentFlags().StringVar(&raw.localHashStorageMode, "local-hash-storage-mode",
		common.EHashStorageMode.Default().String(), "Specify an alternative way to cache file hashes; "+
			"\n valid options are: HiddenFiles (OS Agnostic), "+
			"\n XAttr (Linux/MacOS only; requires user_xattr on all filesystems traversed @ source), "+
			"\n AlternateDataStreams (Windows only; requires named streams on target volume)")

	// temp, to assist users with change in param names, by providing a clearer message when these obsolete ones are accidentally used
	syncCmd.PersistentFlags().StringVar(&raw.legacyInclude, "include", "", "Legacy include param. DO NOT USE")
	syncCmd.PersistentFlags().StringVar(&raw.legacyExclude, "exclude", "", "Legacy exclude param. DO NOT USE")
	_ = syncCmd.PersistentFlags().MarkHidden("include")
	_ = syncCmd.PersistentFlags().MarkHidden("exclude")

	// TODO follow sym link is not implemented, clarify behavior first
	// syncCmd.PersistentFlags().BoolVar(&raw.followSymlinks, "follow-symlinks", false, "follow symbolic links when performing sync from local file system.")

	// TODO sync does not support all BlobAttributes on the command line, this functionality should be added

	// Deprecate the old persist-smb-permissions flag
	_ = syncCmd.PersistentFlags().MarkHidden("preserve-smb-permissions")
	syncCmd.PersistentFlags().BoolVar(&raw.preservePermissions, PreservePermissionsFlag, false, "False by default. "+
		"\nPreserves ACLs between aware resources (Windows and Azure Files SMB or Data Lake Storage to Data Lake Storage)"+
		"and permissions between aware resources(Linux to Azure Files NFS). "+
		"\nFor accounts that have a hierarchical namespace, your security principal must be the owning user of the target container or it must be assigned "+
		"\nthe Storage Blob Data Owner role, scoped to the target container, storage account, parent resource group, or subscription. "+
		"\nFor downloads, you will also need the --backup flag to restore permissions where the new Owner will not be the user running AzCopy. "+
		"\nThis flag applies to both files and folders, unless a file-only filter is specified (e.g. include-pattern).")

	// Deletes destination blobs with uncommitted blocks when staging block, hidden because we want to preserve default behavior
	syncCmd.PersistentFlags().BoolVar(&raw.deleteDestinationFileIfNecessary, "delete-destination-file", false, "False by default. Deletes destination blobs, specifically blobs with uncommitted blocks when staging block.")
	_ = syncCmd.PersistentFlags().MarkHidden("delete-destination-file")

	syncCmd.PersistentFlags().StringVar(&raw.hardlinks, HardlinksFlag, "follow",
		"Follow by default. Preserve hardlinks for NFS resources. "+
			"\n This flag is only applicable when the source is Azure NFS file share or the destination is NFS file share. "+
			"\n Available options: skip, preserve, follow (default 'follow').")
}
