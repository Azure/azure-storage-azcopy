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
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"

	"github.com/spf13/cobra"
)

type rawSyncCmdArgs struct {
	src       string
	dst       string
	recursive bool

	// options from flags
	blockSizeMB           float64
	logVerbosity          string
	include               string
	exclude               string
	excludePath           string
	includeFileAttributes string
	excludeFileAttributes string
	legacyInclude         string // for warning messages only
	legacyExclude         string // for warning messages only

	preserveSMBPermissions bool
	preserveOwner          bool
	preserveSMBInfo        bool
	followSymlinks         bool
	backupMode             bool
	putMd5                 bool
	md5ValidationOption    string
	// this flag indicates the user agreement with respect to deleting the extra files at the destination
	// which do not exists at source. With this flag turned on/off, users will not be asked for permission.
	// otherwise the user is prompted to make a decision
	deleteDestination string

	s2sPreserveAccessTier bool

	forceIfReadOnly bool
}

func (raw *rawSyncCmdArgs) parsePatterns(pattern string) (cookedPatterns []string) {
	cookedPatterns = make([]string, 0)
	rawPatterns := strings.Split(pattern, ";")
	for _, pattern := range rawPatterns {

		// skip the empty patterns
		if len(pattern) != 0 {
			cookedPatterns = append(cookedPatterns, pattern)
		}
	}

	return
}

// it is assume that the given url has the SAS stripped, and safe to print
func (raw *rawSyncCmdArgs) validateURLIsNotServiceLevel(url string, location common.Location) error {
	srcLevel, err := determineLocationLevel(url, location, true)
	if err != nil {
		return err
	}

	if srcLevel == ELocationLevel.Service() {
		return fmt.Errorf("service level URLs (%s) are not supported in sync: ", url)
	}

	return nil
}

// validates and transform raw input into cooked input
func (raw *rawSyncCmdArgs) cook() (cookedSyncCmdArgs, error) {
	cooked := cookedSyncCmdArgs{}

	// this if statement ladder remains instead of being separated to help determine valid combinations for sync
	// consider making a map of valid source/dest combos and consolidating this to generic source/dest setups, akin to the lower if statement
	// TODO: if expand the set of source/dest combos supported by sync, update this method the declarative test framework: // TODO: add support for account-to-account operations (for those from-tos that support that)
	cooked.fromTo = inferFromTo(raw.src, raw.dst)
	var err error
	if cooked.fromTo == common.EFromTo.Unknown() {
		return cooked, fmt.Errorf("Unable to infer the source '%s' / destination '%s'. ", raw.src, raw.dst)
	} else if cooked.fromTo == common.EFromTo.LocalBlob() {
		cooked.destination, err = SplitResourceString(raw.dst, cooked.fromTo.To())
		common.PanicIfErr(err)
	} else if cooked.fromTo == common.EFromTo.BlobLocal() {
		cooked.source, err = SplitResourceString(raw.src, cooked.fromTo.From())
		common.PanicIfErr(err)
	} else if cooked.fromTo == common.EFromTo.BlobBlob() || cooked.fromTo == common.EFromTo.FileFile() {
		cooked.destination, err = SplitResourceString(raw.dst, cooked.fromTo.To())
		common.PanicIfErr(err)
		cooked.source, err = SplitResourceString(raw.src, cooked.fromTo.From())
		common.PanicIfErr(err)
	} else {
		return cooked, fmt.Errorf("source '%s' / destination '%s' combination '%s' not supported for sync command ", raw.src, raw.dst, cooked.fromTo)
	}

	// Do this check seperately so we don't end up with a bunch of code duplication when new src/dsts are added
	if cooked.fromTo.From() == common.ELocation.Local() {
		cooked.source = common.ResourceString{Value: common.ToExtendedPath(cleanLocalPath(raw.src))}
	} else if cooked.fromTo.To() == common.ELocation.Local() {
		cooked.destination = common.ResourceString{Value: common.ToExtendedPath(cleanLocalPath(raw.dst))}
	}

	// we do not support service level sync yet
	if cooked.fromTo.From().IsRemote() {
		err = raw.validateURLIsNotServiceLevel(cooked.source.Value, cooked.fromTo.From())
		if err != nil {
			return cooked, err
		}
	}

	// we do not support service level sync yet
	if cooked.fromTo.To().IsRemote() {
		err = raw.validateURLIsNotServiceLevel(cooked.destination.Value, cooked.fromTo.To())
		if err != nil {
			return cooked, err
		}
	}

	// generate a new job ID
	cooked.jobID = common.NewJobID()

	cooked.blockSize, err = blockSizeInBytes(raw.blockSizeMB)
	if err != nil {
		return cooked, err
	}

	cooked.followSymlinks = raw.followSymlinks
	if err = crossValidateSymlinksAndPermissions(cooked.followSymlinks, true /* replace with real value when available */); err != nil {
		return cooked, err
	}
	cooked.recursive = raw.recursive
	cooked.forceIfReadOnly = raw.forceIfReadOnly
	if err = validateForceIfReadOnly(cooked.forceIfReadOnly, cooked.fromTo); err != nil {
		return cooked, err
	}

	cooked.backupMode = raw.backupMode
	if err = validateBackupMode(cooked.backupMode, cooked.fromTo); err != nil {
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
	cooked.includePatterns = raw.parsePatterns(raw.include)
	cooked.excludePatterns = raw.parsePatterns(raw.exclude)
	cooked.excludePaths = raw.parsePatterns(raw.excludePath)

	// parse the attribute filter patterns
	cooked.includeFileAttributes = raw.parsePatterns(raw.includeFileAttributes)
	cooked.excludeFileAttributes = raw.parsePatterns(raw.excludeFileAttributes)

	err = cooked.logVerbosity.Parse(raw.logVerbosity)
	if err != nil {
		return cooked, err
	}

	if err = validatePreserveSMBPropertyOption(raw.preserveSMBPermissions, cooked.fromTo, nil, "preserve-smb-permissions"); err != nil {
		return cooked, err
	}
	// TODO: the check on raw.preserveSMBPermissions on the next line can be removed once we have full support for these properties in sync
	if err = validatePreserveOwner(raw.preserveOwner, cooked.fromTo); raw.preserveSMBPermissions && err != nil {
		return cooked, err
	}
	cooked.preserveSMBPermissions = common.NewPreservePermissionsOption(raw.preserveSMBPermissions, raw.preserveOwner, cooked.fromTo)

	cooked.preserveSMBInfo = raw.preserveSMBInfo
	if err = validatePreserveSMBPropertyOption(cooked.preserveSMBInfo, cooked.fromTo, nil, "preserve-smb-info"); err != nil {
		return cooked, err
	}

	cooked.putMd5 = raw.putMd5
	if err = validatePutMd5(cooked.putMd5, cooked.fromTo); err != nil {
		return cooked, err
	}

	err = cooked.md5ValidationOption.Parse(raw.md5ValidationOption)
	if err != nil {
		return cooked, err
	}
	if err = validateMd5Option(cooked.md5ValidationOption, cooked.fromTo); err != nil {
		return cooked, err
	}

	if cooked.fromTo.IsS2S() {
		cooked.preserveAccessTier = raw.s2sPreserveAccessTier
	}

	return cooked, nil
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

	source         common.ResourceString
	destination    common.ResourceString
	fromTo         common.FromTo
	credentialInfo common.CredentialInfo

	// filters
	recursive             bool
	followSymlinks        bool
	includePatterns       []string
	excludePatterns       []string
	excludePaths          []string
	includeFileAttributes []string
	excludeFileAttributes []string

	// options
	preserveSMBPermissions common.PreservePermissionsOption
	preserveSMBInfo        bool
	putMd5                 bool
	md5ValidationOption    common.HashValidationOption
	blockSize              uint32
	logVerbosity           common.LogLevel
	forceIfReadOnly        bool
	backupMode             bool

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
	glcm.Init(common.GetStandardInitOutputBuilder(cca.jobID.String(), fmt.Sprintf("%s%s%s.log", azcopyLogPathFolder, common.OS_PATH_SEPARATOR, cca.jobID), false, ""))

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
			throughputString = fmt.Sprintf(", 2-sec Throughput (Mb/s): %v", ste.ToFixed(throughput, 4))
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
	duration := time.Now().Sub(cca.jobStartTime) // report the total run time of the job
	var summary common.ListJobSummaryResponse
	var throughput float64
	var jobDone bool

	// fetch a job status and compute throughput if the first part was dispatched
	if cca.firstPartOrdered() {
		Rpc(common.ERpcCmd.ListJobSummary(), &cca.jobID, &summary)
		Rpc(common.ERpcCmd.GetJobLCMWrapper(), &cca.jobID, &lcm)
		jobDone = summary.JobStatus.IsJobDone()
		totalKnownCount = summary.TotalTransfers

		// compute the average throughput for the last time interval
		bytesInMb := float64(float64(summary.BytesOverWire-cca.intervalBytesTransferred) * 8 / float64(base10Mega))
		timeElapsed := time.Since(cca.intervalStartTime).Seconds()
		throughput = common.Iffloat64(timeElapsed != 0, bytesInMb/timeElapsed, 0)

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

	if jobDone {
		exitCode := common.EExitCode.Success()
		if summary.TransfersFailed > 0 {
			exitCode = common.EExitCode.Error()
		}

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
Total Number Of Copy Transfers: %v
Number of Copy Transfers Completed: %v
Number of Copy Transfers Failed: %v
Number of Deletions at Destination: %v
Total Number of Bytes Transferred: %v
Total Number of Bytes Enumerated: %v
Final Job Status: %v%s%s
`,
				summary.JobID.String(),
				atomic.LoadUint64(&cca.atomicSourceFilesScanned),
				atomic.LoadUint64(&cca.atomicDestinationFilesScanned),
				ste.ToFixed(duration.Minutes(), 4),
				summary.FileTransfers,
				summary.FolderPropertyTransfers,
				summary.TotalTransfers,
				summary.TransfersCompleted,
				summary.TransfersFailed,
				cca.atomicDeletionCount,
				summary.TotalBytesTransferred,
				summary.TotalBytesEnumerated,
				summary.JobStatus,
				screenStats,
				formatPerfAdvice(summary.PerformanceAdvice))

			jobMan, exists := ste.JobsAdmin.JobMgr(summary.JobID)
			if exists {
				jobMan.Log(pipeline.LogInfo, logStats+"\n"+output)
			}

			return output
		}, exitCode)
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
			summary.TotalTransfers, perfString, ste.ToFixed(throughput, 4), diskString)
	})

	return
}

func (cca *cookedSyncCmdArgs) process() (err error) {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	err = common.SetBackupMode(cca.backupMode, cca.fromTo)
	if err != nil {
		return err
	}

	// Verifies credential type and initializes credential info.
	// Note that this is for the destination.
	cca.credentialInfo, _, err = getCredentialInfoForLocation(ctx, cca.fromTo.To(),
		cca.destination.Value, cca.destination.SAS, false)

	if err != nil {
		return err
	}

	// For OAuthToken credential, assign OAuthTokenInfo to CopyJobPartOrderRequest properly,
	// the info will be transferred to STE.
	if cca.credentialInfo.CredentialType == common.ECredentialType.OAuthToken() {
		uotm := GetUserOAuthTokenManagerInstance()
		// Get token from env var or cache.
		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return err
		} else {
			cca.credentialInfo.OAuthTokenInfo = *tokenInfo
		}
	}

	enumerator, err := cca.initEnumerator(ctx)
	if err != nil {
		return err
	}

	// trigger the progress reporting
	cca.waitUntilJobCompletion(false)

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

			cooked, err := raw.cook()
			if err != nil {
				glcm.Error("error parsing the input given by the user. Failed with error " + err.Error())
			}
			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				glcm.Error("Cannot perform sync due to error: " + err.Error())
			}

			glcm.SurrenderControl()
		},
	}

	rootCmd.AddCommand(syncCmd)
	syncCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", true, "True by default, look into sub-directories recursively when syncing between directories. (default true).")

	// TODO: enable (and test) the following when we sort out what sync will do for files and folders where only
	//    the attributes, name-value-metadata (AzureFiles), or SDDL has changed, but there's been no file content change.
	// TODO: when we sort that out, also enable it for copy with IfSourceNewer
	//syncCmd.PersistentFlags().BoolVar(&raw.preserveSMBPermissions, "preserve-smb-permissions", false, "False by default. Preserves SMB ACLs between aware resources (Windows and Azure Files). For downloads, you will also need the --backup flag to restore permissions where the new Owner will not be the user running AzCopy. This flag applies to both files and folders, unless a file-only filter is specified (e.g. include-pattern).")
	//syncCmd.PersistentFlags().BoolVar(&raw.forceIfReadOnly, "force-if-read-only", false, "When overwriting an existing file on Windows or Azure Files, force the overwrite to work even if the existing file has its read-only attribute set")
	// TODO: if/when we enable preserve-smb-info for sync, think about what the transfer of LMTs means for both files and FOLDERS
	//   Note that for folders we don't currently preserve LMTs, because that's not feasible in large download scenarios (and because folder LMTs
	//   don't generally convey useful information).  However, we need to think through what this will mean when we enable preserve-smb-info
	//   for sync.  Will folder sync just work fine as it does now, with no preservation of folder LMTs?
	//syncCmd.PersistentFlags().BoolVar(&raw.preserveOwner, common.PreserveOwnerFlagName, common.PreserveOwnerDefault, "Only has an effect in downloads, and only when --preserve-smb-permissions is used. If true (the default), the file Owner and Group are preserved in downloads. If set to false, --preserve-smb-permissions will still preserve ACLs but Owner and Group will be based on the user running AzCopy")
	//syncCmd.PersistentFlags().BoolVar(&raw.preserveSMBInfo, "preserve-smb-info", (see TO DO on line above!) false, "False by default. Preserves SMB property info (last write time, creation time, attribute bits) between SMB-aware resources (Windows and Azure Files). Only the attribute bits supported by Azure Files will be transferred; any others will be ignored. This flag applies to both files and folders, unless a file-only filter is specified (e.g. include-pattern). The info transferred for folders is the same as that for files, except for Last Write Time which is not preserved for folders. ")
	//syncCmd.PersistentFlags().BoolVar(&raw.backupMode, common.BackupModeFlagName, false, "Activates Windows' SeBackupPrivilege for uploads, or SeRestorePrivilege for downloads, to allow AzCopy to see read all files, regardless of their file system permissions, and to restore all permissions. Requires that the account running AzCopy already has these permissions (e.g. has Administrator rights or is a member of the 'Backup Operators' group). All this flag does is activate privileges that the account already has")

	syncCmd.PersistentFlags().Float64Var(&raw.blockSizeMB, "block-size-mb", 0, "Use this block size (specified in MiB) when uploading to Azure Storage or downloading from Azure Storage. Default is automatically calculated based on file size. Decimal fractions are allowed (For example: 0.25).")
	syncCmd.PersistentFlags().StringVar(&raw.include, "include-pattern", "", "Include only files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")
	syncCmd.PersistentFlags().StringVar(&raw.exclude, "exclude-pattern", "", "Exclude files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")
	syncCmd.PersistentFlags().StringVar(&raw.excludePath, "exclude-path", "", "Exclude these paths when comparing the source against the destination. "+
		"This option does not support wildcard characters (*). Checks relative path prefix(For example: myFolder;myFolder/subDirName/file.pdf).")
	syncCmd.PersistentFlags().StringVar(&raw.includeFileAttributes, "include-attributes", "", "(Windows only) Include only files whose attributes match the attribute list. For example: A;S;R")
	syncCmd.PersistentFlags().StringVar(&raw.excludeFileAttributes, "exclude-attributes", "", "(Windows only) Exclude files whose attributes match the attribute list. For example: A;S;R")
	syncCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "INFO", "Define the log verbosity for the log file, available levels: INFO(all requests and responses), WARNING(slow responses), ERROR(only failed requests), and NONE(no output logs). (default INFO).")
	syncCmd.PersistentFlags().StringVar(&raw.deleteDestination, "delete-destination", "false", "Defines whether to delete extra files from the destination that are not present at the source. Could be set to true, false, or prompt. "+
		"If set to prompt, the user will be asked a question before scheduling files and blobs for deletion. (default 'false').")
	syncCmd.PersistentFlags().BoolVar(&raw.putMd5, "put-md5", false, "Create an MD5 hash of each file, and save the hash as the Content-MD5 property of the destination blob or file. (By default the hash is NOT created.) Only available when uploading.")
	syncCmd.PersistentFlags().StringVar(&raw.md5ValidationOption, "check-md5", common.DefaultHashValidationOption.String(), "Specifies how strictly MD5 hashes should be validated when downloading. This option is only available when downloading. Available values include: NoCheck, LogOnly, FailIfDifferent, FailIfDifferentOrMissing. (default 'FailIfDifferent').")
	syncCmd.PersistentFlags().BoolVar(&raw.s2sPreserveAccessTier, "s2s-preserve-access-tier", true, "Preserve access tier during service to service copy. "+
		"Please refer to [Azure Blob storage: hot, cool, and archive access tiers](https://docs.microsoft.com/azure/storage/blobs/storage-blob-storage-tiers) to ensure destination storage account supports setting access tier. "+
		"In the cases that setting access tier is not supported, please use s2sPreserveAccessTier=false to bypass copying access tier. (default true). ")

	// temp, to assist users with change in param names, by providing a clearer message when these obsolete ones are accidentally used
	syncCmd.PersistentFlags().StringVar(&raw.legacyInclude, "include", "", "Legacy include param. DO NOT USE")
	syncCmd.PersistentFlags().StringVar(&raw.legacyExclude, "exclude", "", "Legacy exclude param. DO NOT USE")
	syncCmd.PersistentFlags().MarkHidden("include")
	syncCmd.PersistentFlags().MarkHidden("exclude")

	// TODO follow sym link is not implemented, clarify behavior first
	//syncCmd.PersistentFlags().BoolVar(&raw.followSymlinks, "follow-symlinks", false, "follow symbolic links when performing sync from local file system.")

	// TODO sync does not support all BlobAttributes on the command line, this functionality should be added
}
