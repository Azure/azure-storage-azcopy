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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"time"

	"net/url"
	"strings"

	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/spf13/cobra"
)

// a max is set because we cannot buffer infinite amount of destination file info in memory
const MaxNumberOfFilesAllowedInSync = 10000000

type rawSyncCmdArgs struct {
	src       string
	dst       string
	recursive bool
	// options from flags
	blockSizeMB         float64
	logVerbosity        string
	include             string
	exclude             string
	followSymlinks      bool
	putMd5              bool
	md5ValidationOption string
	// this flag indicates the user agreement with respect to deleting the extra files at the destination
	// which do not exists at source. With this flag turned on/off, users will not be asked for permission.
	// otherwise the user is prompted to make a decision
	deleteDestination string
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

// given a valid URL, parse out the SAS portion
func (raw *rawSyncCmdArgs) separateSasFromURL(rawURL string) (cleanURL string, sas string) {
	fromUrl, _ := url.Parse(rawURL)

	// TODO add support for other service URLs
	blobParts := azblob.NewBlobURLParts(*fromUrl)
	sas = blobParts.SAS.Encode()

	// get clean URL without SAS and trailing / in the path
	blobParts.SAS = azblob.SASQueryParameters{}
	bUrl := blobParts.URL()
	bUrl.Path = strings.TrimSuffix(bUrl.Path, common.AZCOPY_PATH_SEPARATOR_STRING)
	cleanURL = bUrl.String()

	return
}

// validates and transform raw input into cooked input
func (raw *rawSyncCmdArgs) cook() (cookedSyncCmdArgs, error) {
	cooked := cookedSyncCmdArgs{}

	cooked.fromTo = inferFromTo(raw.src, raw.dst)
	if cooked.fromTo == common.EFromTo.Unknown() {
		return cooked, fmt.Errorf("Unable to infer the source '%s' / destination '%s'. ", raw.src, raw.dst)
	} else if cooked.fromTo == common.EFromTo.LocalBlob() {
		cooked.source = cleanLocalPath(raw.src)
		cooked.destination, cooked.destinationSAS = raw.separateSasFromURL(raw.dst)
	} else if cooked.fromTo == common.EFromTo.BlobLocal() {
		cooked.source, cooked.sourceSAS = raw.separateSasFromURL(raw.src)
		cooked.destination = cleanLocalPath(raw.dst)
	} else {
		return cooked, fmt.Errorf("source '%s' / destination '%s' combination '%s' not supported for sync command ", raw.src, raw.dst, cooked.fromTo)
	}

	// generate a new job ID
	cooked.jobID = common.NewJobID()

	var err error
	cooked.blockSize, err = blockSizeInBytes(raw.blockSizeMB)
	if err != nil {
		return cooked, err
	}

	cooked.followSymlinks = raw.followSymlinks
	cooked.recursive = raw.recursive

	// determine whether we should prompt the user to delete extra files
	err = cooked.deleteDestination.Parse(raw.deleteDestination)
	if err != nil {
		return cooked, err
	}

	// parse the filter patterns
	cooked.include = raw.parsePatterns(raw.include)
	cooked.exclude = raw.parsePatterns(raw.exclude)

	err = cooked.logVerbosity.Parse(raw.logVerbosity)
	if err != nil {
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

	source         string
	sourceSAS      string
	destination    string
	destinationSAS string
	fromTo         common.FromTo
	credentialInfo common.CredentialInfo

	// filters
	recursive      bool
	followSymlinks bool
	include        []string
	exclude        []string

	// options
	putMd5              bool
	md5ValidationOption common.HashValidationOption
	blockSize           uint32
	logVerbosity        common.LogLevel

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
	glcm.Init(common.GetStandardInitOutputBuilder(cca.jobID.String(), fmt.Sprintf("%s/%s.log", azcopyLogPathFolder, cca.jobID)))

	// initialize the times necessary to track progress
	cca.jobStartTime = time.Now()
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = 0

	// hand over control to the lifecycle manager if blocking
	if blocking {
		glcm.InitiateProgressReporting(cca, true)
		glcm.SurrenderControl()
	} else {
		// non-blocking, return after spawning a go routine to watch the job
		glcm.InitiateProgressReporting(cca, true)
	}
}

func (cca *cookedSyncCmdArgs) Cancel(lcm common.LifecycleMgr) {
	// prompt for confirmation, except when enumeration is complete
	if !cca.isEnumerationComplete {
		answer := lcm.Prompt("The source enumeration is not complete, cancelling the job at this point means it cannot be resumed. Please confirm with y/n: ")

		// read a line from stdin, if the answer is not yes, then abort cancel by returning
		if !strings.EqualFold(answer, "y") {
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

func (cca *cookedSyncCmdArgs) getJsonOfSyncJobSummary(summary common.ListSyncJobSummaryResponse) string {
	// TODO figure out if deletions should be done by the enumeration engine or not
	// TODO if not, remove this so that we get the proper number from the ste
	summary.DeleteTotalTransfers = cca.getDeletionCount()
	summary.DeleteTransfersCompleted = cca.getDeletionCount()
	jsonOutput, err := json.Marshal(summary)
	common.PanicIfErr(err)
	return string(jsonOutput)
}

func (cca *cookedSyncCmdArgs) ReportProgressOrExit(lcm common.LifecycleMgr) {
	duration := time.Now().Sub(cca.jobStartTime) // report the total run time of the job
	var summary common.ListSyncJobSummaryResponse
	var throughput float64
	var jobDone bool

	// fetch a job status and compute throughput if the first part was dispatched
	if cca.firstPartOrdered() {
		Rpc(common.ERpcCmd.ListSyncJobSummary(), &cca.jobID, &summary)
		jobDone = summary.JobStatus.IsJobDone()

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
		if summary.CopyTransfersFailed+summary.DeleteTransfersFailed > 0 {
			exitCode = common.EExitCode.Error()
		}

		lcm.Exit(func(format common.OutputFormat) string {
			if format == common.EOutputFormat.Json() {
				return cca.getJsonOfSyncJobSummary(summary)
			}

			output := fmt.Sprintf(
				`
Job %s Summary
Files Scanned at Source: %v
Files Scanned at Destination: %v
Elapsed Time (Minutes): %v
Total Number Of Copy Transfers: %v
Number of Copy Transfers Completed: %v
Number of Copy Transfers Failed: %v
Number of Deletions at Destination: %v
Total Number of Bytes Transferred: %v
Total Number of Bytes Enumerated: %v
Final Job Status: %v
`,
				summary.JobID.String(),
				atomic.LoadUint64(&cca.atomicSourceFilesScanned),
				atomic.LoadUint64(&cca.atomicDestinationFilesScanned),
				ste.ToFixed(duration.Minutes(), 4),
				summary.CopyTotalTransfers,
				summary.CopyTransfersCompleted,
				summary.CopyTransfersFailed,
				cca.atomicDeletionCount,
				summary.TotalBytesTransferred,
				summary.TotalBytesEnumerated,
				summary.JobStatus)

			jobMan, exists := ste.JobsAdmin.JobMgr(summary.JobID)
			if exists {
				jobMan.Log(pipeline.LogInfo, output)
			}

			return output
		}, exitCode)
	}

	lcm.Progress(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			return cca.getJsonOfSyncJobSummary(summary)
		}

		// indicate whether constrained by disk or not
		perfString, diskString := getPerfDisplayText(summary.PerfStrings, summary.PerfConstraint, duration)

		return fmt.Sprintf("%v Done, %v Failed, %v Pending, %v Total%s, 2-sec Throughput (Mb/s): %v%s",
			summary.CopyTransfersCompleted+summary.DeleteTransfersCompleted,
			summary.CopyTransfersFailed+summary.DeleteTransfersFailed,
			summary.CopyTotalTransfers+summary.DeleteTotalTransfers-(summary.CopyTransfersCompleted+summary.DeleteTransfersCompleted+summary.CopyTransfersFailed+summary.DeleteTransfersFailed),
			summary.CopyTotalTransfers+summary.DeleteTotalTransfers, perfString, ste.ToFixed(throughput, 4), diskString)
	})
}

func (cca *cookedSyncCmdArgs) process() (err error) {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// verifies credential type and initializes credential info.
	// For sync, only one side need credential.
	cca.credentialInfo.CredentialType, err = getCredentialType(ctx, rawFromToInfo{
		fromTo:         cca.fromTo,
		source:         cca.source,
		destination:    cca.destination,
		sourceSAS:      cca.sourceSAS,
		destinationSAS: cca.destinationSAS,
	})

	if err != nil {
		return err
	}

	// For OAuthToken credential, assign OAuthTokenInfo to CopyJobPartOrderRequest properly,
	// the info will be transferred to STE.
	if cca.credentialInfo.CredentialType == common.ECredentialType.OAuthToken() {
		// Message user that they are using Oauth token for authentication,
		// in case of silently using cached token without consciousness。
		glcm.Info("Using OAuth token for authentication.")

		uotm := GetUserOAuthTokenManagerInstance()
		// Get token from env var or cache.
		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return err
		} else {
			cca.credentialInfo.OAuthTokenInfo = *tokenInfo
		}
	}

	var enumerator *syncEnumerator

	switch cca.fromTo {
	case common.EFromTo.LocalBlob():
		enumerator, err = newSyncUploadEnumerator(cca)
		if err != nil {
			return err
		}
	case common.EFromTo.BlobLocal():
		enumerator, err = newSyncDownloadEnumerator(cca)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("the given source/destination pair is currently not supported")
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
	syncCmd.PersistentFlags().Float64Var(&raw.blockSizeMB, "block-size-mb", 0, "Use this block size (specified in MiB) when uploading to Azure Storage or downloading from Azure Storage. Default is automatically calculated based on file size. Decimal fractions are allowed (For example: 0.25).")
	syncCmd.PersistentFlags().StringVar(&raw.include, "include", "", "Include only files where the name matches the pattern list. For example: .jpg;.pdf;exactName.")
	syncCmd.PersistentFlags().StringVar(&raw.exclude, "exclude", "", "Exclude files where the name matches the pattern list. For example: .jpg;.pdf;exactName.")
	syncCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "INFO", "Define the log verbosity for the log file, available levels: INFO(all requests and responses), WARNING(slow responses), ERROR(only failed requests), and NONE(no output logs). (default INFO).")
	syncCmd.PersistentFlags().StringVar(&raw.deleteDestination, "delete-destination", "false", "Defines whether to delete extra files from the destination that are not present at the source. Could be set to true, false, or prompt. If set to prompt, the user will be asked a question before scheduling files and blobs for deletion. (default 'false').")
	syncCmd.PersistentFlags().BoolVar(&raw.putMd5, "put-md5", false, "Create an MD5 hash of each file, and save the hash as the Content-MD5 property of the destination blob or file. (By default the hash is NOT created.) Only available when uploading.")
	syncCmd.PersistentFlags().StringVar(&raw.md5ValidationOption, "check-md5", common.DefaultHashValidationOption.String(), "Specifies how strictly MD5 hashes should be validated when downloading. This option is only available when downloading. Available values include: NoCheck, LogOnly, FailIfDifferent, FailIfDifferentOrMissing. (default 'FailIfDifferent').")

	// TODO follow sym link is not implemented, clarify behavior first
	//syncCmd.PersistentFlags().BoolVar(&raw.followSymlinks, "follow-symlinks", false, "follow symbolic links when performing sync from local file system.")

	// TODO sync does not support any BlobAttributes, this functionality should be added
}
