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
	"time"

	"net/url"
	"os"
	"strings"

	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/spf13/cobra"
)

// TODO plug this in
// a max is set because we cannot buffer infinite amount of destination file info in memory
const MaxNumberOfFilesAllowedInSync = 10000000

type rawSyncCmdArgs struct {
	src       string
	dst       string
	recursive bool
	// options from flags
	blockSize           uint32
	logVerbosity        string
	include             string
	exclude             string
	followSymlinks      bool
	output              string
	md5ValidationOption string
	// this flag predefines the user-agreement to delete the files in case sync found some files at destination
	// which doesn't exists at source. With this flag turned on, user will not be asked for permission before
	// deleting the flag.
	force bool
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

	// get clean URL without SAS
	blobParts.SAS = azblob.SASQueryParameters{}
	bUrl := blobParts.URL()
	cleanURL = bUrl.String()

	return
}

func (raw *rawSyncCmdArgs) cleanLocalPath(rawPath string) (cleanPath string) {
	// if the path separator is '\\', it means
	// local path is a windows path
	// to avoid path separator check and handling the windows
	// path differently, replace the path separator with the
	// the linux path separator '/'
	if os.PathSeparator == '\\' {
		cleanPath = strings.Replace(rawPath, common.OS_PATH_SEPARATOR, "/", -1)
	}
	cleanPath = rawPath
	return
}

// validates and transform raw input into cooked input
func (raw *rawSyncCmdArgs) cook() (cookedSyncCmdArgs, error) {
	cooked := cookedSyncCmdArgs{}

	cooked.fromTo = inferFromTo(raw.src, raw.dst)
	if cooked.fromTo == common.EFromTo.Unknown() {
		return cooked, fmt.Errorf("Unable to infer the source '%s' / destination '%s'. ", raw.src, raw.dst)
	} else if cooked.fromTo == common.EFromTo.LocalBlob() {
		cooked.source = raw.cleanLocalPath(raw.src)
		cooked.destination, cooked.destinationSAS = raw.separateSasFromURL(raw.dst)
	} else if cooked.fromTo == common.EFromTo.BlobLocal() {
		cooked.source, cooked.sourceSAS = raw.separateSasFromURL(raw.src)
		cooked.destination = raw.cleanLocalPath(raw.dst)
	} else {
		return cooked, fmt.Errorf("source '%s' / destination '%s' combination '%s' not supported for sync command ", raw.src, raw.dst, cooked.fromTo)
	}

	// generate a new job ID
	cooked.jobID = common.NewJobID()

	cooked.blockSize = raw.blockSize
	cooked.followSymlinks = raw.followSymlinks
	cooked.recursive = raw.recursive
	cooked.force = raw.force

	// parse the filter patterns
	cooked.include = raw.parsePatterns(raw.include)
	cooked.exclude = raw.parsePatterns(raw.exclude)

	err := cooked.logVerbosity.Parse(raw.logVerbosity)
	if err != nil {
		return cooked, err
	}

	err = cooked.md5ValidationOption.Parse(raw.md5ValidationOption)
	if err != nil {
		return cooked, err
	}
	if err = validateMd5Option(cooked.md5ValidationOption, cooked.fromTo); err != nil {
		return cooked, err
	}

	err = cooked.output.Parse(raw.output)
	if err != nil {
		return cooked, err
	}

	return cooked, nil
}

type cookedSyncCmdArgs struct {
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
	md5ValidationOption common.HashValidationOption
	blockSize           uint32
	logVerbosity        common.LogLevel
	output              common.OutputFormat

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

	// defines the scanning status of the sync operation.
	// 0 means scanning is in progress and 1 means scanning is complete.
	atomicScanningStatus uint32
	// defines whether first part has been ordered or not.
	// 0 means first part is not ordered and 1 means first part is ordered.
	atomicFirstPartOrdered uint32
	// defines the number of files listed at the source and compared.
	atomicSourceFilesScanned uint64
	// defines the number of files listed at the destination and compared.
	atomicDestinationFilesScanned uint64
	// this flag determines the user-agreement to delete the files in case sync found files/blobs at the destination
	// that do not exist at the source. With this flag turned on, user will not be prompted for permission before
	// deleting the files/blobs.
	force bool
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
	glcm.Info("\nJob " + cca.jobID.String() + " has started\n")
	glcm.Info(fmt.Sprintf("Log file is located at: %s/%s.log", azcopyLogPathFolder, cca.jobID))

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
	// prompt for confirmation, except when:
	// 1. output is in json format
	// 2. enumeration is complete
	if cca.output == common.EOutputFormat.Text() && !cca.isEnumerationComplete {
		answer := lcm.Prompt("The source enumeration is not complete, cancelling the job at this point means it cannot be resumed. Please confirm with y/n: ")

		// read a line from stdin, if the answer is not yes, then abort cancel by returning
		if !strings.EqualFold(answer, "y") {
			return
		}
	}

	err := cookedCancelCmdArgs{jobID: cca.jobID}.process()
	if err != nil {
		lcm.Exit("error occurred while cancelling the job "+cca.jobID.String()+". Failed with error "+err.Error(), common.EExitCode.Error())
	}
}

func (cca *cookedSyncCmdArgs) ReportProgressOrExit(lcm common.LifecycleMgr) {
	var summary common.ListSyncJobSummaryResponse
	var throughput float64
	var jobDone bool

	// fetch a job status and compute throughput if the first part was dispatched
	if cca.firstPartOrdered() {
		Rpc(common.ERpcCmd.ListSyncJobSummary(), &cca.jobID, &summary)
		jobDone = summary.JobStatus == common.EJobStatus.Completed() || summary.JobStatus == common.EJobStatus.Cancelled()

		// compute the average throughput for the last time interval
		bytesInMb := float64(float64(summary.BytesOverWire-cca.intervalBytesTransferred) * 8 / float64(1024*1024))
		timeElapsed := time.Since(cca.intervalStartTime).Seconds()
		throughput = common.Iffloat64(timeElapsed != 0, bytesInMb/timeElapsed, 0)

		// reset the interval timer and byte count
		cca.intervalStartTime = time.Now()
		cca.intervalBytesTransferred = summary.BytesOverWire
	}

	// first part not dispatched, and we are still scanning
	// so a special message is outputted to notice the user that we are not stalling
	if !jobDone && !cca.scanningComplete() {
		// skip the interactive message if we were triggered by another tool
		if cca.output == common.EOutputFormat.Json() {
			return
		}

		var throughputString string
		if cca.firstPartOrdered() {
			throughputString = fmt.Sprintf(", 2-sec Throughput (Mb/s): %v", ste.ToFixed(throughput, 4))
		}

		lcm.Progress(fmt.Sprintf("%v File Scanned at Source, %v Files Scanned at Destination%s",
			atomic.LoadUint64(&cca.atomicSourceFilesScanned), atomic.LoadUint64(&cca.atomicDestinationFilesScanned), throughputString))
		return
	}

	// if json output is desired, simply marshal and return
	// note that if job is already done, we simply exit
	if cca.output == common.EOutputFormat.Json() {
		//jsonOutput, err := json.MarshalIndent(summary, "", "  ")
		jsonOutput, err := json.Marshal(summary)
		common.PanicIfErr(err)

		if jobDone {
			exitCode := common.EExitCode.Success()
			if summary.CopyTransfersFailed+summary.DeleteTransfersFailed > 0 {
				exitCode = common.EExitCode.Error()
			}
			lcm.Exit(string(jsonOutput), exitCode)
		} else {
			lcm.Info(string(jsonOutput))
			return
		}
	}

	// if json is not desired, and job is done, then we generate a special end message to conclude the job
	if jobDone {
		duration := time.Now().Sub(cca.jobStartTime) // report the total run time of the job
		exitCode := common.EExitCode.Success()
		if summary.CopyTransfersFailed+summary.DeleteTransfersFailed > 0 {
			exitCode = common.EExitCode.Error()
		}
		lcm.Exit(fmt.Sprintf(
			"\n\nJob %s summary\nElapsed Time (Minutes): %v\nTotal Number Of Copy Transfers: %v\nTotal Number Of Delete Transfers: %v\nNumber of Copy Transfers Completed: %v\nNumber of Copy Transfers Failed: %v\nNumber of Delete Transfers Completed: %v\nNumber of Delete Transfers Failed: %v\nFinal Job Status: %v\n",
			summary.JobID.String(),
			ste.ToFixed(duration.Minutes(), 4),
			summary.CopyTotalTransfers,
			summary.DeleteTotalTransfers,
			summary.CopyTransfersCompleted,
			summary.CopyTransfersFailed,
			summary.DeleteTransfersCompleted,
			summary.DeleteTransfersFailed,
			summary.JobStatus), exitCode)
	}

	lcm.Progress(fmt.Sprintf("%v Done, %v Failed, %v Pending, %v Total, 2-sec Throughput (Mb/s): %v",
		summary.CopyTransfersCompleted+summary.DeleteTransfersCompleted,
		summary.CopyTransfersFailed+summary.DeleteTransfersFailed,
		summary.CopyTotalTransfers+summary.DeleteTotalTransfers-(summary.CopyTransfersCompleted+summary.DeleteTransfersCompleted+summary.CopyTransfersFailed+summary.DeleteTransfersFailed),
		summary.CopyTotalTransfers+summary.DeleteTotalTransfers, ste.ToFixed(throughput, 4)))
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
				glcm.Exit("error parsing the input given by the user. Failed with error "+err.Error(), common.EExitCode.Error())
			}
			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				glcm.Exit("Cannot perform sync due to error: "+err.Error(), common.EExitCode.Error())
			}

			glcm.SurrenderControl()
		},
	}

	rootCmd.AddCommand(syncCmd)
	syncCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", true, "true by default, look into sub-directories recursively when syncing between directories.")
	syncCmd.PersistentFlags().Uint32Var(&raw.blockSize, "block-size", 0, "use this block(chunk) size when uploading/downloading to/from Azure Storage.")
	syncCmd.PersistentFlags().StringVar(&raw.include, "include", "", "only include files whose name matches the pattern list. Example: *.jpg;*.pdf;exactName")
	syncCmd.PersistentFlags().StringVar(&raw.exclude, "exclude", "", "exclude files whose name matches the pattern list. Example: *.jpg;*.pdf;exactName")
	syncCmd.PersistentFlags().StringVar(&raw.output, "output", "text", "format of the command's output, the choices include: text, json")
	syncCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "WARNING", "define the log verbosity for the log file, available levels: INFO(all requests/responses), WARNING(slow responses), and ERROR(only failed requests).")
	syncCmd.PersistentFlags().BoolVar(&raw.force, "force", false, "defines user's decision to delete extra files at the destination that are not present at the source. "+
		"If false, user will be prompted with a question while scheduling files/blobs for deletion.")
	syncCmd.PersistentFlags().StringVar(&raw.md5ValidationOption, "md5-validation", common.DefaultHashValidationOption.String(), "specifies how strictly MD5 hashes should be validated when downloading. Only available when downloading.")
	// TODO: should the previous line list the allowable values?

	// TODO follow sym link is not implemented, clarify behavior first
	//syncCmd.PersistentFlags().BoolVar(&raw.followSymlinks, "follow-symlinks", false, "follow symbolic links when performing sync from local file system.")

	// TODO sync does not support any BlobAttributes, this functionality should be added
}
