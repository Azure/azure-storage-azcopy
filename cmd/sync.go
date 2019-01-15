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
	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/spf13/cobra"
)

const MaxNumberOfFilesAllowedInSync = 10000000

type rawSyncCmdArgs struct {
	src       string
	dst       string
	recursive bool
	// options from flags
	blockSize      uint32
	logVerbosity   string
	include        string
	exclude        string
	followSymlinks bool
	output         string
	// this flag predefines the user-agreement to delete the files in case sync found some files at destination
	// which doesn't exists at source. With this flag turned on, user will not be asked for permission before
	// deleting the flag.
	force bool
}

// validates and transform raw input into cooked input
func (raw rawSyncCmdArgs) cook() (cookedSyncCmdArgs, error) {
	cooked := cookedSyncCmdArgs{}

	fromTo := inferFromTo(raw.src, raw.dst)
	if fromTo == common.EFromTo.Unknown() {
		return cooked, fmt.Errorf("Unable to infer the source '%s' / destination '%s'. ", raw.src, raw.dst)
	}
	if fromTo != common.EFromTo.LocalBlob() &&
		fromTo != common.EFromTo.BlobLocal() {
		return cooked, fmt.Errorf("source '%s' / destination '%s' combination '%s' not supported for sync command ", raw.src, raw.dst, fromTo)
	}
	cooked.source = raw.src
	cooked.destination = raw.dst

	cooked.fromTo = fromTo

	cooked.blockSize = raw.blockSize

	cooked.followSymlinks = raw.followSymlinks

	err := cooked.logVerbosity.Parse(raw.logVerbosity)
	if err != nil {
		return cooked, err
	}

	// initialize the include map which contains the list of files to be included
	// parse the string passed in include flag
	// more than one file are expected to be separated by ';'
	cooked.include = make(map[string]int)
	if len(raw.include) > 0 {
		files := strings.Split(raw.include, ";")
		for index := range files {
			// If split of the include string leads to an empty string
			// not include that string
			if len(files[index]) == 0 {
				continue
			}
			cooked.include[files[index]] = index
		}
	}

	// initialize the exclude map which contains the list of files to be excluded
	// parse the string passed in exclude flag
	// more than one file are expected to be separated by ';'
	cooked.exclude = make(map[string]int)
	if len(raw.exclude) > 0 {
		files := strings.Split(raw.exclude, ";")
		for index := range files {
			// If split of the include string leads to an empty string
			// not include that string
			if len(files[index]) == 0 {
				continue
			}
			cooked.exclude[files[index]] = index
		}
	}

	cooked.recursive = raw.recursive
	cooked.output.Parse(raw.output)
	cooked.jobID = common.NewJobID()
	cooked.force = raw.force
	return cooked, nil
}

type cookedSyncCmdArgs struct {
	source         string
	sourceSAS      string
	destination    string
	destinationSAS string
	fromTo         common.FromTo
	recursive      bool
	followSymlinks bool
	// options from flags
	include      map[string]int
	exclude      map[string]int
	blockSize    uint32
	logVerbosity common.LogLevel
	output       common.OutputFormat
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
	// this flag predefines the user-agreement to delete the files in case sync found some files at destination
	// which doesn't exists at source. With this flag turned on, user will not be asked for permission before
	// deleting the flag.
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
	if !(cca.output == common.EOutputFormat.Json() || cca.isEnumerationComplete) {
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

	if !cca.scanningComplete() {
		lcm.Progress(fmt.Sprintf("%v File Scanned at Source, %v Files Scanned at Destination",
			atomic.LoadUint64(&cca.atomicSourceFilesScanned), atomic.LoadUint64(&cca.atomicDestinationFilesScanned)))
		return
	}
	// If the first part isn't ordered yet, no need to fetch the progress summary.
	if !cca.firstPartOrdered() {
		return
	}
	// fetch a job status
	var summary common.ListSyncJobSummaryResponse
	Rpc(common.ERpcCmd.ListSyncJobSummary(), &cca.jobID, &summary)
	jobDone := summary.JobStatus == common.EJobStatus.Completed() || summary.JobStatus == common.EJobStatus.Cancelled()

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

	// if json is not needed, and job is not done, then we generate a message that goes nicely on the same line
	// display a scanning keyword if the job is not completely ordered
	var scanningString = ""
	if !summary.CompleteJobOrdered {
		scanningString = "(scanning...)"
	}

	// compute the average throughput for the last time interval
	bytesInMb := float64(float64(summary.BytesOverWire-cca.intervalBytesTransferred) * 8 / float64(1024*1024))
	timeElapsed := time.Since(cca.intervalStartTime).Seconds()
	throughPut := common.Iffloat64(timeElapsed != 0, bytesInMb/timeElapsed, 0)

	// reset the interval timer and byte count
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = summary.BytesOverWire

	lcm.Progress(fmt.Sprintf("%v Done, %v Failed, %v Pending, %v Total%s, 2-sec Throughput (Mb/s): %v",
		summary.CopyTransfersCompleted+summary.DeleteTransfersCompleted,
		summary.CopyTransfersFailed+summary.DeleteTransfersFailed,
		summary.CopyTotalTransfers+summary.DeleteTotalTransfers-(summary.CopyTransfersCompleted+summary.DeleteTransfersCompleted+summary.CopyTransfersFailed+summary.DeleteTransfersFailed),
		summary.CopyTotalTransfers+summary.DeleteTotalTransfers, scanningString, ste.ToFixed(throughPut, 4)))
}

func (cca *cookedSyncCmdArgs) process() (err error) {
	// initialize the fields that are constant across all job part orders
	jobPartOrder := common.SyncJobPartOrderRequest{
		JobID:            cca.jobID,
		FromTo:           cca.fromTo,
		LogLevel:         cca.logVerbosity,
		BlockSizeInBytes: cca.blockSize,
		Include:          cca.include,
		Exclude:          cca.exclude,
		CommandString:    cca.commandString,
		SourceSAS:        cca.sourceSAS,
		DestinationSAS:   cca.destinationSAS,
		CredentialInfo:   common.CredentialInfo{},
	}

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	// verifies credential type and initializes credential info.
	// For sync, only one side need credential.
	if jobPartOrder.CredentialInfo.CredentialType, err = getCredentialType(ctx, rawFromToInfo{
		fromTo:         cca.fromTo,
		source:         cca.source,
		destination:    cca.destination,
		sourceSAS:      cca.sourceSAS,
		destinationSAS: cca.destinationSAS,
	}); err != nil {
		return err
	}

	// For OAuthToken credential, assign OAuthTokenInfo to CopyJobPartOrderRequest properly,
	// the info will be transferred to STE.
	if jobPartOrder.CredentialInfo.CredentialType == common.ECredentialType.OAuthToken() {
		// Message user that they are using Oauth token for authentication,
		// in case of silently using cached token without consciousness。
		glcm.Info("Using OAuth token for authentication.")

		uotm := GetUserOAuthTokenManagerInstance()
		// Get token from env var or cache.
		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return err
		} else {
			jobPartOrder.CredentialInfo.OAuthTokenInfo = *tokenInfo
		}
	}

	from := cca.fromTo.From()
	to := cca.fromTo.To()
	switch from {
	case common.ELocation.Blob():
		fromUrl, err := url.Parse(cca.source)
		if err != nil {
			return fmt.Errorf("error parsing the source url %s. Failed with error %s", fromUrl.String(), err.Error())
		}
		blobParts := azblob.NewBlobURLParts(*fromUrl)
		cca.sourceSAS = blobParts.SAS.Encode()
		jobPartOrder.SourceSAS = cca.sourceSAS
		blobParts.SAS = azblob.SASQueryParameters{}
		bUrl := blobParts.URL()
		cca.source = bUrl.String()
	case common.ELocation.File():
		fromUrl, err := url.Parse(cca.source)
		if err != nil {
			return fmt.Errorf("error parsing the source url %s. Failed with error %s", fromUrl.String(), err.Error())
		}
		fileParts := azfile.NewFileURLParts(*fromUrl)
		cca.sourceSAS = fileParts.SAS.Encode()
		jobPartOrder.SourceSAS = cca.sourceSAS
		fileParts.SAS = azfile.SASQueryParameters{}
		fUrl := fileParts.URL()
		cca.source = fUrl.String()
	}

	switch to {
	case common.ELocation.Blob():
		toUrl, err := url.Parse(cca.destination)
		if err != nil {
			return fmt.Errorf("error parsing the source url %s. Failed with error %s", toUrl.String(), err.Error())
		}
		blobParts := azblob.NewBlobURLParts(*toUrl)
		cca.destinationSAS = blobParts.SAS.Encode()
		jobPartOrder.DestinationSAS = cca.destinationSAS
		blobParts.SAS = azblob.SASQueryParameters{}
		bUrl := blobParts.URL()
		cca.destination = bUrl.String()
	case common.ELocation.File():
		toUrl, err := url.Parse(cca.destination)
		if err != nil {
			return fmt.Errorf("error parsing the source url %s. Failed with error %s", toUrl.String(), err.Error())
		}
		fileParts := azfile.NewFileURLParts(*toUrl)
		cca.destinationSAS = fileParts.SAS.Encode()
		jobPartOrder.DestinationSAS = cca.destinationSAS
		fileParts.SAS = azfile.SASQueryParameters{}
		fUrl := fileParts.URL()
		cca.destination = fUrl.String()
	}

	if from == common.ELocation.Local() {
		// If the path separator is '\\', it means
		// local path is a windows path
		// To avoid path separator check and handling the windows
		// path differently, replace the path separator with the
		// the linux path separator '/'
		if os.PathSeparator == '\\' {
			cca.source = strings.Replace(cca.source, common.OS_PATH_SEPARATOR, "/", -1)
		}
	}

	if to == common.ELocation.Local() {
		// If the path separator is '\\', it means
		// local path is a windows path
		// To avoid path separator check and handling the windows
		// path differently, replace the path separator with the
		// the linux path separator '/'
		if os.PathSeparator == '\\' {
			cca.destination = strings.Replace(cca.destination, common.OS_PATH_SEPARATOR, "/", -1)
		}
	}

	switch cca.fromTo {
	case common.EFromTo.LocalBlob():
		e := syncUploadEnumerator(jobPartOrder)
		err = e.enumerate(cca)
	case common.EFromTo.BlobLocal():
		e := syncDownloadEnumerator(jobPartOrder)
		err = e.enumerate(cca)
	default:
		return fmt.Errorf("from to destination not supported")
	}
	if err != nil {
		return fmt.Errorf("error starting the sync between source %s and destination %s. Failed with error %s", cca.source, cca.destination, err.Error())
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
				glcm.Exit("error performing the sync between source and destination. Failed with error "+err.Error(), common.EExitCode.Error())
			}

			glcm.SurrenderControl()
		},
	}

	rootCmd.AddCommand(syncCmd)
	syncCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", false, "Filter: Look into sub-directories recursively when syncing destination to source.")
	syncCmd.PersistentFlags().Uint32Var(&raw.blockSize, "block-size", 0, "Use this block size when source to Azure Storage or from Azure Storage.")
	// hidden filters
	syncCmd.PersistentFlags().StringVar(&raw.include, "include", "", "Filter: only include these files when copying. "+
		"Support use of *. More than one file are separated by ';'")
	syncCmd.PersistentFlags().BoolVar(&raw.followSymlinks, "follow-symlinks", false, "Filter: Follow symbolic links when performing sync from local file system.")
	syncCmd.PersistentFlags().StringVar(&raw.exclude, "exclude", "", "Filter: Exclude these files when copying. Support use of *.")
	syncCmd.PersistentFlags().StringVar(&raw.output, "output", "text", "format of the command's output, the choices include: text, json")
	syncCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "WARNING", "define the log verbosity for the log file, available levels: INFO(all requests/responses), WARNING(slow responses), and ERROR(only failed requests).")
	syncCmd.PersistentFlags().BoolVar(&raw.force, "force", false, "defines user's decision to delete extra files at the destination that are not present at the source. "+
		"If false, user will be prompted with a question while scheduling files/blobs for deletion.")

	// TODO sync does not support any BlobAttributes, this functionality should be added
}
