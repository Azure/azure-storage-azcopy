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
	"encoding/json"
	"fmt"
	"time"

	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
	"github.com/spf13/cobra"
)

type rawSyncCmdArgs struct {
	src       string
	dst       string
	recursive bool
	// options from flags
	blockSize    uint32
	logVerbosity string
	include      string
	exclude      string
	output       string
}

// validates and transform raw input into cooked input
func (raw rawSyncCmdArgs) cook() (cookedSyncCmdArgs, error) {
	cooked := cookedSyncCmdArgs{}

	fromTo := inferFromTo(raw.src, raw.dst)
	if fromTo != common.EFromTo.LocalBlob() &&
		fromTo != common.EFromTo.BlobLocal() {
		return cooked, fmt.Errorf("invalid type of source and destination passed for this passed")
	}
	cooked.source = raw.src
	cooked.destination = raw.dst

	cooked.fromTo = fromTo

	cooked.blockSize = raw.blockSize

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
	return cooked, nil
}

type cookedSyncCmdArgs struct {
	source         string
	sourceSAS      string
	destination    string
	destinationSAS string
	fromTo         common.FromTo
	recursive      bool

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
}

// wraps call to lifecycle manager to wait for the job to complete
// if blocking is specified to true, then this method will never return
// if blocking is specified to false, then another goroutine spawns and wait out the job
func (cca *cookedSyncCmdArgs) waitUntilJobCompletion(blocking bool) {
	// print initial message to indicate that the job is starting
	glcm.Info("\nJob " + cca.jobID.String() + " has started\n")
	currentDir, _ := os.Getwd()
	glcm.Info(fmt.Sprintf("%s.log file created in %s", cca.jobID, currentDir))

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
	// fetch a job status
	var summary common.ListJobSummaryResponse
	Rpc(common.ERpcCmd.ListJobSummary(), &cca.jobID, &summary)
	jobDone := summary.JobStatus == common.EJobStatus.Completed() || summary.JobStatus == common.EJobStatus.Cancelled()

	// if json output is desired, simply marshal and return
	// note that if job is already done, we simply exit
	if cca.output == common.EOutputFormat.Json() {
		jsonOutput, err := json.MarshalIndent(summary, "", "  ")
		common.PanicIfErr(err)

		if jobDone {
			exitCode := common.EExitCode.Success()
			if summary.TransfersFailed > 0 {
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
		if summary.TransfersFailed > 0 {
			exitCode = common.EExitCode.Error()
		}
		lcm.Exit(fmt.Sprintf(
			"\n\nJob %s summary\nElapsed Time (Minutes): %v\nTotal Number Of Transfers: %v\nNumber of Transfers Completed: %v\nNumber of Transfers Failed: %v\nNumber of Transfers Skipped: %v\nFinal Job Status: %v\nTotalBytesTransferred: %v\n",
			summary.JobID.String(),
			ste.ToFixed(duration.Minutes(), 4),
			summary.TotalTransfers,
			summary.TransfersCompleted,
			summary.TransfersFailed,
			summary.TransfersSkipped,
			summary.JobStatus, summary.TotalBytesTransferred), exitCode)
	}

	// if json is not needed, and job is not done, then we generate a message that goes nicely on the same line
	// display a scanning keyword if the job is not completely ordered
	var scanningString = ""
	if !summary.CompleteJobOrdered {
		scanningString = "(scanning...)"
	}

	// compute the average throughput for the last time interval
	bytesInMB := float64(float64(summary.BytesOverWire-cca.intervalBytesTransferred) / float64(1024*1024))
	timeElapsed := time.Since(cca.intervalStartTime).Seconds()
	throughPut := common.Iffloat64(timeElapsed != 0, bytesInMB/timeElapsed, 0) * 8

	// reset the interval timer and byte count
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = summary.BytesOverWire

	lcm.Progress(fmt.Sprintf("%v Done, %v Failed, %v Pending, %v Skipped, %v Total%s, 2-sec Throughput (Mb/s): %v",
		summary.TransfersCompleted,
		summary.TransfersFailed,
		summary.TotalTransfers-(summary.TransfersCompleted+summary.TransfersFailed),
		summary.TransfersSkipped,
		summary.TotalTransfers, scanningString, ste.ToFixed(throughPut, 4)))
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
		Short:   "sync replicates source to the destination location. Last modified time is used for comparison",
		Long:    `sync replicates source to the destination location. Last modified time the used for comparison`,
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
	syncCmd.PersistentFlags().StringVar(&raw.exclude, "exclude", "", "Filter: Exclude these files when copying. Support use of *.")
	syncCmd.PersistentFlags().StringVar(&raw.output, "output", "text", "format of the command's output, the choices include: text, json")
	syncCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "WARNING", "defines the log verbosity to be saved to log file")
}
