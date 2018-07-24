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

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/spf13/cobra"
)

type syncCommandArguments struct {
	src       string
	dst       string
	recursive bool
	// options from flags
	blockSize    uint32
	logVerbosity string
	outputJson   bool
	// commandString hold the user given command which is logged to the Job log file
	commandString string
}

// validates and transform raw input into cooked input
func (raw syncCommandArguments) cook() (cookedSyncCmdArgs, error) {
	cooked := cookedSyncCmdArgs{}

	fromTo := inferFromTo(raw.src, raw.dst)
	if fromTo != common.EFromTo.LocalBlob() &&
		fromTo != common.EFromTo.BlobLocal() {
		return cooked, fmt.Errorf("invalid type of source and destination passed for this passed")
	}
	cooked.src = raw.src
	cooked.dst = raw.dst

	cooked.fromTo = fromTo

	cooked.blockSize = raw.blockSize

	err := cooked.logVerbosity.Parse(raw.logVerbosity)
	if err != nil {
		return cooked, err
	}

	cooked.recursive = raw.recursive
	cooked.outputJson = raw.outputJson
	cooked.jobID = common.NewJobID()
	return cooked, nil
}

type cookedSyncCmdArgs struct {
	src       string
	dst       string
	fromTo    common.FromTo
	recursive bool
	// options from flags
	blockSize    uint32
	logVerbosity common.LogLevel
	outputJson   bool
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
}

func (cca *cookedSyncCmdArgs) PrintJobStartedMsg() {
	glcm.Info("\nJob " + cca.jobID.String() + " has started\n")
}

func (cca *cookedSyncCmdArgs) CancelJob() {
	err := cookedCancelCmdArgs{jobID: cca.jobID}.process()
	if err != nil {
		glcm.ExitWithError("error occurred while cancelling the job "+cca.jobID.String()+". Failed with error "+err.Error(), common.EExitCode.Error())
	}
}

func (cca *cookedSyncCmdArgs) InitializeProgressCounters() {
	cca.jobStartTime = time.Now()
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = 0
}

func (cca *cookedSyncCmdArgs) PrintJobProgressStatus() {
	// fetch a job status
	var summary common.ListJobSummaryResponse
	Rpc(common.ERpcCmd.ListJobSummary(), &cca.jobID, &summary)
	jobDone := summary.JobStatus == common.EJobStatus.Completed() || summary.JobStatus == common.EJobStatus.Cancelled()

	// if json output is desired, simply marshal and return
	// note that if job is already done, we simply exit
	if cca.outputJson {
		jsonOutput, err := json.MarshalIndent(summary, "", "  ")
		if err != nil {
			// something serious has gone wrong if we cannot marshal a json
			panic(err)
		}

		if jobDone {
			glcm.ExitWithSuccess(string(jsonOutput), common.EExitCode.Success())
		} else {
			glcm.Info(string(jsonOutput))
			return
		}
	}

	// if json is not desired, and job is done, then we generate a special end message to conclude the job
	if jobDone {
		duration := time.Now().Sub(cca.jobStartTime) // report the total run time of the job

		glcm.ExitWithSuccess(fmt.Sprintf(
			"\n\nJob %s summary\nElapsed Time (Minutes): %v\nTotal Number Of Transfers: %v\nNumber of Transfers Completed: %v\nNumber of Transfers Failed: %v\nFinal Job Status: %v\n",
			summary.JobID.String(),
			ste.ToFixed(duration.Minutes(), 4),
			summary.TotalTransfers,
			summary.TransfersCompleted,
			summary.TransfersFailed,
			summary.JobStatus), common.EExitCode.Success())
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
	throughPut := common.Iffloat64(timeElapsed != 0, bytesInMB/timeElapsed, 0)

	// reset the interval timer and byte count
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = summary.BytesOverWire

	glcm.Progress(fmt.Sprintf("%v Done, %v Failed, %v Pending, %v Total%s, 2-sec Throughput (MB/s): %v",
		summary.TransfersCompleted,
		summary.TransfersFailed,
		summary.TotalTransfers-(summary.TransfersCompleted+summary.TransfersFailed),
		summary.TotalTransfers, scanningString, ste.ToFixed(throughPut, 4)))
}

func (cca *cookedSyncCmdArgs) process() (err error) {
	// initialize the fields that are constant across all job part orders
	jobPartOrder := common.SyncJobPartOrderRequest{
		JobID:            cca.jobID,
		FromTo:           cca.fromTo,
		LogLevel:         cca.logVerbosity,
		BlockSizeInBytes: cca.blockSize,
		CommandString:    cca.commandString,
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
		return fmt.Errorf("error starting the sync between source %s and destination %s. Failed with error %s", cca.src, cca.dst, err.Error())
	}
	return nil
}

func init() {
	raw := syncCommandArguments{}
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
				glcm.ExitWithError("error parsing the input given by the user. Failed with error "+err.Error(), common.EExitCode.Error())
			}
			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				glcm.ExitWithError("error performing the sync between source and destination. Failed with error "+err.Error(), common.EExitCode.Error())
			}

			glcm.SurrenderControl()
		},
	}

	rootCmd.AddCommand(syncCmd)
	syncCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", false, "Filter: Look into sub-directories recursively when syncing destination to source.")
	syncCmd.PersistentFlags().Uint32Var(&raw.blockSize, "block-size", 8*1024*1024, "Use this block size when source to Azure Storage or from Azure Storage.")
	syncCmd.PersistentFlags().BoolVar(&raw.outputJson, "output-json", false, "true if user wants the output in Json format")
	syncCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "WARNING", "defines the log verbosity to be saved to log file")
}
