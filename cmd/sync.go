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
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"sync"
	"time"
)

type syncCommandArguments struct {
	src       string
	dst       string
	recursive bool
	// options from flags
	blockSize    uint32
	logVerbosity string
	outputJson	bool
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
}

func (cca cookedSyncCmdArgs) process() (err error) {
	// initialize the fields that are constant across all job part orders
	jobPartOrder := common.SyncJobPartOrderRequest{
		JobID:            common.NewJobID(),
		FromTo:           cca.fromTo,
		LogLevel:         cca.logVerbosity,
		BlockSizeInBytes: cca.blockSize,
		CommandString:cca.commandString,
	}
	// wait group to monitor the go routines fetching the job progress summary
	var wg sync.WaitGroup
	switch cca.fromTo {
	case common.EFromTo.LocalBlob():
		e := syncUploadEnumerator(jobPartOrder)
		err = e.enumerate(cca.src, cca.recursive, cca.dst, &wg, cca.waitUntilJobCompletion)
	case common.EFromTo.BlobLocal():
		e := syncDownloadEnumerator(jobPartOrder)
		err = e.enumerate(cca.src, cca.recursive, cca.dst, &wg, cca.waitUntilJobCompletion)
	default:
		return fmt.Errorf("from to destination not supported")
	}
	if err != nil {
		return fmt.Errorf("error starting the sync between source %s and destination %s. Failed with error %s", cca.src, cca.dst, err.Error())
	}
	wg.Wait()
	return nil
}

func (cca cookedSyncCmdArgs) waitUntilJobCompletion(jobID common.JobID, wg *sync.WaitGroup) {

	// CancelChannel will be notified when os receives os.Interrupt and os.Kill signals
	// waiting for signals from either CancelChannel or timeOut Channel.
	// if no signal received, will fetch/display a job status update then sleep for a bit
	signal.Notify(CancelChannel, os.Interrupt, os.Kill)

	if !cca.outputJson {
		// added empty line to provide gap after the user given
		fmt.Println("")
		fmt.Println(fmt.Sprintf("Job %s has started ", jobID.String()))
		// added empty line to provide gap between the above line and the Summary
		fmt.Println("")
	}
	// throughputIntervalTime holds the last time value when the progress summary was fetched
	// The value of this variable is used to calculate the throughput
	// It gets updated every time the progress summary is fetched
	throughputIntervalTime := time.Now()
	// jobStartTime holds the time when Job was started
	// The value of this variable is used to calculate the elapsed time
	jobStartTime := throughputIntervalTime
	bytesTransferredInLastInterval := uint64(0)
	for {
		select {
		case <-CancelChannel:
			fmt.Println("Cancelling Job")
			cookedCancelCmdArgs{jobID: jobID}.process()
			os.Exit(1)
		default:
			summary := copyHandlerUtil{}.fetchJobStatus(jobID, &throughputIntervalTime, &bytesTransferredInLastInterval, cca.outputJson)

			// happy ending to the front end
			if summary.JobStatus == common.EJobStatus.Completed() ||
				summary.JobStatus == common.EJobStatus.Cancelled(){
					// print final JobSummary if output-json flag is set to false
					if !cca.outputJson {
						copyHandlerUtil{}.PrintFinalJobProgressSummary(summary, time.Now().Sub(jobStartTime))
					}
				os.Exit(0)
			}

			// wait a bit before fetching job status again, as fetching has costs associated with it on the backend
			//time.Sleep(2 * time.Second)
			time.Sleep(2 * time.Second)
		}
	}
	wg.Done()
}

func init() {
	raw := syncCommandArguments{}
	// syncCmd represents the sync command
	var syncCmd = &cobra.Command{
		Use:     "sync",
		Aliases: []string{"sc", "s"},
		Short:   "Coming soon: sync replicates source to the destination location.",
		Long:    `Coming soon: sync replicates source to the destination location.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("2 arguments source and destination are required for this command. Number of commands passed %d", len(args))
			}
			raw.src = args[0]
			raw.dst = args[1]
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cooked, err := raw.cook()
			if err != nil {
				fmt.Println("error parsing the input given by the user. Failed with error ", err.Error())
				os.Exit(1)
			}
			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				fmt.Println("error performing the sync between source and destination. Failed with error ", err.Error())
				os.Exit(1)
			}
			return nil
		},
		// hide features not relevant to BFS
		// TODO remove after preview release
		Hidden: true,
	}

	rootCmd.AddCommand(syncCmd)
	syncCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", false, "Filter: Look into sub-directories recursively when syncing destination to source.")
	syncCmd.PersistentFlags().Uint32Var(&raw.blockSize, "block-size", 8*1024*1024, "Use this block size when source to Azure Storage or from Azure Storage.")
	syncCmd.PersistentFlags().BoolVar(&raw.outputJson, "output-json", false, "true if user wants the output in Json format")
	syncCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "WARNING", "defines the log verbosity to be saved to log file")
}
