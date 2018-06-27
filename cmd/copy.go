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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"github.com/spf13/cobra"
)

// upload related
const uploadMaxTries = 5
const uploadTryTimeout = time.Minute * 10
const uploadRetryDelay = time.Second * 1
const uploadMaxRetryDelay = time.Second * 3

// download related
const downloadMaxTries = 5
const downloadTryTimeout = time.Minute * 10
const downloadRetryDelay = time.Second * 1
const downloadMaxRetryDelay = time.Second * 3

const numOfSimultaneousUploads = 5

const pipeLocation = "~pipe~"

// represents the raw copy command input from the user
type rawCopyCmdArgs struct {
	// from arguments
	src    string
	dst    string
	fromTo string
	//blobUrlForRedirection string

	// filters from flags
	include        string
	exclude        string
	recursive      bool
	followSymlinks bool
	withSnapshots  bool
	// forceWrite flag is used to define the User behavior
	// to overwrite the existing blobs or not.
	forceWrite bool

	// options from flags
	blockSize                uint32
	metadata                 string
	contentType              string
	contentEncoding          string
	noGuessMimeType          bool
	preserveLastModifiedTime bool
	blockBlobTier            string
	pageBlobTier             string
	background               bool
	outputJson               bool
	acl                      string
	logVerbosity             string
	stdInEnable              bool
	// oauth options
	useInteractiveOAuthUserCredential bool
	tenantID                          string
	aadEndpoint                       string
}

// validates and transform raw input into cooked input
func (raw rawCopyCmdArgs) cook() (cookedCopyCmdArgs, error) {
	cooked := cookedCopyCmdArgs{}

	fromTo, err := validateFromTo(raw.src, raw.dst, raw.fromTo) // TODO: src/dst
	if err != nil {
		return cooked, err
	}
	cooked.src = raw.src
	cooked.dst = raw.dst

	cooked.fromTo = fromTo

	// copy&transform flags to type-safety
	cooked.recursive = raw.recursive
	cooked.followSymlinks = raw.followSymlinks
	cooked.withSnapshots = raw.withSnapshots
	cooked.forceWrite = raw.forceWrite
	cooked.blockSize = raw.blockSize

	err = cooked.blockBlobTier.Parse(raw.blockBlobTier)
	if err != nil {
		return cooked, err
	}
	err = cooked.pageBlobTier.Parse(raw.pageBlobTier)
	if err != nil {
		return cooked, err
	}
	err = cooked.logVerbosity.Parse(raw.logVerbosity)
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

	cooked.metadata = raw.metadata
	cooked.contentType = raw.contentType
	cooked.contentEncoding = raw.contentEncoding
	cooked.noGuessMimeType = raw.noGuessMimeType
	cooked.preserveLastModifiedTime = raw.preserveLastModifiedTime
	cooked.background = raw.background
	cooked.outputJson = raw.outputJson
	cooked.acl = raw.acl

	cooked.useInteractiveOAuthUserCredential = raw.useInteractiveOAuthUserCredential
	cooked.tenantID = raw.tenantID
	cooked.aadEndpoint = raw.aadEndpoint

	return cooked, nil
}

// represents the processed copy command input from the user
type cookedCopyCmdArgs struct {
	// from arguments
	src    string
	dst    string
	fromTo common.FromTo

	// filters from flags
	include        map[string]int
	exclude        map[string]int
	recursive      bool
	followSymlinks bool
	withSnapshots  bool
	forceWrite     bool

	// options from flags
	blockSize                uint32
	blockBlobTier            common.BlockBlobTier
	pageBlobTier             common.PageBlobTier
	metadata                 string
	contentType              string
	contentEncoding          string
	noGuessMimeType          bool
	preserveLastModifiedTime bool
	background               bool
	outputJson               bool
	acl                      string
	logVerbosity             common.LogLevel
	// oauth options
	useInteractiveOAuthUserCredential bool
	tenantID                          string
	aadEndpoint                       string
	// commandString hold the user given command which is logged to the Job log file
	commandString string
}

func (cca cookedCopyCmdArgs) isRedirection() bool {
	switch cca.fromTo {
	// File's piping is not supported temporarily.
	// case common.EFromTo.PipeFile():
	// 	fallthrough
	// case common.EFromTo.FilePipe():
	// 	fallthrough
	case common.EFromTo.BlobPipe():
		fallthrough
	case common.EFromTo.PipeBlob():
		return true
	default:
		return false
	}
}

func (cca cookedCopyCmdArgs) process() error {
	if cca.isRedirection() {
		return cca.processRedirectionCopy()
	}
	return cca.processCopyJobPartOrders()
}

// TODO discuss with Jeff what features should be supported by redirection, such as metadata, content-type, etc.
func (cca cookedCopyCmdArgs) processRedirectionCopy() error {
	if cca.fromTo == common.EFromTo.PipeBlob() {
		return cca.processRedirectionUpload(cca.dst, cca.blockSize)
	} else if cca.fromTo == common.EFromTo.BlobPipe() {
		return cca.processRedirectionDownload(cca.src)
	}

	return fmt.Errorf("unsupported redirection type: %s", cca.fromTo)
}

func (cca cookedCopyCmdArgs) processRedirectionDownload(blobUrl string) error {
	// step 0: check the Stdout before uploading
	_, err := os.Stdout.Stat()
	if err != nil {
		return fmt.Errorf("fatal: cannot write to Stdout due to error: %s", err.Error())
	}

	// step 1: initialize pipeline
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      downloadMaxTries,
			TryTimeout:    downloadTryTimeout,
			RetryDelay:    downloadRetryDelay,
			MaxRetryDelay: downloadMaxRetryDelay,
		},
		Telemetry: azblob.TelemetryOptions{
			Value: common.UserAgent,
		},
	})

	// step 2: parse source url
	u, err := url.Parse(blobUrl)
	if err != nil {
		return fmt.Errorf("fatal: cannot parse source blob URL due to error: %s", err.Error())
	}

	// step 3: start download
	blobURL := azblob.NewBlobURL(*u, p)
	blobStream, err := blobURL.Download(context.Background(), 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		// todo:???
	}

	blobBody := blobStream.Body(azblob.RetryReaderOptions{MaxRetryRequests: downloadMaxTries})
	defer blobBody.Close()

	// step 4: pipe everything into Stdout
	_, err = io.Copy(os.Stdout, blobBody)
	if err != nil {
		return fmt.Errorf("fatal: cannot download blob to Stdout due to error: %s", err.Error())
	}

	return nil
}

func (cca cookedCopyCmdArgs) processRedirectionUpload(blobUrl string, blockSize uint32) error {
	type uploadTask struct {
		buffer        []byte
		blockSize     int
		blockIdBase64 string
	}

	// step 0: initialize pipeline
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      uploadMaxTries,
			TryTimeout:    uploadTryTimeout,
			RetryDelay:    uploadRetryDelay,
			MaxRetryDelay: uploadMaxRetryDelay,
		},
		Telemetry: azblob.TelemetryOptions{
			Value: common.UserAgent,
		},
	})

	// step 1: parse destination url
	u, err := url.Parse(blobUrl)
	if err != nil {
		return fmt.Errorf("fatal: cannot parse destination blob URL due to error: %s", err.Error())
	}

	// step 2: set up source (stdin) and destination (block blob)
	stdInReader := bufio.NewReader(os.Stdin)
	blockBlobUrl := azblob.NewBlockBlobURL(*u, p)

	// step 3: set up channels which are used to sync up go routines for parallel upload
	uploadContext, cancelOperation := context.WithCancel(context.Background())
	fullChannel := make(chan uploadTask, numOfSimultaneousUploads)  // represent buffers filled up and waiting to be uploaded
	emptyChannel := make(chan uploadTask, numOfSimultaneousUploads) // represent buffers ready to be filled up
	errChannel := make(chan error, numOfSimultaneousUploads)        // in case error happens, workers need to communicate the err back
	finishedChunkCount := uint32(0)                                 // used to keep track of how many chunks are completed
	total := uint32(0)                                              // used to keep track of total number of chunks, it is incremented as more data is read in from stdin
	blockIdList := []string{}
	isReadComplete := false

	// step 4: prep empty buffers and dispatch upload workers
	for i := 0; i < numOfSimultaneousUploads; i++ {
		emptyChannel <- uploadTask{buffer: make([]byte, blockSize), blockSize: 0, blockIdBase64: ""}

		go func(fullChannel <-chan uploadTask, emptyChannel chan<- uploadTask, errorChannel chan<- error, workerId int) {
			// wait on fullChannel, if anything comes off, upload it as block
			for full := range fullChannel {
				resp, err := blockBlobUrl.StageBlock(
					uploadContext,
					full.blockIdBase64,
					io.NewSectionReader(bytes.NewReader(full.buffer), 0, int64(full.blockSize)),
					azblob.LeaseAccessConditions{})

				// error, push to error channel
				if err != nil {
					errChannel <- err
					return
				} else {
					resp.Response().Body.Close()

					// success, increment finishedChunkCount
					atomic.AddUint32(&finishedChunkCount, 1)

					// after upload, put the task back onto emptyChannel, so that the buffer can be reused
					emptyChannel <- full
				}
			}
		}(fullChannel, emptyChannel, errChannel, i)
	}

	// the main goroutine serves as the dispatcher
	// it reads in data from stdin and puts uploadTask on fullChannel
	for {
		select {
		case err := <-errChannel:
			cancelOperation()
			return err
		default:
			// if we have finished reading stdin, then wait until finishedChunkCount hits total
			if isReadComplete {
				if atomic.LoadUint32(&finishedChunkCount) == total {
					close(fullChannel)
					_, err := blockBlobUrl.CommitBlockList(uploadContext, blockIdList, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})

					if err != nil {
						return err
					}

					return nil
				} else {
					// sleep a bit and check again, in case an err appears on the errChannel
					time.Sleep(time.Millisecond)
				}

			} else {
				select {
				case empty := <-emptyChannel:

					// read in more data from the Stdin and put onto fullChannel
					n, err := io.ReadFull(stdInReader, empty.buffer)

					if err == nil || err == io.ErrUnexpectedEOF { // read in data successfully
						// prep buffer for workers
						empty.blockSize = n
						empty.blockIdBase64 = copyHandlerUtil{}.blockIDIntToBase64(int(total))
						// keep track of the block IDs in sequence
						blockIdList = append(blockIdList, empty.blockIdBase64)
						total += 1
						fullChannel <- empty

					} else if err == io.EOF { // reached the end of input
						isReadComplete = true

					} else { // unexpected error happened, print&return
						return err
					}
				default:
					time.Sleep(time.Millisecond)
				}
			}
		}
	}
}

// validateCredentialType validate if given credential type is supported with specific copy scenario
func (cca cookedCopyCmdArgs) validateCredentialType(credentialType common.CredentialType) error {
	// oAuthToken is only supported by Blob/BlobFS.
	if credentialType == common.ECredentialType.OAuthToken() &&
		!(cca.fromTo == common.EFromTo.LocalBlob() || cca.fromTo == common.EFromTo.BlobLocal() ||
			cca.fromTo == common.EFromTo.LocalBlobFS() || cca.fromTo == common.EFromTo.BlobFSLocal()) {
		return fmt.Errorf("OAuthToken is not supported for FromTo: %v", cca.fromTo)
	}

	return nil
}

// getCredentialType checks user provided commandline switches, and gets the proper credential type
// for current copy command.
func (cca cookedCopyCmdArgs) getCredentialType() (credentialType common.CredentialType, err error) {
	credentialType = common.ECredentialType.Anonymous()

	if cca.useInteractiveOAuthUserCredential { // User explicty specify to use interactive login per command-line
		credentialType = common.ECredentialType.OAuthToken()
	} else {
		// Could be using oauth session mode or non-oauth scenario which uses SAS authentication or public endpoint,
		// verify credential type with cached token info, src or dest blob resource URL.
		switch cca.fromTo {
		case common.EFromTo.LocalBlob():
			credentialType, err = getBlobCredentialType(context.Background(), cca.dst, false)
			if err != nil {
				return common.ECredentialType.Unknown(), err
			}
		case common.EFromTo.BlobLocal():
			credentialType, err = getBlobCredentialType(context.Background(), cca.src, true)
			if err != nil {
				return common.ECredentialType.Unknown(), err
			}
		case common.EFromTo.LocalBlobFS():
			fallthrough
		case common.EFromTo.BlobFSLocal():
			credentialType, err = getBlobFSCredentialType()
			if err != nil {
				return common.ECredentialType.Unknown(), err
			}

		}
	}

	if cca.validateCredentialType(credentialType) != err {
		return common.ECredentialType.Unknown(), err
	}

	return credentialType, nil
}

// handles the copy command
// dispatches the job order (in parts) to the storage engine
func (cca cookedCopyCmdArgs) processCopyJobPartOrders() (err error) {
	// initialize the fields that are constant across all job part orders
	jobPartOrder := common.CopyJobPartOrderRequest{
		JobID:      common.NewJobID(),
		FromTo:     cca.fromTo,
		ForceWrite: cca.forceWrite,
		Priority:   common.EJobPriority.Normal(),
		LogLevel:   cca.logVerbosity,
		Include:    cca.include,
		Exclude:    cca.exclude,
		BlobAttributes: common.BlobTransferAttributes{
			BlockSizeInBytes:         cca.blockSize,
			ContentType:              cca.contentType,
			ContentEncoding:          cca.contentEncoding,
			BlockBlobTier:            cca.blockBlobTier,
			PageBlobTier:             cca.pageBlobTier,
			Metadata:                 cca.metadata,
			NoGuessMimeType:          cca.noGuessMimeType,
			PreserveLastModifiedTime: cca.preserveLastModifiedTime,
		},
		CommandString: cca.commandString,
	}

	// print out the job id so that the user can note it down
	//if !cca.outputJson {
	//	fmt.Println("Job with id", jobPartOrder.JobID, "has started.")
	//}

	// wait group to monitor the go routines fetching the job progress summary
	var wg sync.WaitGroup
	// lastPartNumber determines the last part number order send for the Job.
	var lastPartNumber common.PartNumber

	// verify credential type and set credential info.
	jobPartOrder.CredentialInfo = common.CredentialInfo{}
	jobPartOrder.CredentialInfo.CredentialType, err = cca.getCredentialType()
	if err != nil {
		return err
	}
	fmt.Printf("Copy using credential type %v\n", jobPartOrder.CredentialInfo.CredentialType) // TODO: use FE logging facility
	// For OAuthToken credential, assign OAuthTokenInfo to CopyJobPartOrderRequest properly,
	// the info will be transferred to STE.
	if jobPartOrder.CredentialInfo.CredentialType == common.ECredentialType.OAuthToken() {
		uotm := GetUserOAuthTokenManagerInstance()

		var tokenInfo *common.OAuthTokenInfo
		if cca.useInteractiveOAuthUserCredential { // Scenario-1: interactive login per copy command
			tokenInfo, err = uotm.LoginWithADEndpoint(cca.tenantID, cca.aadEndpoint, false)
			if err != nil {
				return err
			}
		} else { // Scenario-2: session mode which get token from cache
			tokenInfo, err = uotm.GetCachedTokenInfo()
			if err != nil {
				return err
			}
		}
		jobPartOrder.CredentialInfo.OAuthTokenInfo = *tokenInfo
	}

	// depending on the source and destination type, we process the cp command differently
	// Create enumerator and do enumerating
	switch cca.fromTo {
	case common.EFromTo.LocalBlob():
		fallthrough
	case common.EFromTo.LocalBlobFS():
		fallthrough
	case common.EFromTo.LocalFile():
		e := copyUploadEnumerator(jobPartOrder)
		err = e.enumerate(cca.src, cca.recursive, cca.dst, &wg, cca.waitUntilJobCompletion)
		lastPartNumber = e.PartNum
	case common.EFromTo.BlobLocal():
		e := copyDownloadBlobEnumerator(jobPartOrder)
		err = e.enumerate(cca.src, cca.recursive, cca.dst, &wg, cca.waitUntilJobCompletion)
		lastPartNumber = e.PartNum
	case common.EFromTo.FileLocal():
		e := copyDownloadFileEnumerator(jobPartOrder)
		err = e.enumerate(cca.src, cca.recursive, cca.dst, &wg, cca.waitUntilJobCompletion)
		lastPartNumber = e.PartNum
	case common.EFromTo.BlobFSLocal():
		e := copyDownloadBlobFSEnumerator(jobPartOrder)
		err = e.enumerate(cca.src, cca.recursive, cca.dst, &wg, cca.waitUntilJobCompletion)
		lastPartNumber = e.PartNum
	case common.EFromTo.BlobTrash():
		e := removeBlobEnumerator(jobPartOrder)
		err = e.enumerate(cca.src, cca.recursive, cca.dst, &wg, cca.waitUntilJobCompletion)
		lastPartNumber = e.PartNum
	case common.EFromTo.FileTrash():
		e := removeFileEnumerator(jobPartOrder)
		err = e.enumerate(cca.src, cca.recursive, cca.dst, &wg, cca.waitUntilJobCompletion)
		lastPartNumber = e.PartNum
	}

	if err != nil {
		return fmt.Errorf("cannot start job due to error: %s.\n", err)
	}

	// in background mode we would spit out the job id and quit
	// in foreground mode we would continuously print out status updates for the job, so the job id is not important
	if cca.background {
		return nil
	}

	// If there is only one, part then start fetching the JobPart Order.
	if lastPartNumber == 0 {
		//if !cca.outputJson {
		//	fmt.Println("Job with id", jobPartOrder.JobID, "has started.")
		//}
		wg.Add(1)
		go cca.waitUntilJobCompletion(jobPartOrder.JobID, &wg)
	}
	wg.Wait()
	return nil
}

func (cca cookedCopyCmdArgs) waitUntilJobCompletion(jobID common.JobID, wg *sync.WaitGroup) {

	// CancelChannel will be notified when os receives os.Interrupt and os.Kill signals
	// waiting for signals from either CancelChannel or timeOut Channel.
	// if no signal received, will fetch/display a job status update then sleep for a bit
	signal.Notify(CancelChannel, os.Interrupt, os.Kill)

	// throughputIntervalTime holds the last time value when the progress summary was fetched
	// The value of this variable is used to calculate the throughput
	// It gets updated every time the progress summary is fetched
	throughputIntervalTime := time.Now()
	// jobStartTime holds the time when Job was started
	// The value of this variable is used to calculate the elapsed time
	jobStartTime := throughputIntervalTime
	if !cca.outputJson {
		// added empty line to provide gap after the user given
		fmt.Println("")
		fmt.Println(fmt.Sprintf("Job %s has started ", jobID.String()))
		// added empty line to provide gap between the above line and the Summary
		fmt.Println("")
	}
	bytesTransferredInLastInterval := uint64(0)
	for {
		select {
		case <-CancelChannel:
			err := cookedCancelCmdArgs{jobID: jobID}.process()
			if err != nil {
				fmt.Println(fmt.Sprintf("error occurred while cancelling the job %s. Failed with error %s", jobID, err.Error()))
				os.Exit(1)
			}
		default:
			summary := copyHandlerUtil{}.fetchJobStatus(jobID, &throughputIntervalTime, &bytesTransferredInLastInterval, cca.outputJson)
			// happy ending to the front end
			if summary.JobStatus == common.EJobStatus.Completed() ||
				summary.JobStatus == common.EJobStatus.Cancelled() {
				// print final JobProgress summary if output-json flag is set to false
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

func isStdinPipeIn() (bool, error) {
	// check the Stdin to see if we are uploading or downloading
	info, err := os.Stdin.Stat()
	if err != nil {
		return false, fmt.Errorf("fatal: failed to read from Stdin due to error: %s", err)
	}

	return info.Mode()&os.ModeNamedPipe != 0, nil
}

// TODO check file size, max is 4.75TB
func init() {
	raw := rawCopyCmdArgs{}

	// cpCmd represents the cp command
	cpCmd := &cobra.Command{
		Use:        "copy [source] [destination]",
		Aliases:    []string{"cp", "c"},
		SuggestFor: []string{"cpy", "cy", "mv"}, //TODO why does message appear twice on the console
		Short:      "Move data between two places",
		Long: `
Copy(cp) moves data between two places. Local <=> Azure Data Lake Storage Gen2 are the only scenarios officially supported at the moment.
Please refer to the examples for more information.
`,
		Example: `Upload a single file:
  - azcopy cp "/path/to/file.txt" "https://[account].dfs.core.windows.net/[existing-filesystem]/[path/to/destination/directory/or/file]"

Upload an entire directory:
  - azcopy cp "/path/to/dir" "https://[account].dfs.core.windows.net/[existing-filesystem]/[path/to/destination/directory]" --recursive=true

Upload files using wildcards:
  - azcopy cp "/path/*foo/*bar/*.pdf" "https://[account].dfs.core.windows.net/[existing-filesystem]/[path/to/destination/directory]"

Upload files and/or directories using wildcards:
  - azcopy cp "/path/*foo/*bar*" "https://[account].dfs.core.windows.net/[existing-filesystem]/[path/to/destination/directory]" --recursive=true

Download a single file:
  - azcopy cp "https://[account].dfs.core.windows.net/[existing-filesystem]/[path/to/source/file]" "/path/to/file.txt"

Download an entire directory:
  - azcopy cp "https://[account].dfs.core.windows.net/[existing-filesystem]/[path/to/source/dir]" "/path/to/file.txt" --recursive=true
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 { // redirection
				if stdinPipeIn, err := isStdinPipeIn(); stdinPipeIn == true {
					raw.src = pipeLocation
					raw.dst = args[0]
				} else {
					if err != nil {
						return fmt.Errorf("fatal: failed to read from Stdin due to error: %s", err)
					} else {
						raw.src = args[0]
						raw.dst = pipeLocation
					}
				}
			} else if len(args) == 2 { // normal copy
				raw.src = args[0]
				raw.dst = args[1]
			} else {
				return errors.New("wrong number of arguments, please refer to the help page on usage of this command")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cooked, err := raw.cook()
			if err != nil {
				return fmt.Errorf("failed to parse user input due to error: %s", err)
			}
			// If the stdInEnable is set true, then a separate go routines is reading the standard input.
			// If the "cancel\n" keyword is passed to the standard input, it will cancel the job
			// Any other word keyword provided will panic
			// This feature is to support cancel for node.js applications spawning azcopy
			if raw.stdInEnable {
				go ReadStandardInputToCancelJob(CancelChannel)
			}
			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				return fmt.Errorf("failed to perform copy command due to error: %s", err)
			}
			return nil
		},
	}
	rootCmd.AddCommand(cpCmd)

	// define the flags relevant to the cp command
	// Visible flags
	cpCmd.PersistentFlags().Uint32Var(&raw.blockSize, "block-size", 8*1024*1024, "use this block(chunk) size when uploading/downloading to/from Azure Storage")
	cpCmd.PersistentFlags().BoolVar(&raw.forceWrite, "force", true, "overwrite the conflicting files/blobs at the destination if this flag is set to true")
	cpCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "INFO", "define the log verbosity for the log file, available levels: DEBUG, INFO, WARNING, ERROR, PANIC, and FATAL")
	cpCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", false, "look into sub-directories recursively when uploading from local file system")
	cpCmd.PersistentFlags().BoolVar(&raw.outputJson, "output-json", false, "indicate that the output should be in JSON format")

	// hidden filters
	cpCmd.PersistentFlags().StringVar(&raw.include, "include", "", "Filter: only include these files when copying. "+
		"Support use of *. More than one file are separated by ';'")
	cpCmd.PersistentFlags().StringVar(&raw.exclude, "exclude", "", "Filter: Exclude these files when copying. Support use of *.")
	cpCmd.PersistentFlags().BoolVar(&raw.followSymlinks, "follow-symlinks", false, "Filter: Follow symbolic links when uploading from local file system.")
	cpCmd.PersistentFlags().BoolVar(&raw.withSnapshots, "with-snapshots", false, "Filter: Include the snapshots. Only valid when the source is blobs.")

	// hidden options
	cpCmd.PersistentFlags().StringVar(&raw.blockBlobTier, "block-blob-tier", "None", "Upload block blob to Azure Storage using this blob tier.")
	cpCmd.PersistentFlags().StringVar(&raw.pageBlobTier, "page-blob-tier", "None", "Upload page blob to Azure Storage using this blob tier.")
	cpCmd.PersistentFlags().StringVar(&raw.metadata, "metadata", "", "Upload to Azure Storage with these key-value pairs as metadata.")
	cpCmd.PersistentFlags().StringVar(&raw.contentType, "content-type", "", "Specifies content type of the file. Implies no-guess-mime-type.")
	cpCmd.PersistentFlags().StringVar(&raw.fromTo, "fromTo", "", "Specifies the source destination combination. For Example: LocalBlob, BlobLocal, LocalBlobFS")
	cpCmd.PersistentFlags().StringVar(&raw.contentEncoding, "content-encoding", "", "Upload to Azure Storage using this content encoding.")
	cpCmd.PersistentFlags().BoolVar(&raw.noGuessMimeType, "no-guess-mime-type", false, "This sets the content-type based on the extension of the file.")
	cpCmd.PersistentFlags().BoolVar(&raw.preserveLastModifiedTime, "preserve-last-modified-time", false, "Only available when destination is file system.")
	cpCmd.PersistentFlags().BoolVar(&raw.background, "background-op", false, "true if user has to perform the operations as a background operation")
	cpCmd.PersistentFlags().BoolVar(&raw.stdInEnable, "stdIn-enable", false, "true if user wants to cancel the process by passing 'cancel' "+
		"to the standard Input. This flag enables azcopy reading the standard input while running the operation")
	cpCmd.PersistentFlags().StringVar(&raw.acl, "acl", "", "Access conditions to be used when uploading/downloading from Azure Storage.")

	// oauth options
	cpCmd.PersistentFlags().BoolVar(&raw.useInteractiveOAuthUserCredential, "oauth-user", false, "Use OAuth user credential and do interactive login.")
	cpCmd.PersistentFlags().StringVar(&raw.tenantID, "tenant-id", common.DefaultTenantID, "Tenant id to use for OAuth user interactive login.")
	cpCmd.PersistentFlags().StringVar(&raw.aadEndpoint, "aad-endpoint", common.DefaultActiveDirectoryEndpoint, "Azure active directory endpoint to use for OAuth user interactive login.")

	// hide flags not relevant to BFS
	// TODO remove after preview release
	cpCmd.PersistentFlags().MarkHidden("include")
	cpCmd.PersistentFlags().MarkHidden("exclude")
	cpCmd.PersistentFlags().MarkHidden("follow-symlinks")
	cpCmd.PersistentFlags().MarkHidden("with-snapshots")

	cpCmd.PersistentFlags().MarkHidden("block-blob-tier")
	cpCmd.PersistentFlags().MarkHidden("page-blob-tier")
	cpCmd.PersistentFlags().MarkHidden("metadata")
	cpCmd.PersistentFlags().MarkHidden("content-type")
	cpCmd.PersistentFlags().MarkHidden("content-encoding")
	cpCmd.PersistentFlags().MarkHidden("no-guess-mime-type")
	cpCmd.PersistentFlags().MarkHidden("preserve-last-modified-time")
	cpCmd.PersistentFlags().MarkHidden("background-op")
	cpCmd.PersistentFlags().MarkHidden("fromTo")
	cpCmd.PersistentFlags().MarkHidden("acl")
	cpCmd.PersistentFlags().MarkHidden("stdIn-enable")
}
