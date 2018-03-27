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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"github.com/spf13/cobra"
	"io"
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"time"
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
	exclude        string
	recursive      bool
	followSymlinks bool
	withSnapshots  bool

	// options from flags
	blockSize                uint32
	blobTier                 string
	metadata                 string
	contentType              string
	contentEncoding          string
	noGuessMimeType          bool
	preserveLastModifiedTime bool
	background               bool
	acl                      string
	logVerbosity             byte
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
	/*
		if raw.blobUrlForRedirection != "" { // redirection
			if (validator{}.determineLocationType(raw.blobUrlForRedirection)) != common.ELocation.Blob() {
				return cookedCopyCmdArgs{}, errors.New("the provided blob URL for redirection is not valid")
			}

			cooked.blobUrlForRedirection = raw.blobUrlForRedirection
		} else { // normal copy
			cooked.srcLocation = validator{}.determineLocationType(raw.src)
			if cooked.srcLocation == common.ELocation.Unknown() {
				return cookedCopyCmdArgs{}, errors.New("the provided source is invalid")
			}

			cooked.dstLocation = validator{}.determineLocationType(raw.dst)
			if cooked.dstLocation == common.ELocation.Unknown() {
				return cookedCopyCmdArgs{}, errors.New("the provided destination is invalid")
			}

			if cooked.srcLocation == cooked.dstLocation { //TODO update to take file into account
				return cookedCopyCmdArgs{}, errors.New("the provided source/destination pair is invalid")
			}

			cooked.src = raw.src
			cooked.dst = raw.dst
		}
	*/
	// copy&transform flags to type-safety
	cooked.exclude = raw.exclude
	cooked.recursive = raw.recursive
	cooked.followSymlinks = raw.followSymlinks
	cooked.withSnapshots = raw.withSnapshots

	cooked.blockSize = raw.blockSize
	cooked.blobTier = raw.blobTier
	cooked.metadata = raw.metadata
	cooked.contentType = raw.contentType
	cooked.contentEncoding = raw.contentEncoding
	cooked.noGuessMimeType = raw.noGuessMimeType
	cooked.preserveLastModifiedTime = raw.preserveLastModifiedTime
	cooked.background = raw.background
	cooked.acl = raw.acl
	cooked.logVerbosity = common.LogLevel(raw.logVerbosity)

	return cooked, nil
}

// represents the processed copy command input from the user
type cookedCopyCmdArgs struct {
	// from arguments
	src                   string
	dst                   string
	fromTo                common.FromTo
	blobUrlForRedirection string
	// filters from flags
	exclude        string
	recursive      bool
	followSymlinks bool
	withSnapshots  bool

	// options from flags
	blockSize                uint32
	blobTier                 string //TODO define enum
	metadata                 string
	contentType              string
	contentEncoding          string
	noGuessMimeType          bool
	preserveLastModifiedTime bool
	background               bool
	acl                      string
	logVerbosity             common.LogLevel
}

func (cca cookedCopyCmdArgs) isRedirection() bool {
	switch cca.fromTo{
	case common.EFromTo.PipeFile():
		fallthrough
	case common.EFromTo.FilePipe():
		fallthrough
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
	// check the Stdin to see if we are uploading or downloading
	info, err := os.Stdin.Stat()
	if err != nil {
		return fmt.Errorf("fatal: failed to read from Stdin due to error: %s", err)
	}

	if info.Mode()&os.ModeNamedPipe == 0 {
		// if there's no Stdin pipe, this is a download case
		return cca.processRedirectionDownload(cca.blobUrlForRedirection)
	} else {
		// something is on Stdin, this is the upload case
		return cca.processRedirectionUpload(cca.blobUrlForRedirection, cca.blockSize)
	}
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
	})

	// step 2: parse source url
	u, err := url.Parse(blobUrl)
	if err != nil {
		return fmt.Errorf("fatal: cannot parse source blob URL due to error: %s", err.Error())
	}

	// step 3: start download
	blobURL := azblob.NewBlobURL(*u, p)
	// TODO: use the resilient reader of blob
	blobStream := azblob.NewDownloadStream(context.Background(), blobURL.Download, azblob.DownloadStreamOptions{})
	defer blobStream.Close()

	// step 4: pipe everything into Stdout
	_, err = io.Copy(os.Stdout, blobStream)
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

// handles the copy command
// dispatches the job order (in parts) to the storage engine
func (cca cookedCopyCmdArgs) processCopyJobPartOrders() (err error) {
	// initialize the fields that are constant across all job part orders
	jobPartOrder := common.CopyJobPartOrderRequest{
		JobID:    common.NewJobID(),
		FromTo:   cca.fromTo,
		Priority: common.EJobPriority.Normal(),
		LogLevel: cca.logVerbosity,
		BlobAttributes: common.BlobTransferAttributes{
			BlockSizeInBytes:         cca.blockSize,
			ContentType:              cca.contentType,
			ContentEncoding:          cca.contentEncoding,
			Metadata:                 cca.metadata,
			NoGuessMimeType:          cca.noGuessMimeType,
			PreserveLastModifiedTime: cca.preserveLastModifiedTime,
		},
	}

	// depending on the source and destination type, we process the cp command differently
	switch cca.fromTo {
	case common.EFromTo.LocalBlob():
		e := copyUploadEnumerator(jobPartOrder)
		err = e.enumerate(cca.src, cca.recursive, cca.dst)
	case common.EFromTo.BlobLocal():
		e := copyDownloadEnumerator(jobPartOrder)
		err = e.enumerate(cca.src, cca.recursive, cca.dst)
	}

	if err != nil {
		return fmt.Errorf("cannot start job due to error: %s.\n", err)
	}

	// in background mode we would spit out the job id and quit
	// in foreground mode we would continuously print out status updates for the job, so the job id is not important
	fmt.Println("Job with id", jobPartOrder.JobID, "has started.")
	if cca.background {
		return nil
	}

	cca.waitUntilJobCompletion(jobPartOrder.JobID)
	return nil
}

func (cca cookedCopyCmdArgs) waitUntilJobCompletion(jobID common.JobID) {
	// created a signal channel to receive the Interrupt and Kill signal send to OS
	cancelChannel := make(chan os.Signal, 1)
	// cancelChannel will be notified when os receives os.Interrupt and os.Kill signals
	signal.Notify(cancelChannel, os.Interrupt, os.Kill)

	// waiting for signals from either cancelChannel or timeOut Channel.
	// if no signal received, will fetch/display a job status update then sleep for a bit
	for {
		select {
		case <-cancelChannel:
			fmt.Println("Cancelling Job")
			cookedCancelCmdArgs{jobID: jobID}.process()
			os.Exit(1)
		default:
			jobStatus := copyHandlerUtil{}.fetchJobStatus(jobID)

			// happy ending to the front end
			if jobStatus == common.EJobStatus.Completed() {
				os.Exit(0)
			}

			// wait a bit before fetching job status again, as fetching has costs associated with it on the backend
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func isStdinPipeIn() (bool, error) {
	// check the Stdin to see if we are uploading or downloading
	info, err := os.Stdin.Stat()
	if err != nil {
		return false, fmt.Errorf("fatal: failed to read from Stdin due to error: %s", err)
	}

	return info.Mode()&os.ModeNamedPipe == 0, nil
}

// TODO check file size, max is 4.75TB
func init() {
	raw := rawCopyCmdArgs{}

	// cpCmd represents the cp command
	cpCmd := &cobra.Command{
		Use:        "copy",
		Aliases:    []string{"cp", "c"},
		SuggestFor: []string{"cpy", "cy", "mv"}, //TODO why does message appear twice on the console
		Short:      "copy(cp) moves data between two places.",
		Long: `copy(cp) moves data between two places. The most common cases are:
  - Upload local files/directories into Azure Storage.
  - Download blobs/container from Azure Storage to local file system.
  - Coming soon: Transfer files from Amazon S3 to Azure Storage.
  - Coming soon: Transfer files from Azure Storage to Amazon S3.
  - Coming soon: Transfer files from Google Storage to Azure Storage.
  - Coming soon: Transfer files from Azure Storage to Google Storage.
Usage:
  - azcopy cp <source> <destination> --flags
    - source and destination can either be local file/directory path, or blob/container URL with a SAS token.
  - <command which pumps data to stdout> | azcopy cp <blob_url> --flags
    - This command accepts data from stdin and uploads it to a blob.
  - azcopy cp <blob_url> --flags > <destination_file_path>
    - This command downloads a blob and outputs it on stdout.
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 { // redirection
				if stdinPipeIn, err := isStdinPipeIn(); stdinPipeIn == true {
					raw.src = pipeLocation
					raw.dst = args[0]
				} else{
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
				return errors.New("wrong number of arguments, please refer to help page on usage of this command")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cooked, err := raw.cook()
			if err != nil {
				return fmt.Errorf("failed to parse user input due to error %s", err)
			}

			err = cooked.process()
			if err != nil {
				return fmt.Errorf("failed to perform copy command due to error %s", err)
			}
			return nil
		},
	}
	rootCmd.AddCommand(cpCmd)

	// define the flags relevant to the cp command
	// filters
	cpCmd.PersistentFlags().StringVar(&raw.exclude, "exclude", "", "Filter: Exclude these files when copying. Support use of *.")
	cpCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", false, "Filter: Look into sub-directories recursively when uploading from local file system.")
	cpCmd.PersistentFlags().BoolVar(&raw.followSymlinks, "follow-symlinks", false, "Filter: Follow symbolic links when uploading from local file system.")
	cpCmd.PersistentFlags().BoolVar(&raw.withSnapshots, "with-snapshots", false, "Filter: Include the snapshots. Only valid when the source is blobs.")

	// options
	cpCmd.PersistentFlags().Uint32Var(&raw.blockSize, "block-size", 4*1024*1024, "Use this block size when uploading to Azure Storage.")
	cpCmd.PersistentFlags().StringVar(&raw.blobTier, "blob-tier", "", "Upload to Azure Storage using this blob tier.")
	cpCmd.PersistentFlags().StringVar(&raw.metadata, "metadata", "", "Upload to Azure Storage with these key-value pairs as metadata.")
	cpCmd.PersistentFlags().StringVar(&raw.contentType, "content-type", "", "Specifies content type of the file. Implies no-guess-mime-type.")
	cpCmd.PersistentFlags().StringVar(&raw.contentEncoding, "content-encoding", "", "Upload to Azure Storage using this content encoding.")
	cpCmd.PersistentFlags().BoolVar(&raw.noGuessMimeType, "no-guess-mime-type", false, "This sets the content-type based on the extension of the file.")
	cpCmd.PersistentFlags().BoolVar(&raw.preserveLastModifiedTime, "preserve-last-modified-time", false, "Only available when destination is file system.")
	cpCmd.PersistentFlags().BoolVar(&raw.background, "background-op", false, "true if user has to perform the operations as a background operation")
	cpCmd.PersistentFlags().StringVar(&raw.acl, "acl", "", "Access conditions to be used when uploading/downloading from Azure Storage.")
	cpCmd.PersistentFlags().Uint8Var(&raw.logVerbosity, "Logging", uint8(pipeline.LogWarning), "defines the log verbosity to be saved to log file")
}
