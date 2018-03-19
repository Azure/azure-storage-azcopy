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

package handlers

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"io"
	"net/url"
	"os"
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

// TODO discuss with Jeff what features should be supported by redirection, such as metadata, content-type, etc.
func HandleRedirectionCommand(commandLineInput common.CopyCmdArgsAndFlags) error {
	// check the Stdin to see if we are uploading or downloading
	info, err := os.Stdin.Stat()
	if err != nil {
		return fmt.Errorf("fatal: failed to read from Stdin due to error: %s", err)
	}

	if info.Mode() & os.ModeNamedPipe == 0 {
		// if there's no Stdin pipe, this is a download case
		return handleDownloadBlob(commandLineInput.BlobUrlForRedirection)
	} else {
		// something is on Stdin, this is the upload case
		return handleUploadToBlob(commandLineInput.BlobUrlForRedirection, commandLineInput.BlockSize)
	}
}

func handleDownloadBlob(blobUrl string) error {
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
	blobStream := azblob.NewDownloadStream(context.Background(), blobURL.GetBlob, azblob.DownloadStreamOptions{})
	defer blobStream.Close()

	// step 4: pipe everything into Stdout
	_, err = io.Copy(os.Stdout, blobStream)
	if err != nil {
		return fmt.Errorf("fatal: cannot download blob to Stdout due to error: %s", err.Error())
	}

	return nil
}

func handleUploadToBlob(blobUrl string, blockSize uint32) error {
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
	fullChannel := make(chan uploadTask, numOfSimultaneousUploads) // represent buffers filled up and waiting to be uploaded
	emptyChannel := make(chan uploadTask, numOfSimultaneousUploads) // represent buffers ready to be filled up
	errChannel := make(chan error, numOfSimultaneousUploads) // in case error happens, workers need to communicate the err back
	var finishedChunkCount uint32 = 0 // used to keep track of how many chunks are completed
	var total uint32 = 0 // used to keep track of total number of chunks, it is incremented as more data is read in from stdin
	var blockIdList []string
	isReadComplete := false

	// step 4: prep empty buffers and dispatch upload workers
	for i := 0; i < numOfSimultaneousUploads; i++ {
		emptyChannel <- uploadTask{buffer: make([]byte, blockSize), blockSize: 0, blockIdBase64: ""}

		go func(fullChannel <-chan uploadTask, emptyChannel chan<- uploadTask, errorChannel chan<- error, workerId int) {
			// wait on fullChannel, if anything comes off, upload it as block
			for full := range fullChannel {
				resp, err := blockBlobUrl.PutBlock(
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
					_, err := blockBlobUrl.PutBlockList(uploadContext, blockIdList, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})

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

type uploadTask struct {
	buffer     []byte
	blockSize  int
	blockIdBase64 string
}
