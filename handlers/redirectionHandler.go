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
	"context"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"time"
)

func HandleRedirectionCommand(commandLineInput common.CopyCmdArgsAndFlags) {
	// check the Stdin to see if we are uploading or downloading
	info, err := os.Stdin.Stat()
	if err != nil {
		panic(err)
	}

	// if nothing is on Stdin, this is a download case
	if info.Size() <= 0 {
		handleDownloadBlob(commandLineInput.BlobUrlForRedirection)
	} else { // something is on Stdin, this is the upload case
		handleUploadToBlob(commandLineInput.BlobUrlForRedirection)
	}
}

func handleDownloadBlob(blobUrl string) {
	// step 0: check the Stdout before uploading
	_, err := os.Stdout.Stat()
	if err != nil {
		panic(err)
	}

	// step 1: initialize pipeline
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      3,
			TryTimeout:    time.Second * 60,
			RetryDelay:    time.Second * 1,
			MaxRetryDelay: time.Second * 3,
		},
	})

	// step 2: parse source url
	u, err := url.Parse(blobUrl)
	if err != nil {
		panic(err)
	}

	// step 3: start download
	blockBlobUrl := azblob.NewBlockBlobURL(*u, p)
	blobStream := azblob.NewDownloadStream(context.Background(), blockBlobUrl.GetBlob, azblob.DownloadStreamOptions{})
	defer blobStream.Close()

	// step 4: pipe everything into Stdout
	_, err = io.Copy(os.Stdout, blobStream)
	if err != nil {
		panic(err)
	}
}

func handleUploadToBlob(blobUrl string) {
	// step 0: pipe everything from Stdin into a buffer
	input, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		panic("An error occurred while reading from stdin. Please retry.")
	}

	// step 1: initialize pipeline
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      3,
			TryTimeout:    time.Second * 60,
			RetryDelay:    time.Second * 1,
			MaxRetryDelay: time.Second * 3,
		},
	})

	// step 2: parse destination url
	u, err := url.Parse(blobUrl)
	if err != nil {
		panic(err)
	}

	// step 3: start upload
	blockBlobUrl := azblob.NewBlockBlobURL(*u, p)
	azblob.UploadBufferToBlockBlob(context.Background(), input, blockBlobUrl, azblob.UploadToBlockBlobOptions{})
	if err != nil {
		panic(err)
	}
}
