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

package ste

import (
	"errors"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

// Abstraction of the methods needed to upload one file to a remote location
type uploader interface {

	// ChunkSize returns the chunk size that should be used
	ChunkSize() uint32

	// NumChunks returns the number of chunks that will be required for the target file
	NumChunks() uint32

	// RemoteFileExists is called to see whether the file already exists at the remote location (so we know whether we'll be overwriting it)
	RemoteFileExists() (bool, error)

	// Prologue is called automatically before the first chunkFunc is generated.
	// Implementation should do any initialization that is necessary - e.g.
	// creating the remote file for those destinations that require an explicit
	// creation step. Implementations should call jptm.FailActiveUpload if anything
	// goes wrong during the prologue.
	// Leading bytes are the early bytes of the file, to be used
	// for mime-type detection (or nil if file is empty or the bytes code
	// not be read).
	Prologue(leadingBytes []byte)

	// GenerateUploadFunc returns a func() that will upload the specified portion of the local file to the remote location
	// Instead of taking local file as a parameter, it takes a helper that will read from the file. That keeps details of
	// file IO out of the upload func, and lets that func concentrate only on the details of the remote endpoint
	GenerateUploadFunc(chunkID common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc

	// Epilogue will be called automatically once we know all the chunk funcs have been processed.
	// Implementation should interact with its jptm to do
	// post-success processing if transfer has been successful so far,
	// or post-failure processing otherwise.  Implementations should
	// use jptm.FailActiveUpload if anything fails during the epilogue.
	Epilogue()

	// Md5Channel returns the channel on which localToRemote should send the MD5 hash to the uploader
	Md5Channel() chan<- []byte
}

type uploaderFactory func(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer) (uploader, error)

func newMd5Channel() chan []byte {
	return make(chan []byte, 1) // must be buffered, so as not to hold up the goroutine running localToRemote (which needs to start on the NEXT file after finishing its current one)
}

// Tries to set the MD5 hash using the given function
// Fails the upload if any error happens.
// This should be used only by those uploads that require a separate operation to PUT the hash at the end.
// Others, such as the block blob uploader piggyback their MD5 setting on other calls, and so won't use this.
func tryPutMd5Hash(jptm IJobPartTransferMgr, md5Channel <-chan []byte, worker func(hash []byte) error) {
	md5Hash, ok := <-md5Channel
	if ok {
		err := worker(md5Hash)
		if err != nil {
			jptm.FailActiveUpload("Setting hash", err)
		}
	} else {
		jptm.FailActiveUpload("Setting hash", errNoHash)
	}
}

var errNoHash = errors.New("no hash computed")

func getNumUploadChunks(fileSize int64, chunkSize uint32) uint32 {
	numChunks := uint32(1) // for uploads, we always map zero-size files to ONE (empty) chunk
	if fileSize > 0 {
		chunkSizeI := int64(chunkSize)
		numChunks = common.Iffuint32(
			fileSize%chunkSizeI == 0,
			uint32(fileSize/chunkSizeI),
			uint32(fileSize/chunkSizeI)+1)
	}
	return numChunks
}

func createUploadChunkFunc(jptm IJobPartTransferMgr, id common.ChunkID, body func()) chunkFunc {
	// If uploading, we set the chunk status to done as soon as the chunkFunc completes.
	// But we don't do that for downloads, since for those the chunk is not "done" until its flushed out
	// by the ChunkedFileWriter. (The ChunkedFileWriter will set the status to done at that time.)
	return createChunkFunc(true, jptm, id, body)
}

// createChunkFunc adds a standard prefix, which all chunkFuncs require, to the given body
func createChunkFunc(setDoneStatusOnExit bool, jptm IJobPartTransferMgr, id common.ChunkID, body func()) chunkFunc {
	return func(workerId int) {

		// BEGIN standard prefix that all chunk funcs need
		defer jptm.ReportChunkDone() // whether successful or failed, it's always "done" and we must always tell the jptm

		jptm.OccupyAConnection() // TODO: added the two operations for debugging purpose. remove later
		defer jptm.ReleaseAConnection()

		if jptm.WasCanceled() {
			jptm.LogChunkStatus(id, common.EWaitReason.Cancelled())
			return
		} else {
			if setDoneStatusOnExit {
				defer jptm.LogChunkStatus(id, common.EWaitReason.ChunkDone())
			}
		}
		// END standard prefix

		body()
	}
}
