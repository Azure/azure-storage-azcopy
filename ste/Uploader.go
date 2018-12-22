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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

type ChunkStats struct {
	FileSize int64
	ChunkSize int64
	NumChunks uint32
}

// Abstraction of the methods needed to upload to a remote location
type Uploader interface {

	RemoteFileExists() (bool, error)

	// Returns a func() that will upload the specified portion of the local file to the remote location
	// Instead of taking local file as a parameter, it takes a helper that will read from the file. That keeps details of
	// file IO out out the upload func, and lets that func concentrate only on the details of the remote endpoint
	GenerateUploadFunc(chunkID common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc

	// Epilogue should be called once we know all the chunk funcs have been processed
	// Implementation should interact with its IJobPartTransferMgr to do
	// post-success processing if transfer has been successful so far,
	// or post-failure processing otherwise.
	Epilogue()
}

type UploaderFactory func(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, proposedStats ChunkStats, pacer *pacer) (Uploader, error)

const (
	epilogueNotNeeded = -1
	epilogueNeedUnknown = 0
	epilogueNeeded = 1
)

