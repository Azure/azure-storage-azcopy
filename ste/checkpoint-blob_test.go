// Copyright © Microsoft <wastore@microsoft.com>
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
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
)

type blobCheckpointTestSuite struct{}

var _ = chk.Suite(&blobCheckpointTestSuite{})

func (*blobCheckpointTestSuite) TestBlobCheckpointBasic(c *chk.C) {
	dir, err := os.MkdirTemp("", "checkPointTest*")
	c.Assert(err, chk.Equals, nil)
	checkpointFilePath := filepath.Join(dir,"testCheckpoint")
	logger := func(msg string) { c.Log(msg) }
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	cp := initCheckpoint(ctx, checkpointFilePath, logger)

	// Create a new entry in the file.
	fileId := rand.Intn(100000)
	numChunks := rand.Intn(azblob.BlockBlobMaxBlocks)
	b := NewBlobCheckpointEntry(fileId, cp)
	b.Init(numChunks)

	// Verify the entry is created in cp
	_, ok := cp.fileMap[fileId]
	c.Assert(ok, chk.Equals, true)

	//------------------------------------------------------------

	// Write some random unique 20 chunks
	m := make(map[int]int)
	for i := 0; i < 20; i++ {
		m[rand.Intn(numChunks)] = 1
	}
	completedChunks := make([]int, len(m))

	i := 0
	for k := range m {
		completedChunks[i] = k
		i++
	}

	//------------------------------------------------------------

	// Mark some random chunks to be completed an verify that they
	// are the only ones reported completed.
	for _, chunkID := range completedChunks {
		b.ChunkDone(chunkID)
	}
	
	completedChunksFromMap := b.CompletedChunks()

	// Whatever chunks we marked complete, should be the only ones reported in map
	c.Assert(len(completedChunks), chk.Equals, len(completedChunksFromMap))
	for _, key := range completedChunks {
		_, exists := completedChunksFromMap[key]
		c.Assert(exists, chk.Equals, true)
	}

	//------------------------------------------------------------

	// Mark file complete and verify it is deleted.
	b.TransferDone()
	_, ok = cp.fileMap[fileId]
	c.Assert(ok, chk.Equals, false)
}


func (*blobCheckpointTestSuite) TestBlobCheckpointFlush(c *chk.C) {
	dir, err := os.MkdirTemp("", "checkPointTest*")
	c.Assert(err, chk.Equals, nil)
	checkpointFilePath := filepath.Join(dir,"testCheckpoint")
	logger := func(msg string) { c.Log(msg) }
	ctx, cancel := context.WithCancel(context.TODO())

	cp1 := initCheckpoint(ctx, checkpointFilePath, logger)

	// Create a new entry in the file.
	fileId := rand.Intn(100000)
	numChunks := rand.Intn(azblob.BlockBlobMaxBlocks)
	b1 := NewBlobCheckpointEntry(fileId, cp1)
	b1.Init(numChunks)

	//------------------------------------------------------------

	// Write some random unique 20 chunks
	m := make(map[int]int)
	for i := 0; i < 20; i++ {
		m[rand.Intn(numChunks)] = 1
	}
	completedChunks := make([]int, len(m))

	i := 0
	for k := range m {
		completedChunks[i] = k
		i++
	}

	//------------------------------------------------------------

	// Mark some random chunks to be completed an verify that they
	// are the only ones reported completed.
	for _, chunkID := range completedChunks {
		b1.ChunkDone(chunkID)
	}
	
	completedChunksFromMap1 := b1.CompletedChunks()

	// Sleep for 20 seconds for checkpoint to Flush
	time.Sleep(2 * checkpointFlushInterval)
	cancel() // close the checkpoint thread.

	//------------------------------------------------------------

	//Read checkpoint from Disk and verify the contents are same as

	ctx, cancel = context.WithCancel(context.TODO())
	defer cancel()
	cp2, err := NewCheckpointFromMetafile(ctx, checkpointFilePath, logger)
	c.Assert(err, chk.Equals, nil)

	listOfTransfers := cp2.ListOfTransfersInMetafile()
	c.Assert(len(listOfTransfers), chk.Equals, 1) // We've added only one file
	c.Assert(listOfTransfers[0], chk.Equals, fileId)

	b2 := NewBlobCheckpointEntry(fileId, cp2)
	completedChunksFromMap2 := b2.CompletedChunks()

	c.Assert(len(completedChunksFromMap1), chk.Equals, len(completedChunksFromMap2))
	for k, v := range completedChunksFromMap1 {
		value, ok := completedChunksFromMap2[k]
		c.Assert(ok, chk.Equals, true)
		c.Assert(v, chk.DeepEquals, value)
	}

}