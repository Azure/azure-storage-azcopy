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
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const checkpointFlushInterval = time.Second * 10

type jobCheckpointMetaFile struct {
	mutex sync.Mutex
	fileMap map[int]common.Bitmap
	filePath string
	log	 func(string)
}

func initCheckpoint(ctx context.Context, path string, logger func(string)) *jobCheckpointMetaFile {
	cp := jobCheckpointMetaFile{
		fileMap: make(map[int]common.Bitmap),
		filePath: path,
		log: logger,
	}

	go cp.flush(ctx)

	return &cp
}


func NewCheckpointFromMetafile(ctx context.Context, path string, logger func(string)) (*jobCheckpointMetaFile, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	fileMap := make(map[int]common.Bitmap)
	decoder := gob.NewDecoder(bytes.NewReader(buf))
	if err := decoder.Decode(&fileMap); err != nil {
		return nil, err
	}

	cp := jobCheckpointMetaFile{
		fileMap: fileMap,
		log: logger,
		filePath: path,
	}

	go cp.flush(ctx)

	return &cp, nil
}


func (cp *jobCheckpointMetaFile) NewTransfer(fileID, numChunks int) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	cp.fileMap[fileID] = common.NewBitMap(numChunks)
}

func (cp *jobCheckpointMetaFile) ChunkDone(fileID, chunkIndex int) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	cp.fileMap[fileID].Set(chunkIndex)

}

func (cp *jobCheckpointMetaFile) TransferDone(fileID int) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	delete(cp.fileMap, fileID)

}

func (cp *jobCheckpointMetaFile) CurrentMapForTransfer(fileID int) (ret common.Bitmap) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	ret = cp.fileMap[fileID]
	return
}

func (cp *jobCheckpointMetaFile) ListOfTransfersInMetafile() ([]int) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	ret := make([]int, len(cp.fileMap))

	i := 0
	for k, _ := range cp.fileMap {
		ret[i] = k
		i++
	}

	return ret
}

func (cp *jobCheckpointMetaFile) flush(ctx context.Context) {
	ticker := time.NewTicker(checkpointFlushInterval)

	flushInt := func() {
		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)

		cp.mutex.Lock() // keep critical-section to minimum
		err := encoder.Encode(cp.fileMap)
		cp.mutex.Unlock()
 	
		if err != nil {
			cp.log(fmt.Sprintf("Could not encode checkpoint file: %s, err: %s", cp.filePath, err.Error()))
			return
		}

		if err := os.WriteFile(cp.filePath, buf.Bytes(), 0666); err != nil {
			cp.log(fmt.Sprintf("Failed to write checkpoint to disk: %s, err: %s", cp.filePath, err.Error()))
			return
		}
	}

	for {
		select {
		case <- ctx.Done():
			ticker.Stop()
			return
		case <- ticker.C:
			flushInt()
		}
	}

}

//=============================================================================

type blobCheckpointEntry struct {
	fileID int
	cp     *jobCheckpointMetaFile
}


func NewBlobCheckpointEntry(fileID int, cp *jobCheckpointMetaFile) ICheckpoint {
	b := blobCheckpointEntry{fileID, cp}
	return &b
}

func (b *blobCheckpointEntry) Init(numChunks int) {
	b.cp.NewTransfer(b.fileID, numChunks)
}

func (b *blobCheckpointEntry) ChunkDone(chunkIndex int) {
	b.cp.ChunkDone(b.fileID, chunkIndex)
}

func (b *blobCheckpointEntry) TransferDone() {
	b.cp.TransferDone(b.fileID)
}

func (b *blobCheckpointEntry) CompletedChunks() map[int]int {
	m := b.cp.CurrentMapForTransfer(b.fileID)
	size := m.Size()
	ret := make(map[int]int)
	
	for i := 0; i < size; i++ {
		if m.Test(i) {
			ret[i] = 1
		}
	}

	return ret
}

//=============================================================================

type nilCheckpointEntry int

func NewNullCheckpointEntry() ICheckpoint {return nilCheckpointEntry(0) }
func (nilCheckpointEntry) Init(int) {}
func (nilCheckpointEntry) ChunkDone(int) {}
func (nilCheckpointEntry) TransferDone() {}
func (nilCheckpointEntry) CompletedChunks() map[int]int {return nil}
