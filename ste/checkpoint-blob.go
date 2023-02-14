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
		cp.mutex.Lock()
		defer cp.mutex.Unlock()

		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)

		err := encoder.Encode(cp.fileMap)
 	   	if err != nil {
			cp.log(fmt.Sprintf("Could not encode checkpoint file: %s, err: %s", cp.filePath, err.Error()))
		}

		if err := os.WriteFile(cp.filePath, buf.Bytes(), 0666); err != nil {
			cp.log(fmt.Sprintf("Failed to write checkpoint to disk: %s, err: %s", cp.filePath, err.Error()))
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
