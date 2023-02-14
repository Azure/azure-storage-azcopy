package ste

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const checkPointBufferSize = 512

type checkPointMsgType int

const (
	newFile checkPointMsgType = iota
	chunkDone
	fileDone
	flush
)

type checkPointMsg struct {
	msgType    checkPointMsgType
	fileID     int
	numChunks  int
	chunkIndex int
}

type jobCheckpointFile struct {
	fileMap map[int]common.Bitmap
	action  chan checkPointMsg
	filePath string
	log	 func(string)
}

func initCheckpoint(ctx context.Context, path string, logger func(string)) *jobCheckpointFile {
	cp := jobCheckpointFile{
		fileMap: make(map[int]common.Bitmap),
		action:  make(chan checkPointMsg, checkPointBufferSize),
		log: logger,
	}

	go cp.checkpointMain(ctx)

	return &cp
}

func (cp *jobCheckpointFile) NewFile(fileID, numChunks int) {
	cp.action <- checkPointMsg{
		msgType:   newFile,
		fileID:    fileID,
		numChunks: numChunks,
	}
}

func (cp *jobCheckpointFile) ChunkDone(fileID, chunkIndex int) {
	cp.action <- checkPointMsg{
		msgType:    chunkDone,
		fileID:     fileID,
		chunkIndex: chunkIndex,
	}
}

func (cp *jobCheckpointFile) FileDone(fileID int) {
	cp.action <- checkPointMsg{
		msgType: fileDone,
		fileID:  fileID,
	}
}

func (cp *jobCheckpointFile) Flush() {
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

func (cp *jobCheckpointFile) checkpointMain(ctx context.Context) {

	for {
		select {
		case msg := <-cp.action:
			switch msg.msgType {
			case newFile:
				cp.fileMap[msg.fileID] = common.NewBitMap(msg.numChunks)
			case chunkDone:
				cp.fileMap[msg.fileID].Set(msg.chunkIndex)
			case fileDone:
				delete(cp.fileMap, msg.fileID)
			case flush:
				cp.Flush()
			}
		case <-ctx.Done():
			return
		}
	}
}

//=============================================================================

type blobCheckpointEntry struct {
	fileID int
	cp     *jobCheckpointFile
}


func NewBlobCheckpointEntry(fileID int, cp *jobCheckpointFile) ICheckpoint {
	b := blobCheckpointEntry{fileID, cp}
	return &b
}

func (b *blobCheckpointEntry) Init(numChunks int) {
	b.cp.NewFile(b.fileID, numChunks)
}

func (b *blobCheckpointEntry) ChunkDone(chunkIndex int) {
	b.cp.ChunkDone(b.fileID, chunkIndex)
}

func (b *blobCheckpointEntry) TransferDone() {
	b.cp.FileDone(b.fileID)
}

//=============================================================================

type nilCheckpointEntry int

func NewNullCheckpointEntry() ICheckpoint {return nilCheckpointEntry(0) }
func (nilCheckpointEntry) Init(_ int) {}
func (nilCheckpointEntry) ChunkDone(_ int) {}
func (nilCheckpointEntry) TransferDone() {}
