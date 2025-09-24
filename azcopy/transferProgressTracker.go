package azcopy

import (
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var _ JobProgressTracker = &transferProgressTracker{}

type transferProgressTracker struct {
	jobID   common.JobID
	handler CopyJobHandler
}

func newTransferProgressTracker(jobID common.JobID, handler CopyJobHandler) *transferProgressTracker {
	return &transferProgressTracker{
		jobID:   jobID,
		handler: handler,
	}
}

func (t transferProgressTracker) Start() {
	//TODO implement me
	panic("implement me")
}

func (t transferProgressTracker) CheckProgress() (uint32, bool) {
	//TODO implement me
	panic("implement me")
}

func (t transferProgressTracker) CompletedEnumeration() bool {
	//TODO implement me
	panic("implement me")
}

func (t transferProgressTracker) GetJobID() common.JobID {
	//TODO implement me
	panic("implement me")
}

func (t transferProgressTracker) GetElapsedTime() time.Duration {
	//TODO implement me
	panic("implement me")
}
