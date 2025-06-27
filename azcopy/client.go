package azcopy

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type Client struct {
	CurrentJobID common.JobID // TODO (gapra): In future this should only be set when there is a current job running. On complete, this should be cleared. It can also behave as something we can check to see if a current job is running
}

func NewClient() (Client, error) {
	c := Client{}
	common.InitializeFolders()
	return c, nil
}
