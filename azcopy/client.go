package azcopy

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type Client struct {
	JobPlanFolder string
	LogPathFolder string
	CurrentJobID  common.JobID
}

func NewClient() (Client, error) {
	c := Client{}
	c.LogPathFolder, c.JobPlanFolder = common.InitializeFolders()
	return c, nil
}
