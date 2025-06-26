package azcopy

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type Client struct {
}

func NewClient() (Client, error) {

	err := cmd.Initialize(common.JobID{}, false)
	return Client{}, err
}
