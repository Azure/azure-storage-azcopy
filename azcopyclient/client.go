package azcopyclient

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type ClientOptions cmd.RootOptions

type Client struct {
	ClientOptions
}

// Initialize sets up AzCopy logger, ste and performs the version check
// TODO: this will be made internal and called by the respective commands
func (cc Client) Initialize(resumeJobID common.JobID, isBench bool) error {
	cmd.SetRootOptions(cmd.RootOptions(cc.ClientOptions))
	return cmd.Initialize(resumeJobID, isBench)
}
