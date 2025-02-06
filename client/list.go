package client

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type ListOptions struct {
	Location        common.Location
	MachineReadable bool
	RunningTally    bool
	MegaUnits       bool
	Properties      []cmd.ListProperty // TODO (gapra-msft): This should probably be an enum in common
	TrailingDot     common.TrailingDotOption
}

func (cc Client) List(options ListOptions) error {
	return nil
}
