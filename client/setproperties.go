package client

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type SetPropertiesOptions struct {
	Metadata          common.Metadata
	FromTo            common.FromTo
	IncludePattern    []string
	IncludePath       []string
	ExcludePattern    []string
	ExcludePath       []string
	listOfFiles       string // TODO (gapra-msft): Hide this
	BlockBlobTier     common.BlockBlobTier
	PageBlobTier      common.PageBlobTier
	Recursive         bool
	RehydratePriority common.RehydratePriorityType
	DryRun            bool
	BlobTags          common.BlobTags
	TrailingDot       common.TrailingDotOption
}

func (cc Client) SetProperties(resource string, options SetPropertiesOptions) error {
	return nil
}
