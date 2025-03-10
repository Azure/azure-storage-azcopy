package client

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"time"
)

type RemoveOptions struct {
	Recursive       bool
	IncludePattern  []string
	IncludePath     []string
	ExcludePattern  []string
	ExcludePath     []string
	ForceIfReadOnly bool
	listOfFiles     string // TODO (gapra-msft): Hide this
	DeleteSnapshots common.DeleteSnapshotsOption
	ListOfVersions  string
	DryRun          bool
	FromTo          common.FromTo
	PermanentDelete common.PermanentDeleteOption
	IncludeBefore   *time.Time
	IncludeAfter    *time.Time
	TrailingDot     common.TrailingDotOption
	CpkByName       string
	CpkByValue      bool
}

func (cc Client) Remove(resource string, options RemoveOptions) error {
	return nil
}
