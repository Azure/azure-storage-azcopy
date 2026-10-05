package traverser

import (
	"context"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// SyncJob supplies job configuration and caller-owned execution and reporting hooks.
// Traversal does not own credentials, job plans, console output, or transfer dispatch.
type SyncJob struct {
	Source                common.ResourceString
	Destination           common.ResourceString
	FromTo                common.FromTo
	JobID                 common.JobID
	Recursive             bool
	DeleteDestination     common.DeleteDestination
	PreserveInfo          bool
	UseStreamingMergeJoin bool
	NewTraverser          func(common.ResourceString, common.Location, context.Context, InitResourceTraverserOptions) (ResourceTraverser, error)

	SetCancel                                    func(context.CancelFunc)
	EncodeDestinationPath                        func(string) string
	IncrementSourceFolderEnumerationFailed       func()
	IncrementDestinationFolderEnumerationFailed  func()
	IncrementDestinationFolderEnumerationSkipped func()
	Log                                          func(common.LogLevel, string, bool)
	RegisterStats                                func(common.CustomStatsID, common.CustomStatsCallback)
	UnregisterStats                              func(common.CustomStatsID)
	CollectStats                                 func(common.CustomStatsID)
	LogStats                                     func(string, []common.CustomStatEntry)
}
