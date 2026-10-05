package azcopy

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

type SyncDestinationComparator = traverser.SyncDestinationComparator
type SyncSourceComparator = traverser.SyncSourceComparator

func NewSyncDestinationComparator(indexer *traverser.ObjectIndexer, copy, remove traverser.ObjectProcessor, hash common.SyncHashType, preferSMBTime, disableComparison bool) *SyncDestinationComparator {
	return traverser.NewSyncDestinationComparator(indexer, copy, remove, hash, preferSMBTime, disableComparison,
		common.EDeleteDestination.False(), nil, nil)
}

func NewSyncSourceComparator(indexer *traverser.ObjectIndexer, copy traverser.ObjectProcessor, hash common.SyncHashType, preferSMBTime, disableComparison bool) *SyncSourceComparator {
	return traverser.NewSyncSourceComparator(indexer, copy, hash, preferSMBTime, disableComparison, nil)
}
