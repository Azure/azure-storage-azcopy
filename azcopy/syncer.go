package azcopy

import (
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type syncJob struct {
	source      common.ResourceString
	destination common.ResourceString
	opts        SyncOptions

	// job properties
	commandString string
	jobID         common.JobID

	// job progress tracker properties
	// NOTE: for the 64 bit atomic functions to work on a 32 bit system, we have to guarantee the right 64-bit alignment
	// so the 64 bit integers are placed first in the struct to avoid future breaks
	// refer to: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	// defines the number of files listed at the source and compared.
	atomicSourceFilesScanned uint64
	// defines the number of files listed at the destination and compared.
	atomicDestinationFilesScanned uint64
	// defines the scanning status of the sync operation.
	// 0 means scanning is in progress and 1 means scanning is complete.
	atomicScanningStatus uint32
	// defines whether first part has been ordered or not.
	// 0 means first part is not ordered and 1 means first part is ordered.
	atomicFirstPartOrdered uint32
	// deletion count keeps track of how many extra files from the destination were removed
	atomicDeletionCount           uint32
	atomicSkippedSymlinkCount     uint32
	atomicSkippedSpecialFileCount uint32
	// intervalStartTime holds the last time value when the progress summary was fetched
	// the value of this variable is used to calculate the throughput
	// it gets updated every time the progress summary is fetched
	intervalStartTime        time.Time
	intervalBytesTransferred uint64
	// used to calculate job summary
	jobStartTime time.Time
	// this flag is set by the enumerator
	// it is useful to indicate whether we are simply waiting for the purpose of cancelling
	// this is set to true once the final part has been dispatched
	isEnumerationComplete bool
}

// defaultAndInferSyncOptions fills in any missing values in the SyncOptions with their defaults, and infers values from other values where applicable.
func applyDefaultsAndInferSyncOptions(s SyncOptions, fromTo common.FromTo) SyncOptions {
	clone := s.clone()
	clone.FromTo = fromTo

	if clone.Recursive == nil {
		clone.Recursive = to.Ptr(true)
	}
	if clone.S2SPreserveAccessTier == nil {
		clone.S2SPreserveAccessTier = to.Ptr(true)
	}

	if clone.LocalHashStorageMode == nil {
		mode := common.EHashStorageMode.Default()
		clone.LocalHashStorageMode = &mode
	}

	if clone.PreserveInfo == nil {
		clone.PreserveInfo = to.Ptr(GetPreserveInfoDefault(clone.FromTo))
	}

	if fromTo.IsNFSAware() {
		clone.PreserveInfo = to.Ptr(*clone.PreserveInfo || AreBothLocationsNFSAware(fromTo)) // TODO : (gapra) Pretty sure this is redundant with the defaulting above
		clone.PreservePosixProperties = false
		// Preserve ACLs and Ownership for NFS
		clone.preservePermissions = common.NewPreservePermissionsOption(clone.PreservePermissions, true, fromTo)
	} else {
		clone.PreserveInfo = to.Ptr(*clone.PreserveInfo && AreBothLocationsSMBAware(fromTo))
		clone.preservePermissions = common.NewPreservePermissionsOption(clone.PreservePermissions, false, fromTo)
		clone.Hardlinks = 0
	}

	switch clone.CompareHash {
	case common.ESyncHashType.MD5():
		clone.PutMd5 = true // save any new MD5s on files we download
	default: // no need to put a hash of any kind
	}

	if clone.HashMetaDir != "" {
		common.LocalHashDir = s.HashMetaDir
	}
	common.LocalHashStorageMode = *s.LocalHashStorageMode

	// We only preserve access tier for S2S. For other scenarios, we set it to false
	if !fromTo.IsS2S() {
		clone.S2SPreserveAccessTier = to.Ptr(false)
	}

	return clone
}
