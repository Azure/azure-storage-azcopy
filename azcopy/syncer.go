package azcopy

import (
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type syncer struct {
	source      common.ResourceString
	destination common.ResourceString
	opts        SyncOptions

	// job properties
	jobID common.JobID

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
func applyDefaultsAndInferSyncOptions(s SyncOptions, fromTo common.FromTo) (clone SyncOptions, err error) {
	clone = s.clone()
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
		clone.PreserveInfo = to.Ptr(*clone.PreserveInfo || areBothLocationsNFSAware(fromTo)) // TODO : (gapra) Pretty sure this is redundant with the defaulting above
		clone.PreservePosixProperties = false
		// Preserve ACLs and Ownership for NFS
		clone.preservePermissions = common.NewPreservePermissionsOption(clone.PreservePermissions, true, fromTo)
	} else {
		clone.PreserveInfo = to.Ptr(*clone.PreserveInfo && areBothLocationsSMBAware(fromTo))
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

	clone.blockSize, err = BlockSizeInBytes(clone.BlockSizeMB)
	if err != nil {
		return clone, err
	}
	clone.putBlobSize, err = BlockSizeInBytes(clone.PutBlobSizeMB)
	if err != nil {
		return clone, err
	}

	clone.cpkOptions = common.CpkOptions{
		CpkScopeInfo: clone.CpkByName,  // Setting CPK-N
		CpkInfo:      clone.CpkByValue, // Setting CPK-V
		// Get the key (EncryptionKey and EncryptionKeySHA256) value from environment variables when required.
	}
	// We only support transfer from source encrypted by user key when user wishes to download.
	// Due to service limitation, S2S transfer is not supported for source encrypted by user key.
	if clone.FromTo.IsDownload() && (clone.cpkOptions.CpkScopeInfo != "" || clone.cpkOptions.CpkInfo) {
		common.GetLifecycleMgr().Info("Client Provided Key for encryption/decryption is provided for download scenario. " +
			"Assuming source is encrypted.")
		clone.cpkOptions.IsSourceEncrypted = true
	}

	clone.IncludeDirectoryStubs = (clone.FromTo.From().SupportsHnsACLs() && clone.FromTo.To().SupportsHnsACLs() && clone.preservePermissions.IsTruthy()) || clone.IncludeDirectoryStubs

	return clone, nil
}

const LocalToFileShareWarnMsg = "AzCopy sync is supported but not fully recommended for Azure Files. AzCopy sync doesn't support differential copies at scale, and some file fidelity might be lost."

func (s *syncer) validate() (err error) {
	// service level sync is not supported
	if s.opts.FromTo.From().IsRemote() {
		err = validateURLIsNotServiceLevel(s.source.Value, s.opts.FromTo.From())
		if err != nil {
			return err
		}
	}
	if s.opts.FromTo.To().IsRemote() {
		err = validateURLIsNotServiceLevel(s.destination.Value, s.opts.FromTo.To())
		if err != nil {
			return err
		}
	}

	// force if read only
	if err = ValidateForceIfReadOnly(s.opts.ForceIfReadOnly, s.opts.FromTo); err != nil {
		return err
	}

	// NFS and SMB
	if s.opts.FromTo.IsNFSAware() {
		err = ValidateNFSOptions(s.opts.FromTo, s.opts.preservePermissions, *s.opts.PreserveInfo, common.ESymlinkHandlingType.Skip(), s.opts.Hardlinks)
		if err != nil {
			return err
		}
	} else {
		err = ValidateSMBOptions(s.opts.FromTo, s.opts.preservePermissions, *s.opts.PreserveInfo, s.opts.PreservePosixProperties)
		if err != nil {
			return err
		}
	}

	// put md5
	// In case of S2S transfers, log info message to inform the users that MD5 check doesn't work for S2S Transfers.
	// This is because we cannot calculate MD5 hash of the data stored at a remote locations.
	if s.opts.PutMd5 && s.opts.FromTo.IsS2S() {
		common.GetLifecycleMgr().Info(" --put-md5 flag to check data consistency between source and destination is not applicable for S2S Transfers (i.e. When both the source and the destination are remote). AzCopy cannot compute MD5 hash of data stored at remote location.")
	}

	// check md5
	hasMd5Validation := s.opts.CheckMd5 != common.DefaultHashValidationOption
	if hasMd5Validation && !s.opts.FromTo.IsDownload() {
		return fmt.Errorf("check-md5 is set but the job is not a download")
	}

	// s2s preserve blob tags
	if s.opts.S2SPreserveBlobTags && (s.opts.FromTo.From() != common.ELocation.Blob() || s.opts.FromTo.To() != common.ELocation.Blob()) {
		return fmt.Errorf("either source or destination is not a blob storage. " +
			"blob index tags is a property of blobs only therefore both source and destination must be blob storage")
	}

	// cpk
	if s.opts.CpkByName != "" && s.opts.CpkByValue {
		return errors.New("cannot use both cpk-by-name and cpk-by-value at the same time")
	}

	// sync between local and file warning
	// Reference : https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-files#synchronize-files
	if s.opts.FromTo == common.EFromTo.LocalFile() {
		common.GetLifecycleMgr().Warn(LocalToFileShareWarnMsg)
		common.LogToJobLogWithPrefix(LocalToFileShareWarnMsg, common.LogWarning)
		// TODO : (gapra) Seems odd to also log it during dryrun log it twice? Commenting this for now unless someone has a strong reason to keep it.
		//if cooked.dryrunMode {
		//	glcm.Dryrun(func(of common.OutputFormat) string {
		//		if of == common.EOutputFormat.Json() {
		//			var out struct {
		//				Warn string `json:"warn"`
		//			}
		//
		//			out.Warn = LocalToFileShareWarnMsg
		//			buf, _ := json.Marshal(out)
		//			return string(buf)
		//		}
		//
		//		return fmt.Sprintf("DRYRUN: warn %s", LocalToFileShareWarnMsg)
		//	})
		//}
	}

	// are source and destination resolvable?
	if err := common.VerifyIsURLResolvable(s.source.Value); s.opts.FromTo.From().IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve source: %w", err)
	}

	if err := common.VerifyIsURLResolvable(s.destination.Value); s.opts.FromTo.To().IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve destination: %w", err)
	}

	// system containers are not supported
	if s.opts.FromTo.IsS2S() || s.opts.FromTo.IsUpload() {
		dstContainerName, err := GetContainerName(s.destination.Value, s.opts.FromTo.To())
		if err != nil {
			return fmt.Errorf("failed to get container name from destination (is it formatted correctly?)")
		}
		if common.IsSystemContainer(dstContainerName) {
			return fmt.Errorf("cannot copy to system container '%s'", dstContainerName)
		}
	}

	return nil
}
