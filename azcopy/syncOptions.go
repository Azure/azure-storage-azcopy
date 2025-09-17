package azcopy

import (
	"errors"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

// TODO : This will be made internal as we refactor more of the sync code.
type CookedSyncOptions struct {
	// TODO : Make all these internal as we refactor more of the sync code.
	// defaulted, inferred, processed and validated user options
	source      common.ResourceString
	destination common.ResourceString
	fromTo      common.FromTo

	recursive               bool
	includeDirectoryStubs   bool
	preserveInfo            bool
	preservePosixProperties bool
	forceIfReadOnly         bool
	blockSize               int64
	putBlobSize             int64
	filterOptions           traverser.FilterOptions
	deleteDestination       common.DeleteDestination
	putMd5                  bool                        // TODO: (gapra) Should we make this an enum called PutHash for None/MD5? So user can set the HashType?
	checkMd5                common.HashValidationOption // TODO (gapra) Same comment as above
	s2SPreserveAccessTier   bool
	s2SPreserveBlobTags     bool
	cpkOptions              common.CpkOptions
	mirrorMode              bool
	trailingDot             common.TrailingDotOption
	includeRoot             bool
	compareHash             common.SyncHashType
	preservePermissions     common.PreservePermissionsOption
	hardlinks               common.HardlinkHandlingType

	// AzCopy internal use only
	dryrun                           bool
	dryrunJobPartOrderHandler        func(request common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse
	dryrunDeleteHandler              ObjectDeleter
	deleteDestinationFileIfNecessary bool
	commandString                    string
}

func newCookedSyncOptions(src, dst string, opts SyncOptions) (s *CookedSyncOptions, err error) {
	s = &CookedSyncOptions{}

	err = s.applyFromToSrcDest(src, dst, opts.FromTo)
	if err != nil {
		return nil, err
	}

	err = s.applyDefaultsAndInferOptions(opts)
	if err != nil {
		return nil, err
	}

	err = s.validateOptions()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *CookedSyncOptions) applyFromToSrcDest(src, dst string, fromTo common.FromTo) (err error) {
	// fromTo, source and destination
	userFromTo := common.Iff(fromTo == common.EFromTo.Unknown(), "", fromTo.String())
	s.fromTo, err = InferAndValidateFromTo(src, dst, userFromTo)
	if err != nil {
		return err
	}
	common.SetNFSFlag(AreBothLocationsNFSAware(s.fromTo))
	switch s.fromTo {
	case common.EFromTo.Unknown():
		return fmt.Errorf("unable to infer the source '%s' / destination '%s'. ", src, dst)
	case common.EFromTo.LocalBlob(), common.EFromTo.LocalFile(), common.EFromTo.LocalBlobFS(), common.EFromTo.LocalFileNFS():
		s.source = common.ResourceString{Value: common.ToExtendedPath(common.CleanLocalPath(src))}
		s.destination, err = traverser.SplitResourceString(dst, s.fromTo.To())
		if err != nil {
			return err
		}
	case common.EFromTo.BlobLocal(), common.EFromTo.FileLocal(), common.EFromTo.BlobFSLocal(), common.EFromTo.FileNFSLocal():
		s.source, err = traverser.SplitResourceString(src, s.fromTo.From())
		if err != nil {
			return err
		}
		s.destination = common.ResourceString{Value: common.ToExtendedPath(common.CleanLocalPath(dst))}
	case common.EFromTo.BlobBlob(), common.EFromTo.FileFile(), common.EFromTo.FileNFSFileNFS(), common.EFromTo.BlobFile(), common.EFromTo.FileBlob(), common.EFromTo.BlobFSBlobFS(), common.EFromTo.BlobFSBlob(), common.EFromTo.BlobFSFile(), common.EFromTo.BlobBlobFS(), common.EFromTo.FileBlobFS():
		s.source, err = traverser.SplitResourceString(src, s.fromTo.From())
		if err != nil {
			return err
		}
		s.destination, err = traverser.SplitResourceString(dst, s.fromTo.To())
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("source '%s' / destination '%s' combination '%s' not supported for sync command ", src, dst, s.fromTo)
	}
	return nil
}

func (s *CookedSyncOptions) applyDefaultsAndInferOptions(opts SyncOptions) (err error) {
	// defaults
	s.recursive = common.IffNil(opts.Recursive, true)
	s.preserveInfo = common.IffNil(opts.PreserveInfo, GetPreserveInfoDefault(opts.FromTo))
	common.LocalHashStorageMode = common.IffNil(opts.LocalHashStorageMode, common.EHashStorageMode.Default())
	s.s2SPreserveAccessTier = common.IffNil(opts.S2SPreserveAccessTier, true)

	// 1:1 mappings
	s.includeDirectoryStubs = opts.IncludeDirectoryStubs
	s.preservePosixProperties = opts.PreservePosixProperties
	s.forceIfReadOnly = opts.ForceIfReadOnly
	s.blockSize, err = BlockSizeInBytes(opts.BlockSizeMB)
	if err != nil {
		return err
	}
	s.putBlobSize, err = BlockSizeInBytes(opts.PutBlobSizeMB)
	if err != nil {
		return err
	}
	s.filterOptions = traverser.FilterOptions{
		IncludePatterns:   opts.IncludePatterns,
		ExcludePatterns:   opts.ExcludePatterns,
		ExcludePaths:      opts.ExcludePaths,
		IncludeAttributes: opts.IncludeAttributes,
		ExcludeAttributes: opts.ExcludeAttributes,
		IncludeRegex:      opts.IncludeRegex,
		ExcludeRegex:      opts.ExcludeRegex,
	}
	s.deleteDestination = opts.DeleteDestination
	s.putMd5 = opts.PutMd5
	s.checkMd5 = opts.CheckMd5
	s.s2SPreserveBlobTags = opts.S2SPreserveBlobTags
	s.cpkOptions = common.CpkOptions{
		CpkScopeInfo: opts.CpkByName,
		// Get the key (EncryptionKey and EncryptionKeySHA256) value from environment variables when required.
		CpkInfo: opts.CpkByValue,
		// We only support transfer from source encrypted by user key when user wishes to download.
		// Due to service limitation, S2S transfer is not supported for source encrypted by user key.
		IsSourceEncrypted: opts.FromTo.IsDownload() && (opts.CpkByName != "" || opts.CpkByValue),
	}
	s.mirrorMode = opts.MirrorMode
	s.trailingDot = opts.TrailingDot
	s.includeRoot = opts.IncludeRoot
	s.compareHash = opts.CompareHash
	common.LocalHashDir = opts.HashMetaDir
	preserveOwner := AreBothLocationsNFSAware(s.fromTo)
	s.preservePermissions = common.NewPreservePermissionsOption(opts.PreservePermissions, preserveOwner, s.fromTo)
	s.hardlinks = opts.Hardlinks
	s.dryrun = opts.dryrun
	s.deleteDestinationFileIfNecessary = opts.deleteDestinationFileIfNecessary
	s.commandString = opts.commandString
	s.dryrunJobPartOrderHandler = opts.dryrunJobPartOrderHandler
	s.dryrunDeleteHandler = opts.dryrunDeleteHandler

	// inference
	switch s.compareHash {
	case common.ESyncHashType.MD5():
		// Save any new MD5s on files we download.
		s.putMd5 = true
	default: // no need to put a hash of any kind.
	}

	if !s.fromTo.IsS2S() {
		s.s2SPreserveAccessTier = false
	}

	s.includeDirectoryStubs = (s.fromTo.From().SupportsHnsACLs() && s.fromTo.To().SupportsHnsACLs() && s.preservePermissions.IsTruthy()) || s.includeDirectoryStubs

	if s.trailingDot == common.ETrailingDotOption.Enable() && !s.fromTo.BothSupportTrailingDot() {
		s.trailingDot = common.ETrailingDotOption.Disable()
	}

	return nil
}

// it is assume that the given url has the SAS stripped, and safe to print
func validateURLIsNotServiceLevel(url string, location common.Location) error {
	srcLevel, err := DetermineLocationLevel(url, location, true)
	if err != nil {
		return err
	}

	if srcLevel == ELocationLevel.Service() {
		return fmt.Errorf("service level URLs (%s) are not supported in sync: ", url)
	}

	return nil
}

func (s *CookedSyncOptions) validateOptions() (err error) {
	// we do not support service level sync yet
	if s.fromTo.From().IsRemote() {
		err = validateURLIsNotServiceLevel(s.source.Value, s.fromTo.From())
		if err != nil {
			return err
		}
	}

	if s.fromTo.To().IsRemote() {
		err = validateURLIsNotServiceLevel(s.destination.Value, s.fromTo.To())
		if err != nil {
			return err
		}
	}

	// verify that remote URLs are resolvable
	if err := common.VerifyIsURLResolvable(s.source.Value); s.fromTo.From().IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve source: %w", err)
	}

	if err := common.VerifyIsURLResolvable(s.destination.Value); s.fromTo.To().IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve destination: %w", err)
	}

	// Check if destination is system container
	if s.fromTo.IsS2S() || s.fromTo.IsUpload() {
		dstContainerName, err := GetContainerName(s.destination.Value, s.fromTo.To())
		if err != nil {
			return fmt.Errorf("failed to get container name from destination (is it formatted correctly?)")
		}
		if common.IsSystemContainer(dstContainerName) {
			return fmt.Errorf("cannot copy to system container '%s'", dstContainerName)
		}
	}

	if err = ValidateForceIfReadOnly(s.forceIfReadOnly, s.fromTo); err != nil {
		return err
	}

	if AreBothLocationsNFSAware(s.fromTo) {
		err = PerformNFSSpecificValidation(s.fromTo, s.preservePermissions, s.preserveInfo, common.ESymlinkHandlingType.Skip(), s.hardlinks)
		if err != nil {
			return err
		}
	} else {
		err = PerformSMBSpecificValidation(s.fromTo, s.preservePermissions, s.preserveInfo, s.preservePosixProperties)
		if err != nil {
			return err
		}
	}

	if err = ValidatePutMd5(s.putMd5, s.fromTo); err != nil {
		return err
	}

	if err = ValidateMd5Option(s.checkMd5, s.fromTo); err != nil {
		return err
	}

	// Check if user has provided `s2s-preserve-blob-tags` flag.
	// If yes, we have to ensure that both source and destination must be blob storage.
	if s.s2SPreserveBlobTags && (s.fromTo.From() != common.ELocation.Blob() || s.fromTo.To() != common.ELocation.Blob()) {
		return fmt.Errorf("either source or destination is not a blob storage. " +
			"blob index tags is a property of blobs only therefore both source and destination must be blob storage")
	}

	if s.cpkOptions.CpkScopeInfo != "" && s.cpkOptions.CpkInfo {
		return errors.New("cannot use both cpk-by-name and cpk-by-value at the same time")
	}

	return nil
}

var LocalToFileShareWarnMsg = "AzCopy sync is supported but not fully recommended for Azure Files. AzCopy sync doesn't support differential copies at scale, and some file fidelity might be lost."
