package azcopy

import (
	"errors"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

// TODO : This will be made internal as we refactor more of the sync code.
type Syncer struct {

	// TODO : Make all these internal as we refactor more of the sync code.
	// defaulted, inferred, processed and validated user options
	Source      common.ResourceString
	Destination common.ResourceString
	FromTo      common.FromTo

	Recursive               bool
	IncludeDirectoryStubs   bool
	PreserveInfo            bool
	PreservePosixProperties bool
	ForceIfReadOnly         bool
	BlockSize               int64
	PutBlobSize             int64
	FilterOptions           traverser.FilterOptions
	DeleteDestination       common.DeleteDestination
	PutMd5                  bool                        // TODO: (gapra) Should we make this an enum called PutHash for None/MD5? So user can set the HashType?
	CheckMd5                common.HashValidationOption // TODO (gapra) Same comment as above
	S2SPreserveAccessTier   bool
	S2SPreserveBlobTags     bool
	CpkOptions              common.CpkOptions
	MirrorMode              bool
	TrailingDot             common.TrailingDotOption
	IncludeRoot             bool
	CompareHash             common.SyncHashType
	PreservePermissions     common.PreservePermissionsOption
	Hardlinks               common.HardlinkHandlingType

	Dryrun                           bool
	DeleteDestinationFileIfNecessary bool
}

func newSyncer(src, dst string, opts SyncOptions) (s *Syncer, err error) {
	s = &Syncer{}

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
	s.processOptions()

	return s, nil
}

func (s *Syncer) applyFromToSrcDest(src, dst string, fromTo common.FromTo) (err error) {
	// fromTo, source and destination
	userFromTo := common.Iff(fromTo == common.EFromTo.Unknown(), "", fromTo.String())
	s.FromTo, err = InferAndValidateFromTo(src, dst, userFromTo)
	if err != nil {
		return err
	}
	common.SetNFSFlag(AreBothLocationsNFSAware(s.FromTo))
	switch s.FromTo {
	case common.EFromTo.Unknown():
		return fmt.Errorf("unable to infer the source '%s' / destination '%s'. ", src, dst)
	case common.EFromTo.LocalBlob(), common.EFromTo.LocalFile(), common.EFromTo.LocalBlobFS(), common.EFromTo.LocalFileNFS():
		s.Source = common.ResourceString{Value: common.ToExtendedPath(common.CleanLocalPath(src))}
		s.Destination, err = traverser.SplitResourceString(dst, s.FromTo.To())
		if err != nil {
			return err
		}
	case common.EFromTo.BlobLocal(), common.EFromTo.FileLocal(), common.EFromTo.BlobFSLocal(), common.EFromTo.FileNFSLocal():
		s.Source, err = traverser.SplitResourceString(src, s.FromTo.From())
		if err != nil {
			return err
		}
		s.Destination = common.ResourceString{Value: common.ToExtendedPath(common.CleanLocalPath(dst))}
	case common.EFromTo.BlobBlob(), common.EFromTo.FileFile(), common.EFromTo.FileNFSFileNFS(), common.EFromTo.BlobFile(), common.EFromTo.FileBlob(), common.EFromTo.BlobFSBlobFS(), common.EFromTo.BlobFSBlob(), common.EFromTo.BlobFSFile(), common.EFromTo.BlobBlobFS(), common.EFromTo.FileBlobFS():
		s.Source, err = traverser.SplitResourceString(src, s.FromTo.From())
		if err != nil {
			return err
		}
		s.Destination, err = traverser.SplitResourceString(dst, s.FromTo.To())
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("source '%s' / destination '%s' combination '%s' not supported for sync command ", src, dst, s.FromTo)
	}
	return nil
}

func (s *Syncer) applyDefaultsAndInferOptions(opts SyncOptions) (err error) {
	// defaults
	s.Recursive = common.IffNil(opts.Recursive, true)
	s.PreserveInfo = common.IffNil(opts.PreserveInfo, GetPreserveInfoDefault(opts.FromTo))
	common.LocalHashStorageMode = common.IffNil(opts.LocalHashStorageMode, common.EHashStorageMode.Default())
	s.S2SPreserveAccessTier = common.IffNil(opts.S2SPreserveAccessTier, true)

	// 1:1 mappings
	s.IncludeDirectoryStubs = opts.IncludeDirectoryStubs
	s.PreservePosixProperties = opts.PreservePosixProperties
	s.ForceIfReadOnly = opts.ForceIfReadOnly
	s.BlockSize, err = BlockSizeInBytes(opts.BlockSizeMB)
	if err != nil {
		return err
	}
	s.PutBlobSize, err = BlockSizeInBytes(opts.PutBlobSizeMB)
	if err != nil {
		return err
	}
	s.FilterOptions = traverser.FilterOptions{
		IncludePatterns:   opts.IncludePatterns,
		ExcludePatterns:   opts.ExcludePatterns,
		ExcludePaths:      opts.ExcludePaths,
		IncludeAttributes: opts.IncludeAttributes,
		ExcludeAttributes: opts.ExcludeAttributes,
		IncludeRegex:      opts.IncludeRegex,
		ExcludeRegex:      opts.ExcludeRegex,
	}
	s.DeleteDestination = opts.DeleteDestination
	s.PutMd5 = opts.PutMd5
	s.CheckMd5 = opts.CheckMd5
	s.S2SPreserveBlobTags = opts.S2SPreserveBlobTags
	s.CpkOptions = common.CpkOptions{
		CpkScopeInfo: opts.CpkByName,
		// Get the key (EncryptionKey and EncryptionKeySHA256) value from environment variables when required.
		CpkInfo: opts.CpkByValue,
		// We only support transfer from source encrypted by user key when user wishes to download.
		// Due to service limitation, S2S transfer is not supported for source encrypted by user key.
		IsSourceEncrypted: opts.FromTo.IsDownload() && (opts.CpkByName != "" || opts.CpkByValue),
	}
	s.MirrorMode = opts.MirrorMode
	s.TrailingDot = opts.TrailingDot
	s.IncludeRoot = opts.IncludeRoot
	s.CompareHash = opts.CompareHash
	common.LocalHashDir = opts.HashMetaDir
	preserveOwner := AreBothLocationsNFSAware(s.FromTo)
	s.PreservePermissions = common.NewPreservePermissionsOption(opts.PreservePermissions, preserveOwner, s.FromTo)
	s.Hardlinks = opts.Hardlinks
	s.Dryrun = opts.dryrun
	s.DeleteDestinationFileIfNecessary = opts.deleteDestinationFileIfNecessary

	// inference
	switch s.CompareHash {
	case common.ESyncHashType.MD5():
		// Save any new MD5s on files we download.
		s.PutMd5 = true
	default: // no need to put a hash of any kind.
	}

	if !s.FromTo.IsS2S() {
		s.S2SPreserveAccessTier = false
	}

	s.IncludeDirectoryStubs = (s.FromTo.From().SupportsHnsACLs() && s.FromTo.To().SupportsHnsACLs() && s.PreservePermissions.IsTruthy()) || s.IncludeDirectoryStubs

	if s.TrailingDot == common.ETrailingDotOption.Enable() && !s.FromTo.BothSupportTrailingDot() {
		s.TrailingDot = common.ETrailingDotOption.Disable()
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

func (s *Syncer) validateOptions() (err error) {
	// we do not support service level sync yet
	if s.FromTo.From().IsRemote() {
		err = validateURLIsNotServiceLevel(s.Source.Value, s.FromTo.From())
		if err != nil {
			return err
		}
	}

	if s.FromTo.To().IsRemote() {
		err = validateURLIsNotServiceLevel(s.Destination.Value, s.FromTo.To())
		if err != nil {
			return err
		}
	}

	// verify that remote URLs are resolvable
	if err := common.VerifyIsURLResolvable(s.Source.Value); s.FromTo.From().IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve source: %w", err)
	}

	if err := common.VerifyIsURLResolvable(s.Destination.Value); s.FromTo.To().IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve destination: %w", err)
	}

	// Check if destination is system container
	if s.FromTo.IsS2S() || s.FromTo.IsUpload() {
		dstContainerName, err := GetContainerName(s.Destination.Value, s.FromTo.To())
		if err != nil {
			return fmt.Errorf("failed to get container name from destination (is it formatted correctly?)")
		}
		if common.IsSystemContainer(dstContainerName) {
			return fmt.Errorf("cannot copy to system container '%s'", dstContainerName)
		}
	}

	if err = ValidateForceIfReadOnly(s.ForceIfReadOnly, s.FromTo); err != nil {
		return err
	}

	if AreBothLocationsNFSAware(s.FromTo) {
		err = PerformNFSSpecificValidation(s.FromTo, s.PreservePermissions, s.PreserveInfo, common.ESymlinkHandlingType.Skip(), s.Hardlinks)
		if err != nil {
			return err
		}
	} else {
		err = PerformSMBSpecificValidation(s.FromTo, s.PreservePermissions, s.PreserveInfo, s.PreservePosixProperties)
		if err != nil {
			return err
		}
	}

	if err = ValidatePutMd5(s.PutMd5, s.FromTo); err != nil {
		return err
	}

	if err = ValidateMd5Option(s.CheckMd5, s.FromTo); err != nil {
		return err
	}

	// Check if user has provided `s2s-preserve-blob-tags` flag.
	// If yes, we have to ensure that both source and destination must be blob storage.
	if s.S2SPreserveBlobTags && (s.FromTo.From() != common.ELocation.Blob() || s.FromTo.To() != common.ELocation.Blob()) {
		return fmt.Errorf("either source or destination is not a blob storage. " +
			"blob index tags is a property of blobs only therefore both source and destination must be blob storage")
	}

	if s.CpkOptions.CpkScopeInfo != "" && s.CpkOptions.CpkInfo {
		return errors.New("cannot use both cpk-by-name and cpk-by-value at the same time")
	}

	return nil
}

func (s *Syncer) processOptions() {

	if s.FromTo == common.EFromTo.LocalFile() {
		common.GetLifecycleMgr().Warn(LocalToFileShareWarnMsg)
		common.LogToJobLogWithPrefix(LocalToFileShareWarnMsg, common.LogWarning)
	}

	if s.CpkOptions.IsSourceEncrypted {
		common.GetLifecycleMgr().Info("Client Provided Key for encryption/decryption is provided for download scenario. " +
			"Assuming source is encrypted.")
	}
}

var LocalToFileShareWarnMsg = "AzCopy sync is supported but not fully recommended for Azure Files. AzCopy sync doesn't support differential copies at scale, and some file fidelity might be lost."
