package cmd

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"net/url"
	"runtime"
	"strings"
)

// Note: blockSize and blobTagsMap are set here
func (cca *CookedCopyCmdArgs) validate() (err error) {
	if err = validateForceIfReadOnly(cca.ForceIfReadOnly, cca.FromTo); err != nil {
		return err
	}
	if err = validateSymlinkHandlingMode(cca.preserveSymlinks, cca.FromTo); err != nil {
		return err
	}
	allowAutoDecompress := cca.FromTo == common.EFromTo.BlobLocal() || cca.FromTo == common.EFromTo.FileLocal()
	if cca.autoDecompress && !allowAutoDecompress {
		return errors.New("automatic decompression is only supported for downloads from Blob and Azure Files") // as at Sept 2019, our ADLS Gen 2 Swagger does not include content-encoding for directory (path) listings so we can't support it there
	}
	// If the given blobType is AppendBlob, block-size-mb should not be greater than
	// common.MaxAppendBlobBlockSize.
	cca.blockSize, err = blockSizeInBytes(cca.BlockSizeMB)
	if err != nil {
		return err
	}
	if cca.blobType == common.EBlobType.AppendBlob() && cca.blockSize > common.MaxAppendBlobBlockSize {
		return fmt.Errorf("block size cannot be greater than %dMB for AppendBlob blob type", common.MaxAppendBlobBlockSize/common.MegaByte)
	}
	if (len(cca.IncludePatterns) > 0 || len(cca.ExcludePatterns) > 0) && cca.FromTo == common.EFromTo.BlobFSTrash() {
		return fmt.Errorf("include/exclude flags are not supported for this destination")
		// note there's another, more rigorous check, in removeBfsResources()
	}
	// warn on exclude unsupported wildcards here. Include have to be later, to cover list-of-files
	warnIfAnyHasWildcard(excludeWarningOncer, "exclude-path", cca.ExcludePathPatterns)

	// A combined implementation reduces the amount of code duplication present.
	// However, it _does_ increase the amount of code-intertwining present.
	if cca.ListOfFiles != "" && len(cca.IncludePathPatterns) > 0 {
		return errors.New("cannot combine list of files and include path")
	}
	if cca.FromTo.To() == common.ELocation.None() && strings.EqualFold(cca.metadata, common.MetadataAndBlobTagsClearFlag) { // in case of Blob, BlobFS and Files
		glcm.Warn("*** WARNING *** Metadata will be cleared because of input --metadata=clear ")
	}
	if err = validateMetadataString(cca.metadata); err != nil {
		return err
	}
	if !(cca.FromTo.To() == common.ELocation.Blob() || cca.FromTo == common.EFromTo.BlobNone() || cca.FromTo != common.EFromTo.BlobFSNone()) && cca.blobTags != "" {
		return errors.New("blob tags can only be set when transferring to blob storage")
	}
	if cca.FromTo.To() == common.ELocation.None() && strings.EqualFold(cca.blobTags, common.MetadataAndBlobTagsClearFlag) { // in case of Blob and BlobFS
		glcm.Warn("*** WARNING *** BlobTags will be cleared because of input --blob-tags=clear ")
	}
	cca.blobTagsMap = common.ToCommonBlobTagsMap(cca.blobTags)
	if err = validateBlobTagsKeyValue(cca.blobTagsMap); err != nil {
		return err
	}
	// Check if user has provided `s2s-preserve-blob-tags` flag. If yes, we have to ensure that
	// 1. Both source and destination must be blob storages.
	// 2. `blob-tags` is not present as they create conflicting scenario of whether to preserve blob tags from the source or set user defined tags on the destination
	if cca.S2sPreserveBlobTags {
		if cca.FromTo.From() != common.ELocation.Blob() || cca.FromTo.To() != common.ELocation.Blob() {
			return errors.New("either source or destination is not a blob storage. blob index tags is a property of blobs only therefore both source and destination must be blob storage")
		} else if cca.blobTags != "" {
			return errors.New("both s2s-preserve-blob-tags and blob-tags flags cannot be used in conjunction")
		}
	}
	if cca.cpkByName != "" && cca.cpkByValue {
		return errors.New("cannot use both cpk-by-name and cpk-by-value at the same time")
	}
	if cca.cpkByName != "" || cca.cpkByValue {
		destUrl, _ := url.Parse(cca.Destination.Value)
		if strings.Contains(destUrl.Host, "dfs.core.windows.net") {
			return errors.New("client provided keys (CPK) based encryption is only supported with blob endpoints (blob.core.windows.net)")
		}
	}
	if cca.preservePOSIXProperties && !areBothLocationsPOSIXAware(cca.FromTo) {
		return fmt.Errorf("in order to use --preserve-posix-properties, both the source and destination must be POSIX-aware (Linux->Blob, Blob->Linux, Blob->Blob)")
	}
	if err = validatePreserveSMBPropertyOption(cca.preserveSMBInfo, cca.FromTo, "preserve-smb-info"); err != nil {
		return err
	}

	if err = validatePreserveSMBPropertyOption(cca.isUserPersistingPermissions, cca.FromTo, PreservePermissionsFlag); err != nil {
		return err
	}
	if err = validatePreserveOwner(cca.PreserveOwner.ValueToValidate(), cca.FromTo); err != nil {
		return err
	}

	if err = validateBackupMode(cca.backupMode, cca.FromTo); err != nil {
		return err
	}

	// check for the flag value relative to fromTo location type
	// Example1: for Local to Blob, preserve-last-modified-time flag should not be set to true
	// Example2: for Blob to Local, follow-symlinks, blob-tier flags should not be provided with values.
	switch cca.FromTo {
	case common.EFromTo.LocalBlobFS():
		if cca.blobType != common.EBlobType.Detect() {
			return fmt.Errorf("blob-type is not supported on ADLS Gen 2")
		}
		if cca.preserveLastModifiedTime {
			return fmt.Errorf("preserve-last-modified-time is not supported while uploading")
		}
		if cca.blockBlobTier != common.EBlockBlobTier.None() ||
			cca.pageBlobTier != common.EPageBlobTier.None() {
			return fmt.Errorf("blob-tier is not supported while uploading to ADLS Gen 2")
		}
		if cca.preservePermissionsOption.IsTruthy() {
			return fmt.Errorf("preserve-smb-permissions is not supported while uploading to ADLS Gen 2")
		}
		if cca.s2sPreserveProperties.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-properties is not supported while uploading")
		}
		if cca.s2sPreserveAccessTier.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-access-tier is not supported while uploading")
		}
		if cca.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return fmt.Errorf("s2s-handle-invalid-metadata is not supported while uploading")
		}
		if cca.s2sSourceChangeValidation {
			return fmt.Errorf("s2s-detect-source-changed is not supported while uploading")
		}
	case common.EFromTo.LocalBlob():
		if cca.preserveLastModifiedTime {
			return fmt.Errorf("preserve-last-modified-time is not supported while uploading to Blob Storage")
		}
		if cca.s2sPreserveProperties.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-properties is not supported while uploading to Blob Storage")
		}
		if cca.s2sPreserveAccessTier.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-access-tier is not supported while uploading to Blob Storage")
		}
		if cca.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return fmt.Errorf("s2s-handle-invalid-metadata is not supported while uploading to Blob Storage")
		}
		if cca.s2sSourceChangeValidation {
			return fmt.Errorf("s2s-detect-source-changed is not supported while uploading to Blob Storage")
		}
	case common.EFromTo.LocalFile():
		if cca.preserveLastModifiedTime {
			return fmt.Errorf("preserve-last-modified-time is not supported while uploading")
		}
		if cca.blockBlobTier != common.EBlockBlobTier.None() ||
			cca.pageBlobTier != common.EPageBlobTier.None() {
			return fmt.Errorf("blob-tier is not supported while uploading to Azure File")
		}
		if cca.s2sPreserveProperties.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-properties is not supported while uploading")
		}
		if cca.s2sPreserveAccessTier.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-access-tier is not supported while uploading")
		}
		if cca.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return fmt.Errorf("s2s-handle-invalid-metadata is not supported while uploading")
		}
		if cca.s2sSourceChangeValidation {
			return fmt.Errorf("s2s-detect-source-changed is not supported while uploading")
		}
		if cca.blobType != common.EBlobType.Detect() {
			return fmt.Errorf("blob-type is not supported on Azure File")
		}
	case common.EFromTo.BlobLocal(),
		common.EFromTo.FileLocal(),
		common.EFromTo.BlobFSLocal():
		if cca.SymlinkHandling.Follow() {
			return fmt.Errorf("follow-symlinks flag is not supported while downloading")
		}
		if cca.blockBlobTier != common.EBlockBlobTier.None() ||
			cca.pageBlobTier != common.EPageBlobTier.None() {
			return fmt.Errorf("blob-tier is not supported while downloading")
		}
		if cca.noGuessMimeType {
			return fmt.Errorf("no-guess-mime-type is not supported while downloading")
		}
		if len(cca.contentType) > 0 || len(cca.contentEncoding) > 0 || len(cca.contentLanguage) > 0 || len(cca.contentDisposition) > 0 || len(cca.cacheControl) > 0 || len(cca.metadata) > 0 {
			return fmt.Errorf("content-type, content-encoding, content-language, content-disposition, cache-control, or metadata is not supported while downloading")
		}
		if cca.s2sPreserveProperties.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-properties is not supported while downloading")
		}
		if cca.s2sPreserveAccessTier.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-access-tier is not supported while downloading")
		}
		if cca.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return fmt.Errorf("s2s-handle-invalid-metadata is not supported while downloading")
		}
		if cca.s2sSourceChangeValidation {
			return fmt.Errorf("s2s-detect-source-changed is not supported while downloading")
		}
	case common.EFromTo.BlobFile(),
		common.EFromTo.S3Blob(),
		common.EFromTo.BlobBlob(),
		common.EFromTo.FileBlob(),
		common.EFromTo.FileFile(),
		common.EFromTo.GCPBlob():
		if cca.preserveLastModifiedTime {
			return fmt.Errorf("preserve-last-modified-time is not supported while copying from service to service")
		}
		if cca.SymlinkHandling.Follow() {
			return fmt.Errorf("follow-symlinks flag is not supported while copying from service to service")
		}
		// blob type is not supported if destination is not blob
		if cca.blobType != common.EBlobType.Detect() && cca.FromTo.To() != common.ELocation.Blob() {
			return fmt.Errorf("blob-type is not supported for the scenario (%s)", cca.FromTo.String())
		}

		// Setting blob tier is supported only when destination is a blob storage. Disabling it for all the other transfer scenarios.
		if (cca.blockBlobTier != common.EBlockBlobTier.None() || cca.pageBlobTier != common.EPageBlobTier.None()) &&
			cca.FromTo.To() != common.ELocation.Blob() {
			return fmt.Errorf("blob-tier is not supported for the scenario (%s)", cca.FromTo.String())
		}
		if cca.noGuessMimeType {
			return fmt.Errorf("no-guess-mime-type is not supported while copying from service to service")
		}
		if len(cca.contentType) > 0 || len(cca.contentEncoding) > 0 || len(cca.contentLanguage) > 0 || len(cca.contentDisposition) > 0 || len(cca.cacheControl) > 0 || len(cca.metadata) > 0 {
			return fmt.Errorf("content-type, content-encoding, content-language, content-disposition, cache-control, or metadata is not supported while copying from service to service")
		}
	}

	if err = validatePutMd5(cca.putMd5, cca.FromTo); err != nil {
		return err
	}
	if err = validateMd5Option(cca.md5ValidationOption, cca.FromTo); err != nil {
		return err
	}

	if (len(cca.IncludeFileAttributes) > 0 || len(cca.ExcludeFileAttributes) > 0) && cca.FromTo.From() != common.ELocation.Local() {
		return errors.New("cannot check file attributes on remote objects")
	}
	if azcopyOutputVerbosity == common.EOutputVerbosity.Quiet() || azcopyOutputVerbosity == common.EOutputVerbosity.Essential() {
		if cca.ForceWrite == common.EOverwriteOption.Prompt() {
			return fmt.Errorf("cannot set output level '%s' with overwrite option '%s'", azcopyOutputVerbosity.String(), cca.ForceWrite.String())
		} else if cca.dryrunMode {
			return fmt.Errorf("cannot set output level '%s' with dry-run mode", azcopyOutputVerbosity.String())
		}
	}

	return nil
}

func validateForceIfReadOnly(toForce bool, fromTo common.FromTo) error {
	targetIsFiles := fromTo.To() == common.ELocation.File() ||
		fromTo == common.EFromTo.FileTrash()
	targetIsWindowsFS := fromTo.To() == common.ELocation.Local() &&
		runtime.GOOS == "windows"
	targetIsOK := targetIsFiles || targetIsWindowsFS
	if toForce && !targetIsOK {
		return errors.New("force-if-read-only is only supported when the target is Azure Files or a Windows file system")
	}
	return nil
}
func validatePreserveSMBPropertyOption(toPreserve bool, fromTo common.FromTo, flagName string) error {
	if toPreserve && flagName == PreservePermissionsFlag && (fromTo == common.EFromTo.BlobBlob() || fromTo == common.EFromTo.BlobFSBlob() || fromTo == common.EFromTo.BlobBlobFS() || fromTo == common.EFromTo.BlobFSBlobFS()) {
		// the user probably knows what they're doing if they're trying to persist permissions between blob-type endpoints.
		return nil
	} else if toPreserve && !(fromTo == common.EFromTo.LocalFile() ||
		fromTo == common.EFromTo.FileLocal() ||
		fromTo == common.EFromTo.FileFile()) {
		return fmt.Errorf("%s is set but the job is not between %s-aware resources", flagName, common.Iff(flagName == PreservePermissionsFlag, "permission", "SMB"))
	}

	if toPreserve && (fromTo.IsUpload() || fromTo.IsDownload()) &&
		runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		return fmt.Errorf("%s is set but persistence for up/downloads is supported only in Windows and Linux", flagName)
	}

	return nil
}

func validatePreserveOwner(preserve bool, fromTo common.FromTo) error {
	if fromTo.IsDownload() {
		return nil // it can be used in downloads
	}
	if preserve != common.PreserveOwnerDefault {
		return fmt.Errorf("flag --%s can only be used on downloads", common.PreserveOwnerFlagName)
	}
	return nil
}

func validateSymlinkHandlingMode(preserveSymlinks bool, fromTo common.FromTo) error {
	if preserveSymlinks {
		switch fromTo {
		case common.EFromTo.LocalBlob(), common.EFromTo.BlobLocal(), common.EFromTo.BlobFSLocal(), common.EFromTo.LocalBlobFS():
			return nil // Fine on all OSes that support symlink via the OS package. (Win, MacOS, and Linux do, and that's what we officially support.)
		case common.EFromTo.BlobBlob(), common.EFromTo.BlobFSBlobFS(), common.EFromTo.BlobBlobFS(), common.EFromTo.BlobFSBlob():
			return nil // Blob->Blob doesn't involve any local requirements
		default:
			return fmt.Errorf("flag --%s can only be used on Blob<->Blob or Local<->Blob", common.PreserveSymlinkFlagName)
		}
	}

	return nil // other older symlink handling modes can work on all OSes
}

func validateBackupMode(backupMode bool, fromTo common.FromTo) error {
	if !backupMode {
		return nil
	}
	if runtime.GOOS != "windows" {
		return errors.New(common.BackupModeFlagName + " mode is only supported on Windows")
	}
	if fromTo.IsUpload() || fromTo.IsDownload() {
		return nil
	} else {
		return errors.New(common.BackupModeFlagName + " mode is only supported for uploads and downloads")
	}
}

func validatePutMd5(putMd5 bool, fromTo common.FromTo) error {
	// In case of S2S transfers, log info message to inform the users that MD5 check doesn't work for S2S Transfers.
	// This is because we cannot calculate MD5 hash of the data stored at a remote locations.
	if putMd5 && fromTo.IsS2S() {
		glcm.Info(" --put-md5 flag to check data consistency between source and destination is not applicable for S2S Transfers (i.e. When both the source and the destination are remote). AzCopy cannot compute MD5 hash of data stored at remote location.")
	}
	return nil
}

func validateMd5Option(option common.HashValidationOption, fromTo common.FromTo) error {
	hasMd5Validation := option != common.DefaultHashValidationOption
	if hasMd5Validation && !fromTo.IsDownload() {
		return fmt.Errorf("check-md5 is set but the job is not a download")
	}
	return nil
}

// Valid tag key and value characters include:
// 1. Lowercase and uppercase letters (a-z, A-Z)
// 2. Digits (0-9)
// 3. A space ( )
// 4. Plus (+), minus (-), period (.), solidus (/), colon (:), equals (=), and underscore (_)
func isValidBlobTagsKeyValue(keyVal string) bool {
	for _, c := range keyVal {
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == ' ' || c == '+' ||
			c == '-' || c == '.' || c == '/' || c == ':' || c == '=' || c == '_') {
			return false
		}
	}
	return true
}

// ValidateBlobTagsKeyValue
// The tag set may contain at most 10 tags. Tag keys and values are case sensitive.
// Tag keys must be between 1 and 128 characters, and tag values must be between 0 and 256 characters.
func validateBlobTagsKeyValue(bt common.BlobTags) error {
	if len(bt) > 10 {
		return errors.New("at-most 10 tags can be associated with a blob")
	}
	for k, v := range bt {
		key, err := url.QueryUnescape(k)
		if err != nil {
			return err
		}
		value, err := url.QueryUnescape(v)
		if err != nil {
			return err
		}

		if key == "" || len(key) > 128 || len(value) > 256 {
			return errors.New("tag keys must be between 1 and 128 characters, and tag values must be between 0 and 256 characters")
		}

		if !isValidBlobTagsKeyValue(key) {
			return errors.New("incorrect character set used in key: " + k)
		}

		if !isValidBlobTagsKeyValue(value) {
			return errors.New("incorrect character set used in value: " + v)
		}
	}
	return nil
}

func validateMetadataString(metadata string) error {
	if strings.EqualFold(metadata, common.MetadataAndBlobTagsClearFlag) {
		return nil
	}
	metadataMap, err := common.StringToMetadata(metadata)
	if err != nil {
		return err
	}
	for k := range metadataMap {
		if strings.ContainsAny(k, " !#$%^&*,<>{}|\\:.()+'\"?/") {
			return fmt.Errorf("invalid metadata key value '%s': can't have spaces or special characters", k)
		}
	}

	return nil
}
