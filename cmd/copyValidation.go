package cmd

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// Note: blockSize, blobTagsMap is set here
func (cooked *CookedCopyCmdArgs) validate() (err error) {
	if err = validateForceIfReadOnly(cooked.ForceIfReadOnly, cooked.FromTo); err != nil {
		return err
	}

	if err = validateSymlinkHandlingMode(cooked.SymlinkHandling, cooked.FromTo); err != nil {
		return err
	}

	allowAutoDecompress := cooked.FromTo == common.EFromTo.BlobLocal() || cooked.FromTo == common.EFromTo.FileLocal() || cooked.FromTo == common.EFromTo.FileNFSLocal()
	if cooked.autoDecompress && !allowAutoDecompress {
		return errors.New("automatic decompression is only supported for downloads from Blob and Azure Files") // as at Sept 2019, our ADLS Gen 2 Swagger does not include content-encoding for directory (path) listings so we can't support it there
	}

	cooked.blockSize, err = blockSizeInBytes(cooked.BlockSizeMB)
	if err != nil {
		return err
	}

	// If the given blobType is AppendBlob, block-size-mb should not be greater than
	// common.MaxAppendBlobBlockSize.
	if cooked.blobType == common.EBlobType.AppendBlob() && cooked.blockSize > common.MaxAppendBlobBlockSize {
		return fmt.Errorf("block size cannot be greater than %dMB for AppendBlob blob type", common.MaxAppendBlobBlockSize/common.MegaByte)
	}

	if (len(cooked.IncludePatterns) > 0 || len(cooked.ExcludePatterns) > 0) && cooked.FromTo == common.EFromTo.BlobFSTrash() {
		return fmt.Errorf("include/exclude flags are not supported for this destination")
		// note there's another, more rigorous check, in removeBfsResources()
	}

	// warn on exclude unsupported wildcards here. Include have to be later, to cover list-of-files
	warnIfAnyHasWildcard(excludeWarningOncer, "exclude-path", cooked.ExcludePathPatterns)

	// A combined implementation reduces the amount of code duplication present.
	// However, it _does_ increase the amount of code-intertwining present.
	if cooked.ListOfFiles != "" && len(cooked.IncludePathPatterns) > 0 {
		return errors.New("cannot combine list of files and include path")
	}

	if cooked.FromTo.To() == common.ELocation.None() && strings.EqualFold(cooked.metadata, common.MetadataAndBlobTagsClearFlag) { // in case of Blob, BlobFS and Files
		glcm.Warn("*** WARNING *** Metadata will be cleared because of input --metadata=clear ")
	}
	if err = validateMetadataString(cooked.metadata); err != nil {
		return err
	}

	if !(cooked.FromTo.To() == common.ELocation.Blob() || cooked.FromTo == common.EFromTo.BlobNone() || cooked.FromTo != common.EFromTo.BlobFSNone()) && cooked.blobTags != "" {
		return errors.New("blob tags can only be set when transferring to blob storage")
	}
	if cooked.FromTo.To() == common.ELocation.None() && strings.EqualFold(cooked.blobTags, common.MetadataAndBlobTagsClearFlag) { // in case of Blob and BlobFS
		glcm.Warn("*** WARNING *** BlobTags will be cleared because of input --blob-tags=clear ")
	}

	cooked.blobTagsMap = common.ToCommonBlobTagsMap(cooked.blobTags)
	err = validateBlobTagsKeyValue(cooked.blobTagsMap)
	if err != nil {
		return err
	}

	// Check if user has provided `s2s-preserve-blob-tags` flag. If yes, we have to ensure that
	// 1. Both source and destination must be blob storages.
	// 2. `blob-tags` is not present as they create conflicting scenario of whether to preserve blob tags from the source or set user defined tags on the destination
	if cooked.S2sPreserveBlobTags {
		if cooked.FromTo.From() != common.ELocation.Blob() || cooked.FromTo.To() != common.ELocation.Blob() {
			return errors.New("either source or destination is not a blob storage. blob index tags is a property of blobs only therefore both source and destination must be blob storage")
		} else if cooked.blobTags != "" {
			return errors.New("both s2s-preserve-blob-tags and blob-tags flags cannot be used in conjunction")
		}
	}

	if cooked.cpkByName != "" && cooked.cpkByValue {
		return errors.New("cannot use both cpk-by-name and cpk-by-value at the same time")
	}

	if cooked.cpkByName != "" || cooked.cpkByValue {
		destUrl, _ := url.Parse(cooked.Destination.Value)
		if strings.Contains(destUrl.Host, "dfs.core.windows.net") {
			return errors.New("client provided keys (CPK) based encryption is only supported with blob endpoints (blob.core.windows.net)")
		}
	}

	if cooked.FromTo.IsNFS() {
		if err := performNFSSpecificValidation(
			cooked.FromTo,
			cooked.preservePermissions,
			cooked.preserveInfo,
			&cooked.hardlinks,
			cooked.SymlinkHandling); err != nil {
			return err
		}
	} else {
		if err := performSMBSpecificValidation(
			cooked.FromTo, cooked.preservePermissions, cooked.preserveInfo,
			cooked.preservePOSIXProperties, cooked.SymlinkHandling); err != nil {
			return err
		}

		if err = validatePreserveOwner(cooked.preserveOwner, cooked.FromTo); err != nil {
			return err
		}
	}

	if err = validateBackupMode(cooked.backupMode, cooked.FromTo); err != nil {
		return err
	}

	// check for the flag value relative to fromTo location type
	// Example1: for Local to Blob, preserve-last-modified-time flag should not be set to true
	// Example2: for Blob to Local, follow-symlinks, blob-tier flags should not be provided with values.
	switch cooked.FromTo {
	case common.EFromTo.LocalBlobFS():
		if cooked.blobType != common.EBlobType.Detect() {
			return fmt.Errorf("blob-type is not supported on ADLS Gen 2")
		}
		if cooked.preserveLastModifiedTime {
			return fmt.Errorf("preserve-last-modified-time is not supported while uploading")
		}
		if cooked.blockBlobTier != common.EBlockBlobTier.None() ||
			cooked.pageBlobTier != common.EPageBlobTier.None() {
			return fmt.Errorf("blob-tier is not supported while uploading to ADLS Gen 2")
		}
		if cooked.preservePermissions.IsTruthy() {
			return fmt.Errorf("preserve-permissions is not supported while uploading to ADLS Gen 2")
		}
		if cooked.s2sPreserveProperties.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-properties is not supported while uploading")
		}
		if cooked.s2sPreserveAccessTier.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-access-tier is not supported while uploading")
		}
		if cooked.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return fmt.Errorf("s2s-handle-invalid-metadata is not supported while uploading")
		}
		if cooked.s2sSourceChangeValidation {
			return fmt.Errorf("s2s-detect-source-changed is not supported while uploading")
		}
	case common.EFromTo.LocalBlob():
		if cooked.preserveLastModifiedTime {
			return fmt.Errorf("preserve-last-modified-time is not supported while uploading to Blob Storage")
		}
		if cooked.s2sPreserveProperties.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-properties is not supported while uploading to Blob Storage")
		}
		if cooked.s2sPreserveAccessTier.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-access-tier is not supported while uploading to Blob Storage")
		}
		if cooked.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return fmt.Errorf("s2s-handle-invalid-metadata is not supported while uploading to Blob Storage")
		}
		if cooked.s2sSourceChangeValidation {
			return fmt.Errorf("s2s-detect-source-changed is not supported while uploading to Blob Storage")
		}
	case common.EFromTo.LocalFile(), common.EFromTo.LocalFileNFS():
		if cooked.preserveLastModifiedTime {
			return fmt.Errorf("preserve-last-modified-time is not supported while uploading")
		}
		if cooked.blockBlobTier != common.EBlockBlobTier.None() ||
			cooked.pageBlobTier != common.EPageBlobTier.None() {
			return fmt.Errorf("blob-tier is not supported while uploading to Azure File")
		}
		if cooked.s2sPreserveProperties.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-properties is not supported while uploading")
		}
		if cooked.s2sPreserveAccessTier.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-access-tier is not supported while uploading")
		}
		if cooked.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return fmt.Errorf("s2s-handle-invalid-metadata is not supported while uploading")
		}
		if cooked.s2sSourceChangeValidation {
			return fmt.Errorf("s2s-detect-source-changed is not supported while uploading")
		}
		if cooked.blobType != common.EBlobType.Detect() {
			return fmt.Errorf("blob-type is not supported on Azure File")
		}
	case common.EFromTo.BlobLocal(),
		common.EFromTo.FileLocal(),
		common.EFromTo.FileNFSLocal(),
		common.EFromTo.BlobFSLocal():
		if cooked.SymlinkHandling.Follow() {
			return fmt.Errorf("follow-symlinks flag is not supported while downloading")
		}
		if cooked.blockBlobTier != common.EBlockBlobTier.None() ||
			cooked.pageBlobTier != common.EPageBlobTier.None() {
			return fmt.Errorf("blob-tier is not supported while downloading")
		}
		if cooked.noGuessMimeType {
			return fmt.Errorf("no-guess-mime-type is not supported while downloading")
		}
		if len(cooked.contentType) > 0 || len(cooked.contentEncoding) > 0 || len(cooked.contentLanguage) > 0 || len(cooked.contentDisposition) > 0 || len(cooked.cacheControl) > 0 || len(cooked.metadata) > 0 {
			return fmt.Errorf("content-type, content-encoding, content-language, content-disposition, cache-control, or metadata is not supported while downloading")
		}
		if cooked.s2sPreserveProperties.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-properties is not supported while downloading")
		}
		if cooked.s2sPreserveAccessTier.ValueToValidate() {
			return fmt.Errorf("s2s-preserve-access-tier is not supported while downloading")
		}
		if cooked.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return fmt.Errorf("s2s-handle-invalid-metadata is not supported while downloading")
		}
		if cooked.s2sSourceChangeValidation {
			return fmt.Errorf("s2s-detect-source-changed is not supported while downloading")
		}
	case common.EFromTo.BlobFile(),
		common.EFromTo.S3Blob(),
		common.EFromTo.BlobBlob(),
		common.EFromTo.FileBlob(),
		common.EFromTo.FileFile(),
		common.EFromTo.GCPBlob(),
		common.EFromTo.FileNFSFileNFS():

		if cooked.preserveLastModifiedTime {
			return fmt.Errorf("preserve-last-modified-time is not supported while copying from service to service")
		}
		if cooked.SymlinkHandling.Follow() {
			return fmt.Errorf("follow-symlinks flag is not supported while copying from service to service")
		}
		// blob type is not supported if destination is not blob
		if cooked.blobType != common.EBlobType.Detect() && cooked.FromTo.To() != common.ELocation.Blob() {
			return fmt.Errorf("blob-type is not supported for the scenario (%s)", cooked.FromTo.String())
		}

		// Setting blob tier is supported only when destination is a blob storage. Disabling it for all the other transfer scenarios.
		if (cooked.blockBlobTier != common.EBlockBlobTier.None() || cooked.pageBlobTier != common.EPageBlobTier.None()) &&
			cooked.FromTo.To() != common.ELocation.Blob() {
			return fmt.Errorf("blob-tier is not supported for the scenario (%s)", cooked.FromTo.String())
		}
		if cooked.noGuessMimeType {
			return fmt.Errorf("no-guess-mime-type is not supported while copying from service to service")
		}
		if len(cooked.contentType) > 0 || len(cooked.contentEncoding) > 0 || len(cooked.contentLanguage) > 0 || len(cooked.contentDisposition) > 0 || len(cooked.cacheControl) > 0 || len(cooked.metadata) > 0 {
			return fmt.Errorf("content-type, content-encoding, content-language, content-disposition, cache-control, or metadata is not supported while copying from service to service")
		}
	}

	if err = validatePutMd5(cooked.putMd5, cooked.FromTo); err != nil {
		return err
	}
	if err = validateMd5Option(cooked.md5ValidationOption, cooked.FromTo); err != nil {
		return err
	}
	if (len(cooked.IncludeFileAttributes) > 0 || len(cooked.ExcludeFileAttributes) > 0) && cooked.FromTo.From() != common.ELocation.Local() {
		return errors.New("cannot check file attributes on remote objects")
	}

	if OutputLevel == common.EOutputVerbosity.Quiet() || OutputLevel == common.EOutputVerbosity.Essential() {
		if cooked.ForceWrite == common.EOverwriteOption.Prompt() {
			err = fmt.Errorf("cannot set output level '%s' with overwrite option '%s'", OutputLevel.String(), cooked.ForceWrite.String())
		} else if cooked.dryrunMode {
			err = fmt.Errorf("cannot set output level '%s' with dry-run mode", OutputLevel.String())
		}
	}
	if err != nil {
		return err
	}

	return nil
}
