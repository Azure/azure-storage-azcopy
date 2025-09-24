package azcopy

import (
	"bufio"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

const (
	PreservePermissionsDisabledMsg    = "Note: The preserve-permissions flag is set to false. As a result, AzCopy will not copy SMB ACLs between the source and destination. For more information, visit: https://aka.ms/AzCopyandAzureFiles."
	PreserveNFSPermissionsDisabledMsg = "Note: The preserve-permissions flag is set to false. As a result, AzCopy will not copy NFS permissions between the source and destination."
)

// boolDefaultTrue represents a boolean parameter that defaults to true
// unless explicitly set by the user.
//
// Example:
//
//	// Case 1: not set → defaults to true
//	var a boolDefaultTrue
//	fmt.Println(a.Get())      // true
//	fmt.Println(a.GetIfSet()) // false, false (not explicitly set)
//
//	// Case 2: explicitly set to false
//	v := false
//	b := boolDefaultTrue{value: &v}
//	fmt.Println(b.Get())      // false
//	fmt.Println(b.GetIfSet()) // false, true (explicitly set)
//
//	// Case 3: explicitly set to true
//	v2 := true
//	c := boolDefaultTrue{value: &v2}
//	fmt.Println(c.Get())      // true
//	fmt.Println(c.GetIfSet()) // true, true
//
// This is useful in configuration or CLI parsing where:
//   - The default behavior is "enabled" (true).
//   - Validation rules should only apply if the user explicitly provides a value.
type boolDefaultTrue struct {
	value *bool
}

func (b boolDefaultTrue) Get() bool {
	if b.value == nil {
		return true
	}
	return *b.value
}

// GetIfSet can be used as follows in validation:
//
//	// Only validate if explicitly set
//	if val, ok := flag.GetIfSet(); ok {
//	    if !val {
//	        return fmt.Errorf("foo must be true if specified")
//	    }
//	}
func (b boolDefaultTrue) GetIfSet() (bool, bool) {
	if b.value == nil {
		return false, false
	}
	return *b.value, true
}

type CookedTransferOptions struct {
	source      common.ResourceString
	destination common.ResourceString

	fromTo        common.FromTo
	filterOptions traverser.FilterOptions

	listOfVersionIds chan string
	listOFFiles      chan string

	recursive                      bool
	stripTopDir                    bool
	symlinks                       common.SymlinkHandlingType
	hardlinks                      common.HardlinkHandlingType
	forceWrite                     common.OverwriteOption
	forceIfReadOnly                bool
	isSourceDir                    bool
	autoDecompress                 bool
	blockSize                      int64
	putBlobSize                    int64
	blobType                       common.BlobType
	blobTags                       common.BlobTags
	blockBlobTier                  common.BlockBlobTier
	pageBlobTier                   common.PageBlobTier
	metadata                       string
	contentType                    string
	contentEncoding                string
	contentLanguage                string
	contentDisposition             string
	cacheControl                   string
	noGuessMimeType                bool
	preserveLastModifiedTime       bool
	deleteSnapshotsOption          common.DeleteSnapshotsOption
	putMd5                         bool
	checkMd5                       common.HashValidationOption
	checkLength                    bool
	preservePermissions            common.PreservePermissionsOption
	preservePosixProperties        bool
	backupMode                     bool
	asSubdir                       bool
	s2sPreserveProperties          boolDefaultTrue
	s2sGetPropertiesInBackend      bool
	s2sPreserveAccessTier          boolDefaultTrue
	s2sSourceChangeValidation      bool
	s2sPreserveBlobTags            bool
	s2sInvalidMetadataHandleOption common.InvalidMetadataHandleOption
	includeDirectoryStubs          bool
	disableAutoDecoding            bool
	cpkOptions                     common.CpkOptions
	trailingDot                    common.TrailingDotOption
	preserveInfo                   bool

	// AzCopy internal use only
	commandString                    string
	dryrun                           bool
	dryrunJobPartOrderHandler        func(request common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse
	deleteDestinationFileIfNecessary bool
}

func newCookedCopyOptions(src, dst string, opts CopyOptions) (c *CookedTransferOptions, err error) {
	c = &CookedTransferOptions{}

	err = c.applyFromToSrcDest(src, dst, opts.FromTo)
	if err != nil {
		return nil, err
	}

	// Special handling for list of files  and include paths. This cannot be checked in validateOptions since they are transformed in applyDefaultsAndInferOptions.
	// A combined implementation reduces the amount of code duplication present.
	// However, it _does_ increase the amount of code-intertwining present.
	if opts.listOfFiles != "" && len(opts.IncludePatterns) > 0 {
		return nil, errors.New("cannot combine list of files and include path")
	}

	err = c.applyDefaultsAndInferOptions(opts)
	if err != nil {
		return nil, err
	}

	err = c.validateOptions()
	if err != nil {
		return nil, err
	}

	// need to set this after validation
	err = common.SetBackupMode(c.backupMode, c.fromTo)
	if err != nil {
		return c, err
	}

	return c, nil
}

func (c *CookedTransferOptions) applyFromToSrcDest(src, dst string, fromTo common.FromTo) (err error) {
	// fromTo, source and destination
	userFromTo := common.Iff(fromTo == common.EFromTo.Unknown(), "", fromTo.String())
	c.fromTo, err = InferAndValidateFromTo(src, dst, userFromTo)
	if err != nil {
		return err
	}
	common.SetNFSFlag(AreBothLocationsNFSAware(c.fromTo))

	// Destination
	tempDest := dst
	if strings.EqualFold(tempDest, common.Dev_Null) && runtime.GOOS == "windows" {
		tempDest = common.Dev_Null // map all capitalization of "NUL"/"nul" to one because (on Windows) they all mean the same thing
	}
	// Strip the SAS   from the source and destination whenever there is SAS exists in URL.
	// Note: SAS could exists in source of S2S copy, even if the credential type is OAuth for destination.
	c.destination, err = traverser.SplitResourceString(tempDest, c.fromTo.To())
	if err != nil {
		return err
	}

	// Source
	tempSrc := src
	// Check if source has a trailing wildcard on a URL
	if c.fromTo.From().IsRemote() {
		tempSrc, c.stripTopDir, err = StripTrailingWildcardOnRemoteSource(src, c.fromTo.From())

		if err != nil {
			return err
		}
	}
	c.source, err = traverser.SplitResourceString(tempSrc, c.fromTo.From())
	if err != nil {
		return err
	}
	// c.stripTopDir is effectively a workaround for the lack of wildcards in remote sources.
	// Local, however, still supports wildcards, and thus needs its top directory stripped whenever a wildcard is used.
	// Thus, we check for wildcards and instruct the processor to strip the top dir later instead of repeatedly checking cca.Source for wildcards.
	if c.fromTo.From() == common.ELocation.Local() && strings.Contains(c.source.ValueLocal(), "*") {
		c.stripTopDir = true
	}

	return nil
}

func (c *CookedTransferOptions) applyDefaultsAndInferOptions(opts CopyOptions) (err error) {
	// defaults
	preserveOwner := common.Iff(opts.PreserveOwner == nil, true, *opts.PreserveOwner)
	// --as-subdir is OK on all sources and destinations, but additional verification has to be done down the line. (e.g. https://account.blob.core.windows.net is not a valid root)
	c.asSubdir = common.Iff(opts.AsSubDir == nil, true, *opts.AsSubDir)
	c.s2sGetPropertiesInBackend = common.Iff(opts.s2SGetPropertiesInBackend == nil, true, *opts.s2SGetPropertiesInBackend)
	c.preserveInfo = common.IffNil(opts.PreserveInfo, GetPreserveInfoDefault(opts.FromTo))

	// 1:1 mapping
	c.recursive = opts.Recursive
	c.forceIfReadOnly = opts.ForceIfReadOnly
	c.autoDecompress = opts.AutoDecompress
	c.blockSize, err = BlockSizeInBytes(opts.BlockSizeMB)
	if err != nil {
		return err
	}
	c.putBlobSize, err = BlockSizeInBytes(opts.PutBlobSizeMB)
	if err != nil {
		return err
	}
	listChan, err := getListOfFileChannel(opts.listOfFiles, opts.IncludePaths)
	if err != nil {
		return err
	}
	c.listOFFiles = listChan
	versionsChan, err := getVersionsChannel(opts.ListOfVersionIds)
	if err != nil {
		return err
	}
	c.listOfVersionIds = versionsChan
	c.metadata = getMetadataString(opts.Metadata)
	c.contentType = opts.ContentType
	c.contentEncoding = opts.ContentEncoding
	c.contentLanguage = opts.ContentLanguage
	c.contentDisposition = opts.ContentDisposition
	c.cacheControl = opts.CacheControl
	c.noGuessMimeType = opts.NoGuessMimeType
	c.preserveLastModifiedTime = opts.PreserveLastModifiedTime
	c.disableAutoDecoding = opts.DisableAutoDecoding
	c.blobTags = opts.BlobTags
	c.s2sPreserveBlobTags = opts.S2SPreserveBlobTags
	c.cpkOptions = common.CpkOptions{
		CpkScopeInfo: opts.CpkByName,
		// Get the key (EncryptionKey and EncryptionKeySHA256) value from environment variables when required.
		CpkInfo: opts.CpkByValue,
		// We only support transfer from source encrypted by user key when user wishes to download.
		// Due to service limitation, S2S transfer is not supported for source encrypted by user key.
		IsSourceEncrypted: (c.fromTo.IsDownload() || c.fromTo.IsDelete()) && (opts.CpkByName != "" || opts.CpkByValue),
	}
	c.putMd5 = opts.PutMd5
	c.checkLength = opts.CheckLength
	c.includeDirectoryStubs = opts.IncludeDirectoryStubs
	c.backupMode = opts.BackupMode
	c.s2sPreserveProperties = boolDefaultTrue{value: opts.S2SPreserveProperties}
	c.s2sPreserveAccessTier = boolDefaultTrue{value: opts.S2SPreserveAccessTier}
	c.s2sSourceChangeValidation = opts.S2SDetectSourceChanged
	c.dryrun = opts.dryrun
	c.dryrunJobPartOrderHandler = opts.dryrunJobPartOrderHandler
	c.deleteDestinationFileIfNecessary = opts.deleteDestinationFileIfNecessary
	c.symlinks = opts.Symlinks
	c.forceWrite = opts.Overwrite
	c.blobType = opts.BlobType
	c.blockBlobTier = opts.BlockBlobTier
	c.pageBlobTier = opts.PageBlobTier
	c.filterOptions = traverser.FilterOptions{
		IncludeBefore:     opts.IncludeBefore,
		IncludeAfter:      opts.IncludeAfter,
		ExcludeBlobTypes:  opts.ExcludeBlobTypes,
		IncludePatterns:   opts.IncludePatterns,
		ExcludePatterns:   opts.ExcludePatterns,
		ExcludePaths:      opts.ExcludePaths,
		ExcludeContainers: opts.ExcludeContainers,
		IncludeAttributes: opts.IncludeAttributes,
		ExcludeAttributes: opts.ExcludeAttributes,
		IncludeRegex:      opts.IncludeRegex,
		ExcludeRegex:      opts.ExcludeRegex,
	}
	c.trailingDot = opts.TrailingDot
	c.checkMd5 = opts.CheckMd5
	c.hardlinks = opts.Hardlinks
	c.preservePermissions = common.NewPreservePermissionsOption(opts.PreservePermissions, preserveOwner, c.fromTo)
	c.preservePosixProperties = opts.PreservePosixProperties
	c.s2sInvalidMetadataHandleOption = opts.S2SHandleInvalidateMetadata
	c.commandString = opts.commandString

	// inference
	if opts.ContentType != "" {
		c.noGuessMimeType = true
	}
	// length of devnull will be 0, thus this will always fail unless downloading an empty file
	if c.destination.Value == common.Dev_Null {
		c.checkLength = false
	}
	if opts.PreservePermissions && c.fromTo.From() == common.ELocation.Blob() {
		// If a user is trying to persist from Blob storage with ACLs, they probably want directories too, because ACLs only exist in HNS.
		c.includeDirectoryStubs = true
	}
	return
}

func getMetadataString(m map[string]string) string {
	if m == nil {
		return ""
	}
	if len(m) == 0 {
		return common.MetadataAndBlobTagsClearFlag
	}
	result := ""
	for k, v := range m {
		if result != "" {
			result += ";"
		}
		result += fmt.Sprintf("%s=%s", k, v)
	}
	return result
}

func getListOfFileChannel(listOfFiles string, includePaths []string) (listChan chan string, err error) {
	if listOfFiles == "" && len(includePaths) == 0 {
		return nil, nil
	}
	// Everything uses the new implementation of list-of-files now.
	// This handles both list-of-files and include-path as a list enumerator.
	// This saves us time because we know *exactly* what we're looking for right off the bat.
	// Note that exclude-path is handled as a filter unlike include-path.

	// unbuffered so this reads as we need it to rather than all at once in bulk
	listChan = make(chan string)
	var f *os.File

	if listOfFiles != "" {
		f, err = os.Open(listOfFiles)

		if err != nil {
			return nil, fmt.Errorf("cannot open %s file passed with the list-of-file flag", listOfFiles)
		}

		// Pre-validate the file format before starting the goroutine
		if err := validateListOfFilesFormat(f); err != nil {
			f.Close()
			return nil, err
		}

		// Reset file position for the goroutine to read from the beginning
		f.Seek(0, 0)
	}

	// Prepare UTF-8 byte order marker
	utf8BOM := string([]byte{0xEF, 0xBB, 0xBF})
	anyOncer := &sync.Once{}

	go func() {
		defer close(listChan)

		addToChannel := func(v string, paramName string) {
			// empty strings should be ignored, otherwise the source root itself is selected
			if len(v) > 0 {
				WarnIfHasWildcard(anyOncer, paramName, v)
				listChan <- v
			}
		}

		if f != nil {
			scanner := bufio.NewScanner(f)
			checkBOM := false

			for scanner.Scan() {
				v := scanner.Text()

				// Check if the UTF-8 BOM is on the first line and remove it if necessary.
				// Note that the UTF-8 BOM can be present on the same line feed as the first line of actual data, so just use TrimPrefix.
				// If the line feed were separate, the empty string would be skipped later.
				if !checkBOM {
					v = strings.TrimPrefix(v, utf8BOM)
					checkBOM = true
				}

				addToChannel(v, "list-of-files")
			}
		}

		for _, v := range includePaths {
			addToChannel(v, "include-path")
		}
	}()
	return listChan, nil
}

func getVersionsChannel(listOfVersionIDs string) (versionsChan chan string, err error) {
	if listOfVersionIDs == "" {
		return nil, nil
	}

	// unbuffered so this reads as we need it to rather than all at once in bulk
	versionsChan = make(chan string)
	var f *os.File
	f, err = os.Open(listOfVersionIDs)
	if err != nil {
		return nil, fmt.Errorf("cannot open %s file passed with the list-of-versions flag", listOfVersionIDs)
	}

	utf8BOM := string([]byte{0xEF, 0xBB, 0xBF})

	go func() {
		defer close(versionsChan)

		if f != nil {
			scanner := bufio.NewScanner(f)
			checkBOM := false

			for scanner.Scan() {
				v := scanner.Text()

				if !checkBOM {
					v = strings.TrimPrefix(v, utf8BOM)
					checkBOM = true
				}

				if len(v) > 0 {
					versionsChan <- v
				}
			}
		}
	}()

	return versionsChan, nil

}

func (c *CookedTransferOptions) validateOptions() (err error) {
	if err = ValidateForceIfReadOnly(c.forceIfReadOnly, c.fromTo); err != nil {
		return err
	}
	if err = ValidateSymlinkHandlingMode(c.symlinks, c.fromTo); err != nil {
		return err
	}
	allowAutoDecompress := c.fromTo == common.EFromTo.BlobLocal() || c.fromTo == common.EFromTo.FileLocal() || c.fromTo == common.EFromTo.FileNFSLocal()
	if c.autoDecompress && !allowAutoDecompress {
		return errors.New("automatic decompression is only supported for downloads from Blob and Azure Files") // as at Sept 2019, our ADLS Gen 2 Swagger does not include content-encoding for directory (path) listings so we can't support it there
	}
	// If the given blobType is AppendBlob, block-size-mb should not be greater than
	// common.MaxAppendBlobBlockSize.
	if c.blobType == common.EBlobType.AppendBlob() && c.blockSize > common.MaxAppendBlobBlockSize {
		return fmt.Errorf("block size cannot be greater than %dMB for AppendBlob blob type", common.MaxAppendBlobBlockSize/common.MegaByte)
	}
	if (len(c.filterOptions.IncludePatterns) > 0 || len(c.filterOptions.ExcludePatterns) > 0) && c.fromTo == common.EFromTo.BlobFSTrash() {
		return fmt.Errorf("include/exclude flags are not supported for this destination")
		// note there's another, more rigorous check, in removeBfsResources()
	}
	// warn on exclude unsupported wildcards here. Include have to be later, to cover list-of-files
	WarnIfAnyHasWildcard("exclude-path", c.filterOptions.ExcludePaths)

	// metadata
	if c.fromTo.To() == common.ELocation.None() && strings.EqualFold(c.metadata, common.MetadataAndBlobTagsClearFlag) { // in case of Blob, BlobFS and Files
		common.GetLifecycleMgr().Warn("*** WARNING *** Metadata will be cleared because of input --metadata=clear ")
	}
	if err = ValidateMetadataString(c.metadata); err != nil {
		return err
	}

	// blob tags
	if !(c.fromTo.To() == common.ELocation.Blob() || c.fromTo == common.EFromTo.BlobNone() || c.fromTo != common.EFromTo.BlobFSNone()) && c.blobTags != nil {
		return errors.New("blob tags can only be set when transferring to blob storage")
	}
	if c.fromTo.To() == common.ELocation.None() && c.blobTags != nil && len(c.blobTags) == 0 { // in case of Blob and BlobFS
		common.GetLifecycleMgr().Warn("*** WARNING *** BlobTags will be cleared because of input --blob-tags=clear ")
	}

	err = ValidateBlobTagsKeyValue(c.blobTags)
	if err != nil {
		return err
	}

	// Check if user has provided `s2s-preserve-blob-tags` flag. If yes, we have to ensure that
	// 1. Both source and destination must be blob storages.
	// 2. `blob-tags` is not present as they create conflicting scenario of whether to preserve blob tags from the source or set user defined tags on the destination
	if c.s2sPreserveBlobTags {
		if c.fromTo.From() != common.ELocation.Blob() || c.fromTo.To() != common.ELocation.Blob() {
			return errors.New("either source or destination is not a blob storage. blob index tags is a property of blobs only therefore both source and destination must be blob storage")
		} else if c.blobTags != nil {
			return errors.New("both s2s-preserve-blob-tags and blob-tags flags cannot be used in conjunction")
		}
	}

	if c.cpkOptions.CpkScopeInfo != "" && c.cpkOptions.CpkInfo {
		return errors.New("cannot use both cpk-by-name and cpk-by-value at the same time")
	}

	if c.cpkOptions.CpkScopeInfo != "" || c.cpkOptions.CpkInfo {
		destUrl, _ := url.Parse(c.destination.Value)
		if strings.Contains(destUrl.Host, "dfs.core.windows.net") {
			return errors.New("client provided keys (CPK) based encryption is only supported with blob endpoints (blob.core.windows.net)")
		}
	}

	if AreBothLocationsNFSAware(c.fromTo) {
		err = PerformNFSSpecificValidation(c.fromTo, c.preservePermissions, c.preserveInfo, c.symlinks, c.hardlinks)
		if err != nil {
			return err
		}
	} else {
		err = PerformSMBSpecificValidation(c.fromTo, c.preservePermissions, c.preserveInfo, c.preservePosixProperties)
		if err != nil {
			return err
		}
		if err = ValidatePreserveOwner(c.preservePermissions.IsOwner(), c.fromTo); err != nil {
			return err
		}
	}

	if err = ValidateBackupMode(c.backupMode, c.fromTo); err != nil {
		return err
	}

	// check for the flag value relative to fromTo location type
	// Example1: for Local to Blob, preserve-last-modified-time flag should not be set to true
	// Example2: for Blob to Local, follow-symlinks, blob-tier flags should not be provided with values.
	switch c.fromTo {
	case common.EFromTo.LocalBlobFS():
		if c.blobType != common.EBlobType.Detect() {
			return fmt.Errorf("blob-type is not supported on ADLS Gen 2")
		}
		if c.preserveLastModifiedTime {
			return fmt.Errorf("preserve-last-modified-time is not supported while uploading")
		}
		if c.blockBlobTier != common.EBlockBlobTier.None() ||
			c.pageBlobTier != common.EPageBlobTier.None() {
			return fmt.Errorf("blob-tier is not supported while uploading to ADLS Gen 2")
		}
		if c.preservePermissions.IsTruthy() {
			return fmt.Errorf("preserve-permissions is not supported while uploading to ADLS Gen 2")
		}
		if val, ok := c.s2sPreserveProperties.GetIfSet(); ok && val {
			return fmt.Errorf("s2s-preserve-properties is not supported while uploading")
		}
		if val, ok := c.s2sPreserveAccessTier.GetIfSet(); ok && val {
			return fmt.Errorf("s2s-preserve-access-tier is not supported while uploading")
		}
		if c.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return fmt.Errorf("s2s-handle-invalid-metadata is not supported while uploading")
		}
		if c.s2sSourceChangeValidation {
			return fmt.Errorf("s2s-detect-source-changed is not supported while uploading")
		}
	case common.EFromTo.LocalBlob():
		if c.preserveLastModifiedTime {
			return fmt.Errorf("preserve-last-modified-time is not supported while uploading to Blob Storage")
		}
		if val, ok := c.s2sPreserveProperties.GetIfSet(); ok && val {
			return fmt.Errorf("s2s-preserve-properties is not supported while uploading")
		}
		if val, ok := c.s2sPreserveAccessTier.GetIfSet(); ok && val {
			return fmt.Errorf("s2s-preserve-access-tier is not supported while uploading")
		}
		if c.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return fmt.Errorf("s2s-handle-invalid-metadata is not supported while uploading to Blob Storage")
		}
		if c.s2sSourceChangeValidation {
			return fmt.Errorf("s2s-detect-source-changed is not supported while uploading to Blob Storage")
		}
	case common.EFromTo.LocalFile(), common.EFromTo.LocalFileNFS():
		if c.preserveLastModifiedTime {
			return fmt.Errorf("preserve-last-modified-time is not supported while uploading")
		}
		if c.blockBlobTier != common.EBlockBlobTier.None() ||
			c.pageBlobTier != common.EPageBlobTier.None() {
			return fmt.Errorf("blob-tier is not supported while uploading to Azure File")
		}
		if val, ok := c.s2sPreserveProperties.GetIfSet(); ok && val {
			return fmt.Errorf("s2s-preserve-properties is not supported while uploading")
		}
		if val, ok := c.s2sPreserveAccessTier.GetIfSet(); ok && val {
			return fmt.Errorf("s2s-preserve-access-tier is not supported while uploading")
		}
		if c.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return fmt.Errorf("s2s-handle-invalid-metadata is not supported while uploading")
		}
		if c.s2sSourceChangeValidation {
			return fmt.Errorf("s2s-detect-source-changed is not supported while uploading")
		}
		if c.blobType != common.EBlobType.Detect() {
			return fmt.Errorf("blob-type is not supported on Azure File")
		}
	case common.EFromTo.BlobLocal(),
		common.EFromTo.FileLocal(),
		common.EFromTo.FileNFSLocal(),
		common.EFromTo.BlobFSLocal():
		if c.symlinks.Follow() {
			return fmt.Errorf("follow-symlinks flag is not supported while downloading")
		}
		if c.blockBlobTier != common.EBlockBlobTier.None() ||
			c.pageBlobTier != common.EPageBlobTier.None() {
			return fmt.Errorf("blob-tier is not supported while downloading")
		}
		if c.noGuessMimeType {
			return fmt.Errorf("no-guess-mime-type is not supported while downloading")
		}
		if len(c.contentType) > 0 || len(c.contentEncoding) > 0 || len(c.contentLanguage) > 0 || len(c.contentDisposition) > 0 || len(c.cacheControl) > 0 || len(c.metadata) > 0 {
			return fmt.Errorf("content-type, content-encoding, content-language, content-disposition, cache-control, or metadata is not supported while downloading")
		}
		if val, ok := c.s2sPreserveProperties.GetIfSet(); ok && val {
			return fmt.Errorf("s2s-preserve-properties is not supported while downloading")
		}
		if val, ok := c.s2sPreserveAccessTier.GetIfSet(); ok && val {
			return fmt.Errorf("s2s-preserve-access-tier is not supported while downloading")
		}
		if c.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return fmt.Errorf("s2s-handle-invalid-metadata is not supported while downloading")
		}
		if c.s2sSourceChangeValidation {
			return fmt.Errorf("s2s-detect-source-changed is not supported while downloading")
		}
	case common.EFromTo.BlobFile(),
		common.EFromTo.S3Blob(),
		common.EFromTo.BlobBlob(),
		common.EFromTo.FileBlob(),
		common.EFromTo.FileFile(),
		common.EFromTo.GCPBlob(),
		common.EFromTo.FileNFSFileNFS():

		if c.preserveLastModifiedTime {
			return fmt.Errorf("preserve-last-modified-time is not supported while copying from service to service")
		}
		if c.symlinks.Follow() {
			return fmt.Errorf("follow-symlinks flag is not supported while copying from service to service")
		}
		// blob type is not supported if destination is not blob
		if c.blobType != common.EBlobType.Detect() && c.fromTo.To() != common.ELocation.Blob() {
			return fmt.Errorf("blob-type is not supported for the scenario (%s)", c.fromTo.String())
		}

		// Setting blob tier is supported only when destination is a blob storage. Disabling it for all the other transfer scenarios.
		if (c.blockBlobTier != common.EBlockBlobTier.None() || c.pageBlobTier != common.EPageBlobTier.None()) &&
			c.fromTo.To() != common.ELocation.Blob() {
			return fmt.Errorf("blob-tier is not supported for the scenario (%s)", c.fromTo.String())
		}
		if c.noGuessMimeType {
			return fmt.Errorf("no-guess-mime-type is not supported while copying from service to service")
		}
		if len(c.contentType) > 0 || len(c.contentEncoding) > 0 || len(c.contentLanguage) > 0 || len(c.contentDisposition) > 0 || len(c.cacheControl) > 0 || len(c.metadata) > 0 {
			return fmt.Errorf("content-type, content-encoding, content-language, content-disposition, cache-control, or metadata is not supported while copying from service to service")
		}
	}

	if err = ValidatePutMd5(c.putMd5, c.fromTo); err != nil {
		return err
	}
	if err = ValidateMd5Option(c.checkMd5, c.fromTo); err != nil {
		return err
	}
	if (len(c.filterOptions.IncludeAttributes) > 0 || len(c.filterOptions.ExcludeAttributes) > 0) && c.fromTo.From() != common.ELocation.Local() {
		return errors.New("cannot check file attributes on remote objects")
	}

	if c.cpkOptions.CpkScopeInfo != "" || c.cpkOptions.CpkInfo {
		if c.cpkOptions.IsSourceEncrypted {
			common.GetLifecycleMgr().Info("Client Provided Key (CPK) for encryption/decryption is provided for download or delete scenario. " +
				"Assuming source is encrypted.")
		}
		// TODO: Remove these warnings once service starts supporting it
		if c.blockBlobTier != common.EBlockBlobTier.None() || c.pageBlobTier != common.EPageBlobTier.None() {
			common.GetLifecycleMgr().Info("Tier is provided by user explicitly. Ignoring it because Azure Service currently does" +
				" not support setting tier when client provided keys are involved.")
		}
	}

	if c.preserveInfo && !c.preservePermissions.IsTruthy() {
		if AreBothLocationsNFSAware(c.fromTo) {
			common.GetLifecycleMgr().Info(PreserveNFSPermissionsDisabledMsg)
		} else {
			common.GetLifecycleMgr().Info(PreservePermissionsDisabledMsg)
		}
	}

	return nil
}
