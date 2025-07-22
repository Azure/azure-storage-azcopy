// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"

	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/spf13/cobra"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

// represents the raw copy command input from the user
type rawCopyCmdArgs struct {
	// from arguments
	src    string
	dst    string
	fromTo string
	// blobUrlForRedirection string

	// new include/exclude only apply to file names
	// implemented for remove (and sync) only
	include               string
	exclude               string
	includePath           string // NOTE: This gets handled like list-of-files! It may LOOK like a bug, but it is not.
	excludePath           string
	includeRegex          string
	excludeRegex          string
	includeFileAttributes string
	excludeFileAttributes string
	excludeContainer      string
	includeBefore         string
	includeAfter          string
	legacyInclude         string // used only for warnings
	legacyExclude         string // used only for warnings
	listOfVersionIDs      string

	// Indicates the user wants to upload the symlink itself, not the file on the other end
	preserveSymlinks bool

	// filters from flags
	listOfFilesToCopy string
	recursive         bool
	followSymlinks    bool
	autoDecompress    bool
	// forceWrite flag is used to define the User behavior
	// to overwrite the existing blobs or not.
	forceWrite      string
	forceIfReadOnly bool

	// options from flags
	blockSizeMB              float64
	putBlobSizeMB            float64
	metadata                 string
	contentType              string
	contentEncoding          string
	contentDisposition       string
	contentLanguage          string
	cacheControl             string
	noGuessMimeType          bool
	preserveLastModifiedTime bool
	putMd5                   bool
	md5ValidationOption      string
	CheckLength              bool
	deleteSnapshotsOption    string
	dryrun                   bool

	blobTags string
	// defines the type of the blob at the destination in case of upload / account to account copy
	blobType      string
	blockBlobTier string
	pageBlobTier  string
	output        string // TODO: Is this unused now? replaced with param at root level?
	// list of blobTypes to exclude while enumerating the transfer
	excludeBlobType string
	// Opt-in flag to persist SMB ACLs to Azure Files.
	preserveSMBPermissions bool
	preservePermissions    bool // Separate flag so that we don't get funkiness with two "flags" targeting the same boolean
	preserveOwner          bool // works in conjunction with preserveSmbPermissions
	// Default true; false indicates that the destination is the target directory, rather than something we'd put a directory under (e.g. a container)
	asSubdir bool
	// Opt-in flag to persist additional SMB properties to Azure Files. Named ...info instead of ...properties
	// because the latter was similar enough to preserveSMBPermissions to induce user error
	preserveSMBInfo bool
	// Opt-in flag to persist additional POSIX properties
	preservePOSIXProperties bool
	// Opt-in flag to preserve the blob index tags during service to service transfer.
	s2sPreserveBlobTags bool
	// Flag to enable Window's special privileges
	backupMode bool
	// whether user wants to preserve full properties during service to service copy, the default value is true.
	// For S3 and Azure File non-single file source, as list operation doesn't return full properties of objects/files,
	// to preserve full properties AzCopy needs to send one additional request per object/file.
	s2sPreserveProperties bool
	// useful when preserveS3Properties set to true, enables get S3 objects' or Azure files' properties during s2s copy in backend, the default value is true
	s2sGetPropertiesInBackend bool
	// whether user wants to preserve access tier during service to service copy, the default value is true.
	// In some case, e.g. target is a GPv1 storage account, access tier cannot be set properly.
	// In such cases, use s2sPreserveAccessTier=false to bypass the access tier copy.
	// For more details, please refer to https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
	s2sPreserveAccessTier bool
	// whether user wants to check if source has changed after enumerating, the default value is true.
	// For S2S copy, as source is a remote resource, validating whether source has changed need additional request costs.
	s2sSourceChangeValidation bool
	// specify how user wants to handle invalid metadata.
	s2sInvalidMetadataHandleOption string

	// internal override to enforce strip-top-dir
	internalOverrideStripTopDir bool

	// whether to include blobs that have metadata 'hdi_isfolder = true'
	includeDirectoryStubs bool

	// whether to disable automatic decoding of illegal chars on Windows
	disableAutoDecoding bool

	// Optional flag to encrypt user data with user provided key.
	// Key is provide in the REST request itself
	// Provided key (EncryptionKey and EncryptionKeySHA256) and its hash will be fetched from environment variables
	// Set EncryptionAlgorithm = "AES256" by default.
	cpkInfo bool
	// Key is present in AzureKeyVault and Azure KeyVault is linked with storage account.
	// Provided key name will be fetched from Azure Key Vault and will be used to encrypt the data
	cpkScopeInfo string

	// Optional flag that permanently deletes soft-deleted snapshots/versions
	permanentDeleteOption string

	// Optional. Indicates the priority with which to rehydrate an archived blob. Valid values are High/Standard.
	rehydratePriority string
	// The priority setting can be changed from Standard to High by calling Set Blob Tier with this header set to High and setting x-ms-access-tier to the same value as previously set. The priority setting cannot be lowered from High to Standard.
	trailingDot string

	// when specified, AzCopy deletes the destination blob that has uncommitted blocks, not just the uncommitted blocks
	deleteDestinationFileIfNecessary bool
	// Opt-in flag to persist additional properties to Azure Files
	preserveInfo bool
	hardlinks    string
}

// blockSizeInBytes converts a FLOATING POINT number of MiB, to a number of bytes
// A non-nil error is returned if the conversion is not possible to do accurately (e.g. it comes out of a fractional number of bytes)
// The purpose of using floating point is to allow specialist users (e.g. those who want small block sizes to tune their read IOPS)
// to use fractions of a MiB. E.g.
// 0.25 = 256 KiB
// 0.015625 = 16 KiB
func blockSizeInBytes(rawBlockSizeInMiB float64) (int64, error) {
	if rawBlockSizeInMiB < 0 {
		return 0, errors.New("negative block size not allowed")
	}
	rawSizeInBytes := rawBlockSizeInMiB * 1024 * 1024 // internally we use bytes, but users' convenience the command line uses MiB
	if rawSizeInBytes > math.MaxInt64 {
		return 0, errors.New("block size too big for int64")
	}
	const epsilon = 0.001 // arbitrarily using a tolerance of 1000th of a byte
	_, frac := math.Modf(rawSizeInBytes)
	isWholeNumber := frac < epsilon || frac > 1.0-epsilon // frac is very close to 0 or 1, so rawSizeInBytes is (very close to) an integer
	if !isWholeNumber {
		return 0, fmt.Errorf("while fractional numbers of MiB are allowed as the block size, the fraction must result to a whole number of bytes. %.12f MiB resolves to %.3f bytes", rawBlockSizeInMiB, rawSizeInBytes)
	}
	return int64(math.Round(rawSizeInBytes)), nil
}

func (raw *rawCopyCmdArgs) toOptions() (cooked CookedCopyCmdArgs, err error) {
	cooked = CookedCopyCmdArgs{
		Recursive:                raw.recursive,
		ForceIfReadOnly:          raw.forceIfReadOnly,
		autoDecompress:           raw.autoDecompress,
		BlockSizeMB:              raw.blockSizeMB,
		PutBlobSizeMB:            raw.putBlobSizeMB,
		ListOfFiles:              raw.listOfFilesToCopy,
		ListOfVersionIDs:         raw.listOfVersionIDs,
		metadata:                 raw.metadata,
		contentType:              raw.contentType,
		contentEncoding:          raw.contentEncoding,
		contentLanguage:          raw.contentLanguage,
		contentDisposition:       raw.contentDisposition,
		cacheControl:             raw.cacheControl,
		noGuessMimeType:          raw.noGuessMimeType,
		preserveLastModifiedTime: raw.preserveLastModifiedTime,
		disableAutoDecoding:      raw.disableAutoDecoding,
		blobTags:                 raw.blobTags,
		S2sPreserveBlobTags:      raw.s2sPreserveBlobTags,
		cpkByName:                raw.cpkScopeInfo,
		cpkByValue:               raw.cpkInfo,
		putMd5:                   raw.putMd5,
		CheckLength:              raw.CheckLength,
		preserveOwner:            raw.preserveOwner,

		asSubdir:              raw.asSubdir, // --as-subdir is OK on all sources and destinations, but additional verification has to be done down the line. (e.g. https://account.blob.core.windows.net is not a valid root)
		IncludeDirectoryStubs: raw.includeDirectoryStubs,
		backupMode:            raw.backupMode,
		s2sPreserveProperties: boolDefaultTrue{
			value:         raw.s2sPreserveProperties,
			isManuallySet: cpCmd.Flags().Changed("s2s-preserve-properties"),
		},
		s2sPreserveAccessTier: boolDefaultTrue{
			value:         raw.s2sPreserveAccessTier,
			isManuallySet: cpCmd.Flags().Changed("s2s-preserve-access-tier"),
		},
		s2sGetPropertiesInBackend:        raw.s2sGetPropertiesInBackend,
		s2sSourceChangeValidation:        raw.s2sSourceChangeValidation,
		dryrunMode:                       raw.dryrun,
		deleteDestinationFileIfNecessary: raw.deleteDestinationFileIfNecessary,
	}

	// We infer FromTo and validate it here since it is critical to a lot of other options parsing below.
	cooked.FromTo, err = ValidateFromTo(raw.src, raw.dst, raw.fromTo)
	if err != nil {
		return cooked, err
	}

	// Destination
	tempDest := raw.dst
	if strings.EqualFold(tempDest, common.Dev_Null) && runtime.GOOS == "windows" {
		tempDest = common.Dev_Null // map all capitalization of "NUL"/"nul" to one because (on Windows) they all mean the same thing
	}
	// Strip the SAS from the source and destination whenever there is SAS exists in URL.
	// Note: SAS could exists in source of S2S copy, even if the credential type is OAuth for destination.
	cooked.Destination, err = SplitResourceString(tempDest, cooked.FromTo.To())
	if err != nil {
		return cooked, err
	}

	// Source
	tempSrc := raw.src
	// Check if source has a trailing wildcard on a URL
	if cooked.FromTo.From().IsRemote() {
		tempSrc, cooked.StripTopDir, err = stripTrailingWildcardOnRemoteSource(raw.src, cooked.FromTo.From())

		if err != nil {
			return cooked, err
		}
	}
	cooked.Source, err = SplitResourceString(tempSrc, cooked.FromTo.From())
	if err != nil {
		return cooked, err
	}

	if raw.internalOverrideStripTopDir {
		cooked.StripTopDir = true
	}
	// cooked.StripTopDir is effectively a workaround for the lack of wildcards in remote sources.
	// Local, however, still supports wildcards, and thus needs its top directory stripped whenever a wildcard is used.
	// Thus, we check for wildcards and instruct the processor to strip the top dir later instead of repeatedly checking cca.Source for wildcards.
	if cooked.FromTo.From() == common.ELocation.Local() && strings.Contains(cooked.Source.ValueLocal(), "*") {
		cooked.StripTopDir = true
	}

	if err = cooked.SymlinkHandling.Determine(raw.followSymlinks, raw.preserveSymlinks); err != nil {
		return cooked, err
	}

	err = cooked.ForceWrite.Parse(raw.forceWrite)
	if err != nil {
		return cooked, err
	}

	err = cooked.blobType.Parse(raw.blobType)
	if err != nil {
		return cooked, err
	}

	err = cooked.blockBlobTier.Parse(raw.blockBlobTier)
	if err != nil {
		return cooked, err
	}

	err = cooked.pageBlobTier.Parse(raw.pageBlobTier)
	if err != nil {
		return cooked, err
	}

	if raw.rehydratePriority == "" {
		raw.rehydratePriority = "standard" // default value
	}
	err = cooked.rehydratePriority.Parse(raw.rehydratePriority)
	if err != nil {
		return cooked, err
	}

	if raw.legacyInclude != "" || raw.legacyExclude != "" {
		return cooked, fmt.Errorf("the include and exclude parameters have been replaced by include-pattern; include-path; exclude-pattern and exclude-path. For info, run: azcopy copy help")
	}
	cooked.IncludePathPatterns = parsePatterns(raw.includePath)

	if raw.includeBefore != "" {
		// must set chooseEarliest = false, so that if there's an ambiguous local date, the latest will be returned
		// (since that's safest for includeBefore.  Better to choose the later time and do more work, than the earlier one and fail to pick up a changed file
		parsedIncludeBefore, err := IncludeBeforeDateFilter{}.ParseISO8601(raw.includeBefore, false)
		if err != nil {
			return cooked, err
		}
		cooked.IncludeBefore = &parsedIncludeBefore
	}

	if raw.includeAfter != "" {
		// must set chooseEarliest = true, so that if there's an ambiguous local date, the earliest will be returned
		// (since that's safest for includeAfter.  Better to choose the earlier time and do more work, than the later one and fail to pick up a changed file
		parsedIncludeAfter, err := IncludeAfterDateFilter{}.ParseISO8601(raw.includeAfter, true)
		if err != nil {
			return cooked, err
		}
		cooked.IncludeAfter = &parsedIncludeAfter
	}

	err = cooked.trailingDot.Parse(raw.trailingDot)
	if err != nil {
		return cooked, err
	}

	err = cooked.deleteSnapshotsOption.Parse(raw.deleteSnapshotsOption)
	if err != nil {
		return cooked, err
	}

	if cooked.contentType != "" {
		cooked.noGuessMimeType = true // As specified in the help text, noGuessMimeType is inferred here.
	}

	err = cooked.md5ValidationOption.Parse(raw.md5ValidationOption)
	if err != nil {
		return cooked, err
	}

	// length of devnull will be 0, thus this will always fail unless downloading an empty file
	if cooked.Destination.Value == common.Dev_Null {
		cooked.CheckLength = false
	}

	// This will only happen in CLI commands, not AzCopy as a library.
	// if redirection is triggered, avoid printing any output
	if cooked.isRedirection() {
		glcm.SetOutputFormat(common.EOutputFormat.None())
	}

	if common.IsNFSCopy() {
		cooked.preserveInfo = raw.preserveInfo && areBothLocationsNFSAware(cooked.FromTo)
		cooked.preservePermissions = common.NewPreservePermissionsOption(raw.preservePermissions,
			true,
			cooked.FromTo)
		if err = cooked.hardlinks.Parse(raw.hardlinks); err != nil {
			return cooked, err
		}
	} else {
		cooked.preserveInfo = raw.preserveInfo && areBothLocationsSMBAware(cooked.FromTo)
		cooked.preservePOSIXProperties = raw.preservePOSIXProperties
		cooked.preservePermissions = common.NewPreservePermissionsOption(raw.preservePermissions,
			raw.preserveOwner,
			cooked.FromTo)
	}
	//fmt.Println("IsNFS", common.IsNFSCopy(), "preserveInfo", cooked.preserveInfo, "preservePOSIXProperties", cooked.preservePOSIXProperties, "preservePermissions", cooked.preservePermissions, "hardlinks", cooked.hardlinks)
	// TODO: Figure out this preservePermissinos stuff
	if cooked.preservePermissions.IsTruthy() && cooked.FromTo.From() == common.ELocation.Blob() {
		// If a user is trying to persist from Blob storage with ACLs, they probably want directories too, because ACLs only exist in HNS.
		cooked.IncludeDirectoryStubs = true
	}

	err = cooked.permanentDeleteOption.Parse(raw.permanentDeleteOption)
	if err != nil {
		return cooked, err
	}

	// If the user has provided some input with excludeBlobType flag, parse the input.
	if len(raw.excludeBlobType) > 0 {
		// Split the string using delimiter ';' and parse the individual blobType
		blobTypes := strings.Split(raw.excludeBlobType, ";")
		for _, blobType := range blobTypes {
			var eBlobType common.BlobType
			err := eBlobType.Parse(blobType)
			if err != nil {
				return cooked, fmt.Errorf("error parsing the exclude-blob-type %s provided with exclude-blob-type flag ", blobType)
			}
			cooked.excludeBlobType = append(cooked.excludeBlobType, eBlobType.ToBlobType())
		}
	}

	err = cooked.s2sInvalidMetadataHandleOption.Parse(raw.s2sInvalidMetadataHandleOption)
	if err != nil {
		return cooked, err
	}

	cooked.IncludePatterns = parsePatterns(raw.include)
	cooked.ExcludePatterns = parsePatterns(raw.exclude)
	cooked.ExcludePathPatterns = parsePatterns(raw.excludePath)
	cooked.excludeContainer = parsePatterns(raw.excludeContainer)
	cooked.IncludeFileAttributes = parsePatterns(raw.includeFileAttributes)
	cooked.ExcludeFileAttributes = parsePatterns(raw.excludeFileAttributes)
	cooked.includeRegex = parsePatterns(raw.includeRegex)
	cooked.excludeRegex = parsePatterns(raw.excludeRegex)

	return cooked, nil
}

func (raw rawCopyCmdArgs) cook() (cooked CookedCopyCmdArgs, err error) {
	if cooked, err = raw.toOptions(); err != nil {
		return cooked, err
	}
	if err = cooked.validate(); err != nil {
		return cooked, err
	}
	if err = cooked.processArgs(); err != nil {
		return cooked, err
	}
	return cooked, nil
}

var excludeWarningOncer = &sync.Once{}
var includeWarningOncer = &sync.Once{}

// When other commands use the copy command arguments to cook cook, set the blobType to None and validation option
// else parsing the arguments will fail.
func (raw *rawCopyCmdArgs) setMandatoryDefaults() {
	raw.blobType = common.EBlobType.Detect().String()
	raw.blockBlobTier = common.EBlockBlobTier.None().String()
	raw.pageBlobTier = common.EPageBlobTier.None().String()
	raw.md5ValidationOption = common.DefaultHashValidationOption.String()
	raw.s2sInvalidMetadataHandleOption = common.DefaultInvalidMetadataHandleOption.String()
	raw.forceWrite = common.EOverwriteOption.True().String()
	raw.preserveOwner = common.PreserveOwnerDefault
	raw.hardlinks = common.DefaultHardlinkHandlingType.String()
}

func validateForceIfReadOnly(toForce bool, fromTo common.FromTo) error {
	targetIsFiles := (fromTo.To() == common.ELocation.File() || fromTo.To() == common.ELocation.FileNFS()) ||
		fromTo == common.EFromTo.FileTrash()
	targetIsWindowsFS := fromTo.To() == common.ELocation.Local() &&
		runtime.GOOS == "windows"
	targetIsOK := targetIsFiles || targetIsWindowsFS
	if toForce && !targetIsOK {
		return errors.New("force-if-read-only is only supported when the target is Azure Files or a Windows file system")
	}
	return nil
}

func areBothLocationsPOSIXAware(fromTo common.FromTo) bool {
	// POSIX properties are stored in blob metadata-- They don't need a special persistence strategy for S2S methods.
	switch fromTo {
	case common.EFromTo.BlobLocal(), common.EFromTo.LocalBlob(), common.EFromTo.BlobFSLocal(), common.EFromTo.LocalBlobFS():
		return runtime.GOOS == "linux"
	case common.EFromTo.BlobBlob(), common.EFromTo.BlobFSBlobFS(), common.EFromTo.BlobFSBlob(), common.EFromTo.BlobBlobFS():
		return true
	default:
		return false
	}
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

func validateSymlinkHandlingMode(symlinkHandling common.SymlinkHandlingType, fromTo common.FromTo) error {
	if symlinkHandling.Preserve() {
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

// represents the processed copy command input from the user
type CookedCopyCmdArgs struct {
	// from arguments
	Source      common.ResourceString
	Destination common.ResourceString
	FromTo      common.FromTo

	// new include/exclude only apply to file names
	// implemented for remove (and sync) only
	// includePathPatterns are handled like a list-of-files. Do not panic. This is not a bug that it is not present here.
	IncludePatterns       []string
	ExcludePatterns       []string
	ExcludePathPatterns   []string
	excludeContainer      []string
	IncludeFileAttributes []string
	ExcludeFileAttributes []string
	IncludeBefore         *time.Time
	IncludeAfter          *time.Time

	// include/exclude filters with regular expression (also for sync)
	includeRegex []string
	excludeRegex []string

	// list of version ids
	ListOfVersionIDsChannel chan string
	// filters from flags
	ListOfFilesChannel chan string // Channels are nullable.
	Recursive          bool
	StripTopDir        bool
	SymlinkHandling    common.SymlinkHandlingType
	ForceWrite         common.OverwriteOption // says whether we should try to overwrite
	ForceIfReadOnly    bool                   // says whether we should _force_ any overwrites (triggered by forceWrite) to work on Azure Files objects that are set to read-only
	IsSourceDir        bool

	autoDecompress bool

	// options from flags
	blockSize   int64
	putBlobSize int64
	// list of blobTypes to exclude while enumerating the transfer
	excludeBlobType []blob.BlobType
	blobType        common.BlobType
	// Blob index tags categorize data in your storage account utilizing key-value tag attributes.
	// These tags are automatically indexed and exposed as a queryable multi-dimensional index to easily find data.
	blobTags                 string
	blockBlobTier            common.BlockBlobTier
	pageBlobTier             common.PageBlobTier
	metadata                 string
	contentType              string
	contentEncoding          string
	contentLanguage          string
	contentDisposition       string
	cacheControl             string
	noGuessMimeType          bool
	preserveLastModifiedTime bool
	deleteSnapshotsOption    common.DeleteSnapshotsOption
	putMd5                   bool
	md5ValidationOption      common.HashValidationOption
	CheckLength              bool
	// commandString hold the user given command which is logged to the Job log file
	commandString string

	// generated
	jobID common.JobID

	// extracted from the input
	credentialInfo common.CredentialInfo

	// variables used to calculate progress
	// intervalStartTime holds the last time value when the progress summary was fetched
	// the value of this variable is used to calculate the throughput
	// it gets updated every time the progress summary is fetched
	intervalStartTime        time.Time
	intervalBytesTransferred uint64

	// used to calculate job summary
	jobStartTime time.Time

	// this flag is set by the enumerator
	// it is useful to indicate whether we are simply waiting for the purpose of cancelling
	isEnumerationComplete bool

	// Whether the user wants to preserve the SMB ACLs assigned to their files when moving between resources that are SMB ACL aware.
	preservePermissions common.PreservePermissionsOption

	// Whether the user wants to preserve the POSIX properties ...
	preservePOSIXProperties bool

	// Whether to enable Windows special privileges
	backupMode bool

	// Whether to rename/share the root
	asSubdir bool

	// whether user wants to preserve full properties during service to service copy, the default value is true.
	// For S3 and Azure File non-single file source, as list operation doesn't return full properties of objects/files,
	// to preserve full properties AzCopy needs to send one additional request per object/file.
	s2sPreserveProperties boolDefaultTrue
	// useful when preserveS3Properties set to true, enables get S3 objects' or Azure files' properties during s2s copy in backend, the default value is true
	s2sGetPropertiesInBackend bool // TODO: This is default true, how do we deal with this in AzCopy as a library
	// whether user wants to preserve access tier during service to service copy, the default value is true.
	// In some case, e.g. target is a GPv1 storage account, access tier cannot be set properly.
	// In such cases, use s2sPreserveAccessTier=false to bypass the access tier copy.
	// For more details, please refer to https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
	s2sPreserveAccessTier boolDefaultTrue
	// whether user wants to check if source has changed after enumerating, the default value is true.
	// For S2S copy, as source is a remote resource, validating whether source has changed need additional request costs.
	s2sSourceChangeValidation bool
	// To specify whether user wants to preserve the blob index tags during service to service transfer.
	S2sPreserveBlobTags bool
	// specify how user wants to handle invalid metadata.
	s2sInvalidMetadataHandleOption common.InvalidMetadataHandleOption

	// followup/cleanup properties are NOT available on resume, and so should not be used for jobs that may be resumed
	// TODO: consider find a way to enforce that, or else to allow them to be preserved. Initially, they are just for benchmark jobs, so not a problem immediately because those jobs can't be resumed, by design.
	followupJobArgs   *CookedCopyCmdArgs
	priorJobExitCode  *common.ExitCode
	isCleanupJob      bool // triggers abbreviated status reporting, since we don't want full reporting for cleanup jobs
	cleanupJobMessage string

	// whether to include blobs that have metadata 'hdi_isfolder = true'
	IncludeDirectoryStubs bool

	// whether to disable automatic decoding of illegal chars on Windows
	disableAutoDecoding bool

	// specify if dry run mode on
	dryrunMode bool

	CpkOptions common.CpkOptions

	// Optional flag that permanently deletes soft deleted blobs
	permanentDeleteOption common.PermanentDeleteOption

	// Optional flag that sets rehydrate priority for rehydration
	rehydratePriority common.RehydratePriorityType

	// Bitmasked uint checking which properties to transfer
	propertiesToTransfer common.SetPropertiesFlags

	trailingDot common.TrailingDotOption

	deleteDestinationFileIfNecessary bool
	// Whether the user wants to preserve the properties of a file...
	preserveInfo                  bool
	hardlinks                     common.HardlinkHandlingType
	atomicSkippedSymlinkCount     uint32
	atomicSkippedSpecialFileCount uint32

	BlockSizeMB         float64
	PutBlobSizeMB       float64
	IncludePathPatterns []string
	ListOfFiles         string
	ListOfVersionIDs    string
	blobTagsMap         common.BlobTags
	cpkByName           string
	cpkByValue          bool
	preserveOwner       bool
}

func (cca *CookedCopyCmdArgs) isRedirection() bool {
	switch cca.FromTo {
	case common.EFromTo.BlobPipe():
		fallthrough
	case common.EFromTo.PipeBlob():
		return true
	default:
		return false
	}
}

func (cca *CookedCopyCmdArgs) process() error {

	err := common.SetBackupMode(cca.backupMode, cca.FromTo)
	if err != nil {
		return err
	}

	if cca.isRedirection() {
		err := cca.processRedirectionCopy()

		if err != nil {
			return err
		}

		// if no error, the operation is now complete
		glcm.Exit(nil, common.EExitCode.Success())
	}
	return cca.processCopyJobPartOrders()
}

// TODO discuss with Jeff what features should be supported by redirection, such as metadata, content-type, etc.
func (cca *CookedCopyCmdArgs) processRedirectionCopy() error {
	if cca.FromTo == common.EFromTo.PipeBlob() {
		return cca.processRedirectionUpload(cca.Destination, cca.blockSize)
	} else if cca.FromTo == common.EFromTo.BlobPipe() {
		return cca.processRedirectionDownload(cca.Source)
	}

	return fmt.Errorf("unsupported redirection type: %s", cca.FromTo)
}

func (cca *CookedCopyCmdArgs) processRedirectionDownload(blobResource common.ResourceString) error {

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// step 0: check the Stdout before uploading
	_, err := os.Stdout.Stat()
	if err != nil {
		return fmt.Errorf("fatal: cannot write to Stdout due to error: %s", err.Error())
	}

	// The isPublic flag is useful in S2S transfers but doesn't much matter for download. Fortunately, no S2S happens here.
	// This means that if there's auth, there's auth. We're happy and can move on.
	// GetCredentialInfoForLocation also populates oauth token fields... so, it's very easy.
	credInfo, _, err := GetCredentialInfoForLocation(ctx, common.ELocation.Blob(), blobResource, true, cca.CpkOptions)

	if err != nil {
		return fmt.Errorf("fatal: cannot find auth on source blob URL: %s", err.Error())
	}

	// step 1: create client options
	// note: dstCred is nil, as we could not reauth effectively because stdout is a pipe.
	options := &blockblob.ClientOptions{ClientOptions: createClientOptions(azcopyScanningLogger, nil, nil)}

	// step 2: parse source url
	u, err := blobResource.FullURL()
	if err != nil {
		return fmt.Errorf("fatal: cannot parse source blob URL due to error: %s", err.Error())
	}

	var blobClient *blockblob.Client
	if credInfo.CredentialType.IsAzureOAuth() {
		blobClient, err = blockblob.NewClient(u.String(), credInfo.OAuthTokenInfo.TokenCredential, options)
	} else {
		blobClient, err = blockblob.NewClientWithNoCredential(u.String(), options)
	}
	if err != nil {
		return fmt.Errorf("fatal: Could not create client: %s", err.Error())
	}

	// step 3: start download

	blobStream, err := blobClient.DownloadStream(ctx, &blob.DownloadStreamOptions{
		CPKInfo:      cca.CpkOptions.GetCPKInfo(),
		CPKScopeInfo: cca.CpkOptions.GetCPKScopeInfo(),
	})
	if err != nil {
		return fmt.Errorf("fatal: cannot download blob due to error: %s", err.Error())
	}

	blobBody := blobStream.NewRetryReader(ctx, &blob.RetryReaderOptions{MaxRetries: ste.MaxRetryPerDownloadBody})
	defer blobBody.Close()

	// step 4: pipe everything into Stdout
	_, err = io.Copy(os.Stdout, blobBody)
	if err != nil {
		return fmt.Errorf("fatal: cannot download blob to Stdout due to error: %s", err.Error())
	}

	return nil
}

func (cca *CookedCopyCmdArgs) processRedirectionUpload(blobResource common.ResourceString, blockSize int64) error {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// Use the concurrency environment value
	concurrencyEnvVar := common.GetEnvironmentVariable(common.EEnvironmentVariable.ConcurrencyValue())

	pipingUploadParallelism := pipingUploadParallelism
	if concurrencyEnvVar != "" {
		// handle when the concurrency value is AUTO
		if concurrencyEnvVar == "AUTO" {
			return errors.New("concurrency auto-tuning is not possible when using redirection transfers (AZCOPY_CONCURRENCY_VALUE = AUTO)")
		}

		// convert the concurrency value to int
		concurrencyValue, err := strconv.ParseInt(concurrencyEnvVar, 10, 32)

		//handle the error if the conversion fails
		if err != nil {
			return fmt.Errorf("AZCOPY_CONCURRENCY_VALUE is not set to a valid value, an integer is expected (current value: %s): %w", concurrencyEnvVar, err)
		}

		pipingUploadParallelism = int(concurrencyValue) // Cast to Integer
	}

	// if no block size is set, then use default value
	if blockSize == 0 {
		blockSize = pipingDefaultBlockSize
	}

	// GetCredentialInfoForLocation populates oauth token fields... so, it's very easy.
	credInfo, _, err := GetCredentialInfoForLocation(ctx, common.ELocation.Blob(), blobResource, false, cca.CpkOptions)

	if err != nil {
		return fmt.Errorf("fatal: cannot find auth on destination blob URL: %s", err.Error())
	}

	var reauthTok *common.ScopedAuthenticator
	if at, ok := credInfo.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok {
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		reauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}

	// step 0: initialize pipeline
	// Reauthentication is theoretically possible here, since stdin is blocked.
	options := &blockblob.ClientOptions{ClientOptions: createClientOptions(common.AzcopyCurrentJobLogger, nil, reauthTok)}

	// step 1: parse destination url
	u, err := blobResource.FullURL()
	if err != nil {
		return fmt.Errorf("fatal: cannot parse destination blob URL due to error: %s", err.Error())
	}

	// step 2: leverage high-level call in Blob SDK to upload stdin in parallel
	var blockBlobClient *blockblob.Client
	if credInfo.CredentialType.IsAzureOAuth() {
		blockBlobClient, err = blockblob.NewClient(u.String(), credInfo.OAuthTokenInfo.TokenCredential, options)
	} else {
		blockBlobClient, err = blockblob.NewClientWithNoCredential(u.String(), options)
	}
	if err != nil {
		return fmt.Errorf("fatal: Could not construct blob client: %s", err.Error())
	}

	metadataString := cca.metadata
	metadataMap := common.Metadata{}
	if len(metadataString) > 0 {
		for _, keyAndValue := range strings.Split(metadataString, ";") { // key/value pairs are separated by ';'
			kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
			metadataMap[kv[0]] = &kv[1]
		}
	}
	blobTags := cca.blobTagsMap
	var bbAccessTier *blob.AccessTier
	if cca.blockBlobTier != common.EBlockBlobTier.None() {
		bbAccessTier = to.Ptr(blob.AccessTier(cca.blockBlobTier.String()))
	}
	_, err = blockBlobClient.UploadStream(ctx, os.Stdin, &blockblob.UploadStreamOptions{
		BlockSize:   blockSize,
		Concurrency: pipingUploadParallelism,
		Metadata:    metadataMap,
		Tags:        blobTags,
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType:        common.IffNotEmpty(cca.contentType),
			BlobContentLanguage:    common.IffNotEmpty(cca.contentLanguage),
			BlobContentEncoding:    common.IffNotEmpty(cca.contentEncoding),
			BlobContentDisposition: common.IffNotEmpty(cca.contentDisposition),
			BlobCacheControl:       common.IffNotEmpty(cca.cacheControl),
		},
		AccessTier:   bbAccessTier,
		CPKInfo:      cca.CpkOptions.GetCPKInfo(),
		CPKScopeInfo: cca.CpkOptions.GetCPKScopeInfo(),
	})

	return err
}

// get source credential - if there is a token it will be used to get passed along our pipeline
func (cca *CookedCopyCmdArgs) getSrcCredential(ctx context.Context, jpo *common.CopyJobPartOrderRequest) (common.CredentialInfo, error) {
	switch cca.FromTo.From() {
	case common.ELocation.Local(), common.ELocation.Benchmark():
		return common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}, nil
	case common.ELocation.S3():
		return common.CredentialInfo{CredentialType: common.ECredentialType.S3AccessKey()}, nil
	case common.ELocation.GCP():
		return common.CredentialInfo{CredentialType: common.ECredentialType.GoogleAppCredentials()}, nil
	case common.ELocation.Pipe():
		panic("Invalid Source")
	}

	srcCredInfo, isPublic, err := GetCredentialInfoForLocation(ctx, cca.FromTo.From(), cca.Source, true, cca.CpkOptions)
	if err != nil {
		return srcCredInfo, err
		// If S2S and source takes OAuthToken as its cred type (OR) source takes anonymous as its cred type, but it's not public and there's no SAS
	} else if cca.FromTo.IsS2S() &&
		((srcCredInfo.CredentialType == common.ECredentialType.OAuthToken() && !cca.FromTo.To().CanForwardOAuthTokens()) || // Blob can forward OAuth tokens; BlobFS inherits this.
			(srcCredInfo.CredentialType == common.ECredentialType.Anonymous() && !isPublic && cca.Source.SAS == "")) {
		return srcCredInfo, errors.New("a SAS token (or S3 access key) is required as a part of the source in S2S transfers, unless the source is a public resource. Blob and BlobFS additionally support OAuth on both source and destination")
	} else if cca.FromTo.IsS2S() && (srcCredInfo.CredentialType == common.ECredentialType.SharedKey() || jpo.CredentialInfo.CredentialType == common.ECredentialType.SharedKey()) {
		return srcCredInfo, errors.New("shared key auth is not supported for S2S operations")
	}

	if cca.Source.SAS != "" && cca.FromTo.IsS2S() && jpo.CredentialInfo.CredentialType == common.ECredentialType.OAuthToken() {
		glcm.Info("Authentication: If the source and destination accounts are in the same AAD tenant & the user/spn/msi has appropriate permissions on both, the source SAS token is not required and OAuth can be used round-trip.")
	}

	if cca.FromTo.IsS2S() {
		jpo.S2SSourceCredentialType = srcCredInfo.CredentialType

		if jpo.S2SSourceCredentialType.IsAzureOAuth() {
			uotm := GetUserOAuthTokenManagerInstance()
			// get token from env var or cache
			if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
				return srcCredInfo, err
			} else if _, err := tokenInfo.GetTokenCredential(); err != nil {
				// we just verified we can get a token credential
				return srcCredInfo, err
			}
		}
	}
	return srcCredInfo, nil
}

// handles the copy command
// dispatches the job order (in parts) to the storage engine
func (cca *CookedCopyCmdArgs) processCopyJobPartOrders() (err error) {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	// Make AUTO default for Azure Files since Azure Files throttles too easily unless user specified concurrency value
	if jobsAdmin.JobsAdmin != nil &&
		((cca.FromTo.From() == common.ELocation.File() || cca.FromTo.From() == common.ELocation.FileNFS()) ||
			(cca.FromTo.To() == common.ELocation.File() || cca.FromTo.To() == common.ELocation.FileNFS())) &&
		common.GetEnvironmentVariable(common.EEnvironmentVariable.ConcurrencyValue()) == "" {
		jobsAdmin.JobsAdmin.SetConcurrencySettingsToAuto()
	}

	if err := common.VerifyIsURLResolvable(cca.Source.Value); cca.FromTo.From().IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve source: %w", err)
	}

	if err := common.VerifyIsURLResolvable(cca.Destination.Value); cca.FromTo.To().IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve destination: %w", err)
	}

	// Note: credential info here is only used by remove at the moment.
	// TODO: Get the entirety of remove into the new copyEnumeratorInit script so we can remove this
	//       and stop having two places in copy that we get credential info
	// verifies credential type and initializes credential info.
	// Note: Currently, only one credential type is necessary for source and destination.
	// For upload&download, only one side need credential.
	// For S2S copy, as azcopy-v10 use Put*FromUrl, only one credential is needed for destination.
	if cca.credentialInfo.CredentialType, err = getCredentialType(ctx, rawFromToInfo{
		fromTo:      cca.FromTo,
		source:      cca.Source,
		destination: cca.Destination,
	}, cca.CpkOptions); err != nil {
		return err
	}

	// For OAuthToken credential, assign OAuthTokenInfo to CopyJobPartOrderRequest properly,
	// the info will be transferred to STE.
	if cca.credentialInfo.CredentialType.IsAzureOAuth() {
		uotm := GetUserOAuthTokenManagerInstance()
		// Get token from env var or cache.
		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return err
		} else {
			cca.credentialInfo.OAuthTokenInfo = *tokenInfo
		}
	}

	// initialize the fields that are constant across all job part orders,
	// and for which we have sufficient info now to set them
	jobPartOrder := common.CopyJobPartOrderRequest{
		JobID:               cca.jobID,
		FromTo:              cca.FromTo,
		ForceWrite:          cca.ForceWrite,
		ForceIfReadOnly:     cca.ForceIfReadOnly,
		AutoDecompress:      cca.autoDecompress,
		Priority:            common.EJobPriority.Normal(),
		LogLevel:            LogLevel,
		ExcludeBlobType:     cca.excludeBlobType,
		SymlinkHandlingType: cca.SymlinkHandling,
		BlobAttributes: common.BlobTransferAttributes{
			BlobType:                 cca.blobType,
			BlockSizeInBytes:         cca.blockSize,
			PutBlobSizeInBytes:       cca.putBlobSize,
			ContentType:              cca.contentType,
			ContentEncoding:          cca.contentEncoding,
			ContentLanguage:          cca.contentLanguage,
			ContentDisposition:       cca.contentDisposition,
			CacheControl:             cca.cacheControl,
			BlockBlobTier:            cca.blockBlobTier,
			PageBlobTier:             cca.pageBlobTier,
			Metadata:                 cca.metadata,
			NoGuessMimeType:          cca.noGuessMimeType,
			PreserveLastModifiedTime: cca.preserveLastModifiedTime,
			PutMd5:                   cca.putMd5,
			MD5ValidationOption:      cca.md5ValidationOption,
			DeleteSnapshotsOption:    cca.deleteSnapshotsOption,
			// Setting tags when tags explicitly provided by the user through blob-tags flag
			BlobTagsString:                   cca.blobTagsMap.ToString(),
			DeleteDestinationFileIfNecessary: cca.deleteDestinationFileIfNecessary,
		},
		CommandString:  cca.commandString,
		CredentialInfo: cca.credentialInfo,
		FileAttributes: common.FileTransferAttributes{
			TrailingDot: cca.trailingDot,
		},
	}

	srcCredInfo, err := cca.getSrcCredential(ctx, &jobPartOrder)
	if err != nil {
		return err
	}

	var srcReauth *common.ScopedAuthenticator
	if at, ok := srcCredInfo.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok {
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		srcReauth = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}

	options := createClientOptions(common.AzcopyCurrentJobLogger, nil, srcReauth)
	var azureFileSpecificOptions any
	if cca.FromTo.From() == common.ELocation.File() || cca.FromTo.From() == common.ELocation.FileNFS() {
		azureFileSpecificOptions = &common.FileClientOptions{
			AllowTrailingDot: cca.trailingDot.IsEnabled(),
		}
	}

	jobPartOrder.SrcServiceClient, err = common.GetServiceClientForLocation(
		cca.FromTo.From(),
		cca.Source,
		srcCredInfo.CredentialType,
		srcCredInfo.OAuthTokenInfo.TokenCredential,
		&options,
		azureFileSpecificOptions,
	)
	if err != nil {
		return err
	}

	if cca.FromTo.To() == common.ELocation.File() || cca.FromTo.To() == common.ELocation.FileNFS() {
		azureFileSpecificOptions = &common.FileClientOptions{
			AllowTrailingDot: cca.trailingDot.IsEnabled(),
			AllowSourceTrailingDot: cca.trailingDot.IsEnabled() &&
				(cca.FromTo.From() == common.ELocation.File() || cca.FromTo.From() == common.ELocation.FileNFS()),
		}
	}

	var dstReauthTok *common.ScopedAuthenticator
	if at, ok := cca.credentialInfo.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok {
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		dstReauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}

	var srcCred *common.ScopedToken
	if cca.FromTo.IsS2S() && srcCredInfo.CredentialType.IsAzureOAuth() {
		srcCred = common.NewScopedCredential(srcCredInfo.OAuthTokenInfo.TokenCredential, srcCredInfo.CredentialType)
	}
	options = createClientOptions(common.AzcopyCurrentJobLogger, srcCred, dstReauthTok)
	jobPartOrder.DstServiceClient, err = common.GetServiceClientForLocation(
		cca.FromTo.To(),
		cca.Destination,
		cca.credentialInfo.CredentialType,
		cca.credentialInfo.OAuthTokenInfo.TokenCredential,
		&options,
		azureFileSpecificOptions,
	)
	if err != nil {
		return err
	}

	jobPartOrder.DestinationRoot = cca.Destination
	jobPartOrder.SourceRoot = cca.Source
	jobPartOrder.SourceRoot.Value, err = GetResourceRoot(cca.Source.Value, cca.FromTo.From())
	if err != nil {
		return err
	}

	// Stripping the trailing /* for local occurs much later than stripping the trailing /* for remote resources.
	// TODO: Move these into the same place for maintainability.
	if diff := strings.TrimPrefix(cca.Source.Value, jobPartOrder.SourceRoot.Value); cca.FromTo.From().IsLocal() &&
		diff == "*" || diff == common.OS_PATH_SEPARATOR+"*" || diff == common.AZCOPY_PATH_SEPARATOR_STRING+"*" {
		// trim the /*
		cca.Source.Value = jobPartOrder.SourceRoot.Value
		// set stripTopDir to true so that --list-of-files/--include-path play nice
		cca.StripTopDir = true
	}

	// TODO: Remove this check when FileBlob w/ File OAuth works.
	if cca.FromTo.IsS2S() && (cca.FromTo.From() == common.ELocation.File() || cca.FromTo.From() == common.ELocation.FileNFS()) &&
		srcCredInfo.CredentialType.IsAzureOAuth() && (cca.FromTo.To() != common.ELocation.File() || cca.FromTo.To() != common.ELocation.FileNFS()) {
		return fmt.Errorf("S2S copy from Azure File authenticated with Azure AD to Blob/BlobFS is not supported")
	}

	// Check if destination is system container
	if cca.FromTo.IsS2S() || cca.FromTo.IsUpload() {
		dstContainerName, err := GetContainerName(cca.Destination.Value, cca.FromTo.To())
		if err != nil {
			return fmt.Errorf("failed to get container name from destination (is it formatted correctly?): %w", err)
		}
		if common.IsSystemContainer(dstContainerName) {
			return fmt.Errorf("cannot copy to system container '%s'", dstContainerName)
		}
	}

	// Check protocol compatibility for File Shares
	if err := validateProtocolCompatibility(ctx, cca.FromTo, cca.Source, cca.Destination, jobPartOrder.SrcServiceClient, jobPartOrder.DstServiceClient); err != nil {
		return err
	}

	switch {
	case cca.FromTo.IsUpload(), cca.FromTo.IsDownload(), cca.FromTo.IsS2S():
		// Execute a standard copy command
		var e *CopyEnumerator
		e, err = cca.initEnumerator(jobPartOrder, srcCredInfo, ctx)
		if err != nil {
			return fmt.Errorf("failed to initialize enumerator: %w", err)
		}
		err = e.enumerate()

	case cca.FromTo.IsDelete():
		// Delete gets ran through copy, so handle delete
		if cca.FromTo.From() == common.ELocation.BlobFS() {
			// TODO merge with BlobTrash case
			// Currently, Blob Delete in STE does not appropriately handle folders. In addition, dfs delete is free-ish.
			err = removeBfsResources(cca)
		} else {
			e, createErr := newRemoveEnumerator(cca)
			if createErr != nil {
				return fmt.Errorf("failed to initialize enumerator: %w", createErr)
			}

			err = e.enumerate()
		}

	case cca.FromTo.IsSetProperties():
		// Set properties as well
		e, createErr := setPropertiesEnumerator(cca)
		if createErr != nil {
			return fmt.Errorf("failed to initialize enumerator: %w", createErr)
		}
		err = e.enumerate()

	default:
		return fmt.Errorf("copy direction %v is not supported", cca.FromTo)
	}

	if err != nil {
		if err == ErrNothingToRemove || err == NothingScheduledError {
			return err // don't wrap it with anything that uses the word "error"
		} else {
			return fmt.Errorf("cannot start job due to error %s", err)
		}
	}

	return nil
}

// wraps call to lifecycle manager to wait for the job to complete
// if blocking is specified to true, then this method will never return
// if blocking is specified to false, then another goroutine spawns and wait out the job
func (cca *CookedCopyCmdArgs) waitUntilJobCompletion(blocking bool) {
	// print initial message to indicate that the job is starting
	// if on dry run mode do not want to print message since no  job is being done
	if !cca.dryrunMode {
		// Output the log location if log-level is set to other then NONE
		var logPathFolder string
		if azcopyLogPathFolder != "" {
			logPathFolder = fmt.Sprintf("%s%s%s.log", azcopyLogPathFolder, common.OS_PATH_SEPARATOR, cca.jobID)
		}
		glcm.Init(common.GetStandardInitOutputBuilder(cca.jobID.String(),
			logPathFolder,
			cca.isCleanupJob,
			cca.cleanupJobMessage))
	}

	// initialize the times necessary to track progress
	cca.jobStartTime = time.Now()
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = 0

	// hand over control to the lifecycle manager if blocking
	if blocking {
		glcm.InitiateProgressReporting(cca)
		glcm.SurrenderControl()
	} else {
		// non-blocking, return after spawning a go routine to watch the job
		glcm.InitiateProgressReporting(cca)
	}
}

func (cca *CookedCopyCmdArgs) Cancel(lcm common.LifecycleMgr) {
	// prompt for confirmation, except when enumeration is complete
	if !cca.isEnumerationComplete {
		answer := lcm.Prompt("The source enumeration is not complete, "+
			"cancelling the job at this point means it cannot be resumed.",
			common.PromptDetails{
				PromptType: common.EPromptType.Cancel(),
				ResponseOptions: []common.ResponseOption{
					common.EResponseOption.Yes(),
					common.EResponseOption.No(),
				},
			})

		if answer != common.EResponseOption.Yes() {
			// user aborted cancel
			return
		}
	}

	err := cookedCancelCmdArgs{jobID: cca.jobID}.process()
	if err != nil {
		lcm.Error("error occurred while cancelling the job " + cca.jobID.String() + ": " + err.Error())
	}
}

func (cca *CookedCopyCmdArgs) hasFollowup() bool {
	return cca.followupJobArgs != nil
}

func (cca *CookedCopyCmdArgs) launchFollowup(priorJobExitCode common.ExitCode) {
	go func() {
		glcm.AllowReinitiateProgressReporting()
		cca.followupJobArgs.priorJobExitCode = &priorJobExitCode
		err := cca.followupJobArgs.process()
		if err == ErrNothingToRemove {
			glcm.Info("Cleanup completed (nothing needed to be deleted)")
			glcm.Exit(nil, common.EExitCode.Success())
		} else if err != nil {
			glcm.Error("failed to perform followup/cleanup job due to error: " + err.Error())
		}
		glcm.SurrenderControl()
	}()
}

func (cca *CookedCopyCmdArgs) getSuccessExitCode() common.ExitCode {
	if cca.priorJobExitCode != nil {
		return *cca.priorJobExitCode // in a chain of jobs our best case outcome is whatever the predecessor(s) finished with
	} else {
		return common.EExitCode.Success()
	}
}

func (cca *CookedCopyCmdArgs) ReportProgressOrExit(lcm common.LifecycleMgr) (totalKnownCount uint32) {
	// fetch a job status
	var summary common.ListJobSummaryResponse
	Rpc(common.ERpcCmd.ListJobSummary(), &cca.jobID, &summary)
	glcmSwapOnce.Do(func() {
		Rpc(common.ERpcCmd.GetJobLCMWrapper(), &cca.jobID, &glcm)
	})
	summary.IsCleanupJob = cca.isCleanupJob // only FE knows this, so we can only set it here
	cleanupStatusString := fmt.Sprintf("Cleanup %v/%v", summary.TransfersCompleted, summary.TotalTransfers)

	jobDone := summary.JobStatus.IsJobDone()
	totalKnownCount = summary.TotalTransfers

	// if json is not desired, and job is done, then we generate a special end message to conclude the job
	duration := time.Since(cca.jobStartTime) // report the total run time of the job

	var computeThroughput = func() float64 {
		// compute the average throughput for the last time interval
		bytesInMb := float64(float64(summary.BytesOverWire-cca.intervalBytesTransferred) / float64(base10Mega))
		timeElapsed := time.Since(cca.intervalStartTime).Seconds()

		// reset the interval timer and byte count
		cca.intervalStartTime = time.Now()
		cca.intervalBytesTransferred = summary.BytesOverWire

		return common.Iff(timeElapsed != 0, bytesInMb/timeElapsed, 0) * 8
	}
	glcm.Progress(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(summary)
			common.PanicIfErr(err)
			return string(jsonOutput)
		} else {
			// abbreviated output for cleanup jobs
			if cca.isCleanupJob {
				return cleanupStatusString
			}

			// if json is not needed, then we generate a message that goes nicely on the same line
			// display a scanning keyword if the job is not completely ordered
			var scanningString = " (scanning...)"
			if summary.CompleteJobOrdered {
				scanningString = ""
			}

			throughput := computeThroughput()
			throughputString := fmt.Sprintf("2-sec Throughput (Mb/s): %v", jobsAdmin.ToFixed(throughput, 4))
			if throughput == 0 {
				// As there would be case when no bits sent from local, e.g. service side copy, when throughput = 0, hide it.
				throughputString = ""
			}

			// indicate whether constrained by disk or not
			isBenchmark := cca.FromTo.From() == common.ELocation.Benchmark()
			perfString, diskString := getPerfDisplayText(summary.PerfStrings, summary.PerfConstraint, duration, isBenchmark)
			return fmt.Sprintf("%.1f %%, %v Done, %v Failed, %v Pending, %v Skipped, %v Total%s, %s%s%s",
				summary.PercentComplete,
				summary.TransfersCompleted,
				summary.TransfersFailed,
				summary.TotalTransfers-(summary.TransfersCompleted+summary.TransfersFailed+summary.TransfersSkipped),
				summary.TransfersSkipped, summary.TotalTransfers, scanningString, perfString, throughputString, diskString)
		}
	})

	if jobDone {
		summary.SkippedSymlinkCount = atomic.LoadUint32(&cca.atomicSkippedSymlinkCount)
		summary.SkippedSpecialFileCount = atomic.LoadUint32(&cca.atomicSkippedSpecialFileCount)

		exitCode := cca.getSuccessExitCode()
		if summary.TransfersFailed > 0 || summary.JobStatus == common.EJobStatus.Cancelled() || summary.JobStatus == common.EJobStatus.Cancelling() {
			exitCode = common.EExitCode.Error()
		}

		builder := func(format common.OutputFormat) string {
			if format == common.EOutputFormat.Json() {
				jsonOutput, err := json.Marshal(summary)
				common.PanicIfErr(err)
				return string(jsonOutput)
			} else {
				screenStats, logStats := formatExtraStats(cca.FromTo, summary.AverageIOPS, summary.AverageE2EMilliseconds, summary.NetworkErrorPercentage, summary.ServerBusyPercentage)

				output := fmt.Sprintf(
					`

Job %s summary
Elapsed Time (Minutes): %v
Number of File Transfers: %v
Number of Folder Property Transfers: %v
Number of Symlink Transfers: %v
Total Number of Transfers: %v
Number of File Transfers Completed: %v
Number of Folder Transfers Completed: %v
Number of File Transfers Failed: %v
Number of Folder Transfers Failed: %v
Number of File Transfers Skipped: %v
Number of Folder Transfers Skipped: %v
Number of Symbolic Links Skipped: %v
Number of Hardlinks Converted: %v
Number of Special Files Skipped: %v
Total Number of Bytes Transferred: %v
Final Job Status: %v%s%s
`,
					summary.JobID.String(),
					jobsAdmin.ToFixed(duration.Minutes(), 4),
					summary.FileTransfers,
					summary.FolderPropertyTransfers,
					summary.SymlinkTransfers,
					summary.TotalTransfers,
					summary.TransfersCompleted-summary.FoldersCompleted,
					summary.FoldersCompleted,
					summary.TransfersFailed-summary.FoldersFailed,
					summary.FoldersFailed,
					summary.TransfersSkipped-summary.FoldersSkipped,
					summary.FoldersSkipped,
					summary.SkippedSymlinkCount,
					summary.HardlinksConvertedCount,
					summary.SkippedSpecialFileCount,
					summary.TotalBytesTransferred,
					summary.JobStatus,
					screenStats,
					formatPerfAdvice(summary.PerformanceAdvice))

				// abbreviated output for cleanup jobs
				if cca.isCleanupJob {
					output = fmt.Sprintf("%s: %s)", cleanupStatusString, summary.JobStatus)
				}

				// log to job log
				jobMan, exists := jobsAdmin.JobsAdmin.JobMgr(summary.JobID)
				if exists {
					// Passing this as LogError ensures the stats are always logged.
					jobMan.Log(common.LogError, logStats+"\n"+output)
				}
				return output
			}
		}

		if cca.hasFollowup() {
			lcm.Exit(builder, common.EExitCode.NoExit()) // leave the app running to process the followup
			cca.launchFollowup(exitCode)
			lcm.SurrenderControl() // the followup job will run on its own goroutines
		} else {
			lcm.Exit(builder, exitCode)
		}
	}

	return
}

func formatPerfAdvice(advice []common.PerformanceAdvice) string {
	if len(advice) == 0 {
		return ""
	}
	b := strings.Builder{}
	b.WriteString("\n\n") // two newlines to separate the perf results from everything else
	b.WriteString("Performance benchmark results: \n")
	b.WriteString("Note: " + common.BenchmarkPreviewNotice + "\n")
	for _, a := range advice {
		b.WriteString("\n")
		pri := "Main"
		if !a.PriorityAdvice {
			pri = "Additional"
		}
		b.WriteString(pri + " Result:\n")
		b.WriteString("  Code:   " + a.Code + "\n")
		b.WriteString("  Desc:   " + a.Title + "\n")
		b.WriteString("  Reason: " + a.Reason + "\n")
	}
	b.WriteString("\n")
	b.WriteString(common.BenchmarkFinalDisclaimer)
	if runtime.GOOS == "linux" {
		b.WriteString(common.BenchmarkLinuxExtraDisclaimer)
	}
	return b.String()
}

// format extra stats to include in the log.  If benchmarking, also output them on screen (but not to screen in normal
// usage because too cluttered)
func formatExtraStats(fromTo common.FromTo, avgIOPS int, avgE2EMilliseconds int, networkErrorPercent float32, serverBusyPercent float32) (screenStats, logStats string) {
	logStats = fmt.Sprintf(
		`

Diagnostic stats:
IOPS: %v
End-to-end ms per request: %v
Network Errors: %.2f%%
Server Busy: %.2f%%`,
		avgIOPS, avgE2EMilliseconds, networkErrorPercent, serverBusyPercent)

	if fromTo.From() == common.ELocation.Benchmark() {
		screenStats = logStats
		logStats = "" // since will display in the screen stats, and they get logged too
	}

	return
}

// Is disk speed looking like a constraint on throughput?  Ignore the first little-while,
// to give an (arbitrary) amount of time for things to reach steady-state.
func getPerfDisplayText(perfDiagnosticStrings []string, constraint common.PerfConstraint, durationOfJob time.Duration, isBench bool) (perfString string, diskString string) {
	perfString = ""
	if shouldDisplayPerfStates() {
		perfString = "[States: " + strings.Join(perfDiagnosticStrings, ", ") + "], "
	}

	haveBeenRunningLongEnoughToStabilize := durationOfJob.Seconds() > 30                                    // this duration is an arbitrary guesstimate
	if constraint != common.EPerfConstraint.Unknown() && haveBeenRunningLongEnoughToStabilize && !isBench { // don't display when benchmarking, because we got some spurious slow "disk" constraint reports there - which would be confusing given there is no disk in release 1 of benchmarking
		diskString = fmt.Sprintf(" (%s may be limiting speed)", constraint)
	} else {
		diskString = ""
	}
	return
}

func shouldDisplayPerfStates() bool {
	return common.GetEnvironmentVariable(common.EEnvironmentVariable.ShowPerfStates()) != ""
}

func isStdinPipeIn() (bool, error) {
	// check the Stdin to see if we are uploading or downloading
	info, err := os.Stdin.Stat()
	if err != nil {
		return false, fmt.Errorf("fatal: failed to read from Stdin due to error: %s", err)
	}

	// if the stdin is a named pipe, then we assume there will be data on the stdin
	// the reason for this assumption is that we do not know when will the data come in
	// it could come in right away, or come in 10 minutes later
	return info.Mode()&(os.ModeNamedPipe|os.ModeSocket) != 0, nil
}

var cpCmd *cobra.Command

// TODO check file size, max is 4.75TB
func init() {
	raw := rawCopyCmdArgs{}

	// cpCmd represents the cp command
	cpCmd = &cobra.Command{
		Use:        "copy [source] [destination]",
		Aliases:    []string{"cp", "c"},
		SuggestFor: []string{"cpy", "cy", "mv"}, // TODO why does message appear twice on the console
		Short:      copyCmdShortDescription,
		Long:       copyCmdLongDescription,
		Example:    copyCmdExample,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 { // redirection
				// Enforce the usage of from-to flag when pipes are involved
				if raw.fromTo == "" {
					return fmt.Errorf("fatal: from-to argument required, PipeBlob (upload) or BlobPipe (download) is acceptable")
				}
				var userFromTo common.FromTo
				err := userFromTo.Parse(raw.fromTo)
				if err != nil || (userFromTo != common.EFromTo.PipeBlob() && userFromTo != common.EFromTo.BlobPipe()) {
					return fmt.Errorf("fatal: invalid from-to argument passed: %s", raw.fromTo)
				}

				if userFromTo == common.EFromTo.PipeBlob() {
					// Case 1: PipeBlob. Check for the std input pipe
					stdinPipeIn, err := isStdinPipeIn()
					if !stdinPipeIn || err != nil {
						return fmt.Errorf("fatal: failed to read from Stdin due to error: %s", err)
					}
					raw.src = pipeLocation
					raw.dst = args[0]
				} else {
					// Case 2: BlobPipe. In this case if pipe is missing, content will be echoed on the terminal
					raw.src = args[0]
					raw.dst = pipeLocation
				}
			} else if len(args) == 2 { // normal copy
				raw.src = args[0]
				raw.dst = args[1]

				// under normal copy, we may ask the user questions such as whether to overwrite a file
				glcm.EnableInputWatcher()
				if cancelFromStdin {
					glcm.EnableCancelFromStdIn()
				}
			} else {
				return errors.New("wrong number of arguments, please refer to the help page on usage of this command")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			// We infer FromTo and validate it here since it is critical to a lot of other options parsing below.
			userFromTo, err := ValidateFromTo(raw.src, raw.dst, raw.fromTo)
			if err != nil {
				glcm.Error("failed to parse --from-to user input due to error: " + err.Error())
			}

			raw.preserveInfo, raw.preservePermissions = ComputePreserveFlags(cmd, userFromTo,
				raw.preserveInfo, raw.preserveSMBInfo, raw.preservePermissions, raw.preserveSMBPermissions)
			// TODO: Remove. Added for debugging purposes.
			//fmt.Println(fmt.Sprintf("PreserveInfo: %v, PreservePermissions: %v, NFS: %v, fromTo: %v", raw.preserveInfo, raw.preservePermissions, common.IsNFSCopy(), userFromTo.String()))

			cooked, err := raw.cook()
			if err != nil {
				glcm.Error("failed to parse user input due to error: " + err.Error())
			}
			glcm.Info("Scanning...")

			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				glcm.Error("failed to perform copy command due to error: " + err.Error() + getErrorCodeUrl(err))
			}

			if cooked.dryrunMode {
				glcm.Exit(nil, common.EExitCode.Success())
			}

			glcm.SurrenderControl()
		},
	}
	rootCmd.AddCommand(cpCmd)

	// filters change which files get transferred
	cpCmd.PersistentFlags().BoolVar(&raw.followSymlinks, "follow-symlinks", false,
		"False by default. Follow symbolic links when uploading from local file system.")
	cpCmd.PersistentFlags().StringVar(&raw.includeBefore, common.IncludeBeforeFlagName, "",
		"Include only those files were modified before or on the given date/time. \n "+
			"The value should be in ISO8601 format. If no timezone is specified, the value is assumed to be in the local timezone of the machine running AzCopy. "+
			"\n E.g. '2020-08-19T15:04:00Z' for a UTC time, or '2020-08-19' for midnight (00:00) in the local timezone. "+
			"\n As of AzCopy 10.7, this flag applies only to files, not folders, so folder properties won't be copied when using this flag with --preserve-info or --preserve-permissions.")
	cpCmd.PersistentFlags().StringVar(&raw.includeAfter, common.IncludeAfterFlagName, "",
		"Include only those files modified on or after the given date/time. \n "+
			"The value should be in ISO8601 format. If no timezone is specified, the value is assumed to be in the local timezone of the machine running AzCopy. "+
			"\n E.g. '2020-08-19T15:04:00Z' for a UTC time, or '2020-08-19' for midnight (00:00) in the local timezone. "+
			"\n As of AzCopy 10.5, this flag applies only to files, not folders, so folder properties won't be copied when using this flag with --preserve-info or --preserve-permissions.")
	cpCmd.PersistentFlags().StringVar(&raw.include, "include-pattern", "",
		"Include only these files when copying. "+
			"\n This option supports wildcard characters (*). Separate files by using a ';' (For example: *.jpg;*.pdf;exactName).")
	cpCmd.PersistentFlags().StringVar(&raw.includePath, "include-path", "",
		"Include only these paths when copying. "+
			"This option does not support wildcard characters (*). \n Checks the relative path prefix (For example: myFolder;myFolder/subDirName/file.pdf).")
	cpCmd.PersistentFlags().StringVar(&raw.excludePath, "exclude-path", "",
		"Exclude these paths when copying. "+ // Currently, only exclude-path is supported alongside account traversal.
			"This option does not support wildcard characters (*). \n Checks relative path prefix (For example: myFolder;myFolder/subDirName/file.pdf). "+
			"\n When used in combination with account traversal, paths do not include the container name.")
	cpCmd.PersistentFlags().StringVar(&raw.includeRegex, "include-regex", "",
		"Include only the relative path of the files that align with regular expressions. "+
			"\n Separate regular expressions with ';'.")
	cpCmd.PersistentFlags().StringVar(&raw.excludeRegex, "exclude-regex", "",
		"Exclude all the relative path of the files that align with regular expressions. Separate regular expressions with ';'.")
	// This flag is implemented only for Storage Explorer.
	cpCmd.PersistentFlags().StringVar(&raw.listOfFilesToCopy, "list-of-files", "",
		"Defines the location of text file which has the list of files to be copied. "+
			"\n The text file should contain paths from root for each file name or directory written on a separate line.")
	cpCmd.PersistentFlags().StringVar(&raw.exclude, "exclude-pattern", "",
		"Exclude these files when copying. This option supports wildcard characters (*). "+
			"\n Separate files by using a ';' (For example: *.jpg;*.pdf;exactName).")
	cpCmd.PersistentFlags().StringVar(&raw.forceWrite, "overwrite", "true",
		"Overwrite the conflicting files and blobs at the destination if this flag is set to true (default 'true'). "+
			"\n Possible values include 'true', 'false', 'prompt', and 'ifSourceNewer'."+
			"\n  For destinations that support folders, conflicting folder-level properties will be overwritten if this flag is 'true' or if a positive response is provided to the prompt.")
	cpCmd.PersistentFlags().BoolVar(&raw.autoDecompress, "decompress", false,
		"False by default. Automatically decompress files when downloading, if their content-encoding indicates that they are compressed."+
			"\n  The supported content-encoding values are 'gzip' and 'deflate'. "+
			"\n File extensions of '.gz'/'.gzip' or '.zz' aren't necessary, but will be removed if present.")
	cpCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", false,
		"False by default. Look into sub-directories recursively when uploading from local file system.")
	cpCmd.PersistentFlags().StringVar(&raw.fromTo, "from-to", "", fromToHelp)
	cpCmd.PersistentFlags().StringVar(&raw.excludeBlobType, "exclude-blob-type", "",
		"Optionally specifies the type of blob (BlockBlob/ PageBlob/ AppendBlob) to exclude when copying blobs from the container "+
			"or the account. \n Use of this flag is not applicable for copying data from non azure-service to service. "+
			"\n More than one blob should be separated by ';'. ")
	// options change how the transfers are performed
	cpCmd.PersistentFlags().Float64Var(&raw.blockSizeMB, "block-size-mb", 0,
		"Use this block size (specified in MiB) when uploading to Azure Storage, and downloading from Azure Storage. "+
			"\n The default value is automatically calculated based on file size. Decimal fractions are allowed (For example: 0.25)."+
			"\n When uploading or downloading, maximum allowed block size is 0.75 * AZCOPY_BUFFER_GB. "+
			"\n Please refer https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-optimize#optimize-memory-use.")
	cpCmd.PersistentFlags().Float64Var(&raw.putBlobSizeMB, "put-blob-size-mb", 0,
		"Use this size (specified in MiB) as a threshold to determine whether to upload a blob as a single PUT request when uploading to Azure Storage. "+
			"\n The default value is automatically calculated based on file size. "+
			"\n Decimal fractions are allowed (For example: 0.25).")
	cpCmd.PersistentFlags().StringVar(&raw.blobType, "blob-type", "Detect",
		"Defines the type of blob at the destination. \n This is used for uploading blobs and when copying between accounts (default 'Detect')."+
			"\n  Valid values include 'Detect', 'BlockBlob', 'PageBlob', and 'AppendBlob'. "+
			"\n When copying between accounts, a value of 'Detect' causes AzCopy to use the type of source blob to determine the type of the destination blob. "+
			"\n When uploading a file, 'Detect' determines if the file is a VHD or a VHDX file based on the file extension. If the file is either a VHD or VHDX file, AzCopy treats the file as a page blob.")
	cpCmd.PersistentFlags().StringVar(&raw.blockBlobTier, "block-blob-tier", "None",
		"Upload block blob to Azure Storage using this blob tier. (default 'None'). "+
			"\n Valid options are Hot, Cold, Cool, Archive")
	cpCmd.PersistentFlags().StringVar(&raw.pageBlobTier, "page-blob-tier", "None",
		"Upload page blob to Azure Storage using this blob tier. (default 'None'). "+
			"\n Valid options are P10, P15, P20, P30, P4, P40, P50, P6")
	cpCmd.PersistentFlags().StringVar(&raw.metadata, "metadata", "",
		"Upload to Azure Storage with these key-value pairs as metadata. "+
			"\n Multiple key-value pairs should be separated by ';', i.e. 'foo=bar;some=thing'")
	cpCmd.PersistentFlags().StringVar(&raw.contentType, "content-type", "",
		"Specifies the content type of the file. \n Implies no-guess-mime-type flag is set to true. Returned on download.")
	cpCmd.PersistentFlags().StringVar(&raw.contentEncoding, "content-encoding", "",
		"Set the content-encoding header. Returned on download.")
	cpCmd.PersistentFlags().StringVar(&raw.contentDisposition, "content-disposition", "",
		"Set the content-disposition header. Returned on download.")
	cpCmd.PersistentFlags().StringVar(&raw.contentLanguage, "content-language", "",
		"Set the content-language header. Returned on download.")
	cpCmd.PersistentFlags().StringVar(&raw.cacheControl, "cache-control", "", "Set the cache-control header. Returned on download.")
	cpCmd.PersistentFlags().BoolVar(&raw.noGuessMimeType, "no-guess-mime-type", false, "False by default. Prevents AzCopy from detecting the content-type based on the extension or content of the file.")
	cpCmd.PersistentFlags().BoolVar(&raw.preserveLastModifiedTime, "preserve-last-modified-time", false, "False by default. Preserves Last Modified Time. Only available when destination is file system.")
	cpCmd.PersistentFlags().BoolVar(&raw.preserveSMBPermissions, "preserve-smb-permissions", false, "False by default. Preserves SMB ACLs between aware resources (Windows and Azure Files). "+
		"\n For downloads, you will also need the --backup flag to restore permissions where the new Owner will not be the user running AzCopy. "+
		"\n This flag applies to both files and folders, unless a file-only filter is specified (e.g. include-pattern).")
	cpCmd.PersistentFlags().BoolVar(&raw.asSubdir, "as-subdir", true, "True by default. Places folder sources as subdirectories under the destination.")
	cpCmd.PersistentFlags().BoolVar(&raw.preserveOwner, common.PreserveOwnerFlagName, common.PreserveOwnerDefault, "Only has an effect in downloads, and only when --preserve-smb-permissions is used. "+
		"\n If true (the default), the file Owner and Group are preserved in downloads. "+
		"\n If set to false, --preserve-smb-permissions will still preserve ACLs but Owner and Group will be based on the user running AzCopy")

	cpCmd.PersistentFlags().BoolVar(&raw.preserveSMBInfo, "preserve-smb-info", (runtime.GOOS == "windows"), "Preserves SMB property info (last write time, creation time, attribute bits) between SMB-aware resources (Windows and Azure Files). "+
		"\n On windows, this flag will be set to true by default. If the source or destination is a volume mounted on Linux using SMB protocol, this flag will have to be explicitly set to true. "+
		"\n Only the attribute bits supported by Azure Files will be transferred; any others will be ignored. This flag applies to both files and folders, unless a file-only filter is specified (e.g. include-pattern). "+
		"\n The info transferred for folders is the same as that for files, except for Last Write Time which is never preserved for folders.")
	//Marking this flag as hidden as we might not support it in the future
	_ = cpCmd.PersistentFlags().MarkHidden("preserve-smb-info")
	cpCmd.PersistentFlags().BoolVar(&raw.preserveInfo, PreserveInfoFlag, false, "Specify this flag if you want to preserve properties during the transfer operation.The previously available flag for SMB (--preserve-smb-info) is now redirected to --preserve-info flag for both SMB and NFS operations. The default value is true for Windows when copying to Azure Files SMB share and for Linux when copying to Azure Files NFS share. ")

	cpCmd.PersistentFlags().BoolVar(&raw.preservePOSIXProperties, "preserve-posix-properties", false, "False by default. 'Preserves' property info gleaned from stat or statx into object metadata.")
	cpCmd.PersistentFlags().BoolVar(&raw.preserveSymlinks, common.PreserveSymlinkFlagName, false, "False by default. If enabled, symlink destinations are preserved as the blob content, rather than uploading the file/folder on the other end of the symlink")
	cpCmd.PersistentFlags().BoolVar(&raw.forceIfReadOnly, "force-if-read-only", false, "False by default. When overwriting an existing file on Windows or Azure Files, force the overwrite to work even if the existing file has its read-only attribute set")
	cpCmd.PersistentFlags().BoolVar(&raw.backupMode, common.BackupModeFlagName, false, "False by default. Activates Windows' SeBackupPrivilege for uploads, or SeRestorePrivilege for downloads, to allow AzCopy to see read all files, regardless of their file system permissions, and to restore all permissions. "+
		"\n Requires that the account running AzCopy already has these permissions (e.g. has Administrator rights or is a member of the 'Backup Operators' group). "+
		"\n All this flag does is activate privileges that the account already has.")
	cpCmd.PersistentFlags().BoolVar(&raw.putMd5, "put-md5", false, "Create an MD5 hash of each file, and save the hash as the Content-MD5 property of the destination blob or file. "+
		"\n By default the hash is NOT created. Only available when uploading.")
	cpCmd.PersistentFlags().StringVar(&raw.md5ValidationOption, "check-md5", common.DefaultHashValidationOption.String(), "Specifies how strictly MD5 hashes should be validated when downloading. Only available when downloading. "+
		"\n Available options: NoCheck, LogOnly, FailIfDifferent, FailIfDifferentOrMissing (default 'FailIfDifferent').")
	cpCmd.PersistentFlags().StringVar(&raw.includeFileAttributes, "include-attributes", "", "(Windows only) Include files whose attributes match the attribute list. For example: A;S;R")
	cpCmd.PersistentFlags().StringVar(&raw.excludeFileAttributes, "exclude-attributes", "", "(Windows only) Exclude files whose attributes match the attribute list. For example: A;S;R")
	cpCmd.PersistentFlags().StringVar(&raw.excludeContainer, "exclude-container", "", "Exclude these containers when transferring from Account to Account only. "+
		"\n Multiple containers can be separated with ';', i.e. 'containername;containernametwo'.")
	cpCmd.PersistentFlags().BoolVar(&raw.CheckLength, "check-length", true, "True by default. Check the length of a file on the destination after the transfer. If there is a mismatch between source and destination, the transfer is marked as failed.")
	cpCmd.PersistentFlags().BoolVar(&raw.s2sPreserveProperties, "s2s-preserve-properties", true, "Preserve full properties during service to service copy. "+
		"\n For AWS S3 and Azure File non-single file source, the list operation doesn't return full properties of objects and files."+
		"\n To preserve full properties, AzCopy needs to send one additional request per object or file. (default true)")
	cpCmd.PersistentFlags().BoolVar(&raw.s2sPreserveAccessTier, "s2s-preserve-access-tier", true, "Preserve access tier during service to service copy. "+
		"\n Please refer to [Azure Blob storage: hot, cool, cold, and archive access tiers](https://docs.microsoft.com/azure/storage/blobs/storage-blob-storage-tiers) to ensure destination storage account supports setting access tier. "+
		"\n In the cases that setting access tier is not supported, please use s2sPreserveAccessTier=false to bypass copying access tier. (default true). ")
	cpCmd.PersistentFlags().BoolVar(&raw.s2sSourceChangeValidation, "s2s-detect-source-changed", false, "False by default. Detect if the source file/blob changes while it is being read. "+
		"\n This parameter only applies to service to service copies, because the corresponding check is permanently enabled for uploads and downloads.")
	cpCmd.PersistentFlags().StringVar(&raw.s2sInvalidMetadataHandleOption, "s2s-handle-invalid-metadata", common.DefaultInvalidMetadataHandleOption.String(), "Specifies how invalid metadata keys are handled. "+
		"\n Available options: ExcludeIfInvalid, FailIfInvalid, RenameIfInvalid (default 'ExcludeIfInvalid').")
	cpCmd.PersistentFlags().StringVar(&raw.listOfVersionIDs, "list-of-versions", "", "Specifies a path to a text file where each version id is listed on a separate line. "+
		"\n Ensure that the source must point to a single blob and all the version ids specified in the file using this flag must belong to the source blob only. "+
		"\n AzCopy will download the specified versions in the destination folder provided.")
	cpCmd.PersistentFlags().StringVar(&raw.blobTags, "blob-tags", "", "Set tags on blobs to categorize data in your storage account. "+
		"\n Multiple blob tags should be separated by '&', i.e. 'foo=bar&some=thing'.")
	cpCmd.PersistentFlags().BoolVar(&raw.s2sPreserveBlobTags, "s2s-preserve-blob-tags", false, "False by default. Preserve blob tags during service to service transfer from one blob storage to another.")
	cpCmd.PersistentFlags().BoolVar(&raw.includeDirectoryStubs, "include-directory-stub", false, "False by default to ignore directory stubs. Directory stubs are blobs with metadata 'hdi_isfolder:true'. "+
		"\n Setting value to true will preserve directory stubs during transfers. Including this flag with no value defaults to true (e.g, azcopy copy --include-directory-stub is the same as azcopy copy --include-directory-stub=true).")
	cpCmd.PersistentFlags().BoolVar(&raw.disableAutoDecoding, "disable-auto-decoding", false, "False by default to enable automatic decoding of illegal chars on Windows. "+
		"\n Can be set to true to disable automatic decoding.")
	cpCmd.PersistentFlags().BoolVar(&raw.dryrun, "dry-run", false, "False by default. Prints the file paths that would be copied by this command. This flag does not copy the actual files. The --overwrite flag has no effect. If you set the --overwrite flag to false, files in the source directory are listed even if those files exist in the destination directory.")
	// s2sGetPropertiesInBackend is an optional flag for controlling whether S3 object's or Azure file's full properties are get during enumerating in frontend or
	// right before transferring in ste(backend).
	// The traditional behavior of all existing enumerator is to get full properties during enumerating(more specifically listing),
	// while this could cause big performance issue for S3 and Azure file, where listing doesn't return full properties,
	// and enumerating logic do fetching properties sequentially!
	// To achieve better performance and at same time have good control for overall go routine numbers, getting property in ste is introduced,
	// so properties can be get in parallel, at same time no additional go routines are created for this specific job.
	// The usage of this hidden flag is to provide fallback to traditional behavior, when service supports returning full properties during list.
	cpCmd.PersistentFlags().BoolVar(&raw.s2sGetPropertiesInBackend, "s2s-get-properties-in-backend", true, "True by default. Gets S3 objects' or Azure files' properties in backend, if properties need to be accessed. "+
		"\n Properties need to be accessed if s2s-preserve-properties is true, and in certain other cases where we need the properties for modification time checks or MD5 checks.")
	cpCmd.PersistentFlags().StringVar(&raw.trailingDot, "trailing-dot", "", "Available options: "+strings.Join(common.ValidTrailingDotOptions(), ", ")+". "+
		"\n 'Enable'(Default) treats trailing dot file operations in a safe manner between systems that support these files. "+
		"\n On Windows, the transfers will not occur to stop risk of data corruption. "+
		"\n See 'AllowToUnsafeDestination' to bypass this."+
		"\n 'Disable' reverts to the legacy functionality, where trailing dot files are ignored. "+
		"\n This can result in potential data corruption if the transfer contains two paths that differ only by a trailing dot (E.g 'path/foo' and 'path/foo.'). "+
		"\n If this flag is set to 'Disable' and AzCopy encounters a trailing dot file, it will warn customers in the scanning log but will not attempt to abort the operation."+
		"\n If the destination does not support trailing dot files (Windows or Blob Storage), AzCopy will fail if the trailing dot file is the root of the transfer and skip any trailing dot paths encountered during enumeration."+
		"\n 'AllowToUnsafeDestination' supports transferring trailing dot files to systems that do not support them e.g Windows. "+
		"\n Use with caution acknowledging risk of data corruption, when two files with different contents 'path/bar' and 'path/bar.' (differ only by a trailing dot) are seen as identical.")

	// Public Documentation: https://docs.microsoft.com/en-us/azure/storage/blobs/encryption-customer-provided-keys
	// Clients making requests against Azure Blob storage have the option to provide an encryption key on a per-request basis.
	// Including the encryption key on the request provides granular control over encryption settings for Blob storage operations.
	// Customer-provided keys can be stored in Azure Key Vault or in another key store linked to storage account.
	cpCmd.PersistentFlags().StringVar(&raw.cpkScopeInfo, "cpk-by-name", "", "\n Client provided key by name lets clients making requests against "+
		"\n Azure Blob storage an option to provide an encryption key on a per-request basis. "+
		"\n Provided key name will be fetched from Azure Key Vault and will be used to encrypt the data.")
	cpCmd.PersistentFlags().BoolVar(&raw.cpkInfo, "cpk-by-value", false, "False by default. Client provided key by name lets clients making requests against "+
		"\n Azure Blob storage an option to provide an encryption key on a per-request basis. "+
		"\n Provided key and its hash will be fetched from environment variables (CPK_ENCRYPTION_KEY and CPK_ENCRYPTION_KEY_SHA256 must be set).")

	// permanently hidden
	// Hide the list-of-files flag since it is implemented only for Storage Explorer.
	_ = cpCmd.PersistentFlags().MarkHidden("list-of-files")
	_ = cpCmd.PersistentFlags().MarkHidden("s2s-get-properties-in-backend")

	// temp, to assist users with change in param names, by providing a clearer message when these obsolete ones are accidentally used
	cpCmd.PersistentFlags().StringVar(&raw.legacyInclude, "include", "", "Legacy include param. DO NOT USE")
	cpCmd.PersistentFlags().StringVar(&raw.legacyExclude, "exclude", "", "Legacy exclude param. DO NOT USE")
	_ = cpCmd.PersistentFlags().MarkHidden("include")
	_ = cpCmd.PersistentFlags().MarkHidden("exclude")

	// Hide the flush-threshold flag since it is implemented only for CI.
	cpCmd.PersistentFlags().Uint32Var(&ste.ADLSFlushThreshold, "flush-threshold", 7500, "Adjust the number of blocks to flush at once on accounts that have a hierarchical namespace.")
	_ = cpCmd.PersistentFlags().MarkHidden("flush-threshold")

	// Deprecate the old persist-smb-permissions flag
	_ = cpCmd.PersistentFlags().MarkHidden("preserve-smb-permissions")
	cpCmd.PersistentFlags().BoolVar(&raw.preservePermissions, PreservePermissionsFlag, false, "False by default."+
		" Preserves ACLs between aware resources (Windows and Azure Files SMB, or Data Lake Storage to Data Lake Storage) and "+
		"\n permissions between aware resources(Linux to Azure Files NFS). \n"+
		"For accounts that have a hierarchical namespace, your security principal must be the owning user of the target container or "+
		"\n it must be assigned the Storage Blob Data Owner role, scoped to the target container, storage account, parent resource group, or subscription."+
		"\n  For downloads, you will also need the --backup flag to restore permissions where the new Owner will not be the user running AzCopy."+
		"\n  This flag applies to both files and folders, unless a file-only filter is specified (e.g. include-pattern).")

	// Deletes destination blobs with uncommitted blocks when staging block, hidden because we want to preserve default behavior
	cpCmd.PersistentFlags().BoolVar(&raw.deleteDestinationFileIfNecessary, "delete-destination-file", false, "False by default. "+
		"\n Deletes destination blobs, specifically blobs with uncommitted blocks when staging block.")
	_ = cpCmd.PersistentFlags().MarkHidden("delete-destination-file")
	cpCmd.PersistentFlags().StringVar(&raw.hardlinks, HardlinksFlag, "follow",
		"Specifies how hardlinks should be handled. "+
			"\n This flag is only applicable when downloading from an NFS file share, uploading to an NFS share, or performing service-to-service copies involving NFS. \n"+
			"\n The only supported option is 'follow' (default), which copies hardlinks as regular, independent files at the destination.")
}
