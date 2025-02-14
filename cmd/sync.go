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
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"

	"github.com/spf13/cobra"
)

var LocalToFileShareWarnMsg = "AzCopy sync is supported but not fully recommended for Azure Files. AzCopy sync doesn't support differential copies at scale, and some file fidelity might be lost."

type RawSyncCmdArgs struct {
	Src       string
	Dst       string
	Recursive bool
	FromTo    string

	// options from flags
	blockSizeMB           float64
	putBlobSizeMB         float64
	include               string
	exclude               string
	excludePath           string
	includeFileAttributes string
	excludeFileAttributes string
	legacyInclude         string // for warning messages only
	legacyExclude         string // for warning messages only
	includeRegex          string
	ExcludeRegex          string
	CompareHash           string
	LocalHashStorageMode  string

	includeDirectoryStubs   bool // Includes hdi_isfolder objects in the sync even w/o preservePermissions.
	PreservePermissions     bool
	preserveSMBPermissions  bool // deprecated and synonymous with preservePermissions
	preserveOwner           bool
	PreserveSMBInfo         bool
	PreservePOSIXProperties bool
	FollowSymlinks          bool
	preserveSymlinks        bool
	backupMode              bool
	putMd5                  bool
	Md5ValidationOption     string
	includeRoot             bool
	// this flag indicates the user agreement with respect to deleting the extra files at the destination
	// which do not exists at source. With this flag turned on/off, users will not be asked for permission.
	// otherwise the user is prompted to make a decision
	DeleteDestination string

	// this flag is to disable comparator and overwrite files at destination irrespective
	mirrorMode bool

	s2sPreserveAccessTier bool
	// Opt-in flag to preserve the blob index tags during service to service transfer.
	s2sPreserveBlobTags bool

	ForceIfReadOnly bool

	// Optional flag to encrypt user data with user provided key.
	// Key is provide in the REST request itself
	// Provided key (EncryptionKey and EncryptionKeySHA256) and its hash will be fetched from environment variables
	// Set EncryptionAlgorithm = "AES256" by default.
	cpkInfo bool
	// Key is present in AzureKeyVault and Azure KeyVault is linked with storage account.
	// Provided key name will be fetched from Azure Key Vault and will be used to encrypt the data
	cpkScopeInfo string
	// dry run mode bool
	dryrun      bool
	trailingDot string

	// when specified, AzCopy deletes the destination blob that has uncommitted blocks, not just the uncommitted blocks
	deleteDestinationFileIfNecessary bool

	// Limit on size of ObjectIndexerMap in memory.
	// For more information, please refer to cookedSyncCmdArgs.
	MaxObjectIndexerMapSizeInGB string

	// Change file detection mode.
	// For more information, please refer to cookedSyncCmdArgs.
	CfdMode string

	// For more information, please refer to cookedSyncCmdArgs.
	MetaDataOnlySync bool

	// This is the time of last sync in ISO8601 format. For more information, please refer to cookedSyncCmdArgs.
	LastSyncTime string
}

func (raw *RawSyncCmdArgs) parsePatterns(pattern string) (cookedPatterns []string) {
	cookedPatterns = make([]string, 0)
	rawPatterns := strings.Split(pattern, ";")
	for _, pattern := range rawPatterns {

		// skip the empty patterns
		if len(pattern) != 0 {
			cookedPatterns = append(cookedPatterns, pattern)
		}
	}

	return
}

// it is assume that the given url has the SAS stripped, and safe to print
func (raw *RawSyncCmdArgs) validateURLIsNotServiceLevel(url string, location common.Location) error {
	srcLevel, err := DetermineLocationLevel(url, location, true)
	if err != nil {
		return err
	}

	if srcLevel == ELocationLevel.Service() {
		return fmt.Errorf("service level URLs (%s) are not supported in sync: ", url)
	}

	return nil
}

// validates and transform raw input into cooked input
func (raw *RawSyncCmdArgs) Cook() (cookedSyncCmdArgs, error) {
	cooked := cookedSyncCmdArgs{}

	// set up the front end scanning logger
	azcopyScanningLogger = common.NewJobLogger(azcopyCurrentJobID, AzcopyLogVerbosity, azcopyLogPathFolder, "-scanning")
	azcopyScanningLogger.OpenLog()
	glcm.RegisterCloseFunc(func() {
		azcopyScanningLogger.CloseLog()
	})

	// if no logging, set this empty so that we don't display the log location
	if AzcopyLogVerbosity == common.LogNone {
		azcopyLogPathFolder = ""
	}

	// this if statement ladder remains instead of being separated to help determine valid combinations for sync
	// consider making a map of valid source/dest combos and consolidating this to generic source/dest setups, akin to the lower if statement
	// TODO: if expand the set of source/dest combos supported by sync, update this method the declarative test framework:

	var err error
	err = cooked.trailingDot.Parse(raw.trailingDot)
	if err != nil {
		return cooked, err
	}

	cooked.fromTo, err = ValidateFromTo(raw.Src, raw.Dst, raw.FromTo)
	if err != nil {
		return cooked, err
	}

	// display a warning message to console and job log file if there is a sync operation being performed from local to file share.
	// Reference : https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-files#synchronize-files
	if cooked.fromTo == common.EFromTo.LocalFile() {

		glcm.Warn(LocalToFileShareWarnMsg)
		if jobsAdmin.JobsAdmin != nil {
			jobsAdmin.JobsAdmin.LogToJobLog(LocalToFileShareWarnMsg, common.LogWarning)
		}
		if raw.dryrun {
			glcm.Dryrun(func(of common.OutputFormat) string {
				if of == common.EOutputFormat.Json() {
					var out struct {
						Warn string `json:"warn"`
					}

					out.Warn = LocalToFileShareWarnMsg
					buf, _ := json.Marshal(out)
					return string(buf)
				}

				return fmt.Sprintf("DRYRUN: warn %s", LocalToFileShareWarnMsg)
			})
		}
	}

	switch cooked.fromTo {
	case common.EFromTo.Unknown():
		return cooked, fmt.Errorf("Unable to infer the source '%s' / destination '%s'. ", raw.Src, raw.Dst)
	case common.EFromTo.LocalBlob(), common.EFromTo.LocalFile(), common.EFromTo.LocalBlobFS():
		cooked.Destination, err = SplitResourceString(raw.Dst, cooked.fromTo.To())
		common.PanicIfErr(err)
	case common.EFromTo.BlobLocal(), common.EFromTo.FileLocal(), common.EFromTo.BlobFSLocal():
		cooked.Source, err = SplitResourceString(raw.Src, cooked.fromTo.From())
		common.PanicIfErr(err)
	case common.EFromTo.BlobBlob(), common.EFromTo.FileFile(), common.EFromTo.BlobFile(), common.EFromTo.FileBlob(), common.EFromTo.BlobFSBlobFS(), common.EFromTo.BlobFSBlob(), common.EFromTo.BlobFSFile(), common.EFromTo.BlobBlobFS(), common.EFromTo.FileBlobFS():
		cooked.Destination, err = SplitResourceString(raw.Dst, cooked.fromTo.To())
		common.PanicIfErr(err)
		cooked.Source, err = SplitResourceString(raw.Src, cooked.fromTo.From())
		common.PanicIfErr(err)
	default:
		return cooked, fmt.Errorf("source '%s' / destination '%s' combination '%s' not supported for sync command ", raw.Src, raw.Dst, cooked.fromTo)
	}

	// Do this check separately so we don't end up with a bunch of code duplication when new src/dstn are added
	if cooked.fromTo.From() == common.ELocation.Local() {
		cooked.Source = common.ResourceString{Value: common.ToExtendedPath(cleanLocalPath(raw.Src))}
	} else if cooked.fromTo.To() == common.ELocation.Local() {
		cooked.Destination = common.ResourceString{Value: common.ToExtendedPath(cleanLocalPath(raw.Dst))}
	}

	// we do not support service level sync yet
	if cooked.fromTo.From().IsRemote() {
		err = raw.validateURLIsNotServiceLevel(cooked.Source.Value, cooked.fromTo.From())
		if err != nil {
			return cooked, err
		}
	}

	// we do not support service level sync yet
	if cooked.fromTo.To().IsRemote() {
		err = raw.validateURLIsNotServiceLevel(cooked.Destination.Value, cooked.fromTo.To())
		if err != nil {
			return cooked, err
		}
	}

	// use the globally generated JobID
	cooked.JobID = azcopyCurrentJobID

	cooked.blockSize, err = blockSizeInBytes(raw.blockSizeMB)
	if err != nil {
		return cooked, err
	}
	cooked.putBlobSize, err = blockSizeInBytes(raw.putBlobSizeMB)
	if err != nil {
		return cooked, err
	}

	if err = cooked.symlinkHandling.Determine(raw.FollowSymlinks, raw.preserveSymlinks); err != nil {
		return cooked, err
	}
	cooked.recursive = raw.Recursive
	cooked.forceIfReadOnly = raw.ForceIfReadOnly
	if err = validateForceIfReadOnly(cooked.forceIfReadOnly, cooked.fromTo); err != nil {
		return cooked, err
	}

	cooked.backupMode = raw.backupMode
	if err = validateBackupMode(cooked.backupMode, cooked.fromTo); err != nil {
		return cooked, err
	}

	// determine whether we should prompt the user to delete extra files
	err = cooked.deleteDestination.Parse(raw.DeleteDestination)
	if err != nil {
		return cooked, err
	}

	// warn on legacy filters
	if raw.legacyInclude != "" || raw.legacyExclude != "" {
		return cooked, fmt.Errorf("the include and exclude parameters have been replaced by include-pattern and exclude-pattern. They work on filenames only (not paths)")
	}

	// parse the filter patterns
	cooked.includePatterns = raw.parsePatterns(raw.include)
	cooked.excludePatterns = raw.parsePatterns(raw.exclude)
	cooked.excludePaths = raw.parsePatterns(raw.excludePath)

	// parse the attribute filter patterns
	cooked.includeFileAttributes = raw.parsePatterns(raw.includeFileAttributes)
	cooked.excludeFileAttributes = raw.parsePatterns(raw.excludeFileAttributes)

	cooked.preserveSMBInfo = raw.PreserveSMBInfo && areBothLocationsSMBAware(cooked.fromTo)

	if err = validatePreserveSMBPropertyOption(cooked.preserveSMBInfo, cooked.fromTo, nil, "preserve-smb-info"); err != nil {
		return cooked, err
	}

	isUserPersistingPermissions := raw.preserveSMBPermissions || raw.PreservePermissions
	if cooked.preserveSMBInfo && !isUserPersistingPermissions {
		glcm.Info("Please note: the preserve-permissions flag is set to false, thus AzCopy will not copy SMB ACLs between the source and destination. To learn more: https://aka.ms/AzCopyandAzureFiles.")
	}

	if err = validatePreserveSMBPropertyOption(isUserPersistingPermissions, cooked.fromTo, nil, PreservePermissionsFlag); err != nil {
		return cooked, err
	}
	// TODO: the check on raw.preservePermissions on the next line can be removed once we have full support for these properties in sync
	// if err = validatePreserveOwner(raw.preserveOwner, cooked.fromTo); raw.preservePermissions && err != nil {
	//	return cooked, err
	// }
	cooked.preservePermissions = common.NewPreservePermissionsOption(isUserPersistingPermissions, raw.preserveOwner, cooked.fromTo)

	cooked.preservePOSIXProperties = raw.PreservePOSIXProperties
	if cooked.preservePOSIXProperties && !areBothLocationsPOSIXAware(cooked.fromTo) {
		return cooked, fmt.Errorf("in order to use --preserve-posix-properties, both the source and destination must be POSIX-aware (valid pairings are Linux->Blob, Blob->Linux, Blob->Blob)")
	}

	if err = cooked.compareHash.Parse(raw.CompareHash); err != nil {
		return cooked, err
	} else {
		switch cooked.compareHash {
		case common.ESyncHashType.MD5():
			// Save any new MD5s on files we download.
			raw.putMd5 = true
		default: // no need to put a hash of any kind.
		}
	}

	if err = common.LocalHashStorageMode.Parse(raw.LocalHashStorageMode); err != nil {
		return cooked, err
	}

	cooked.putMd5 = raw.putMd5
	if err = validatePutMd5(cooked.putMd5, cooked.fromTo); err != nil {
		return cooked, err
	}

	err = cooked.md5ValidationOption.Parse(raw.Md5ValidationOption)
	if err != nil {
		return cooked, err
	}
	if err = validateMd5Option(cooked.md5ValidationOption, cooked.fromTo); err != nil {
		return cooked, err
	}

	if cooked.fromTo.IsS2S() {
		cooked.preserveAccessTier = raw.s2sPreserveAccessTier
	}

	// Check if user has provided `s2s-preserve-blob-tags` flag.
	// If yes, we have to ensure that both source and destination must be blob storages.
	if raw.s2sPreserveBlobTags {
		if cooked.fromTo.From() != common.ELocation.Blob() || cooked.fromTo.To() != common.ELocation.Blob() {
			return cooked, fmt.Errorf("either source or destination is not a blob storage. " +
				"blob index tags is a property of blobs only therefore both source and destination must be blob storage")
		} else {
			cooked.s2sPreserveBlobTags = raw.s2sPreserveBlobTags
		}
	}

	// Setting CPK-N
	cpkOptions := common.CpkOptions{}
	// Setting CPK-N
	if raw.cpkScopeInfo != "" {
		if raw.cpkInfo {
			return cooked, fmt.Errorf("cannot use both cpk-by-name and cpk-by-value at the same time")
		}
		cpkOptions.CpkScopeInfo = raw.cpkScopeInfo
	}

	// Setting CPK-V
	// Get the key (EncryptionKey and EncryptionKeySHA256) value from environment variables when required.
	cpkOptions.CpkInfo = raw.cpkInfo

	// We only support transfer from source encrypted by user key when user wishes to download.
	// Due to service limitation, S2S transfer is not supported for source encrypted by user key.
	if cooked.fromTo.IsDownload() && (cpkOptions.CpkScopeInfo != "" || cpkOptions.CpkInfo) {
		glcm.Info("Client Provided Key for encryption/decryption is provided for download scenario. " +
			"Assuming source is encrypted.")
		cpkOptions.IsSourceEncrypted = true
	}

	cooked.cpkOptions = cpkOptions

	cooked.mirrorMode = raw.mirrorMode

	cooked.includeRegex = raw.parsePatterns(raw.includeRegex)
	cooked.excludeRegex = raw.parsePatterns(raw.ExcludeRegex)

	cooked.dryrunMode = raw.dryrun

	if azcopyOutputVerbosity == common.EOutputVerbosity.Quiet() || azcopyOutputVerbosity == common.EOutputVerbosity.Essential() {
		if cooked.deleteDestination == common.EDeleteDestination.Prompt() {
			err = fmt.Errorf("cannot set output level '%s' with delete-destination option '%s'", azcopyOutputVerbosity.String(), cooked.deleteDestination.String())
		} else if cooked.dryrunMode {
			err = fmt.Errorf("cannot set output level '%s' with dry-run mode", azcopyOutputVerbosity.String())
		}
	}
	if err != nil {
		return cooked, err
	}

	cooked.deleteDestinationFileIfNecessary = raw.deleteDestinationFileIfNecessary

	cooked.includeDirectoryStubs = raw.includeDirectoryStubs
	cooked.includeRoot = raw.includeRoot

	// This check is in-line with copy mode.
	// Note: * can be filename in that case, which means user want to copy file with name "*". For files we don't copy
	//      top directory. So it will not break any functionality.
	if cooked.fromTo.From() == common.ELocation.Local() && strings.HasSuffix(cooked.Source.ValueLocal(), "/*") {
		cooked.StripTopDir = true
		cooked.Source.Value = strings.TrimSuffix(cooked.Source.Value, "/*")
	} else {
		cooked.StripTopDir = false
	}

	return cooked, nil
}

type cookedSyncCmdArgs struct {
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
	// defines the number of folders listed at the source and compared.
	atomicSourceFoldersScanned             uint64
	atomicSourceFilesTransferNotRequired   uint64
	atomicSourceFoldersTransferNotRequired uint64

	// deletion count keeps track of how many extra files from the destination were removed
	atomicDeletionCount uint64

	Source                  common.ResourceString
	Destination             common.ResourceString
	fromTo                  common.FromTo
	credentialInfo          common.CredentialInfo
	s2sSourceCredentialType common.CredentialType

	// filters
	recursive             bool
	symlinkHandling       common.SymlinkHandlingType
	includePatterns       []string
	excludePatterns       []string
	excludePaths          []string
	includeFileAttributes []string
	excludeFileAttributes []string
	includeRegex          []string
	excludeRegex          []string

	// options
	compareHash             common.SyncHashType
	preservePermissions     common.PreservePermissionsOption
	preserveSMBInfo         bool
	preservePOSIXProperties bool
	putMd5                  bool
	md5ValidationOption     common.HashValidationOption
	blockSize               int64
	putBlobSize             int64
	forceIfReadOnly         bool
	backupMode              bool
	includeDirectoryStubs   bool
	includeRoot             bool

	// commandString hold the user given command which is logged to the Job log file
	commandString string

	// generated
	JobID common.JobID

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
	// this is set to true once the final part has been dispatched
	isEnumerationComplete bool

	// this flag indicates the user agreement with respect to deleting the extra files at the destination
	// which do not exists at source. With this flag turned on/off, users will not be asked for permission.
	// otherwise the user is prompted to make a decision
	deleteDestination common.DeleteDestination

	preserveAccessTier bool
	// To specify whether user wants to preserve the blob index tags during service to service transfer.
	s2sPreserveBlobTags bool

	cpkOptions common.CpkOptions

	mirrorMode bool

	dryrunMode  bool
	trailingDot common.TrailingDotOption

	deleteDestinationFileIfNecessary bool

	////////////////////////  Sliding window variables  //////////////////////////

	// Whether we want top dir to be included in target or not. If it's true, we skip copying top directory at source, only
	// underneath files will be coped.
	// If source has following directory structure
	// dir1/file1.txt
	// dir1/file2.txt
	// In case of StripTopDir is true, after transfer target looks like this:-
	// accountName.blob.core.windows.net/container/file1.txt
	// accountName.blob.core.windows.net/container/file2.txt
	//
	// In case of StripTopDir is false, after transfer target looks like this:-
	// accountName.blob.core.windows.net/container/dir1/file1.txt
	// accountName.blob.core.windows.net/container/dir1/file2.txt
	StripTopDir bool

	// Limit on size of ObjectIndexerMap in memory.
	// This is used only by the sync process and it controls how much the source traverser can fill the ObjectIndexerMap before it has to wait
	// for target traverser to process and empty it. This should be kept to a value less than the system RAM to avoid thrashing.
	// If you are not interested in source traverser scanning all the files for estimation purpose you can keep it low, just enough to never have the
	// target traverser wait for directories to scan. The only reason we would want to keep it high is to let the source complete scanning irrespective
	// of the target traverser speed, so that the scanned information can then be used for ETA estimation.
	//
	maxObjectIndexerSizeInGB uint32

	//
	// This is the time of last sync in ISO8601 format. This is compared with the source files' ctime/mtime value to find out if they have changed
	// since the last sync and hence need to be copied. It's not used if the CFDMode is anything other than CTimeMTime and CTime, since in other
	// CFDModes it's not considered safe to trust file times and we need to compare every file with target to find out which files have changed.
	// There are a few subtelties that caller should be aware of:
	//
	// Since sync takes finite time, this should be set to the start of sync and not any later, else files that changed in the source while the
	// last sync was running may be (incorrectly) skipped in this sync. Infact, to correctly account for any time skew between the machine running AzCopy
	// and the machine hosting the source filesystem (if they are different, f.e., when the source is an NFS/SMB mount) this should be set to few seconds
	// in the past. 60 seconds is a good value for skew adjustment. A larger skew value could cause more data to be sync'ed while a smaller skew value may
	// cause us to miss some changed files, latter is obviously not desirable. If we are not sure what skew value to use, it's best to create a temp file
	// on the source and compare its ctime with the nodes time and use that as a baseline.
	//
	// Time of last sync, used by the sync process.
	// A file/directory having ctime/mtime greater than lastSyncTime is considered "changed", though the exact interpretation depends on the CFDMode
	// used and other variables. Depending on CFDMode this will be compared with source files' ctime/mtime and/or target files' ctime/mtime.
	//
	lastSyncTime time.Time

	//
	// Change file detection mode.
	// This controls how target traverser decides whether a file has changed (and hence needs to be sync'ed to the target) by looking at the file
	// properties stored in the sourceFolderIndex. Valid Enums will be TargetCompare, Ctime, CtimeMtime.
	//
	// TargetCompare - This is the most generic and the slowest of all. It enumerates all target directories
	//                 and compares the children at source and target to find out if an object has changed.
	//
	// CtimeMtime    - Compare source file’s mtime/ctime (Unix/NFS) or LastWriteTime/ChangeTime (Windows/SMB) with LastSyncTime for detecting changed files.
	//                 If mtime/LastWriteTime > LastSyncTime then it means both data and metadata have changed else if ctime/ChangeTime > LastSyncTime then
	//                 only metadata has changed. For detecting changed directories this uses ctime/ChangeTime (not mtime/LastWriteTime) of the directory,
	//                 changed directories will be enumerated in the target and every object will be compared with the source to find changes. This is needed
	//                 to cover the case of directory renames where a completely different directory in the source can have the same name as a target directory
	//                 and checking only mtime/LastWriteTime of children is not safe since rename of a directory won’t affect mtime of its children. If a directory
	//                 has not changed then it’ll compare mtime/LastWriteTime and ctime/ChangeTime of its children with LastSyncTime for finding data/metadata changes,
	//                 thus it enumerates only target directories that have changed.
	//                 This is the most efficient of all and should be used when we safely can use it, i.e., we know that source updates ctime/mtime correctly.
	//
	// Ctime         - If we don’t want to depend on mtime/LastWriteTime (since it can be changed by applications) but we still don’t want to lose the advantage of
	//                 ctime/ChangeTime which is much more dependable, we can use this mode. In this mode we use only ctime/ChangeTime to detect if there’s a change
	//                 to a file/directory and if there’s a change, to find the exact change (data/metadata/both) we compare the file with the target. This has the
	//                 advantage that we use the more robust ctime/ChangeTime and only for files/directories which could have potentially changed we have
	//                 to compare with the target. This results in a much smaller set of files to enumerate at the target.
	//
	// So, in the order of preference we have,
	//
	// CtimeMtime -> Ctime -> TargetCompare.
	//
	cfdMode common.CFDMode

	//
	// Sync only file metadata if only metadata has changed and not the file content, else for changed files both file data and metadata are sync’ed.
	// The latter could be expensive especially for cases where user updates some file metadata (owner/mode/atime/mtime/etc) recursively. How we find out
	// whether only metadata has changed or only data has changed, or both have changed depends on the CFDMode that controls how changed files are detected.
	//
	metaDataOnlySync bool

	////////////////////////  Sliding window variables  //////////////////////////
}

func (cca *cookedSyncCmdArgs) incrementDeletionCount() {
	atomic.AddUint64(&cca.atomicDeletionCount, 1)
}

func (cca *cookedSyncCmdArgs) GetDeletionCount() uint64 {
	return atomic.LoadUint64(&cca.atomicDeletionCount)
}

// setFirstPartOrdered sets the value of atomicFirstPartOrdered to 1
func (cca *cookedSyncCmdArgs) setFirstPartOrdered() {
	atomic.StoreUint32(&cca.atomicFirstPartOrdered, 1)
}

// FirstPartOrdered returns the value of atomicFirstPartOrdered.
func (cca *cookedSyncCmdArgs) FirstPartOrdered() bool {
	return atomic.LoadUint32(&cca.atomicFirstPartOrdered) > 0
}

// setScanningComplete sets the value of atomicScanningStatus to 1.
func (cca *cookedSyncCmdArgs) setScanningComplete() {
	atomic.StoreUint32(&cca.atomicScanningStatus, 1)
}

// ScanningComplete returns the value of atomicScanningStatus.
func (cca *cookedSyncCmdArgs) ScanningComplete() bool {
	return atomic.LoadUint32(&cca.atomicScanningStatus) > 0
}

// GetSourceFilesScanned returns files scanned at source.
func (cca *cookedSyncCmdArgs) GetSourceFoldersScanned() uint64 {
	return atomic.LoadUint64(&cca.atomicSourceFoldersScanned)
}

// GetSourceFilesTransferredNotRequired returns number of files not changed, hence require no transfer.
func (cca *cookedSyncCmdArgs) GetSourceFilesTransferredNotRequired() uint64 {
	return atomic.LoadUint64(&cca.atomicSourceFilesTransferNotRequired)
}

// GetSourceFoldersTransferredNotRequired returns number of folders not changed, hence require no transfer.
func (cca *cookedSyncCmdArgs) GetSourceFoldersTransferredNotRequired() uint64 {
	return atomic.LoadUint64(&cca.atomicSourceFoldersTransferNotRequired)
}

// GetSourceFilesScanned returns files scanned at source.
func (cca *cookedSyncCmdArgs) GetSourceFilesScanned() uint64 {
	return atomic.LoadUint64(&cca.atomicSourceFilesScanned)
}

// GetDestinationFilesScanned returns files scanned at destination.
func (cca *cookedSyncCmdArgs) GetDestinationFilesScanned() uint64 {
	return atomic.LoadUint64(&cca.atomicDestinationFilesScanned)
}

// wraps call to lifecycle manager to wait for the job to complete
// if blocking is specified to true, then this method will never return
// if blocking is specified to false, then another goroutine spawns and wait out the job
func (cca *cookedSyncCmdArgs) waitUntilJobCompletion(blocking bool) {
	// print initial message to indicate that the job is starting
	// Output the log location if log-level is set to other then NONE
	var logPathFolder string
	if azcopyLogPathFolder != "" {
		logPathFolder = fmt.Sprintf("%s%s%s.log", azcopyLogPathFolder, common.OS_PATH_SEPARATOR, cca.JobID)
	}
	glcm.Init(common.GetStandardInitOutputBuilder(cca.JobID.String(), logPathFolder, false, ""))

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

func (cca *cookedSyncCmdArgs) Cancel(lcm common.LifecycleMgr) {
	// prompt for confirmation, except when enumeration is complete
	if !cca.isEnumerationComplete {
		answer := lcm.Prompt("The enumeration (source/destination comparison) is not complete, "+
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

	err := cookedCancelCmdArgs{jobID: cca.JobID}.process()
	if err != nil {
		lcm.Error("error occurred while cancelling the job " + cca.JobID.String() + ". Failed with error " + err.Error())
	}
}

type scanningProgressJsonTemplate struct {
	FilesScannedAtSource      uint64
	FilesScannedAtDestination uint64
}

func (cca *cookedSyncCmdArgs) reportScanningProgress(lcm common.LifecycleMgr, throughput float64) {

	lcm.Progress(func(format common.OutputFormat) string {
		srcScanned := atomic.LoadUint64(&cca.atomicSourceFilesScanned)
		dstScanned := atomic.LoadUint64(&cca.atomicDestinationFilesScanned)

		if format == common.EOutputFormat.Json() {
			jsonOutputTemplate := scanningProgressJsonTemplate{
				FilesScannedAtSource:      srcScanned,
				FilesScannedAtDestination: dstScanned,
			}
			outputString, err := json.Marshal(jsonOutputTemplate)
			common.PanicIfErr(err)
			return string(outputString)
		}

		// text output
		throughputString := ""
		if cca.FirstPartOrdered() {
			throughputString = fmt.Sprintf(", 2-sec Throughput (Mb/s): %v", jobsAdmin.ToFixed(throughput, 4))
		}
		return fmt.Sprintf("%v Files Scanned at Source, %v Files Scanned at Destination%s",
			srcScanned, dstScanned, throughputString)
	})
}

func (cca *cookedSyncCmdArgs) getJsonOfSyncJobSummary(summary common.ListJobSummaryResponse) string {
	wrapped := common.ListSyncJobSummaryResponse{ListJobSummaryResponse: summary}
	wrapped.DeleteTotalTransfers = cca.GetDeletionCount()
	wrapped.DeleteTransfersCompleted = cca.GetDeletionCount()
	jsonOutput, err := json.Marshal(wrapped)
	common.PanicIfErr(err)
	return string(jsonOutput)
}

func (cca *cookedSyncCmdArgs) ReportProgressOrExit(lcm common.LifecycleMgr) (totalKnownCount uint32) {
	duration := time.Since(cca.jobStartTime) // report the total run time of the job
	var summary common.ListJobSummaryResponse
	var throughput float64
	var jobDone bool

	// fetch a job status and compute throughput if the first part was dispatched
	if cca.FirstPartOrdered() {
		Rpc(common.ERpcCmd.ListJobSummary(), &cca.JobID, &summary)
		Rpc(common.ERpcCmd.GetJobLCMWrapper(), &cca.JobID, &lcm)
		jobDone = summary.JobStatus.IsJobDone()
		totalKnownCount = summary.TotalTransfers

		// compute the average throughput for the last time interval
		bytesInMb := float64(float64(summary.BytesOverWire-cca.intervalBytesTransferred) * 8 / float64(base10Mega))
		timeElapsed := time.Since(cca.intervalStartTime).Seconds()
		throughput = common.Iff(timeElapsed != 0, bytesInMb/timeElapsed, 0)

		// reset the interval timer and byte count
		cca.intervalStartTime = time.Now()
		cca.intervalBytesTransferred = summary.BytesOverWire
	}

	// first part not dispatched, and we are still scanning
	// so a special message is outputted to notice the user that we are not stalling
	if !cca.ScanningComplete() {
		cca.reportScanningProgress(lcm, throughput)
		return
	}

	lcm.Progress(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			return cca.getJsonOfSyncJobSummary(summary)
		}

		// indicate whether constrained by disk or not
		perfString, diskString := getPerfDisplayText(summary.PerfStrings, summary.PerfConstraint, duration, false)

		return fmt.Sprintf("%.1f %%, %v Done, %v Failed, %v Pending, %v Total%s, 2-sec Throughput (Mb/s): %v%s",
			summary.PercentComplete,
			summary.TransfersCompleted,
			summary.TransfersFailed,
			summary.TotalTransfers-summary.TransfersCompleted-summary.TransfersFailed,
			summary.TotalTransfers, perfString, jobsAdmin.ToFixed(throughput, 4), diskString)
	})

	if jobDone {
		exitCode := common.EExitCode.Success()
		if summary.TransfersFailed > 0 || summary.JobStatus == common.EJobStatus.Cancelled() || summary.JobStatus == common.EJobStatus.Cancelling() {
			exitCode = common.EExitCode.Error()
		}

		lcm.Exit(func(format common.OutputFormat) string {
			if format == common.EOutputFormat.Json() {
				return cca.getJsonOfSyncJobSummary(summary)
			}
			screenStats, logStats := formatExtraStats(cca.fromTo, summary.AverageIOPS, summary.AverageE2EMilliseconds, summary.NetworkErrorPercentage, summary.ServerBusyPercentage)

			output := fmt.Sprintf(
				`
Job %s Summary
Files Scanned at Source: %v
Files Scanned at Destination: %v
Elapsed Time (Minutes): %v
Number of Copy Transfers for Files: %v
Number of Copy Transfers for Folder Properties: %v 
Number of Copy Transfers for Files Properties: %v
Total Number of Copy Transfers: %v
Number of Copy Transfers Completed: %v
Number of Copy Transfers Failed: %v
Number of Deletions at Destination: %v
Total Number of Bytes Transferred: %v
Total Number of Bytes Enumerated: %v
Final Job Status: %v%s%s
`,
				summary.JobID.String(),
				atomic.LoadUint64(&cca.atomicSourceFilesScanned),
				atomic.LoadUint64(&cca.atomicDestinationFilesScanned),
				jobsAdmin.ToFixed(duration.Minutes(), 4),
				summary.FileTransfers,
				summary.FolderPropertyTransfers,
				summary.FilePropertyTransfers,
				summary.TotalTransfers,
				summary.TransfersCompleted,
				summary.TransfersFailed,
				cca.atomicDeletionCount,
				summary.TotalBytesTransferred,
				summary.TotalBytesEnumerated,
				summary.JobStatus,
				screenStats,
				formatPerfAdvice(summary.PerformanceAdvice))

			jobMan, exists := jobsAdmin.JobsAdmin.JobMgr(summary.JobID)
			if exists {
				jobMan.Log(common.LogInfo, logStats+"\n"+output)
			}

			return output
		}, exitCode)
	}

	return
}

func (cca *cookedSyncCmdArgs) CredentialInfo(ctx context.Context) error {

	err := common.SetBackupMode(cca.backupMode, cca.fromTo)
	if err != nil {
		return err
	}

	if err := common.VerifyIsURLResolvable(cca.Source.Value); cca.fromTo.From().IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve source: %w", err)
	}

	if err := common.VerifyIsURLResolvable(cca.Destination.Value); cca.fromTo.To().IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve destination: %w", err)
	}

	// Verifies credential type and initializes credential info.
	// Note that this is for the destination.
	cca.credentialInfo, _, err = GetCredentialInfoForLocation(ctx, cca.fromTo.To(), cca.Destination, false, cca.cpkOptions)
	if err != nil {
		return err
	}

	srcCredInfo, _, err := GetCredentialInfoForLocation(ctx, cca.fromTo.From(), cca.Source, true, cca.cpkOptions)
	if err != nil {
		return err
	}
	cca.s2sSourceCredentialType = srcCredInfo.CredentialType
	// Download is the only time our primary credential type will be based on source
	if cca.fromTo.IsDownload() {
		cca.credentialInfo = srcCredInfo
	} else if cca.fromTo.IsS2S() {
		cca.s2sSourceCredentialType = srcCredInfo.CredentialType // Assign the source credential type in S2S
	}

	// For OAuthToken credential, assign OAuthTokenInfo to CopyJobPartOrderRequest properly,
	// the info will be transferred to STE.
	if cca.credentialInfo.CredentialType.IsAzureOAuth() || srcCredInfo.CredentialType.IsAzureOAuth() {
		uotm := GetUserOAuthTokenManagerInstance()
		// Get token from env var or cache.
		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return err
		} else if _, err := tokenInfo.GetTokenCredential(); err != nil {
			return err
		}
	}

	// TODO: Remove this check when FileBlob w/ File OAuth works.
	if cca.fromTo.IsS2S() && cca.fromTo.From() == common.ELocation.File() && srcCredInfo.CredentialType.IsAzureOAuth() && cca.fromTo.To() != common.ELocation.File() {
		return fmt.Errorf("S2S sync from Azure File authenticated with Azure AD to Blob/BlobFS is not supported")
	}

	// Check if destination is system container
	if cca.fromTo.IsS2S() || cca.fromTo.IsUpload() {
		dstContainerName, err := GetContainerName(cca.Destination.Value, cca.fromTo.To())
		if err != nil {
			return fmt.Errorf("failed to get container name from destination (is it formatted correctly?)")
		}
		if common.IsSystemContainer(dstContainerName) {
			return fmt.Errorf("cannot copy to system container '%s'", dstContainerName)
		}
	}
	return nil
}

func (cca *cookedSyncCmdArgs) process() (err error) {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	err = cca.CredentialInfo(ctx)
	if err != nil {
		return err
	}

	enumerator, err := cca.InitEnumerator(ctx, nil)
	if err != nil {
		return err
	}

	// trigger the progress reporting
	if !cca.dryrunMode {
		cca.waitUntilJobCompletion(false)
	}

	// trigger the enumeration
	err = enumerator.Enumerate()
	if err != nil {
		return err
	}
	return nil
}

func init() {
	raw := RawSyncCmdArgs{}
	// syncCmd represents the sync command
	var syncCmd = &cobra.Command{
		Use:     "sync",
		Aliases: []string{"sc", "s"},
		Short:   syncCmdShortDescription,
		Long:    syncCmdLongDescription,
		Example: syncCmdExample,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("2 arguments source and destination are required for this command. Number of commands passed %d", len(args))
			}
			raw.Src = args[0]
			raw.Dst = args[1]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			glcm.EnableInputWatcher()
			if cancelFromStdin {
				glcm.EnableCancelFromStdIn()
			}

			cooked, err := raw.Cook()
			if err != nil {
				glcm.Error("error parsing the input given by the user. Failed with error " + err.Error() + getErrorCodeUrl(err))
			}

			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				glcm.Error("Cannot perform sync due to error: " + err.Error() + getErrorCodeUrl(err))
			}
			if cooked.dryrunMode {
				glcm.Exit(nil, common.EExitCode.Success())
			}

			glcm.SurrenderControl()
		},
	}

	rootCmd.AddCommand(syncCmd)
	syncCmd.PersistentFlags().BoolVar(&raw.Recursive, "recursive", true, "True by default, look into sub-directories recursively when syncing between directories. (default true).")
	syncCmd.PersistentFlags().StringVar(&raw.FromTo, "from-to", "", "Optionally specifies the source destination combination. For Example: LocalBlob, BlobLocal, LocalFile, FileLocal, BlobFile, FileBlob, etc.")
	syncCmd.PersistentFlags().BoolVar(&raw.includeDirectoryStubs, "include-directory-stub", false, "False by default, includes blobs with the hdi_isfolder metadata in the transfer.")

	// TODO: enable for copy with IfSourceNewer
	// smb info/permissions can be persisted in the scenario of File -> File
	syncCmd.PersistentFlags().BoolVar(&raw.preserveSMBPermissions, "preserve-smb-permissions", false, "False by default. Preserves SMB ACLs between aware resources (Azure Files). This flag applies to both files and folders, unless a file-only filter is specified (e.g. include-pattern).")
	syncCmd.PersistentFlags().BoolVar(&raw.PreserveSMBInfo, "preserve-smb-info", (runtime.GOOS == "windows"), "Preserves SMB property info (last write time, creation time, attribute bits)"+
		" between SMB-aware resources (Windows and Azure Files). On windows, this flag will be set to true by default. If the source or destination is a "+
		"volume mounted on Linux using SMB protocol, this flag will have to be explicitly set to true. Only the attribute bits supported by Azure Files "+
		"will be transferred; any others will be ignored. This flag applies to both files and folders, unless a file-only filter is specified "+
		"(e.g. include-pattern). The info transferred for folders is the same as that for files, except for Last Write Time which is never preserved for folders.")
	syncCmd.PersistentFlags().BoolVar(&raw.PreservePOSIXProperties, "preserve-posix-properties", false, "False by default. 'Preserves' property info gleaned from stat or statx into object metadata.")

	// TODO: enable when we support local <-> File
	syncCmd.PersistentFlags().BoolVar(&raw.ForceIfReadOnly, "force-if-read-only", false, "False by default. When overwriting an existing file on Windows or Azure Files, force the overwrite to work even if the existing file has its read-only attribute set.")
	// syncCmd.PersistentFlags().BoolVar(&raw.preserveOwner, common.PreserveOwnerFlagName, common.PreserveOwnerDefault, "Only has an effect in downloads, and only when --preserve-smb-permissions is used. If true (the default), the file Owner and Group are preserved in downloads. If set to false, --preserve-smb-permissions will still preserve ACLs but Owner and Group will be based on the user running AzCopy")
	// syncCmd.PersistentFlags().BoolVar(&raw.backupMode, common.BackupModeFlagName, false, "Activates Windows' SeBackupPrivilege for uploads, or SeRestorePrivilege for downloads, to allow AzCopy to see read all files, regardless of their file system permissions, and to restore all permissions. Requires that the account running AzCopy already has these permissions (e.g. has Administrator rights or is a member of the 'Backup Operators' group). All this flag does is activate privileges that the account already has")

	syncCmd.PersistentFlags().Float64Var(&raw.blockSizeMB, "block-size-mb", 0, "Use this block size (specified in MiB) when uploading to Azure Storage or downloading from Azure Storage. Default is automatically calculated based on file size. Decimal fractions are allowed (For example: 0.25).")
	syncCmd.PersistentFlags().Float64Var(&raw.putBlobSizeMB, "put-blob-size-mb", 0, "Use this size (specified in MiB) as a threshold to determine whether to upload a blob as a single PUT request when uploading to Azure Storage. The default value is automatically calculated based on file size. Decimal fractions are allowed (For example: 0.25).")
	syncCmd.PersistentFlags().StringVar(&raw.include, "include-pattern", "", "Include only files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")
	syncCmd.PersistentFlags().StringVar(&raw.exclude, "exclude-pattern", "", "Exclude files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")
	syncCmd.PersistentFlags().StringVar(&raw.excludePath, "exclude-path", "", "Exclude these paths when comparing the source against the destination. "+
		"This option does not support wildcard characters (*). Checks relative path prefix(For example: myFolder;myFolder/subDirName/file.pdf).")
	syncCmd.PersistentFlags().StringVar(&raw.includeFileAttributes, "include-attributes", "", "(Windows only) Include only files whose attributes match the attribute list. For example: A;S;R")
	syncCmd.PersistentFlags().StringVar(&raw.excludeFileAttributes, "exclude-attributes", "", "(Windows only) Exclude files whose attributes match the attribute list. For example: A;S;R")
	syncCmd.PersistentFlags().StringVar(&raw.includeRegex, "include-regex", "", "Include the relative path of the files that match with the regular expressions. Separate regular expressions with ';'.")
	syncCmd.PersistentFlags().StringVar(&raw.ExcludeRegex, "exclude-regex", "", "Exclude the relative path of the files that match with the regular expressions. Separate regular expressions with ';'.")
	syncCmd.PersistentFlags().StringVar(&raw.DeleteDestination, "delete-destination", "false", "Defines whether to delete extra files from the destination that are not present at the source. Could be set to true, false, or prompt. "+
		"If set to prompt, the user will be asked a question before scheduling files and blobs for deletion. (default 'false').")
	syncCmd.PersistentFlags().BoolVar(&raw.putMd5, "put-md5", false, "Create an MD5 hash of each file, and save the hash as the Content-MD5 property of the destination blob or file. (By default the hash is NOT created.) Only available when uploading.")
	syncCmd.PersistentFlags().StringVar(&raw.Md5ValidationOption, "check-md5", common.DefaultHashValidationOption.String(), "Specifies how strictly MD5 hashes should be validated when downloading. This option is only available when downloading. Available values include: NoCheck, LogOnly, FailIfDifferent, FailIfDifferentOrMissing. (default 'FailIfDifferent').")
	syncCmd.PersistentFlags().BoolVar(&raw.s2sPreserveAccessTier, "s2s-preserve-access-tier", true, "Preserve access tier during service to service copy. "+
		"Please refer to [Azure Blob storage: hot, cool, and archive access tiers](https://docs.microsoft.com/azure/storage/blobs/storage-blob-storage-tiers) to ensure destination storage account supports setting access tier. "+
		"In the cases that setting access tier is not supported, please use s2sPreserveAccessTier=false to bypass copying access tier (default true). ")
	syncCmd.PersistentFlags().BoolVar(&raw.s2sPreserveBlobTags, "s2s-preserve-blob-tags", false, "False by default. Preserve index tags during service to service sync from one blob storage to another.")
	// Public Documentation: https://docs.microsoft.com/en-us/azure/storage/blobs/encryption-customer-provided-keys
	// Clients making requests against Azure Blob storage have the option to provide an encryption key on a per-request basis.
	// Including the encryption key on the request provides granular control over encryption settings for Blob storage operations.
	// Customer-provided keys can be stored in Azure Key Vault or in another key store linked to storage account.
	syncCmd.PersistentFlags().StringVar(&raw.cpkScopeInfo, "cpk-by-name", "", "Client provided key by name let clients making requests against Azure Blob storage an option to provide an encryption key on a per-request basis. Provided key name will be fetched from Azure Key Vault and will be used to encrypt the data")
	syncCmd.PersistentFlags().BoolVar(&raw.cpkInfo, "cpk-by-value", false, "False by default. Client provided key by name let clients making requests against Azure Blob storage an option to provide an encryption key on a per-request basis. Provided key and its hash will be fetched from environment variables (CPK_ENCRYPTION_KEY and CPK_ENCRYPTION_KEY_SHA256 must be set).")
	syncCmd.PersistentFlags().BoolVar(&raw.mirrorMode, "mirror-mode", false, "Disable last-modified-time based comparison and overwrites the conflicting files and blobs at the destination if this flag is set to true. Default is false.")
	syncCmd.PersistentFlags().BoolVar(&raw.dryrun, "dry-run", false, "False by default. Prints the path of files that would be copied or removed by the sync command. This flag does not copy or remove the actual files.")
	syncCmd.PersistentFlags().StringVar(&raw.trailingDot, "trailing-dot", "", "'Enable' by default to treat file share related operations in a safe manner. Available options: "+strings.Join(common.ValidTrailingDotOptions(), ", ")+". "+
		"Choose 'Disable' to go back to legacy (potentially unsafe) treatment of trailing dot files where the file service will trim any trailing dots in paths. This can result in potential data corruption if the transfer contains two paths that differ only by a trailing dot (ex: mypath and mypath.). If this flag is set to 'Disable' and AzCopy encounters a trailing dot file, it will warn customers in the scanning log but will not attempt to abort the operation."+
		"If the destination does not support trailing dot files (Windows or Blob Storage), AzCopy will fail if the trailing dot file is the root of the transfer and skip any trailing dot paths encountered during enumeration.")
	syncCmd.PersistentFlags().BoolVar(&raw.includeRoot, "include-root", false, "Disabled by default. Enable to include the root directory's properties when persisting properties such as SMB or HNS ACLs")

	syncCmd.PersistentFlags().StringVar(&raw.CompareHash, "compare-hash", "None", "Inform sync to rely on hashes as an alternative to LMT. Missing hashes at a remote source will throw an error. (None, MD5) Default: None")
	syncCmd.PersistentFlags().StringVar(&common.LocalHashDir, "hash-meta-dir", "", "When using `--local-hash-storage-mode=HiddenFiles` you can specify an alternate directory to store hash metadata files in (as opposed to next to the related files in the source)")
	syncCmd.PersistentFlags().StringVar(&raw.LocalHashStorageMode, "local-hash-storage-mode", common.EHashStorageMode.Default().String(), "Specify an alternative way to cache file hashes; valid options are: HiddenFiles (OS Agnostic), XAttr (Linux/MacOS only; requires user_xattr on all filesystems traversed @ source), AlternateDataStreams (Windows only; requires named streams on target volume)")

	// temp, to assist users with change in param names, by providing a clearer message when these obsolete ones are accidentally used
	syncCmd.PersistentFlags().StringVar(&raw.legacyInclude, "include", "", "Legacy include param. DO NOT USE")
	syncCmd.PersistentFlags().StringVar(&raw.legacyExclude, "exclude", "", "Legacy exclude param. DO NOT USE")
	_ = syncCmd.PersistentFlags().MarkHidden("include")
	_ = syncCmd.PersistentFlags().MarkHidden("exclude")

	// TODO follow sym link is not implemented, clarify behavior first
	// syncCmd.PersistentFlags().BoolVar(&raw.followSymlinks, "follow-symlinks", false, "follow symbolic links when performing sync from local file system.")

	// TODO sync does not support all BlobAttributes on the command line, this functionality should be added

	// Deprecate the old persist-smb-permissions flag
	_ = syncCmd.PersistentFlags().MarkHidden("preserve-smb-permissions")
	syncCmd.PersistentFlags().BoolVar(&raw.PreservePermissions, PreservePermissionsFlag, false, "False by default. Preserves ACLs between aware resources (Windows and Azure Files, or ADLS Gen 2 to ADLS Gen 2). For Hierarchical Namespace accounts, you will need a container SAS or OAuth token with Modify Ownership and Modify Permissions permissions. For downloads, you will also need the --backup flag to restore permissions where the new Owner will not be the user running AzCopy. This flag applies to both files and folders, unless a file-only filter is specified (e.g. include-pattern).")

	// Deletes destination blobs with uncommitted blocks when staging block, hidden because we want to preserve default behavior
	syncCmd.PersistentFlags().BoolVar(&raw.deleteDestinationFileIfNecessary, "delete-destination-file", false, "False by default. Deletes destination blobs, specifically blobs with uncommitted blocks when staging block.")
	_ = syncCmd.PersistentFlags().MarkHidden("delete-destination-file")
}
