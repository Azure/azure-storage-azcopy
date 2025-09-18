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
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

type rawSyncCmdArgs struct {
	src       string
	dst       string
	recursive bool
	fromTo    string

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
	excludeRegex          string
	compareHash           string
	localHashStorageMode  string

	includeDirectoryStubs   bool // Includes hdi_isfolder objects in the sync even w/o preservePermissions.
	preservePermissions     bool
	preserveSMBPermissions  bool // deprecated and synonymous with preservePermissions
	preserveOwner           bool
	preserveSMBInfo         bool
	preservePOSIXProperties bool
	backupMode              bool
	putMd5                  bool
	md5ValidationOption     string
	includeRoot             bool
	// this flag indicates the user agreement with respect to deleting the extra files at the destination
	// which do not exists at source. With this flag turned on/off, users will not be asked for permission.
	// otherwise the user is prompted to make a decision
	deleteDestination string

	// this flag is to disable comparator and overwrite files at destination irrespective
	mirrorMode bool

	s2sPreserveAccessTier bool
	// Opt-in flag to preserve the blob index tags during service to service transfer.
	s2sPreserveBlobTags bool

	forceIfReadOnly bool

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
	// Opt-in flag to persist additional properties to Azure Files
	preserveInfo bool
	hardlinks    string

	hashMetaDir string
}

func (raw rawSyncCmdArgs) toOptions() (opts azcopy.SyncOptions, err error) {
	opts = azcopy.SyncOptions{
		Recursive:               to.Ptr(raw.recursive),
		IncludeDirectoryStubs:   raw.includeDirectoryStubs,
		PreserveInfo:            to.Ptr(raw.preserveInfo),
		PreservePosixProperties: raw.preservePOSIXProperties,
		ForceIfReadOnly:         raw.forceIfReadOnly,
		BlockSizeMB:             raw.blockSizeMB,
		PutBlobSizeMB:           raw.putBlobSizeMB,
		PutMd5:                  raw.putMd5,
		S2SPreserveAccessTier:   to.Ptr(raw.s2sPreserveAccessTier), // finalize code will ensure this is only set for S2S
		S2SPreserveBlobTags:     raw.s2sPreserveBlobTags,
		CpkByName:               raw.cpkScopeInfo,
		CpkByValue:              raw.cpkInfo,
		MirrorMode:              raw.mirrorMode,
		IncludeRoot:             raw.includeRoot,
		HashMetaDir:             raw.hashMetaDir,
		PreservePermissions:     raw.preservePermissions,
	}
	opts.FromTo, err = azcopy.InferAndValidateFromTo(raw.src, raw.dst, raw.fromTo)
	if err != nil {
		return opts, err
	}
	opts.IncludePatterns = parsePatterns(raw.include)
	opts.ExcludePatterns = parsePatterns(raw.exclude)
	opts.ExcludePaths = parsePatterns(raw.excludePath)
	opts.IncludeAttributes = parsePatterns(raw.includeFileAttributes)
	opts.ExcludeAttributes = parsePatterns(raw.excludeFileAttributes)
	opts.IncludeRegex = parsePatterns(raw.includeRegex)
	opts.ExcludeRegex = parsePatterns(raw.excludeRegex)
	err = opts.DeleteDestination.Parse(raw.deleteDestination)
	if err != nil {
		return opts, err
	}
	err = opts.CheckMd5.Parse(raw.md5ValidationOption)
	if err != nil {
		return opts, err
	}
	err = opts.TrailingDot.Parse(raw.trailingDot)
	if err != nil {
		return opts, err
	}
	err = opts.CompareHash.Parse(raw.compareHash)
	if err != nil {
		return opts, err
	}
	var hashStorageMode common.HashStorageMode
	err = hashStorageMode.Parse(raw.localHashStorageMode)
	if err != nil {
		return opts, err
	}
	opts.LocalHashStorageMode = to.Ptr(hashStorageMode)
	err = opts.Hardlinks.Parse(raw.hardlinks)
	if err != nil {
		return opts, err
	}
	// set internal only options
	cmd := gCopyUtil.ConstructCommandStringFromArgs()
	opts.SetInternalOptions(raw.dryrun, raw.deleteDestinationFileIfNecessary, cmd, dryrunNewCopyJobPartOrder, dryrunDelete)
	return opts, nil
}

// TODO : (gapra) We need to wrap glcm since Golang does not support method overloading.
// We could consider naming the methods different per job type - then we can just pass glcm.
type CLISyncHandler struct {
}

func (C CLISyncHandler) OnStart(ctx common.JobContext) {
	glcm.OnStart(ctx)
}

func (C CLISyncHandler) OnTransferProgress(progress azcopy.SyncJobProgress) {
	glcm.OnTransferProgress(common.TransferProgress{
		ListJobSummaryResponse: progress.ListJobSummaryResponse,
		Throughput:             progress.Throughput,
		ElapsedTime:            progress.ElapsedTime,
		JobType:                common.EJobType.Resume(),
	})
}

func (C CLISyncHandler) OnScanProgress(progress common.ScanProgress) {
	glcm.OnScanProgress(progress)
}

func init() {
	raw := rawSyncCmdArgs{}
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
			raw.src = args[0]
			raw.dst = args[1]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			glcm.EnableInputWatcher()
			if cancelFromStdin {
				glcm.EnableCancelFromStdIn()
			}

			// Deprecated flags
			if raw.legacyInclude != "" || raw.legacyExclude != "" {
				glcm.Error("the include and exclude parameters have been replaced by include-pattern and exclude-pattern. They work on filenames only (not paths)")
			}

			// We infer FromTo and validate it here since it is critical to a lot of other options parsing below.
			userFromTo, err := azcopy.InferAndValidateFromTo(raw.src, raw.dst, raw.fromTo)
			if err != nil {
				glcm.Error("failed to parse --from-to user input due to error: " + err.Error())
			}

			if azcopy.AreBothLocationsNFSAware(userFromTo) {
				if (raw.preserveSMBInfo && runtime.GOOS == "linux") || raw.preserveSMBPermissions {
					glcm.Error(InvalidFlagsForNFSMsg)
				}
			}

			// if both flags are set, we honor the new flag and ignore the old one
			if cmd.Flags().Changed(PreserveInfoFlag) && cmd.Flags().Changed(PreserveSMBInfoFlag) {
				raw.preserveInfo = raw.preserveInfo
			} else if cmd.Flags().Changed(PreserveInfoFlag) {
				raw.preserveInfo = raw.preserveInfo
			} else if cmd.Flags().Changed(PreserveSMBInfoFlag) {
				raw.preserveInfo = raw.preserveSMBInfo
			} else {
				raw.preserveInfo = azcopy.GetPreserveInfoDefault(userFromTo)
			}
			// if transfer is NFS aware, honor the preserve-permissions flag, otherwise honor preserve-smb-permissions flag
			if azcopy.AreBothLocationsNFSAware(userFromTo) {
				raw.preservePermissions = raw.preservePermissions
			} else {
				raw.preservePermissions = raw.preservePermissions || raw.preserveSMBPermissions
			}

			if OutputLevel == common.EOutputVerbosity.Quiet() || OutputLevel == common.EOutputVerbosity.Essential() {
				if strings.EqualFold(raw.deleteDestination, common.EDeleteDestination.Prompt().String()) {
					err = fmt.Errorf("cannot set output level '%s' with delete-destination option '%s'", OutputLevel.String(), raw.deleteDestination)
				} else if raw.dryrun {
					err = fmt.Errorf("cannot set output level '%s' with dry-run mode", OutputLevel.String())
				}
			}

			var opts azcopy.SyncOptions
			if opts, err = raw.toOptions(); err != nil {
				glcm.Error("error parsing the input given by the user. Failed with error " + err.Error() + getErrorCodeUrl(err))
			}

			// Create a context that can be cancelled by Ctrl-C
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Set up signal handling for graceful cancellation
			go func() {
				sigChan := make(chan os.Signal, 1)
				signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
				<-sigChan
				cancel()
			}()

			result, err := Client.Sync(ctx, raw.src, raw.dst, opts, CLISyncHandler{})
			if err != nil {
				glcm.Error("Cannot perform sync due to error: " + err.Error() + getErrorCodeUrl(err))
			}
			if raw.dryrun {
				glcm.Exit(nil, common.EExitCode.Success())
			} else {
				// Print summary
				exitCode := common.EExitCode.Success()
				if result.TransfersFailed > 0 || result.JobStatus == common.EJobStatus.Cancelled() || result.JobStatus == common.EJobStatus.Cancelling() {
					exitCode = common.EExitCode.Error()
				}
				summary := common.JobSummary{
					ExitCode:                 exitCode,
					ListJobSummaryResponse:   result.ListJobSummaryResponse,
					DeleteTransfersCompleted: result.DeleteTransfersCompleted,
					DeleteTotalTransfers:     result.DeleteTotalTransfers,
					SourceFilesScanned:       result.SourceFilesScanned,
					DestinationFilesScanned:  result.DestinationFilesScanned,
					ElapsedTime:              result.ElapsedTime,
					JobType:                  common.EJobType.Sync(),
				}
				glcm.OnComplete(summary)
			}
			// Wait for the user to see the final output before exiting
			glcm.SurrenderControl()
		},
	}

	rootCmd.AddCommand(syncCmd)
	syncCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", true,
		"True by default, look into sub-directories recursively when syncing between directories. (default true).")

	syncCmd.PersistentFlags().StringVar(&raw.fromTo, "from-to", "",
		"Source-to-destination combination. Required for NFS transfers; optional for SMB."+
			"Examples: LocalBlob, BlobLocal, LocalFileSMB, FileSMBLocal, BlobFile, FileBlob, LocaFileNFS, "+
			"FileNFSLocal, FileNFSFileNFS, etc.")

	syncCmd.PersistentFlags().BoolVar(&raw.includeDirectoryStubs, "include-directory-stub", false,
		"False by default, includes blobs with the hdi_isfolder metadata in the transfer.")

	// TODO: enable for copy with IfSourceNewer
	// smb info/permissions can be persisted in the scenario of File -> File
	syncCmd.PersistentFlags().BoolVar(&raw.preserveSMBPermissions, "preserve-smb-permissions", false,
		"False by default. "+
			"\n Preserves SMB ACLs between aware resources (Azure Files). "+
			"\n This flag applies to both files and folders, unless a file-only filter is specified (e.g. include-pattern).")

	syncCmd.PersistentFlags().BoolVar(&raw.preserveSMBInfo, "preserve-smb-info", (runtime.GOOS == "windows"),
		"Preserves SMB property info (last write time, creation time, attribute bits)"+
			" between SMB-aware resources (Windows and Azure Files SMB). "+
			"On windows, this flag will be set to true by default. \n If the source or destination is a "+
			"\n volume mounted on Linux using SMB protocol, this flag will have to be explicitly set to true."+
			"\n  Only the attribute bits supported by Azure Files will be transferred; any others will be ignored. "+
			"\n This flag applies to both files and folders, unless a file-only filter is specified "+
			"(e.g. include-pattern). \n The info transferred for folders is the same as that for files, "+
			"except for Last Write Time which is never preserved for folders.")

	//Marking this flag as hidden as we might not support it in the future
	_ = syncCmd.PersistentFlags().MarkHidden("preserve-smb-info")
	syncCmd.PersistentFlags().BoolVar(&raw.preserveInfo, PreserveInfoFlag, false,
		"Specify this flag if you want to preserve properties during the transfer operation."+
			"The previously available flag for SMB (--preserve-smb-info) is now redirected to --preserve-info "+
			"flag for both SMB and NFS operations. The default value is true for Windows when copying to Azure Files SMB"+
			"share and for Linux when copying to Azure Files NFS share. ")

	syncCmd.PersistentFlags().BoolVar(&raw.preservePOSIXProperties, "preserve-posix-properties", false,
		"False by default. 'Preserves' property info gleaned from stat or statx into object metadata.")

	// TODO: enable when we support local <-> File
	syncCmd.PersistentFlags().BoolVar(&raw.forceIfReadOnly, "force-if-read-only", false, "False by default. "+
		"\n When overwriting an existing file on Windows or Azure Files, force the overwrite to work even if the"+
		"existing file has its read-only attribute set.")
	// syncCmd.PersistentFlags().BoolVar(&raw.preserveOwner, common.PreserveOwnerFlagName, common.PreserveOwnerDefault, "Only has an effect in downloads, and only when --preserve-smb-permissions is used. If true (the default), the file Owner and Group are preserved in downloads. If set to false, --preserve-smb-permissions will still preserve ACLs but Owner and Group will be based on the user running AzCopy")
	// syncCmd.PersistentFlags().BoolVar(&raw.backupMode, common.BackupModeFlagName, false, "Activates Windows' SeBackupPrivilege for uploads, or SeRestorePrivilege for downloads, to allow AzCopy to see read all files, regardless of their file system permissions, and to restore all permissions. Requires that the account running AzCopy already has these permissions (e.g. has Administrator rights or is a member of the 'Backup Operators' group). All this flag does is activate privileges that the account already has")

	syncCmd.PersistentFlags().Float64Var(&raw.blockSizeMB, "block-size-mb", 0,
		"Use this block size (specified in MiB) when uploading to Azure Storage or downloading from Azure Storage. "+
			"\n Default is automatically calculated based on file size. Decimal fractions are allowed (For example: 0.25).")

	syncCmd.PersistentFlags().Float64Var(&raw.putBlobSizeMB, "put-blob-size-mb", 0,
		"Use this size (specified in MiB) as a threshold to determine whether to upload a blob as a single PUT request"+
			"when uploading to Azure Storage. \n The default value is automatically calculated based on file size."+
			"Decimal fractions are allowed (For example: 0.25).")

	syncCmd.PersistentFlags().StringVar(&raw.include, "include-pattern", "",
		"Include only files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")

	syncCmd.PersistentFlags().StringVar(&raw.exclude, "exclude-pattern", "",
		"Exclude files where the name matches the pattern list.\n For example: *.jpg;*.pdf;exactName")

	syncCmd.PersistentFlags().StringVar(&raw.excludePath, "exclude-path", "",
		"Exclude these paths when comparing the source against the destination. "+
			"\n This option does not support wildcard characters (*). "+
			"\n Checks relative path prefix(For example: myFolder;myFolder/subDirName/file.pdf).")

	syncCmd.PersistentFlags().StringVar(&raw.includeFileAttributes, "include-attributes", "",
		"(Windows only) Include only files whose attributes match the attribute list.\n For example: A;S;R")

	syncCmd.PersistentFlags().StringVar(&raw.excludeFileAttributes, "exclude-attributes", "",
		"(Windows only) Exclude files whose attributes match the attribute list.\n For example: A;S;R")

	syncCmd.PersistentFlags().StringVar(&raw.includeRegex, "include-regex", "",
		"Include the relative path of the files that match with the regular expressions. "+
			"\n Separate regular expressions with ';'.")

	syncCmd.PersistentFlags().StringVar(&raw.excludeRegex, "exclude-regex", "",
		"Exclude the relative path of the files that match with the regular expressions. "+
			"\n Separate regular expressions with ';'.")

	syncCmd.PersistentFlags().StringVar(&raw.deleteDestination, "delete-destination", "false",
		"Defines whether to delete extra files from the destination that are not present at the source. "+
			"\n Could be set to true, false, or prompt. "+
			"\n If set to prompt, the user will be asked a question before scheduling files and blobs for deletion. (default 'false').")

	syncCmd.PersistentFlags().BoolVar(&raw.putMd5, "put-md5", false,
		"Create an MD5 hash of each file, and save the hash as the Content-MD5 property of the destination blob or file. "+
			"\n (By default the hash is NOT created.) Only available when uploading.")

	syncCmd.PersistentFlags().StringVar(&raw.md5ValidationOption, "check-md5", common.DefaultHashValidationOption.String(),
		"Specifies how strictly MD5 hashes should be validated when downloading. "+
			"\n This option is only available when downloading. "+
			"\n Available values include: NoCheck, LogOnly, FailIfDifferent, FailIfDifferentOrMissing. (default 'FailIfDifferent').")

	syncCmd.PersistentFlags().BoolVar(&raw.s2sPreserveAccessTier, "s2s-preserve-access-tier", true,
		"Preserve access tier during service to service copy. "+
			"\n Please refer to [Azure Blob storage: hot, cool, and archive access tiers](https://docs.microsoft.com/azure/storage/blobs/storage-blob-storage-tiers) to ensure destination storage account supports setting access tier. "+
			"\n In the cases that setting access tier is not supported, please use s2sPreserveAccessTier=false to bypass copying access tier (default true). ")

	syncCmd.PersistentFlags().BoolVar(&raw.s2sPreserveBlobTags, "s2s-preserve-blob-tags", false,
		"False by default. "+
			"\n Preserve index tags during service to service sync from one blob storage to another.")

	// Public Documentation: https://docs.microsoft.com/en-us/azure/storage/blobs/encryption-customer-provided-keys
	// Clients making requests against Azure Blob storage have the option to provide an encryption key on a per-request basis.
	// Including the encryption key on the request provides granular control over encryption settings for Blob storage operations.
	// Customer-provided keys can be stored in Azure Key Vault or in another key Store linked to storage account.
	syncCmd.PersistentFlags().StringVar(&raw.cpkScopeInfo, "cpk-by-name", "",
		"Client provided key by name let clients making requests against Azure Blob storage an option "+
			"\n to provide an encryption key on a per-request basis. "+
			"\n Provided key name will be fetched from Azure Key Vault and will be used to encrypt the data")

	syncCmd.PersistentFlags().BoolVar(&raw.cpkInfo, "cpk-by-value", false,
		"False by default. Client provided key by name let clients making requests against Azure Blob storage an option "+
			"\n to provide an encryption key on a per-request basis. "+
			"\n Provided key and its hash will be fetched from environment variables (CPK_ENCRYPTION_KEY and CPK_ENCRYPTION_KEY_SHA256 must be set).")

	syncCmd.PersistentFlags().BoolVar(&raw.mirrorMode, "mirror-mode", false,
		"Disable last-modified-time based comparison and "+
			"\n overwrites the conflicting files and blobs at the destination if this flag is set to true. "+
			"\n Default is false.")

	syncCmd.PersistentFlags().BoolVar(&raw.dryrun, "dry-run", false,
		"False by default. Prints the path of files that would be copied or removed by the sync command. "+
			"\n This flag does not copy or remove the actual files.")

	syncCmd.PersistentFlags().StringVar(&raw.trailingDot, "trailing-dot", "",
		"'Enable' by default to treat file share related operations in a safe manner."+
			"\n  Available options: "+strings.Join(common.ValidTrailingDotOptions(), ", ")+". "+
			"\n Choose 'Disable' to go back to legacy (potentially unsafe) treatment of trailing dot files where the file service will trim any trailing dots in paths. "+
			"\n This can result in potential data corruption if the transfer contains two paths that differ only by a trailing dot (ex: mypath and mypath.). "+
			"\n If this flag is set to 'Disable' and AzCopy encounters a trailing dot file, it will warn customers in the scanning log but will not attempt to abort the operation."+
			"\n If the destination does not support trailing dot files (Windows or Blob Storage), "+
			"\n AzCopy will fail if the trailing dot file is the root of the transfer and skip any trailing dot paths encountered during enumeration.")

	syncCmd.PersistentFlags().BoolVar(&raw.includeRoot, "include-root", false, "Disabled by default. "+
		"\n Enable to include the root directory's properties when persisting properties such as SMB or HNS ACLs")

	syncCmd.PersistentFlags().StringVar(&raw.compareHash, "compare-hash", "None",
		"Inform sync to rely on hashes as an alternative to LMT. "+
			"\n Missing hashes at a remote source will throw an error. (None, MD5) Default: None")

	syncCmd.PersistentFlags().StringVar(&raw.hashMetaDir, "hash-meta-dir", "",
		"When using `--local-hash-storage-mode=HiddenFiles` "+
			"\n you can specify an alternate directory to Store hash metadata files in (as opposed to next to the related files in the source)")

	syncCmd.PersistentFlags().StringVar(&raw.localHashStorageMode, "local-hash-storage-mode",
		common.EHashStorageMode.Default().String(), "Specify an alternative way to cache file hashes; "+
			"\n valid options are: HiddenFiles (OS Agnostic), "+
			"\n XAttr (Linux/MacOS only; requires user_xattr on all filesystems traversed @ source), "+
			"\n AlternateDataStreams (Windows only; requires named streams on target volume)")

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
	syncCmd.PersistentFlags().BoolVar(&raw.preservePermissions, PreservePermissionsFlag, false, "False by default. "+
		"\nPreserves ACLs between aware resources (Windows and Azure Files SMB or Data Lake Storage to Data Lake Storage)"+
		"and permissions between aware resources(Linux to Azure Files NFS). "+
		"\nFor accounts that have a hierarchical namespace, your security principal must be the owning user of the target container or it must be assigned "+
		"\nthe Storage Blob Data Owner role, scoped to the target container, storage account, parent resource group, or subscription. "+
		"\nFor downloads, you will also need the --backup flag to restore permissions where the new Owner will not be the user running AzCopy. "+
		"\nThis flag applies to both files and folders, unless a file-only filter is specified (e.g. include-pattern).")

	// Deletes destination blobs with uncommitted blocks when staging block, hidden because we want to preserve default behavior
	syncCmd.PersistentFlags().BoolVar(&raw.deleteDestinationFileIfNecessary, "delete-destination-file", false, "False by default. Deletes destination blobs, specifically blobs with uncommitted blocks when staging block.")
	_ = syncCmd.PersistentFlags().MarkHidden("delete-destination-file")

	syncCmd.PersistentFlags().StringVar(&raw.hardlinks, HardlinksFlag, "follow",
		"Follow by default. Preserve hardlinks for NFS resources. "+
			"\n This flag is only applicable when the source is Azure NFS file share or the destination is NFS file share. "+
			"\n Available options: skip, preserve, follow (default 'follow').")
}
