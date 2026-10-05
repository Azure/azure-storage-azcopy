package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
)

func (cooked *CookedCopyCmdArgs) processArgs() (err error) {
	cooked.jobID = Client.CurrentJobID
	if !cooked.FromTo.IsDelete() && !cooked.FromTo.IsSetProperties() {
		// Copy owns its log lifetime in the library; remove and set-properties still use command progress.
		return cooked.prepareCopyProperties()
	}
	// set up the front end scanning logger
	common.AzcopyScanningLogger = common.NewJobLogger(Client.CurrentJobID, LogLevel, common.LogPathFolder, "-scanning")
	common.AzcopyScanningLogger.OpenLog()
	glcm.RegisterCloseFunc(func() {
		common.AzcopyScanningLogger.CloseLog()
	})

	// if no logging, set this empty so that we don't display the log location
	if LogLevel == common.LogNone {
		common.LogPathFolder = ""
	}

	cooked.putBlobSize, err = azcopy.BlockSizeInBytes(cooked.PutBlobSizeMB)
	if err != nil {
		return err
	}

	// Everything uses the new implementation of list-of-files now.
	// This handles both list-of-files and include-path as a list enumerator.
	// This saves us time because we know *exactly* what we're looking for right off the bat.
	// Note that exclude-path is handled as a filter unlike include-path.

	// unbuffered so this reads as we need it to rather than all at once in bulk
	listChan := make(chan string)
	var f *os.File

	if cooked.ListOfFiles != "" {
		f, err = os.Open(cooked.ListOfFiles)

		if err != nil {
			return fmt.Errorf("cannot open %s file passed with the list-of-file flag", cooked.ListOfFiles)
		}
	}

	// Prepare UTF-8 byte order marker
	utf8BOM := string([]byte{0xEF, 0xBB, 0xBF})

	go func() {
		defer close(listChan)
		if f != nil {
			defer f.Close()
		}

		addToChannel := func(v string, paramName string) {
			// empty strings should be ignored, otherwise the source root itself is selected
			if len(v) > 0 {
				azcopy.WarnIfHasWildcard(includeWarningOncer, paramName, v)
				listChan <- v
			}
		}

		if f != nil {
			scanner := bufio.NewScanner(f)
			checkBOM := false
			headerLineNum := 0
			firstLineIsCurlyBrace := false

			for scanner.Scan() {
				v := scanner.Text()

				// Check if the UTF-8 BOM is on the first line and remove it if necessary.
				// Note that the UTF-8 BOM can be present on the same line feed as the first line of actual data, so just use TrimPrefix.
				// If the line feed were separate, the empty string would be skipped later.
				if !checkBOM {
					v = strings.TrimPrefix(v, utf8BOM)
					checkBOM = true
				}

				// provide clear warning if user uses old (obsolete) format by mistake
				if headerLineNum <= 1 {
					cleanedLine := strings.Replace(strings.Replace(v, " ", "", -1), "\t", "", -1)
					cleanedLine = strings.TrimSuffix(cleanedLine, "[") // don't care which line this is on, could be third line
					if cleanedLine == "{" && headerLineNum == 0 {
						firstLineIsCurlyBrace = true
					} else {
						const jsonStart = "{\"Files\":"
						jsonStartNoBrace := strings.TrimPrefix(jsonStart, "{")
						isJson := cleanedLine == jsonStart || firstLineIsCurlyBrace && cleanedLine == jsonStartNoBrace
						if isJson {
							glcm.Error("The format for list-of-files has changed. The old JSON format is no longer supported")
						}
					}
					headerLineNum++
				}

				addToChannel(v, "list-of-files")
			}
		}

		for _, v := range cooked.IncludePathPatterns {
			addToChannel(v, "include-path")
		}
	}()

	if cooked.ListOfFiles != "" || len(cooked.IncludePathPatterns) > 0 {
		cooked.ListOfFilesChannel = listChan
	}
	versionsChan := make(chan string)
	var filePtr *os.File
	// Get file path from user which would contain list of all versionIDs
	// Process the file line by line and then prepare a list of all version ids of the blob.
	if cooked.ListOfVersionIDs != "" {
		filePtr, err = os.Open(cooked.ListOfVersionIDs)
		if err != nil {
			return fmt.Errorf("cannot open %s file passed with the list-of-versions flag", cooked.ListOfVersionIDs)
		}
	}

	go func() {
		defer close(versionsChan)
		if filePtr != nil {
			defer filePtr.Close()
		}
		addToChannel := func(v string) {
			if len(v) > 0 {
				versionsChan <- v
			}
		}

		if filePtr != nil {
			scanner := bufio.NewScanner(filePtr)
			checkBOM := false
			for scanner.Scan() {
				v := scanner.Text()

				if !checkBOM {
					v = strings.TrimPrefix(v, utf8BOM)
					checkBOM = true
				}

				addToChannel(v)
			}
		}
	}()

	if cooked.ListOfVersionIDs != "" {
		cooked.ListOfVersionIDsChannel = versionsChan
	}

	return cooked.prepareCopyProperties()
}

func (cooked *CookedCopyCmdArgs) prepareCopyProperties() error {
	var err error
	cooked.putBlobSize, err = azcopy.BlockSizeInBytes(cooked.PutBlobSizeMB)
	if err != nil {
		return err
	}
	cooked.CpkOptions = common.CpkOptions{
		CpkScopeInfo: cooked.cpkByName,  // Setting CPK-N
		CpkInfo:      cooked.cpkByValue, // Setting CPK-V
		// Get the key (EncryptionKey and EncryptionKeySHA256) value from environment variables when required.
	}
	if cooked.CpkOptions.CpkScopeInfo != "" || cooked.CpkOptions.CpkInfo {
		// We only support transfer from source encrypted by user key when user wishes to download.
		// Due to service limitation, S2S transfer is not supported for source encrypted by user key.
		if cooked.FromTo.IsDownload() || cooked.FromTo.IsDelete() {
			glcm.Info("Client Provided Key (CPK) for encryption/decryption is provided for download or delete scenario. " +
				"Assuming source is encrypted.")
			cooked.CpkOptions.IsSourceEncrypted = true
		}

		// TODO: Remove these warnings once service starts supporting it
		if cooked.blockBlobTier != common.EBlockBlobTier.None() || cooked.pageBlobTier != common.EPageBlobTier.None() {
			glcm.Info("Tier is provided by user explicitly. Ignoring it because Azure Service currently does" +
				" not support setting tier when client provided keys are involved.")
		}
	}

	if cooked.preserveInfo && !cooked.preservePermissions.IsTruthy() {
		if cooked.FromTo.IsNFS() {
			// Skip logging this msg for cross-protocol transfers
			// because --preserve-permissions flag is not applicable.
			if !(cooked.FromTo == common.EFromTo.FileSMBFileNFS() || cooked.FromTo == common.EFromTo.FileNFSFileSMB()) {
				glcm.Info(azcopy.PreserveNFSPermissionsDisabledMsg)
			}
		} else {
			glcm.Info(azcopy.PreservePermissionsDisabledMsg)
		}
	}

	return nil
}

// ToCopyOptions adapts the public Mover command model to the library's execution options.
func (cooked *CookedCopyCmdArgs) ToCopyOptions() (azcopy.CopyOptions, error) {
	metadata, err := getMetadata(cooked.metadata)
	if err != nil {
		return azcopy.CopyOptions{}, err
	}
	preserveProperties := cooked.s2sPreserveProperties.Value()
	preserveTier := cooked.s2sPreserveAccessTier.Value()
	options := azcopy.CopyOptions{
		Handler:                   cookedCopyHandler{cooked},
		RetainJobState:            buildmode.IsMover,
		SourceCredentialName:      cooked.SrcCredName,
		DestinationCredentialName: cooked.DstCredName,
		IncludeBefore:             cooked.IncludeBefore, IncludeAfter: cooked.IncludeAfter,
		IncludePatterns: cooked.IncludePatterns, ExcludePatterns: cooked.ExcludePatterns,
		IncludePaths: cooked.IncludePathPatterns, ExcludePaths: cooked.ExcludePathPatterns,
		IncludeRegex: cooked.includeRegex, ExcludeRegex: cooked.excludeRegex,
		IncludeAttributes: cooked.IncludeFileAttributes, ExcludeAttributes: cooked.ExcludeFileAttributes,
		ExcludeContainers: cooked.excludeContainer, ExcludeBlobTypes: cooked.excludeBlobType,
		Overwrite: cooked.ForceWrite, ForceIfReadOnly: cooked.ForceIfReadOnly,
		AutoDecompress: cooked.autoDecompress, Recursive: cooked.Recursive, FromTo: cooked.FromTo,
		BlockSizeMB:   float64(cooked.blockSize) / common.MegaByte,
		PutBlobSizeMB: float64(cooked.putBlobSize) / common.MegaByte,
		BlobType:      cooked.blobType, BlockBlobTier: cooked.blockBlobTier, PageBlobTier: cooked.pageBlobTier,
		Metadata: metadata, BlobTags: cooked.blobTagsMap,
		ContentType: cooked.contentType, ContentEncoding: cooked.contentEncoding,
		ContentDisposition: cooked.contentDisposition, ContentLanguage: cooked.contentLanguage,
		CacheControl: cooked.cacheControl, NoGuessMimeType: cooked.noGuessMimeType,
		PreserveLastModifiedTime: cooked.preserveLastModifiedTime,
		PreservePermissions:      cooked.preservePermissions.IsTruthy(),
		PreserveOwner:            &cooked.preserveOwner, PreserveInfo: &cooked.preserveInfo,
		PreservePosixProperties: cooked.preservePOSIXProperties, AsSubDir: &cooked.asSubdir,
		Symlinks: cooked.SymlinkHandling, Hardlinks: cooked.hardlinks, BackupMode: cooked.backupMode,
		PutMd5: cooked.putMd5, CheckMd5: cooked.md5ValidationOption, CheckLength: cooked.CheckLength,
		S2SPreserveProperties: &preserveProperties, S2SPreserveAccessTier: &preserveTier,
		S2SDetectSourceChanged:      cooked.s2sSourceChangeValidation,
		S2SHandleInvalidateMetadata: cooked.s2sInvalidMetadataHandleOption,
		S2SPreserveBlobTags:         cooked.S2sPreserveBlobTags, ListOfVersionIds: cooked.ListOfVersionIDs,
		IncludeDirectoryStubs: cooked.IncludeDirectoryStubs, DisableAutoDecoding: cooked.disableAutoDecoding,
		TrailingDot: cooked.trailingDot, CpkByName: cooked.CpkOptions.CpkScopeInfo, CpkByValue: cooked.CpkOptions.CpkInfo,
	}
	options.SetInternalOptions(cooked.ListOfFiles, &cooked.s2sGetPropertiesInBackend,
		cooked.dryrunMode, dryrunNewCopyJobPartOrder, cooked.deleteDestinationFileIfNecessary, cooked.commandString)
	options.SetCookedOptions(cooked.jobID, cooked.ListOfFilesChannel, cooked.ListOfVersionIDsChannel, cooked.StripTopDir,
		func(isDirectory bool) { cooked.IsSourceDir = isDirectory })
	options.SetCookedCredentialCallback(func(_, destination cred.CredentialInfo) { cooked.credentialInfo = destination })
	return options, nil
}

func (cooked *CookedCopyCmdArgs) processLibraryCopy() error {
	options, err := cooked.ToCopyOptions()
	if err != nil {
		return err
	}
	source, destination := cooked.Source.Value, cooked.Destination.Value
	if cooked.FromTo.From().IsRemote() {
		source, err = cooked.Source.String()
		if err != nil {
			return err
		}
	}
	if cooked.FromTo.To().IsRemote() {
		destination, err = cooked.Destination.String()
		if err != nil {
			return err
		}
	}
	ctx, cancel := WithJobCancellation(context.Background())
	defer cancel()
	_, err = Client.Copy(ctx, source, destination, options)
	return err
}

type cookedCopyHandler struct{ cooked *CookedCopyCmdArgs }

func (h cookedCopyHandler) OnStart(ctx azcopy.JobContext) {
	h.cooked.jobID = ctx.JobID
	StartSystemStatsMonitorForJobID(ctx.JobID)
	h.cooked.jobStartTime = time.Now()
	glcm.Init(GetStandardInitOutputBuilder(ctx.JobID.String(), ctx.LogPath, h.cooked.isCleanupJob, h.cooked.cleanupJobMessage))
}

func (h cookedCopyHandler) OnTransferProgress(progress azcopy.CopyProgress) {
	h.cooked.updateLibraryCounters(progress.ListJobSummaryResponse)
	cliCopyHandler{}.OnTransferProgress(progress)
}

func (h cookedCopyHandler) OnComplete(result azcopy.CopyResult) {
	h.cooked.updateLibraryCounters(result.ListJobSummaryResponse)
	h.cooked.isEnumerationComplete = true
	if h.cooked.hasFollowup() {
		exitCode := h.cooked.getSuccessExitCode()
		if result.TransfersFailed > 0 || result.JobStatus == common.EJobStatus.Cancelled() {
			exitCode = EExitCode.Error()
		}
		h.cooked.launchFollowup(exitCode)
		return
	}
	cliCopyHandler{}.OnComplete(result)
}

func (cooked *CookedCopyCmdArgs) updateLibraryCounters(summary common.ListJobSummaryResponse) {
	cooked.isEnumerationComplete = summary.CompleteJobOrdered
	atomic.StoreUint32(&cooked.atomicSkippedSymlinkCount, summary.SkippedSymlinkCount)
	atomic.StoreUint32(&cooked.atomicSkippedSpecialFileCount, summary.SkippedSpecialFileCount)
	atomic.StoreUint32(&cooked.atomicSkippedHardlinkCount, summary.SkippedHardlinkCount)
	atomic.StoreUint64(&cooked.atomicSkippedArchiveFileCount, summary.SkippedArchiveFileCount)
}
