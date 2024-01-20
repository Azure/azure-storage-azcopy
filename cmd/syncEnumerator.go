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
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// -------------------------------------- Implemented Enumerators -------------------------------------- \\

func (cca *cookedSyncCmdArgs) initEnumerator(ctx context.Context) (enumerator *syncEnumerator, err error) {

	srcCredInfo, _, err := GetCredentialInfoForLocation(ctx, cca.fromTo.From(), cca.source.Value, cca.source.SAS, true, cca.cpkOptions)

	if err != nil {
		return nil, err
	}

	if cca.fromTo.IsS2S() && srcCredInfo.CredentialType != common.ECredentialType.Anonymous() {
		if srcCredInfo.CredentialType.IsAzureOAuth() && cca.fromTo.To().CanForwardOAuthTokens() {
			// no-op, this is OK
		} else if srcCredInfo.CredentialType == common.ECredentialType.GoogleAppCredentials() || srcCredInfo.CredentialType == common.ECredentialType.S3AccessKey() || srcCredInfo.CredentialType == common.ECredentialType.S3PublicBucket() {
			// this too, is OK
		} else if srcCredInfo.CredentialType == common.ECredentialType.Anonymous() {
			// this is OK
		} else {
			return nil, fmt.Errorf("the source of a %s->%s sync must either be public, or authorized with a SAS token; blob destinations can forward OAuth", cca.fromTo.From(), cca.fromTo.To())
		}
	}

	// TODO: enable symlink support in a future release after evaluating the implications
	// TODO: Consider passing an errorChannel so that enumeration errors during sync can be conveyed to the caller.
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	dest := cca.fromTo.To()
	sourceTraverser, err := InitResourceTraverser(cca.source, cca.fromTo.From(), &ctx, &srcCredInfo, common.ESymlinkHandlingType.Skip(), nil, cca.recursive, true, cca.isHNSToHNS, common.EPermanentDeleteOption.None(), func(entityType common.EntityType) {
		if entityType == common.EEntityType.File() {
			atomic.AddUint64(&cca.atomicSourceFilesScanned, 1)
		}
	}, nil, cca.s2sPreserveBlobTags, cca.compareHash, cca.preservePermissions, azcopyLogVerbosity, cca.cpkOptions, nil, false, cca.trailingDot, &dest, nil)

	if err != nil {
		return nil, err
	}

	// Because we can't trust cca.credinfo, given that it's for the overall job, not the individual traversers, we get cred info again here.
	dstCredInfo, _, err := GetCredentialInfoForLocation(ctx, cca.fromTo.To(), cca.destination.Value,
		cca.destination.SAS, false, cca.cpkOptions)

	if err != nil {
		return nil, err
	}

	// TODO: enable symlink support in a future release after evaluating the implications
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	destinationTraverser, err := InitResourceTraverser(cca.destination, cca.fromTo.To(), &ctx, &dstCredInfo, common.ESymlinkHandlingType.Skip(), nil, cca.recursive, true, cca.isHNSToHNS, common.EPermanentDeleteOption.None(), func(entityType common.EntityType) {
		if entityType == common.EEntityType.File() {
			atomic.AddUint64(&cca.atomicDestinationFilesScanned, 1)
		}
	}, nil, cca.s2sPreserveBlobTags, cca.compareHash, cca.preservePermissions, azcopyLogVerbosity, cca.cpkOptions, nil, false, cca.trailingDot, nil, nil)
	if err != nil {
		return nil, err
	}

	// verify that the traversers are targeting the same type of resources
	sourceIsDir, _ := sourceTraverser.IsDirectory(true)
	destIsDir, _ := destinationTraverser.IsDirectory(true)
	if sourceIsDir != destIsDir {
		return nil, errors.New("trying to sync between different resource types (either file <-> directory or directory <-> file) which is not allowed." +
			"sync must happen between source and destination of the same type, e.g. either file <-> file or directory <-> directory." +
			"To make sure target is handled as a directory, add a trailing '/' to the target.")
	}

	// set up the filters in the right order
	// Note: includeFilters and includeAttrFilters are ANDed
	// They must both pass to get the file included
	// Same rule applies to excludeFilters and excludeAttrFilters
	filters := buildIncludeFilters(cca.includePatterns)
	if cca.fromTo.From() == common.ELocation.Local() {
		includeAttrFilters := buildAttrFilters(cca.includeFileAttributes, cca.source.ValueLocal(), true)
		filters = append(filters, includeAttrFilters...)
	}

	filters = append(filters, buildExcludeFilters(cca.excludePatterns, false)...)
	filters = append(filters, buildExcludeFilters(cca.excludePaths, true)...)
	if cca.fromTo.From() == common.ELocation.Local() {
		excludeAttrFilters := buildAttrFilters(cca.excludeFileAttributes, cca.source.ValueLocal(), false)
		filters = append(filters, excludeAttrFilters...)
	}

	// includeRegex
	filters = append(filters, buildRegexFilters(cca.includeRegex, true)...)
	filters = append(filters, buildRegexFilters(cca.excludeRegex, false)...)

	// after making all filters, log any search prefix computed from them
	if jobsAdmin.JobsAdmin != nil {
		if prefixFilter := FilterSet(filters).GetEnumerationPreFilter(cca.recursive); prefixFilter != "" {
			jobsAdmin.JobsAdmin.LogToJobLog("Search prefix, which may be used to optimize scanning, is: "+prefixFilter, common.LogInfo) // "May be used" because we don't know here which enumerators will use it
		}
	}

	// decide our folder transfer strategy
	fpo, folderMessage := NewFolderPropertyOption(cca.fromTo, cca.recursive, true, filters, cca.preserveSMBInfo, cca.preservePermissions.IsTruthy(), false, strings.EqualFold(cca.destination.Value, common.Dev_Null), false) // sync always acts like stripTopDir=true
	if !cca.dryrunMode {
		glcm.Info(folderMessage)
	}
	if jobsAdmin.JobsAdmin != nil {
		jobsAdmin.JobsAdmin.LogToJobLog(folderMessage, common.LogInfo)
	}

	if cca.trailingDot == common.ETrailingDotOption.Enable() && !cca.fromTo.BothSupportTrailingDot() {
		cca.trailingDot = common.ETrailingDotOption.Disable()
	}

	copyJobTemplate := &common.CopyJobPartOrderRequest{
		JobID:               cca.jobID,
		CommandString:       cca.commandString,
		FromTo:              cca.fromTo,
		Fpo:                 fpo,
		SymlinkHandlingType: cca.symlinkHandling,
		SourceRoot:          cca.source.CloneWithConsolidatedSeparators(),
		DestinationRoot:     cca.destination.CloneWithConsolidatedSeparators(),
		CredentialInfo:      cca.credentialInfo,

		// flags
		BlobAttributes: common.BlobTransferAttributes{
			PreserveLastModifiedTime:         cca.preserveSMBInfo, // true by default for sync so that future syncs have this information available
			PutMd5:                           cca.putMd5,
			MD5ValidationOption:              cca.md5ValidationOption,
			BlockSizeInBytes:                 cca.blockSize,
			DeleteDestinationFileIfNecessary: cca.deleteDestinationFileIfNecessary,
		},
		ForceWrite:                     common.EOverwriteOption.True(), // once we decide to transfer for a sync operation, we overwrite the destination regardless
		ForceIfReadOnly:                cca.forceIfReadOnly,
		LogLevel:                       azcopyLogVerbosity,
		PreserveSMBPermissions:         cca.preservePermissions,
		PreserveSMBInfo:                cca.preserveSMBInfo,
		PreservePOSIXProperties:        cca.preservePOSIXProperties,
		S2SSourceChangeValidation:      true,
		DestLengthValidation:           true,
		S2SGetPropertiesInBackend:      true,
		S2SInvalidMetadataHandleOption: common.EInvalidMetadataHandleOption.RenameIfInvalid(),
		CpkOptions:                     cca.cpkOptions,
		S2SPreserveBlobTags:            cca.s2sPreserveBlobTags,

		S2SSourceCredentialType: cca.s2sSourceCredentialType,
		FileAttributes: common.FileTransferAttributes{
			TrailingDot: cca.trailingDot,
		},
	}


	options := createClientOptions(common.AzcopyCurrentJobLogger, nil)
	
	// Create Source Client. 
	var azureFileSpecificOptions any
	if cca.fromTo.From() == common.ELocation.File() {
		azureFileSpecificOptions = &common.FileClientOptions{
			AllowTrailingDot: cca.trailingDot == common.ETrailingDotOption.Enable(),
		}
	}

	sourceURL, _ := cca.source.String()
	copyJobTemplate.SrcServiceClient, err = common.GetServiceClientForLocation(
		cca.fromTo.From(),
		sourceURL,
		srcCredInfo.CredentialType,
		srcCredInfo.OAuthTokenInfo.TokenCredential,
		&options,
		azureFileSpecificOptions,
	)
	if err != nil {
		return nil, err
	}

	// Create Destination client
	if cca.fromTo.To() == common.ELocation.File() {
		azureFileSpecificOptions = &common.FileClientOptions{
			AllowTrailingDot:       cca.trailingDot == common.ETrailingDotOption.Enable(),
			AllowSourceTrailingDot: (cca.trailingDot == common.ETrailingDotOption.Enable() && cca.fromTo.To() == common.ELocation.File()),
		}
	}

	var srcTokenCred *common.ScopedCredential
	if cca.fromTo.IsS2S() && srcCredInfo.CredentialType.IsAzureOAuth() {
		srcTokenCred = common.NewScopedCredential(srcCredInfo.OAuthTokenInfo.TokenCredential, srcCredInfo.CredentialType)
	}
	
	options = createClientOptions(common.AzcopyCurrentJobLogger, srcTokenCred)
	dstURL, _ := cca.destination.String()
	copyJobTemplate.DstServiceClient, err = common.GetServiceClientForLocation(
		cca.fromTo.To(),
		dstURL,
		dstCredInfo.CredentialType,
		dstCredInfo.OAuthTokenInfo.TokenCredential,
		&options,
		azureFileSpecificOptions,
	)

	transferScheduler := newSyncTransferProcessor(cca, NumOfFilesPerDispatchJobPart, fpo, copyJobTemplate)

	// set up the comparator so that the source/destination can be compared
	indexer := newObjectIndexer()
	var comparator objectProcessor
	var finalize func() error

	switch cca.fromTo {
	case common.EFromTo.LocalBlob(), common.EFromTo.LocalFile():
		// Upload implies transferring from a local disk to a remote resource.
		// In this scenario, the local disk (source) is scanned/indexed first because it is assumed that local file systems will be faster to enumerate than remote resources
		// Then the destination is scanned and filtered based on what the destination contains
		destinationCleaner, err := newSyncDeleteProcessor(cca, fpo, copyJobTemplate.DstServiceClient)
		if err != nil {
			return nil, fmt.Errorf("unable to instantiate destination cleaner due to: %s", err.Error())
		}
		destCleanerFunc := newFpoAwareProcessor(fpo, destinationCleaner.removeImmediately)

		// when uploading, we can delete remote objects immediately, because as we traverse the remote location
		// we ALREADY have available a complete map of everything that exists locally
		// so as soon as we see a remote destination object we can know whether it exists in the local source

		comparator = newSyncDestinationComparator(indexer, transferScheduler.scheduleCopyTransfer, destCleanerFunc, cca.compareHash, cca.preserveSMBInfo, cca.mirrorMode, cca.deleteDestinationFileIfNecessary).processIfNecessary
		finalize = func() error {
			// schedule every local file that doesn't exist at the destination
			err = indexer.traverse(transferScheduler.scheduleCopyTransfer, filters)
			if err != nil {
				return err
			}

			jobInitiated, err := transferScheduler.dispatchFinalPart()
			// sync cleanly exits if nothing is scheduled.
			if err != nil && err != NothingScheduledError {
				return err
			}

			quitIfInSync(jobInitiated, cca.getDeletionCount() > 0, cca)
			cca.setScanningComplete()
			return nil
		}

		return newSyncEnumerator(sourceTraverser, destinationTraverser, indexer, filters, comparator, finalize), nil
	default:
		indexer.isDestinationCaseInsensitive = IsDestinationCaseInsensitive(cca.fromTo)
		// in all other cases (download and S2S), the destination is scanned/indexed first
		// then the source is scanned and filtered based on what the destination contains
		comparator = newSyncSourceComparator(indexer, transferScheduler.scheduleCopyTransfer, cca.compareHash, cca.preserveSMBInfo, cca.mirrorMode, cca.deleteDestinationFileIfNecessary).processIfNecessary

		finalize = func() error {
			// remove the extra files at the destination that were not present at the source
			// we can only know what needs to be deleted when we have FINISHED traversing the remote source
			// since only then can we know which local files definitely don't exist remotely
			var deleteScheduler objectProcessor
			switch cca.fromTo.To() {
			case common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS():
				deleter, err := newSyncDeleteProcessor(cca, fpo, copyJobTemplate.DstServiceClient)
				if err != nil {
					return err
				}
				deleteScheduler = newFpoAwareProcessor(fpo, deleter.removeImmediately)
			default:
				deleteScheduler = newFpoAwareProcessor(fpo, newSyncLocalDeleteProcessor(cca, fpo).removeImmediately)
			}

			err = indexer.traverse(deleteScheduler, nil)
			if err != nil {
				return err
			}

			// let the deletions happen first
			// otherwise if the final part is executed too quickly, we might quit before deletions could finish
			jobInitiated, err := transferScheduler.dispatchFinalPart()
			// sync cleanly exits if nothing is scheduled.
			if err != nil && err != NothingScheduledError {
				return err
			}

			quitIfInSync(jobInitiated, cca.getDeletionCount() > 0, cca)
			cca.setScanningComplete()
			return nil
		}

		return newSyncEnumerator(destinationTraverser, sourceTraverser, indexer, filters, comparator, finalize), nil
	}
}

func IsDestinationCaseInsensitive(fromTo common.FromTo) bool {
	if fromTo.IsDownload() && runtime.GOOS == "windows" {
		return true
	} else {
		return false
	}
}

func quitIfInSync(transferJobInitiated, anyDestinationFileDeleted bool, cca *cookedSyncCmdArgs) {
	if !transferJobInitiated && !anyDestinationFileDeleted {
		cca.reportScanningProgress(glcm, 0)
		glcm.Exit(func(format common.OutputFormat) string {
			return "The source and destination are already in sync."
		}, common.EExitCode.Success())
	} else if !transferJobInitiated && anyDestinationFileDeleted {
		// some files were deleted but no transfer scheduled
		cca.reportScanningProgress(glcm, 0)
		glcm.Exit(func(format common.OutputFormat) string {
			return "The source and destination are now in sync."
		}, common.EExitCode.Success())
	}
}
