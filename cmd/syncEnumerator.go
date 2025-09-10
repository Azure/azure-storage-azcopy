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

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// -------------------------------------- Implemented Enumerators -------------------------------------- \\

func (cca *cookedSyncCmdArgs) initEnumerator(ctx context.Context) (enumerator *traverser.SyncEnumerator, err error) {

	uotm := Client.GetUserOAuthTokenManagerInstance()
	srcCredInfo, _, err := GetCredentialInfoForLocation(ctx, cca.fromTo.From(), cca.source, true, uotm, cca.cpkOptions)

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

	includeDirStubs := (cca.fromTo.From().SupportsHnsACLs() && cca.fromTo.To().SupportsHnsACLs() && cca.preservePermissions.IsTruthy()) || cca.includeDirectoryStubs

	var srcReauthTok *common.ScopedAuthenticator
	if at, ok := srcCredInfo.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok {
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		srcReauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}

	options := traverser.CreateClientOptions(common.AzcopyCurrentJobLogger, nil, srcReauthTok)

	// Create Source Client.
	var azureFileSpecificOptions any
	if cca.fromTo.From() == common.ELocation.File() || cca.fromTo.From() == common.ELocation.FileNFS() {
		azureFileSpecificOptions = &common.FileClientOptions{
			AllowTrailingDot: cca.trailingDot == common.ETrailingDotOption.Enable(),
		}
	}

	srcServiceClient, err := common.GetServiceClientForLocation(
		cca.fromTo.From(),
		cca.source,
		srcCredInfo.CredentialType,
		srcCredInfo.OAuthTokenInfo.TokenCredential,
		&options,
		azureFileSpecificOptions,
	)
	if err != nil {
		return nil, err
	}

	// Create Destination client
	if cca.fromTo.To() == common.ELocation.File() || cca.fromTo.To() == common.ELocation.FileNFS() {
		azureFileSpecificOptions = &common.FileClientOptions{
			AllowTrailingDot:       cca.trailingDot == common.ETrailingDotOption.Enable(),
			AllowSourceTrailingDot: (cca.trailingDot == common.ETrailingDotOption.Enable() && cca.fromTo.To() == common.ELocation.File()),
		}
	}

	// Because we can't trust cca.credinfo, given that it's for the overall job, not the individual traversers, we get cred info again here.
	dstCredInfo, _, err := GetCredentialInfoForLocation(ctx, cca.fromTo.To(), cca.destination, false, uotm, cca.cpkOptions)

	if err != nil {
		return nil, err
	}

	var dstReauthTok *common.ScopedAuthenticator
	if at, ok := dstCredInfo.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok {
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		dstReauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}

	var srcTokenCred *common.ScopedToken
	if cca.fromTo.IsS2S() && srcCredInfo.CredentialType.IsAzureOAuth() {
		srcTokenCred = common.NewScopedCredential(srcCredInfo.OAuthTokenInfo.TokenCredential, srcCredInfo.CredentialType)
	}

	options = traverser.CreateClientOptions(common.AzcopyCurrentJobLogger, srcTokenCred, dstReauthTok)
	dstServiceClient, err := common.GetServiceClientForLocation(
		cca.fromTo.To(),
		cca.destination,
		dstCredInfo.CredentialType,
		dstCredInfo.OAuthTokenInfo.TokenCredential,
		&options,
		azureFileSpecificOptions,
	)

	// TODO: enable symlink support in a future release after evaluating the implications
	// TODO: Consider passing an errorChannel so that enumeration errors during sync can be conveyed to the caller.
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	dest := cca.fromTo.To()
	sourceTraverser, err := traverser.InitResourceTraverser(cca.source, cca.fromTo.From(), ctx, traverser.InitResourceTraverserOptions{
		DestResourceType: &dest,

		Client:         srcServiceClient,
		CredentialType: srcCredInfo.CredentialType,
		IncrementEnumeration: func(entityType common.EntityType) {
			if entityType == common.EEntityType.File() {
				atomic.AddUint64(&cca.atomicSourceFilesScanned, 1)
			}
			if common.IsNFSCopy() {
				if entityType == common.EEntityType.Other() {
					atomic.AddUint32(&cca.atomicSkippedSpecialFileCount, 1)
				} else if entityType == common.EEntityType.Symlink() {
					atomic.AddUint32(&cca.atomicSkippedSymlinkCount, 1)
				}
			}
		},

		CpkOptions: cca.cpkOptions,

		SyncHashType:        cca.compareHash,
		PreservePermissions: cca.preservePermissions,
		TrailingDotOption:   cca.trailingDot,

		Recursive:               cca.recursive,
		GetPropertiesInFrontend: true,
		IncludeDirectoryStubs:   includeDirStubs,
		PreserveBlobTags:        cca.s2sPreserveBlobTags,
		HardlinkHandling:        cca.hardlinks,
	})

	if err != nil {
		return nil, err
	}

	// TODO: enable symlink support in a future release after evaluating the implications
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	destinationTraverser, err := traverser.InitResourceTraverser(cca.destination, cca.fromTo.To(), ctx, traverser.InitResourceTraverserOptions{
		Client:         dstServiceClient,
		CredentialType: dstCredInfo.CredentialType,
		IncrementEnumeration: func(entityType common.EntityType) {
			if entityType == common.EEntityType.File() {
				atomic.AddUint64(&cca.atomicDestinationFilesScanned, 1)
			}
		},

		CpkOptions: cca.cpkOptions,

		SyncHashType:        cca.compareHash,
		PreservePermissions: cca.preservePermissions,
		TrailingDotOption:   cca.trailingDot,

		Recursive:               cca.recursive,
		GetPropertiesInFrontend: true,
		IncludeDirectoryStubs:   includeDirStubs,
		PreserveBlobTags:        cca.s2sPreserveBlobTags,
		HardlinkHandling:        common.EHardlinkHandlingType.Follow(),
	})
	if err != nil {
		return nil, err
	}

	// verify that the traversers are targeting the same type of resources
	sourceIsDir, _ := sourceTraverser.IsDirectory(true)
	destIsDir, err := destinationTraverser.IsDirectory(true)

	var resourceMismatchError = errors.New("trying to sync between different resource types (either file <-> directory or directory <-> file) which is not allowed." +
		"sync must happen between source and destination of the same type, e.g. either file <-> file or directory <-> directory." +
		"To make sure target is handled as a directory, add a trailing '/' to the target.")

	if cca.fromTo.To() == common.ELocation.Blob() || cca.fromTo.To() == common.ELocation.BlobFS() {

		/*
			This is an "opinionated" choice. Blob has no formal understanding of directories. As such, we don't care about if it's a directory.

			If they sync a lone blob, they sync a lone blob.
			If it lands on a directory stub, FNS is OK with this, but HNS isn't. It'll fail in that case. This is still semantically valid in FNS.
			If they sync a prefix of blobs, they sync a prefix of blobs. This will always succeed, and won't break any semantics about FNS.

			So my (Adele's) opinion moving forward is:
			- Hierarchies don't exist in flat namespaces.
			- Instead, there are objects and prefixes.
			- Stubs exist to clarify prefixes.
			- Stubs do not exist to enforce naming conventions.
			- We are a tool, tools can be misused. It is up to the customer to validate everything they intend to do.
		*/

		if bloberror.HasCode(err, bloberror.ContainerNotFound) { // We can resolve a missing container. Let's create it.
			bt := destinationTraverser.(*traverser.BlobTraverser)
			sc := bt.ServiceClient                                                   // it being a blob traverser is a relatively safe assumption, because
			bUrlParts, _ := blob.ParseURL(bt.RawURL)                                 // it should totally have succeeded by now anyway
			_, err = sc.NewContainerClient(bUrlParts.ContainerName).Create(ctx, nil) // If it doesn't work out, this will surely bubble up later anyway. It won't be long.
			if err != nil {
				glcm.Warn(fmt.Sprintf("Failed to create the missing destination container: %v", err))
			}
			// At this point, we'll let the destination be written to with the original resource type.
		}
	} else if err != nil && fileerror.HasCode(err, fileerror.ShareNotFound) {
		return nil, fmt.Errorf("%s Destination file share: %s", DstShareDoesNotExists, cca.destination.Value)
	} else if err == nil && sourceIsDir != destIsDir {
		// If the destination exists, and isn't blob though, we have to match resource types.
		return nil, resourceMismatchError
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
	if prefixFilter := traverser.FilterSet(filters).GetEnumerationPreFilter(cca.recursive); prefixFilter != "" {
		common.LogToJobLogWithPrefix("Search prefix, which may be used to optimize scanning, is: "+prefixFilter, common.LogInfo) // "May be used" because we don't know here which enumerators will use it
	}

	// decide our folder transfer strategy
	// sync always acts like stripTopDir=true, but if we intend to persist the root, we must tell NewFolderPropertyOption stripTopDir=false.
	fpo, folderMessage := NewFolderPropertyOption(cca.fromTo, cca.recursive, !cca.includeRoot, filters, cca.preserveInfo, cca.preservePermissions.IsTruthy(), false, strings.EqualFold(cca.destination.Value, common.Dev_Null), cca.includeDirectoryStubs)
	if !cca.dryrunMode {
		glcm.Info(folderMessage)
	}
	common.LogToJobLogWithPrefix(folderMessage, common.LogInfo)

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

		// flags
		BlobAttributes: common.BlobTransferAttributes{
			PreserveLastModifiedTime:         cca.preserveInfo, // true by default for sync so that future syncs have this information available
			PutMd5:                           cca.putMd5,
			MD5ValidationOption:              cca.md5ValidationOption,
			BlockSizeInBytes:                 cca.blockSize,
			PutBlobSizeInBytes:               cca.putBlobSize,
			DeleteDestinationFileIfNecessary: cca.deleteDestinationFileIfNecessary,
		},
		ForceWrite:                     common.EOverwriteOption.True(), // once we decide to transfer for a sync operation, we overwrite the destination regardless
		ForceIfReadOnly:                cca.forceIfReadOnly,
		LogLevel:                       Client.GetLogLevel(),
		PreservePermissions:            cca.preservePermissions,
		PreserveInfo:                   cca.preserveInfo,
		PreservePOSIXProperties:        cca.preservePOSIXProperties,
		S2SSourceChangeValidation:      true,
		DestLengthValidation:           true,
		S2SGetPropertiesInBackend:      true,
		S2SInvalidMetadataHandleOption: common.EInvalidMetadataHandleOption.RenameIfInvalid(),
		CpkOptions:                     cca.cpkOptions,
		S2SPreserveBlobTags:            cca.s2sPreserveBlobTags,

		S2SSourceCredentialType: srcCredInfo.CredentialType,
		FileAttributes: common.FileTransferAttributes{
			TrailingDot: cca.trailingDot,
		},
		JobErrorHandler:  glcm,
		SrcServiceClient: srcServiceClient,
		DstServiceClient: dstServiceClient,
	}

	// Check protocol compatibility for File Shares
	if err := validateProtocolCompatibility(ctx, cca.fromTo, cca.source, cca.destination, copyJobTemplate.SrcServiceClient, copyJobTemplate.DstServiceClient); err != nil {
		return nil, err
	}

	transferScheduler := newSyncTransferProcessor(cca, NumOfFilesPerDispatchJobPart, fpo, copyJobTemplate)

	// set up the comparator so that the source/destination can be compared
	indexer := traverser.NewObjectIndexer()
	var comparator traverser.ObjectProcessor
	var finalize func() error

	switch cca.fromTo {
	case common.EFromTo.LocalBlob(), common.EFromTo.LocalFile(), common.EFromTo.LocalFileNFS():
		// Upload implies transferring from a local disk to a remote resource.
		// In this scenario, the local disk (source) is scanned/indexed first because it is assumed that local file systems will be faster to enumerate than remote resources
		// Then the destination is scanned and filtered based on what the destination contains
		destinationCleaner, err := newSyncDeleteProcessor(cca, fpo, copyJobTemplate.DstServiceClient)
		if err != nil {
			return nil, fmt.Errorf("unable to instantiate destination cleaner due to: %s", err.Error())
		}
		destCleanerFunc := traverser.NewFpoAwareProcessor(fpo, destinationCleaner.removeImmediately)

		// when uploading, we can delete remote objects immediately, because as we Traverse the remote location
		// we ALREADY have available a complete map of everything that exists locally
		// so as soon as we see a remote destination object we can know whether it exists in the local source

		comparator = newSyncDestinationComparator(indexer, transferScheduler.scheduleCopyTransfer, destCleanerFunc, cca.compareHash, cca.preserveInfo, cca.mirrorMode).processIfNecessary
		finalize = func() error {
			// schedule every local file that doesn't exist at the destination
			err = indexer.Traverse(transferScheduler.scheduleCopyTransfer, filters)
			if err != nil {
				return err
			}

			_, err := transferScheduler.dispatchFinalPart()
			// sync cleanly exits if nothing is scheduled.
			if err != nil && err != NothingScheduledError {
				return err
			}
			return nil
		}

		return traverser.NewSyncEnumerator(sourceTraverser, destinationTraverser, indexer, filters, comparator, finalize), nil
	default:
		indexer.IsDestinationCaseInsensitive = IsDestinationCaseInsensitive(cca.fromTo)
		// in all other cases (download and S2S), the destination is scanned/indexed first
		// then the source is scanned and filtered based on what the destination contains
		comparator = newSyncSourceComparator(indexer, transferScheduler.scheduleCopyTransfer, cca.compareHash, cca.preserveInfo, cca.mirrorMode).processIfNecessary

		finalize = func() error {
			// remove the extra files at the destination that were not present at the source
			// we can only know what needs to be deleted when we have FINISHED traversing the remote source
			// since only then can we know which local files definitely don't exist remotely
			var deleteScheduler traverser.ObjectProcessor
			switch cca.fromTo.To() {
			case common.ELocation.Blob(), common.ELocation.File(), common.ELocation.FileNFS(), common.ELocation.BlobFS():
				deleter, err := newSyncDeleteProcessor(cca, fpo, copyJobTemplate.DstServiceClient)
				if err != nil {
					return err
				}
				deleteScheduler = traverser.NewFpoAwareProcessor(fpo, deleter.removeImmediately)
			default:
				deleteScheduler = traverser.NewFpoAwareProcessor(fpo, newSyncLocalDeleteProcessor(cca, fpo).removeImmediately)
			}

			err = indexer.Traverse(deleteScheduler, nil)
			if err != nil {
				return err
			}

			// let the deletions happen first
			// otherwise if the final part is executed too quickly, we might quit before deletions could finish
			_, err := transferScheduler.dispatchFinalPart()
			// sync cleanly exits if nothing is scheduled.
			if err != nil && err != NothingScheduledError {
				return err
			}
			return nil
		}

		return traverser.NewSyncEnumerator(destinationTraverser, sourceTraverser, indexer, filters, comparator, finalize), nil
	}
}

func IsDestinationCaseInsensitive(fromTo common.FromTo) bool {
	return fromTo.IsDownload() && runtime.GOOS == "windows"
}
