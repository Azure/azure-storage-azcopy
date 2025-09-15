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
	// TODO: enable symlink support in a future release after evaluating the implications
	// TODO: Consider passing an errorChannel so that enumeration errors during sync can be conveyed to the caller.
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	dest := cca.s.FromTo.To()
	sourceTraverser, err := traverser.InitResourceTraverser(cca.s.Source, cca.s.FromTo.From(), ctx, traverser.InitResourceTraverserOptions{
		DestResourceType: &dest,

		Client:         cca.srcServiceClient,
		CredentialType: cca.srcCredType,
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

		CpkOptions: cca.s.CpkOptions,

		SyncHashType:        cca.s.CompareHash,
		PreservePermissions: cca.s.PreservePermissions,
		TrailingDotOption:   cca.s.TrailingDot,

		Recursive:               cca.s.Recursive,
		GetPropertiesInFrontend: true,
		IncludeDirectoryStubs:   cca.s.IncludeDirectoryStubs,
		PreserveBlobTags:        cca.s.S2SPreserveBlobTags,
		HardlinkHandling:        cca.s.Hardlinks,
	})

	if err != nil {
		return nil, err
	}

	// TODO: enable symlink support in a future release after evaluating the implications
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	destinationTraverser, err := traverser.InitResourceTraverser(cca.s.Destination, cca.s.FromTo.To(), ctx, traverser.InitResourceTraverserOptions{
		Client:         cca.dstServiceClient,
		CredentialType: cca.dstCredType,
		IncrementEnumeration: func(entityType common.EntityType) {
			if entityType == common.EEntityType.File() {
				atomic.AddUint64(&cca.atomicDestinationFilesScanned, 1)
			}
		},

		CpkOptions: cca.s.CpkOptions,

		SyncHashType:        cca.s.CompareHash,
		PreservePermissions: cca.s.PreservePermissions,
		TrailingDotOption:   cca.s.TrailingDot,

		Recursive:               cca.s.Recursive,
		GetPropertiesInFrontend: true,
		IncludeDirectoryStubs:   cca.s.IncludeDirectoryStubs,
		PreserveBlobTags:        cca.s.S2SPreserveBlobTags,
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

	if cca.s.FromTo.To() == common.ELocation.Blob() || cca.s.FromTo.To() == common.ELocation.BlobFS() {

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
		return nil, fmt.Errorf("%s Destination file share: %s", DstShareDoesNotExists, cca.s.Destination.Value)
	} else if err == nil && sourceIsDir != destIsDir {
		// If the destination exists, and isn't blob though, we have to match resource types.
		return nil, resourceMismatchError
	}

	// set up the filters in the right order
	// Note: includeFilters and includeAttrFilters are ANDed
	// They must both pass to get the file included
	// Same rule applies to excludeFilters and excludeAttrFilters
	filters := traverser.BuildIncludeFilters(cca.s.FilterOptions.IncludePatterns)
	if cca.s.FromTo.From() == common.ELocation.Local() {
		includeAttrFilters := traverser.BuildAttrFilters(cca.s.FilterOptions.IncludeAttributes, cca.s.Source.ValueLocal(), true)
		filters = append(filters, includeAttrFilters...)
	}

	filters = append(filters, traverser.BuildExcludeFilters(cca.s.FilterOptions.ExcludePatterns, false)...)
	filters = append(filters, traverser.BuildExcludeFilters(cca.s.FilterOptions.ExcludePaths, true)...)
	if cca.s.FromTo.From() == common.ELocation.Local() {
		excludeAttrFilters := traverser.BuildAttrFilters(cca.s.FilterOptions.ExcludeAttributes, cca.s.Source.ValueLocal(), false)
		filters = append(filters, excludeAttrFilters...)
	}

	// includeRegex
	filters = append(filters, traverser.BuildRegexFilters(cca.s.FilterOptions.IncludeRegex, true)...)
	filters = append(filters, traverser.BuildRegexFilters(cca.s.FilterOptions.ExcludeRegex, false)...)

	// after making all filters, log any search prefix computed from them
	if prefixFilter := traverser.FilterSet(filters).GetEnumerationPreFilter(cca.s.Recursive); prefixFilter != "" {
		common.LogToJobLogWithPrefix("Search prefix, which may be used to optimize scanning, is: "+prefixFilter, common.LogInfo) // "May be used" because we don't know here which enumerators will use it
	}

	// decide our folder transfer strategy
	// sync always acts like stripTopDir=true, but if we intend to persist the root, we must tell NewFolderPropertyOption stripTopDir=false.
	fpo, folderMessage := NewFolderPropertyOption(cca.s.FromTo, cca.s.Recursive, !cca.s.IncludeRoot, filters, cca.s.PreserveInfo, cca.s.PreservePermissions.IsTruthy(), false, strings.EqualFold(cca.s.Destination.Value, common.Dev_Null), cca.s.IncludeDirectoryStubs)
	if !cca.s.Dryrun {
		glcm.Info(folderMessage)
	}
	common.LogToJobLogWithPrefix(folderMessage, common.LogInfo)

	copyJobTemplate := &common.CopyJobPartOrderRequest{
		JobID:           cca.jobID,
		CommandString:   cca.commandString,
		FromTo:          cca.s.FromTo,
		Fpo:             fpo,
		SourceRoot:      cca.s.Source.CloneWithConsolidatedSeparators(),
		DestinationRoot: cca.s.Destination.CloneWithConsolidatedSeparators(),

		// flags
		BlobAttributes: common.BlobTransferAttributes{
			PreserveLastModifiedTime:         cca.s.PreserveInfo, // true by default for sync so that future syncs have this information available
			PutMd5:                           cca.s.PutMd5,
			MD5ValidationOption:              cca.s.CheckMd5,
			BlockSizeInBytes:                 cca.s.BlockSize,
			PutBlobSizeInBytes:               cca.s.PutBlobSize,
			DeleteDestinationFileIfNecessary: cca.s.DeleteDestinationFileIfNecessary,
		},
		ForceWrite:                     common.EOverwriteOption.True(), // once we decide to transfer for a sync operation, we overwrite the destination regardless
		ForceIfReadOnly:                cca.s.ForceIfReadOnly,
		LogLevel:                       Client.GetLogLevel(),
		PreservePermissions:            cca.s.PreservePermissions,
		PreserveInfo:                   cca.s.PreserveInfo,
		PreservePOSIXProperties:        cca.s.PreservePosixProperties,
		S2SSourceChangeValidation:      true,
		DestLengthValidation:           true,
		S2SGetPropertiesInBackend:      true,
		S2SInvalidMetadataHandleOption: common.EInvalidMetadataHandleOption.RenameIfInvalid(),
		CpkOptions:                     cca.s.CpkOptions,
		S2SPreserveBlobTags:            cca.s.S2SPreserveBlobTags,

		S2SSourceCredentialType: cca.srcCredType,
		FileAttributes: common.FileTransferAttributes{
			TrailingDot: cca.s.TrailingDot,
		},
		JobErrorHandler:  glcm,
		SrcServiceClient: cca.srcServiceClient,
		DstServiceClient: cca.dstServiceClient,
	}

	// Check protocol compatibility for File Shares
	if err := validateProtocolCompatibility(ctx, cca.s.FromTo, cca.s.Source, cca.s.Destination, copyJobTemplate.SrcServiceClient, copyJobTemplate.DstServiceClient); err != nil {
		return nil, err
	}

	// transferScheduler is responsible for batching up transfers and sending them to the job service
	transferScheduler := newSyncTransferProcessor(cca, NumOfFilesPerDispatchJobPart, fpo, copyJobTemplate)

	// indexer keeps track of the destination (source in case of upload) files and folders
	indexer := traverser.NewObjectIndexer()

	// deleter is responsible for deleting files at the destination that no longer exist at the source
	var deleter *interactiveDeleteProcessor
	if cca.s.Dryrun {
		deleter = newSyncDryRunDeleteProcessor(cca, common.Iff(cca.s.FromTo.To() == common.ELocation.Local(), LocalFileObjectType, cca.s.FromTo.To().String()))
	} else if cca.s.FromTo.To().IsAzure() {
		deleter, err = newSyncDeleteProcessor(cca, fpo, copyJobTemplate.DstServiceClient)
		if err != nil {
			return nil, fmt.Errorf("unable to instantiate destination cleaner due to: %s", err.Error())
		}
	} else {
		deleter = newSyncLocalDeleteProcessor(cca, fpo)
	}
	deleteScheduler := traverser.NewFpoAwareProcessor(fpo, deleter.removeImmediately)

	var comparator traverser.ObjectProcessor
	var finalize func() error

	if cca.s.FromTo.IsUpload() {
		// Upload implies transferring from a local disk to a remote resource.
		// In this scenario, the local disk (source) is scanned/indexed first because it is assumed that local file systems will be faster to enumerate than remote resources
		// Then the destination is scanned and filtered based on what the destination contains

		// when uploading, we can delete remote objects immediately, because as we Traverse the remote location
		// we ALREADY have available a complete map of everything that exists locally
		// so as soon as we see a remote destination object we can know whether it exists in the local source

		comparator = newSyncDestinationComparator(indexer, transferScheduler.scheduleCopyTransfer, deleteScheduler, cca.s.CompareHash, cca.s.PreserveInfo, cca.s.MirrorMode).processIfNecessary
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
	} else {
		indexer.IsDestinationCaseInsensitive = IsDestinationCaseInsensitive(cca.s.FromTo)
		// in all other cases (download and S2S), the destination is scanned/indexed first
		// then the source is scanned and filtered based on what the destination contains
		comparator = newSyncSourceComparator(indexer, transferScheduler.scheduleCopyTransfer, cca.s.CompareHash, cca.s.PreserveInfo, cca.s.MirrorMode).processIfNecessary

		finalize = func() error {
			// remove the extra files at the destination that were not present at the source
			// we can only know what needs to be deleted when we have FINISHED traversing the remote source
			// since only then can we know which local files definitely don't exist remotely

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
