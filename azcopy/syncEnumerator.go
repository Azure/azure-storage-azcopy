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

package azcopy

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// -------------------------------------- Implemented Enumerators -------------------------------------- \\

func (s *syncer) initEnumerator(ctx context.Context, logLevel common.LogLevel, mgr *JobLifecycleManager) (se *traverser.SyncEnumerator, err error) {

	// TODO: enable symlink support in a future release after evaluating the implications
	// TODO: Consider passing an errorChannel so that enumeration errors during sync can be conveyed to the caller.
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	dest := s.opts.fromTo.To()
	sourceTraverser, err := traverser.InitResourceTraverser(s.opts.source, s.opts.fromTo.From(), ctx, traverser.InitResourceTraverserOptions{
		DestResourceType: &dest,

		Client:         s.srp.srcServiceClient,
		CredentialType: s.srp.srcCredType,
		IncrementEnumeration: func(entityType common.EntityType, symlinkOption common.SymlinkHandlingType, hardlinkHandling common.HardlinkHandlingType) {
			if s.opts.fromTo.IsNFS() {
				s.spt.incSourceEnumeration(entityType, symlinkOption, hardlinkHandling)
			} else {
				// For non NFS targets, we only track files scanned
				if entityType == common.EEntityType.File() {
					s.spt.incSourceEnumeration(entityType, symlinkOption, hardlinkHandling)
				}
			}
		},

		CpkOptions: s.opts.cpkOptions,

		SyncHashType:        s.opts.compareHash,
		PreservePermissions: s.opts.preservePermissions,
		TrailingDotOption:   s.opts.trailingDot,

		Recursive:               s.opts.recursive,
		GetPropertiesInFrontend: true,
		IncludeDirectoryStubs:   s.opts.includeDirectoryStubs,
		PreserveBlobTags:        s.opts.s2SPreserveBlobTags,
		HardlinkHandling:        s.opts.hardlinks,
		SymlinkHandling:         s.opts.symlinks,
		FromTo:                  s.opts.fromTo,
		StripTopDir:             !s.opts.includeRoot,
		IncludeRoot:             s.opts.includeRoot,
	})

	if err != nil {
		return nil, err
	}
	// TODO: enable symlink support in a future release after evaluating the implications
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	destinationTraverser, err := traverser.InitResourceTraverser(s.opts.destination, s.opts.fromTo.To(), ctx, traverser.InitResourceTraverserOptions{
		Client:               s.srp.dstServiceClient,
		CredentialType:       s.srp.dstCredType,
		IncrementEnumeration: s.spt.incDestEnumeration,

		CpkOptions: s.opts.cpkOptions,

		SyncHashType:        s.opts.compareHash,
		PreservePermissions: s.opts.preservePermissions,
		TrailingDotOption:   s.opts.trailingDot,

		Recursive:               s.opts.recursive,
		GetPropertiesInFrontend: true,
		IncludeDirectoryStubs:   s.opts.includeDirectoryStubs,
		PreserveBlobTags:        s.opts.s2SPreserveBlobTags,
		HardlinkHandling:        common.EHardlinkHandlingType.Follow(),
		SymlinkHandling:         s.opts.symlinks,
		FromTo:                  s.opts.fromTo,
		StripTopDir:             !s.opts.includeRoot,
		IncludeRoot:             s.opts.includeRoot,
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

	if s.opts.fromTo.To() == common.ELocation.Blob() || s.opts.fromTo.To() == common.ELocation.BlobFS() {

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
				common.GetLifecycleMgr().Warn(fmt.Sprintf("Failed to create the missing destination container: %v", err))
			}
			// At this point, we'll let the destination be written to with the original resource type.
		}
	} else if err != nil && fileerror.HasCode(err, fileerror.ShareNotFound) {
		return nil, fmt.Errorf("%s Destination file share: %s", DstShareDoesNotExists, s.opts.destination.Value)
	} else if err == nil && sourceIsDir != destIsDir {
		// If the destination exists, and isn't blob though, we have to match resource types.
		return nil, resourceMismatchError
	}

	// construct filters
	filters := traverser.BuildFilters(s.opts.fromTo, s.opts.source, s.opts.recursive, s.opts.filterOptions)

	// decide our folder transfer strategy
	var fpo common.FolderPropertyOption
	var folderMessage string
	// sync always acts like stripTopDir=true, but if we intend to persist the root, we must tell NewFolderPropertyOption stripTopDir=false.
	fpo, folderMessage = NewFolderPropertyOption(s.opts.fromTo, s.opts.recursive, !s.opts.includeRoot, filters, s.opts.preserveInfo,
		s.opts.preservePermissions.IsTruthy(), s.opts.preservePosixProperties, strings.EqualFold(s.opts.destination.Value, common.Dev_Null),
		s.opts.includeDirectoryStubs)
	if !s.opts.dryrun {
		common.GetLifecycleMgr().Info(folderMessage)
	}
	common.LogToJobLogWithPrefix(folderMessage, common.LogInfo)

	copyJobTemplate := &common.CopyJobPartOrderRequest{
		JobID:               s.spt.jobID,
		CommandString:       s.opts.commandString,
		FromTo:              s.opts.fromTo,
		Fpo:                 fpo,
		SymlinkHandlingType: s.opts.symlinks,
		SourceRoot:          s.opts.source.CloneWithConsolidatedSeparators(),
		DestinationRoot:     s.opts.destination.CloneWithConsolidatedSeparators(),

		// flags
		BlobAttributes: common.BlobTransferAttributes{
			PreserveLastModifiedTime:         s.opts.preserveInfo, // true by default for sync so that future syncs have this information available
			PutMd5:                           s.opts.putMd5,
			MD5ValidationOption:              s.opts.checkMd5,
			BlockSizeInBytes:                 s.opts.blockSize,
			PutBlobSizeInBytes:               s.opts.putBlobSize,
			DeleteDestinationFileIfNecessary: s.opts.deleteDestinationFileIfNecessary,
		},
		ForceWrite:                     common.EOverwriteOption.True(), // once we decide to transfer for a sync operation, we overwrite the destination regardless
		ForceIfReadOnly:                s.opts.forceIfReadOnly,
		LogLevel:                       logLevel,
		PreservePermissions:            s.opts.preservePermissions,
		PreserveInfo:                   s.opts.preserveInfo,
		PreservePOSIXProperties:        s.opts.preservePosixProperties,
		S2SSourceChangeValidation:      true,
		DestLengthValidation:           true,
		S2SGetPropertiesInBackend:      true,
		S2SInvalidMetadataHandleOption: common.EInvalidMetadataHandleOption.RenameIfInvalid(),
		CpkOptions:                     s.opts.cpkOptions,
		S2SPreserveBlobTags:            s.opts.s2SPreserveBlobTags,

		S2SSourceCredentialType: s.srp.srcCredType,
		FileAttributes: common.FileTransferAttributes{
			TrailingDot: s.opts.trailingDot,
		},
		JobErrorHandler:  mgr,
		SrcServiceClient: s.srp.srcServiceClient,
		DstServiceClient: s.srp.dstServiceClient,
	}

	// transferScheduler is responsible for batching up transfers and sending them to the job service
	transferScheduler := s.newSyncTransferProcessor(NumOfFilesPerDispatchJobPart, copyJobTemplate)

	// indexer keeps track of the destination (source in case of upload) files and folders
	indexer := traverser.NewObjectIndexer()
	// deleter is responsible for deleting files at the destination that no longer exist at the source
	var deleter ObjectDeleter
	if s.opts.dryrun {
		deleter = s.opts.dryrunDeleteHandler
	} else if s.opts.fromTo.To().IsAzure() {
		rawURL, err := s.opts.destination.FullURL()
		if err != nil {
			return nil, fmt.Errorf("invalid destination URL: %s. Error: %s", s.opts.destination.Value, err.Error())
		}
		remoteDeleter, err := NewRemoteResourceDeleter(ctx, s.srp.dstServiceClient, rawURL, fpo, s.opts.forceIfReadOnly)
		if err != nil {
			return nil, fmt.Errorf("unable to instantiate destination cleaner due to: %s", err.Error())
		}
		deleter = remoteDeleter.Delete
	} else {
		localDeleter := NewLocalFileDeleter(fpo)
		deleter = localDeleter.Delete
	}
	deleteProcessor := newInteractiveDeleteProcessor(deleter, s.opts.deleteDestination, s.opts.fromTo.To(), s.opts.destination, s.spt.incrementDeletionCount)
	deleteScheduler := traverser.NewFpoAwareProcessor(fpo, deleteProcessor.removeImmediately)

	var comparator traverser.ObjectProcessor
	var finalize func() error

	switch s.opts.fromTo {
	case common.EFromTo.LocalBlob(), common.EFromTo.LocalFile(), common.EFromTo.LocalFileNFS():
		// Upload implies transferring from a local disk to a remote resource.
		// In this scenario, the local disk (source) is scanned/indexed first because it is assumed that local file systems will be faster to enumerate than remote resources
		// Then the destination is scanned and filtered based on what the destination contains

		// when uploading, we can delete remote objects immediately, because as we traverse the remote location
		// we ALREADY have available a complete map of everything that exists locally
		// so as soon as we see a remote destination object we can know whether it exists in the local source

		comparator = newSyncDestinationComparator(indexer, transferScheduler.ScheduleCopyTransfer, deleteScheduler, s.opts.compareHash, s.opts.preserveInfo, s.opts.mirrorMode).processIfNecessary
		finalize = func() error {
			// schedule every local file that doesn't exist at the destination
			err = indexer.Traverse(transferScheduler.ScheduleCopyTransfer, filters)
			if err != nil {
				return err
			}

			_, err := transferScheduler.DispatchFinalPart()
			// sync cleanly exits if nothing is scheduled.
			if err != nil && err != NothingScheduledError {
				return err
			}
			return nil
		}

		return traverser.NewSyncEnumerator(sourceTraverser, destinationTraverser, indexer, filters, comparator, finalize), nil
	default:
		indexer.IsDestinationCaseInsensitive = IsDestinationCaseInsensitive(s.opts.fromTo)
		// in all other cases (download and S2S), the destination is scanned/indexed first
		// then the source is scanned and filtered based on what the destination contains
		comparator = newSyncSourceComparator(indexer, transferScheduler.ScheduleCopyTransfer, s.opts.compareHash, s.opts.preserveInfo, s.opts.mirrorMode).processIfNecessary

		finalize = func() error {
			err = indexer.Traverse(deleteScheduler, nil)
			if err != nil {
				return err
			}

			// let the deletions happen first
			// otherwise if the final part is executed too quickly, we might quit before deletions could finish
			_, err := transferScheduler.DispatchFinalPart()
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
