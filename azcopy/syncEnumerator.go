package azcopy

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

func (s *syncer) initEnumerator(ctx context.Context, logLevel common.LogLevel, errorHandler common.JobErrorHandler) (enumerator *traverser.SyncEnumerator, err error) {

	sourceTraverser, err := traverser.InitResourceTraverser(s.opts.source, s.opts.FromTo.From(), ctx,
		traverser.InitResourceTraverserOptions{
			Client:                  s.opts.srcServiceClient,
			CredentialType:          s.opts.srcCredType,
			DestResourceType:        to.Ptr(s.opts.FromTo.To()),
			CpkOptions:              s.opts.cpkOptions,
			SyncHashType:            s.opts.CompareHash,
			PreservePermissions:     s.opts.preservePermissions,
			TrailingDotOption:       s.opts.TrailingDot,
			Recursive:               *s.opts.Recursive,
			GetPropertiesInFrontend: true, // Sync always gets properties in frontend so that we can compare them
			IncludeDirectoryStubs:   s.opts.IncludeDirectoryStubs,
			PreserveBlobTags:        s.opts.S2SPreserveBlobTags,
			HardlinkHandling:        s.opts.Hardlinks,
			// traverser tracking options
			IncrementEnumeration: func(entityType common.EntityType) {
				if entityType == common.EEntityType.File() {
					atomic.AddUint64(&s.atomicSourceFilesScanned, 1)
				}
				if common.IsNFSCopy() {
					if entityType == common.EEntityType.Other() {
						atomic.AddUint32(&s.atomicSkippedSpecialFileCount, 1)
					} else if entityType == common.EEntityType.Symlink() {
						atomic.AddUint32(&s.atomicSkippedSymlinkCount, 1)
					}
				}
			},
		})
	if err != nil {
		return nil, err
	}

	destinationTraverser, err := traverser.InitResourceTraverser(s.opts.destination, s.opts.FromTo.To(), ctx,
		traverser.InitResourceTraverserOptions{
			Client:                  s.opts.destServiceClient,
			CredentialType:          s.opts.destCredType,
			CpkOptions:              s.opts.cpkOptions,
			SyncHashType:            s.opts.CompareHash,
			PreservePermissions:     s.opts.preservePermissions,
			TrailingDotOption:       s.opts.TrailingDot,
			Recursive:               *s.opts.Recursive,
			GetPropertiesInFrontend: true, // Sync always gets properties in frontend so that we can compare them
			IncludeDirectoryStubs:   s.opts.IncludeDirectoryStubs,
			PreserveBlobTags:        s.opts.S2SPreserveBlobTags,
			HardlinkHandling:        common.EHardlinkHandlingType.Follow(),
			// traverser tracking options
			IncrementEnumeration: func(entityType common.EntityType) {
				if entityType == common.EEntityType.File() {
					atomic.AddUint64(&s.atomicDestinationFilesScanned, 1)
				}
			},
		})
	if err != nil {
		return nil, err
	}

	// Verify that the traversers are targeting the same type of resource
	err = verifyTraverserCompatibility(ctx, sourceTraverser, destinationTraverser, s.opts.FromTo, s.opts.destination.Value)
	if err != nil {
		return nil, err
	}

	copyJobTemplate := &common.CopyJobPartOrderRequest{
		JobID:           s.jobID,
		CommandString:   s.opts.commandString,
		FromTo:          s.opts.FromTo,
		Fpo:             s.opts.folderPropertyOption,
		SourceRoot:      s.opts.source.CloneWithConsolidatedSeparators(),
		DestinationRoot: s.opts.destination.CloneWithConsolidatedSeparators(),

		BlobAttributes: common.BlobTransferAttributes{
			PreserveLastModifiedTime:         *s.opts.PreserveInfo, // true by default for sync so that future syncs have this information available
			PutMd5:                           s.opts.PutMd5,
			MD5ValidationOption:              s.opts.CheckMd5,
			BlockSizeInBytes:                 s.opts.blockSize,
			PutBlobSizeInBytes:               s.opts.putBlobSize,
			DeleteDestinationFileIfNecessary: s.opts.DeleteDestinationFileIfNecessary,
		},
		ForceWrite:                     common.EOverwriteOption.True(), // once we decide to transfer for a sync operation, we overwrite the destination regardless
		ForceIfReadOnly:                s.opts.ForceIfReadOnly,
		LogLevel:                       logLevel,
		PreservePermissions:            s.opts.preservePermissions,
		PreserveInfo:                   *s.opts.PreserveInfo,
		PreservePOSIXProperties:        s.opts.PreservePosixProperties,
		S2SSourceChangeValidation:      true,
		DestLengthValidation:           true,
		S2SGetPropertiesInBackend:      true,
		S2SInvalidMetadataHandleOption: common.EInvalidMetadataHandleOption.RenameIfInvalid(),
		CpkOptions:                     s.opts.cpkOptions,
		S2SPreserveBlobTags:            s.opts.S2SPreserveBlobTags,

		S2SSourceCredentialType: s.opts.srcCredType,
		FileAttributes: common.FileTransferAttributes{
			TrailingDot: s.opts.TrailingDot,
		},
		JobErrorHandler:  errorHandler,
		SrcServiceClient: s.opts.srcServiceClient,
		DstServiceClient: s.opts.destServiceClient,
	}

	// Download and S2S flow for sync
	// 1. traverse the destination and index it
	// 2. traverse the source and compare it with the index, scheduling transfers as necessary
	// 3.

	// indexer keeps track of the destination (source in case of upload) files and folders
	indexer := traverser.NewObjectIndexer()

	// dispatcher is responsible for batching up transfers and sending them to the job service
	dispatcher := newTransferDispatcher(NumOfFilesPerDispatchJobPart, copyJobTemplate, s.opts.source, s.opts.destination, s, *s.opts.S2SPreserveAccessTier)

	var deleter *interactiveDeleter
	if s.opts.FromTo.To().IsAzure() {
		deleter, err = s.newRemoteDeleter()
		if err != nil {
			return nil, fmt.Errorf("unable to instantiate destination cleaner due to: %s", err.Error())
		}
	} else {
		deleter = s.newLocalDeleter()
	}
	deleteScheduler := traverser.NewFpoAwareProcessor(s.opts.folderPropertyOption, deleter.removeImmediately)

	if s.opts.FromTo.IsUpload() {
		// In this scenario, the local disk (source) is scanned/indexed first because it is assumed that local file systems will be faster to enumerate than remote resources
		// Then the destination is scanned and filtered based on what the destination contains

		// 1. Traverse the source and build an index of its paths.
		// 2. Traverse the destination and compare each path against the index:
		//    - If path exists in both → schedule transfer (if needed), then remove from index.
		//    - If path exists only in destination → schedule deletion.
		// 3. For any paths still left in the index (source-only) → schedule transfer.
		comparator := newSyncDestinationComparator(indexer, dispatcher.scheduleTransfer, deleteScheduler, s.opts.CompareHash, s.opts.PreserveInfo, s.opts.MirrorMode).processIfNecessary
		finalize := func() error {
			// schedule every local file that doesn't exist at the destination
			err = indexer.Traverse(dispatcher.scheduleTransfer, s.opts.filters)
			if err != nil {
				return err
			}
			var jobInitiated bool
			jobInitiated, err = dispatcher.dispatchFinalPart()
			// sync cleanly exits if nothing is scheduled.
			if err != nil && err != NothingScheduledError {
				return err
			}
			// Set

			return nil
		}
		return traverser.NewSyncEnumerator(sourceTraverser, destinationTraverser, indexer, s.opts.filters, comparator, finalize), nil
	} else {
		// In all other scenarios (download and S2S), the destination is scanned/indexed first
		// Then the source is scanned and filtered based on what the destination contains
		indexer.IsDestinationCaseInsensitive = s.opts.FromTo.IsDownload() && runtime.GOOS == "windows"

		finalize := func() error {
			// remove the extra files at the destination that were not present at the source
			// we can only know what needs to be deleted when we have FINISHED traversing the remote source
			// since only then can we know which local files definitely don't exist remotely

		}
	}
}

func verifyTraverserCompatibility(ctx context.Context, src, dst traverser.ResourceTraverser, fromTo common.FromTo, dstURL string) error {
	srcIsDir, _ := src.IsDirectory(true)
	dstIsDir, err := dst.IsDirectory(true)

	var resourceMismatchError = errors.New("trying to sync between different resource types (either file <-> directory or directory <-> file) which is not allowed." +
		"sync must happen between source and destination of the same type, e.g. either file <-> file or directory <-> directory." +
		"To make sure target is handled as a directory, add a trailing '/' to the target.")

	if fromTo.To() == common.ELocation.Blob() || fromTo.To() == common.ELocation.BlobFS() {

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
			bt := dst.(*traverser.BlobTraverser)
			sc := bt.ServiceClient                                                   // it being a blob traverser is a relatively safe assumption, because
			bUrlParts, _ := blob.ParseURL(bt.RawURL)                                 // it should totally have succeeded by now anyway
			_, err = sc.NewContainerClient(bUrlParts.ContainerName).Create(ctx, nil) // If it doesn't work out, this will surely bubble up later anyway. It won't be long.
			if err != nil {
				common.GetLifecycleMgr().Warn(fmt.Sprintf("Failed to create the missing destination container: %v", err))
			}
			// At this point, we'll let the destination be written to with the original resource type.
		}
	} else if err != nil && fileerror.HasCode(err, fileerror.ShareNotFound) {
		return fmt.Errorf("%s Destination file share: %s", DstShareDoesNotExists, dstURL)
	} else if err == nil && srcIsDir != dstIsDir {
		// If the destination exists, and isn't blob though, we have to match resource types.
		return resourceMismatchError
	}
	return nil
}
