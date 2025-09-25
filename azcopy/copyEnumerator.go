package azcopy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

func (t *transferExecutor) initCopyEnumerator(ctx context.Context, logLevel common.LogLevel, mgr *JobLifecycleManager) (enumerator *traverser.CopyEnumerator, err error) {

	// Initialize the source traverser
	dest := t.opts.fromTo.To()
	var sourceTraverser traverser.ResourceTraverser
	sourceTraverser, err = traverser.InitResourceTraverser(t.opts.source, t.opts.fromTo.From(), ctx, traverser.InitResourceTraverserOptions{
		DestResourceType: &dest,

		Client:         t.trp.srcServiceClient,
		CredentialType: t.trp.srcCredType,

		ListOfFiles:      t.opts.listOfFiles,
		ListOfVersionIDs: t.opts.listOfVersionIds,

		CpkOptions: t.opts.cpkOptions,

		PreservePermissions: t.opts.preservePermissions,
		SymlinkHandling:     t.opts.symlinks,
		SyncHashType:        common.ESyncHashType.None(),
		TrailingDotOption:   t.opts.trailingDot,

		Recursive:               t.opts.recursive,
		GetPropertiesInFrontend: t.opts.getPropertiesInFrontend,
		IncludeDirectoryStubs:   t.opts.includeDirectoryStubs,
		PreserveBlobTags:        t.opts.s2sPreserveBlobTags,
		StripTopDir:             t.opts.stripTopDir,

		ExcludeContainers: t.opts.excludeContainers,
		IncrementEnumeration: func(entityType common.EntityType) {
			if common.IsNFSCopy() {
				if entityType == common.EEntityType.Other() {
					atomic.AddUint32(&cca.atomicSkippedSpecialFileCount, 1)
				} else if entityType == common.EEntityType.Symlink() {
					atomic.AddUint32(&cca.atomicSkippedSymlinkCount, 1)
				}
			}
		},
	})
	if err != nil {
		return nil, err
	}

	// Ensure we're only copying a directory under valid conditions
	isSourceDir, err := sourceTraverser.IsDirectory(true)
	if isSourceDir &&
		!t.opts.recursive && // Copies the folder & everything under it
		!t.opts.stripTopDir { // Copies only everything under it
		// todo: dir only transfer, also todo: support syncing the root folder's acls on sync.
		return nil, errors.New("cannot use directory as source without --recursive or a trailing wildcard (/*)")
	}
	// check if error is file not found - if it is then we need to make sure it's not a wild card
	if err != nil && strings.EqualFold(err.Error(), common.FILE_NOT_FOUND) && !t.opts.stripTopDir {
		return nil, err
	}

	// Check if the destination is a directory to correctly decide where our files land
	isDestDir := isResourceDirectory(ctx, t.opts.destination, t.opts.fromTo.To(), t.trp.dstServiceClient, false)
	if t.opts.listOfFiles != nil &&
		(t.opts.fromTo != common.EFromTo.BlobLocal() || isSourceDir || !isDestDir) {
		return nil, errors.New("either source is not a blob or destination is not a local folder")
	}

	// Resolve containers
	// TODO : I think we can add a more lightweight resolver for Local/Azure scenarios
	// Create a Remote resource resolver
	// Giving it nothing to work with as new names will be added as we Traverse.
	var containerResolver BucketToContainerNameResolver
	containerResolver = NewS3BucketNameToAzureResourcesResolver(nil)
	if t.opts.fromTo == common.EFromTo.GCPBlob() {
		containerResolver = NewGCPBucketNameToAzureResourcesResolver(nil)
	}
	existingContainers := make(map[string]bool)
	var logDstContainerCreateFailureOnce sync.Once
	seenFailedContainers := make(map[string]bool) // Create map of already failed container conversions so we don't log a million items just for one container.

	dstContainerName := ""
	// Extract the existing destination container name
	if t.opts.fromTo.To().IsRemote() {
		dstContainerName, err = GetContainerName(t.opts.destination.Value, t.opts.fromTo.To())

		if err != nil {
			return nil, err
		}

		// only create the destination container in S2S scenarios
		if t.opts.fromTo.From().IsRemote() && dstContainerName != "" { // if the destination has a explicit container name
			// Attempt to create the container. If we fail, fail silently.
			err = createContainer(ctx, dstContainerName, t.opts.destination, t.trp.dstServiceClient, t.opts.fromTo, existingContainers)
			// For file share,if the share does not exist, azcopy will fail, prompting the customer to create
			// the share manually with the required quota and settings.
			if fileerror.HasCode(err, fileerror.ShareNotFound) {
				return nil, fmt.Errorf("the destination file share %s does not exist; please create it manually with the required quota and settings before running the copy —refer to https://learn.microsoft.com/en-us/azure/storage/files/storage-how-to-create-file-share?tabs=azure-portal for SMB or https://learn.microsoft.com/en-us/azure/storage/files/storage-files-quick-create-use-linux for NFS.", dstContainerName)
			}

			// check against seenFailedContainers so we don't spam the job log with initialization failed errors
			if _, ok := seenFailedContainers[dstContainerName]; err != nil && jobsAdmin.JobsAdmin != nil && !ok {
				common.LogToJobLogWithPrefix(fmt.Sprintf("Failed attempt to create destination container (this is not blocking): %v", err), common.LogDebug)
				seenFailedContainers[dstContainerName] = true
			}
		} else if t.opts.fromTo.From().IsRemote() { // if the destination has implicit container names
			if acctTraverser, ok := sourceTraverser.(traverser.AccountTraverser); ok && t.opts.dstLevel == ELocationLevel.Service() {
				containers, err := acctTraverser.ListContainers()

				if err != nil {
					return nil, fmt.Errorf("failed to list containers: %w", err)
				}

				// Resolve all container names up front.
				// If we were to resolve on-the-fly, then name order would affect the results inconsistently.
				if t.opts.fromTo == common.EFromTo.S3Blob() {
					containerResolver = NewS3BucketNameToAzureResourcesResolver(containers)
				} else if t.opts.fromTo == common.EFromTo.GCPBlob() {
					containerResolver = NewGCPBucketNameToAzureResourcesResolver(containers)
				}

				for _, v := range containers {
					bucketName, err := containerResolver.ResolveName(v)

					if err != nil {
						// Silently ignore the failure; it'll get logged later.
						continue
					}

					err = createContainer(ctx, bucketName, t.opts.destination, t.trp.dstServiceClient, t.opts.fromTo, existingContainers)
					// For file share,if the share does not exist, azcopy will fail, prompting the customer to create
					// the share manually with the required quota and settings.
					if fileerror.HasCode(err, fileerror.ShareNotFound) {
						return nil, fmt.Errorf("%s Destination file share: %s", DstShareDoesNotExists, dstContainerName)
					}

					// if JobsAdmin is nil, we're probably in testing mode.
					// As a result, container creation failures are expected as we don't give the SAS tokens adequate permissions.
					// check against seenFailedContainers so we don't spam the job log with initialization failed errors
					if _, ok := seenFailedContainers[bucketName]; err != nil && jobsAdmin.JobsAdmin != nil && !ok {
						logDstContainerCreateFailureOnce.Do(func() {
							common.GetLifecycleMgr().Warn("Failed to create one or more destination container(s). Your transfers may still succeed if the container already exists.")
						})
						common.LogToJobLogWithPrefix(fmt.Sprintf("failed to initialize destination container %s; the transfer will continue (but be wary it may fail).", bucketName), common.LogWarning)
						common.LogToJobLogWithPrefix(fmt.Sprintf("Error %v", err), common.LogDebug)
						seenFailedContainers[bucketName] = true
					}
				}
			} else {
				cName, err := GetContainerName(t.opts.source.Value, t.opts.fromTo.From())

				if err != nil || cName == "" {
					// this will probably never be reached
					return nil, fmt.Errorf("failed to get container name from source (is it formatted correctly?)")
				}
				resName, err := containerResolver.ResolveName(cName)

				if err == nil {
					err = createContainer(ctx, resName, t.opts.destination, t.trp.dstServiceClient, t.opts.fromTo, existingContainers)
					// For file share,if the share does not exist, azcopy will fail, prompting the customer to create
					// the share manually with the required quota and settings.
					if fileerror.HasCode(err, fileerror.ShareNotFound) {
						return nil, fmt.Errorf("the destination file share %s does not exist; please create it manually with the required quota and settings before running the copy —refer to https://learn.microsoft.com/en-us/azure/storage/files/storage-how-to-create-file-share?tabs=azure-portal for SMB or https://learn.microsoft.com/en-us/azure/storage/files/storage-files-quick-create-use-linux for NFS.", dstContainerName)
					}

					if _, ok := seenFailedContainers[dstContainerName]; err != nil && jobsAdmin.JobsAdmin != nil && !ok {
						logDstContainerCreateFailureOnce.Do(func() {
							common.GetLifecycleMgr().Warn("Failed to create one or more destination container(s). Your transfers may still succeed if the container already exists.")
						})
						common.LogToJobLogWithPrefix(fmt.Sprintf("failed to initialize destination container %s; the transfer will continue (but be wary it may fail).", resName), common.LogWarning)
						common.LogToJobLogWithPrefix(fmt.Sprintf("Error %v", err), common.LogDebug)
						seenFailedContainers[dstContainerName] = true
					}
				}
			}
		}
	}
	// initialize the fields that are constant across all job part orders,
	// and for which we have sufficient info now to set them
	jobPartOrder := common.CopyJobPartOrderRequest{
		JobID:               t.tpt.jobID,
		FromTo:              t.opts.fromTo,
		ForceWrite:          t.opts.forceWrite,
		ForceIfReadOnly:     t.opts.forceIfReadOnly,
		AutoDecompress:      t.opts.autoDecompress,
		Priority:            common.EJobPriority.Normal(),
		LogLevel:            logLevel,
		SymlinkHandlingType: t.opts.symlinks,
		BlobAttributes: common.BlobTransferAttributes{
			BlobType:                 t.opts.blobType,
			BlockSizeInBytes:         t.opts.blockSize,
			PutBlobSizeInBytes:       t.opts.putBlobSize,
			ContentType:              t.opts.contentType,
			ContentEncoding:          t.opts.contentEncoding,
			ContentLanguage:          t.opts.contentLanguage,
			ContentDisposition:       t.opts.contentDisposition,
			CacheControl:             t.opts.cacheControl,
			BlockBlobTier:            t.opts.blockBlobTier,
			PageBlobTier:             t.opts.pageBlobTier,
			Metadata:                 t.opts.metadata,
			NoGuessMimeType:          t.opts.noGuessMimeType,
			PreserveLastModifiedTime: t.opts.preserveLastModifiedTime,
			PutMd5:                   t.opts.putMd5,
			MD5ValidationOption:      t.opts.checkMd5,
			DeleteSnapshotsOption:    t.opts.deleteSnapshotsOption,
			// Setting tags when tags explicitly provided by the user through blob-tags flag
			BlobTagsString:                   t.opts.blobTags.ToString(),
			DeleteDestinationFileIfNecessary: t.opts.deleteDestinationFileIfNecessary,
		},
		CommandString: t.opts.commandString,
		FileAttributes: common.FileTransferAttributes{
			TrailingDot: t.opts.trailingDot,
		},
		CpkOptions:                     t.opts.cpkOptions,
		PreservePermissions:            t.opts.preservePermissions,
		PreserveInfo:                   t.opts.preserveInfo,
		PreservePOSIXProperties:        t.opts.preservePosixProperties,
		S2SGetPropertiesInBackend:      t.opts.s2sGetPropertiesInBackend,
		S2SSourceChangeValidation:      t.opts.s2sSourceChangeValidation,
		DestLengthValidation:           t.opts.checkLength,
		S2SInvalidMetadataHandleOption: t.opts.s2sInvalidMetadataHandleOption,
		S2SPreserveBlobTags:            t.opts.s2sPreserveBlobTags,
		S2SSourceCredentialType:        t.trp.srcCredType,
		JobErrorHandler:                mgr,
		SrcServiceClient:               t.trp.srcServiceClient,
		DstServiceClient:               t.trp.dstServiceClient,
		DestinationRoot:                t.opts.destination,
		SourceRoot:                     t.opts.source,
	}

}

func isResourceDirectory(ctx context.Context, res common.ResourceString, loc common.Location, serviceClient *common.ServiceClient, isSource bool) bool {
	rt, err := traverser.InitResourceTraverser(res, loc, ctx,
		traverser.InitResourceTraverserOptions{
			Client: serviceClient,
		})

	if err != nil {
		return false
	}

	isDir, _ := rt.IsDirectory(isSource)
	return isDir
}

type BucketToContainerNameResolver interface {
	ResolveName(bucketName string) (string, error)
}

func createContainer(ctx context.Context, containerName string, destination common.ResourceString, dstServiceClient *common.ServiceClient, fromTo common.FromTo, existingContainers map[string]bool) (err error) {
	if _, ok := existingContainers[containerName]; ok {
		return
	}
	existingContainers[containerName] = true

	// Because the only use-cases for createDstContainer will be on service-level S2S and service-level download
	// We only need to create "containers" on local and blob.
	// TODO: Reduce code dupe somehow
	switch fromTo.To() {
	case common.ELocation.Local():
		err = os.MkdirAll(common.GenerateFullPath(destination.ValueLocal(), containerName), os.ModeDir|os.ModePerm)
	case common.ELocation.Blob():
		bsc, _ := dstServiceClient.BlobServiceClient()
		bcc := bsc.NewContainerClient(containerName)

		_, err = bcc.GetProperties(ctx, nil)
		if err == nil {
			return err // Container already exists, return gracefully
		}

		_, err = bcc.Create(ctx, nil)
		if bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
			return nil
		}
		return err
	case common.ELocation.File(), common.ELocation.FileNFS():
		fsc, _ := dstServiceClient.FileServiceClient()
		sc := fsc.NewShareClient(containerName)

		_, err = sc.GetProperties(ctx, nil)
		if err == nil {
			return err // If err is nil share already exists, return gracefully
		}

		// For file shares using NFS and SMB, we will not create the share if it does not exist.
		//
		// Rationale: This decision was made after evaluating the implications of the upcoming V2 APIs.
		// In V2, customers are billed based on the provisioned quota rather than actual usage.
		// Automatically creating a share with a default quota could result in unintended charges,
		// even if the share is never used.
		//
		// Therefore, to avoid unexpected billing, we will not auto-create the share here.
		return err
	case common.ELocation.BlobFS():
		dsc, _ := dstServiceClient.DatalakeServiceClient()
		fsc := dsc.NewFileSystemClient(containerName)
		_, err = fsc.GetProperties(ctx, nil)
		if err == nil {
			return err
		}

		_, err = fsc.Create(ctx, nil)
		if datalakeerror.HasCode(err, datalakeerror.FileSystemAlreadyExists) {
			return nil
		}
		return err
	default:
		panic(fmt.Sprintf("cannot create a destination container at location %s.", fromTo.To()))
	}
	return
}
