// Copyright © 2025 Microsoft <wastore@microsoft.com>
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
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

func (t *transferExecutor) initCopyEnumerator(ctx context.Context, logLevel common.LogLevel, mgr *jobLifecycleManager) (enumerator *traverser.CopyEnumerator, err error) {

	// source root trailing slash handling
	normalizedSource := t.opts.source
	normalizedSource.Value, err = NormalizeResourceRoot(t.opts.source.Value, t.opts.fromTo.From())
	if err != nil {
		return nil, err
	}

	// Handle special case: local source paths with trailing "/*".
	if t.opts.fromTo.From().IsLocal() {
		diff := strings.TrimPrefix(t.opts.source.Value, normalizedSource.Value)

		// Match either "*" or "/*" with OS or AzCopy separators.
		if diff == "*" ||
			diff == common.OS_PATH_SEPARATOR+"*" ||
			diff == common.AZCOPY_PATH_SEPARATOR_STRING+"*" {
			t.opts.stripTopDir = true
		}
	}

	// initialize the fields that are constant across all job part orders,
	// and for which we have sufficient info now to set them
	jobPartOrder := &common.CopyJobPartOrderRequest{
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
		CpkOptions:          t.opts.cpkOptions,
		PreservePermissions: t.opts.preservePermissions,
		PreserveInfo:        t.opts.preserveInfo,
		// We set preservePOSIXProperties if the customer has explicitly asked for this in transfer or if it is just a Posix-property only transfer
		PreservePOSIXProperties:        t.opts.preservePosixProperties || t.opts.forceWrite == common.EOverwriteOption.PosixProperties(),
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
		SourceRoot:                     normalizedSource,
	}

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
		HardlinkHandling:        t.opts.hardlinks,
		FromTo:                  t.opts.fromTo,

		ExcludeContainers:    t.opts.excludeContainers,
		IncrementEnumeration: t.tpt.incEnumeration,
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
	isDestDir := isResourceDirectory(ctx, t.opts.destination, t.opts.fromTo.To(), t.trp.dstServiceClient, false, t.opts.trailingDot)
	if t.opts.listOfVersionIds != nil &&
		(!(t.opts.fromTo == common.EFromTo.BlobLocal() || t.opts.fromTo == common.EFromTo.BlobTrash()) || isSourceDir || !isDestDir) {
		return nil, errors.New("either source is not a blob or destination is not a local folder")
	}

	// for the source dir checking to work, we can only infer these options here.
	// When copying a container directly to a container, strip the top directory, unless we're attempting to persist permissions.
	if t.opts.srcLevel == ELocationLevel.Container() && t.opts.dstLevel == ELocationLevel.Container() && t.opts.fromTo.IsS2S() {
		if t.opts.preservePermissions.IsTruthy() {
			// if we're preserving permissions, we need to keep the top directory, but with container->container, we don't need to add the container name to the path.
			// asSubdir is a better option than stripTopDir as stripTopDir disincludes the root.
			t.opts.asSubdir = false
		} else {
			t.opts.stripTopDir = true
		}
	}

	// Resolve containers
	// TODO : I think we can add a more lightweight resolver for Local/Azure scenarios
	// TODO : I also think this should be its own processor that runs before copy transfer scheduling
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

	// construct filters
	filters := traverser.BuildFilters(t.opts.fromTo, t.opts.source, t.opts.recursive, t.opts.filterOptions)

	// folder transfer strategy
	var folderMessage string
	jobPartOrder.Fpo, folderMessage = NewFolderPropertyOption(t.opts.fromTo, t.opts.recursive, t.opts.stripTopDir, filters, t.opts.preserveInfo, t.opts.preservePermissions.IsTruthy(), t.opts.preservePosixProperties, strings.EqualFold(t.opts.destination.Value, common.Dev_Null), t.opts.includeDirectoryStubs)
	if !t.opts.dryrun {
		common.GetLifecycleMgr().Info(folderMessage)
	}
	common.LogToJobLogWithPrefix(folderMessage, common.LogInfo)

	transferScheduler := t.newCopyTransferProcessor(NumOfFilesPerDispatchJobPart, jobPartOrder)

	processor := func(object traverser.StoredObject) error {
		// Start by resolving the name and creating the container
		if object.ContainerName != "" {
			// set up the destination container name.
			cName := dstContainerName
			// if a destination container name is not specified OR copying service to container/folder, append the src container name.
			if cName == "" || (t.opts.srcLevel == ELocationLevel.Service() && t.opts.dstLevel > ELocationLevel.Service()) {
				cName, err = containerResolver.ResolveName(object.ContainerName)

				if err != nil {
					if _, ok := seenFailedContainers[object.ContainerName]; !ok {
						traverser.WarnStdoutAndScanningLog(fmt.Sprintf("failed to add transfers from container %s as it has an invalid name. Please manually transfer from this container to one with a valid name.", object.ContainerName))
						seenFailedContainers[object.ContainerName] = true
					}
					return nil
				}

				object.DstContainerName = cName
			}
		}

		// If above the service level, we already know the container name and don't need to supply it to makeEscapedRelativePath
		if t.opts.srcLevel != ELocationLevel.Service() {
			object.ContainerName = ""

			// When copying directly TO a container or object from a container, don't drop under a sub directory
			if t.opts.dstLevel >= ELocationLevel.Container() {
				object.DstContainerName = ""
			}
		}

		srcRelPath := t.MakeEscapedRelativePath(true, isDestDir, object)
		dstRelPath := t.MakeEscapedRelativePath(false, isDestDir, object)
		return transferScheduler.scheduleTransfer(srcRelPath, dstRelPath, object)
	}
	finalizer := func() error {
		_, err := transferScheduler.DispatchFinalPart()
		// cleanly exits if nothing is scheduled.
		if err != nil && err != NothingScheduledError {
			return err
		}
		return nil
	}

	return traverser.NewCopyEnumerator(sourceTraverser, filters, processor, finalizer), nil
}

// TODO : This function should probably be split into escape source and escape destination. As is this method is a little confusing imo.
func (t *transferExecutor) MakeEscapedRelativePath(source bool, dstIsDir bool, object traverser.StoredObject) (relativePath string) {
	// write straight to /dev/null, do not determine a indirect path
	if !source && t.opts.destination.Value == common.Dev_Null {
		return "" // ignore path encode rules
	}

	if object.RelativePath == "\x00" { // Short circuit, our relative path is requesting root/
		return "\x00"
	}

	// source is a EXACT path to the file
	if object.IsSingleSourceFile() {
		// If we're finding an object from the source, it returns "" if it's already got it.
		// If we're finding an object on the destination and we get "", we need to hand it the object name (if it's pointing to a folder)
		if source {
			relativePath = ""
		} else {
			if dstIsDir {
				// Our source points to a specific file (and so has no relative path)
				// but our dest does not point to a specific file, it just points to a directory,
				// and so relativePath needs the _name_ of the source.
				processedVID := ""
				if len(object.BlobVersionID) > 0 {
					processedVID = strings.ReplaceAll(object.BlobVersionID, ":", "-") + "-"
				}
				relativePath += "/" + processedVID + object.Name
			} else {
				relativePath = ""
			}
		}

		return PathEncodeRules(relativePath, t.opts.fromTo, t.opts.disableAutoDecoding, source)
	}

	// If it's out here, the object is contained in a folder, or was found via a wildcard, or object.isSourceRootFolder == true
	if object.IsSourceRootFolder() {
		relativePath = "" // otherwise we get "/" from the line below, and that breaks some clients, e.g. blobFS
	} else {
		relativePath = "/" + strings.Replace(object.RelativePath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
	}

	if common.Iff(source, object.ContainerName, object.DstContainerName) != "" {
		relativePath = `/` + common.Iff(source, object.ContainerName, object.DstContainerName) + relativePath
	} else if !source && !t.opts.stripTopDir && t.opts.asSubdir { // Avoid doing this where the root is shared or renamed.
		// We ONLY need to do this adjustment to the destination.
		// The source SAS has already been removed. No need to convert it to a URL or whatever.
		// Save to a directory
		rootDir := filepath.Base(t.opts.source.Value)

		/* In windows, when a user tries to copy whole volume (eg. D:\),  the upload destination
		will contains "//"" in the files/directories names because of rootDir = "\" prefix.
		(e.g. D:\file.txt will end up as //file.txt).
		Following code will get volume name from source and add volume name as prefix in rootDir
		*/
		if runtime.GOOS == "windows" && rootDir == `\` {
			rootDir = filepath.VolumeName(common.ToShortPath(t.opts.source.Value))
		}

		if t.opts.fromTo.From().IsRemote() {
			ueRootDir, err := url.PathUnescape(rootDir)

			// Realistically, err should never not be nil here.
			if err == nil {
				rootDir = ueRootDir
			} else {
				panic("unexpected inescapable rootDir name")
			}
		}

		relativePath = "/" + rootDir + relativePath
	}

	return PathEncodeRules(relativePath, t.opts.fromTo, t.opts.disableAutoDecoding, source)
}

// extract the right info from cooked arguments and instantiate a generic copy transfer processor from it
func (t *transferExecutor) newCopyTransferProcessor(numOfTransfersPerPart int,
	copyJobTemplate *common.CopyJobPartOrderRequest) *CopyTransferProcessor {
	reportFirstPart := func(jobStarted bool) { t.tpt.setFirstPartOrdered() }
	reportFinalPart := func() { t.tpt.setScanningComplete() }
	// note that the source and destination, along with the template are given to the generic processor's constructor
	// this means that given an object with a relative path, this processor already knows how to schedule the right kind of transfers
	return NewCopyTransferProcessor(true, copyJobTemplate, numOfTransfersPerPart, t.opts.source, t.opts.destination, reportFirstPart, reportFinalPart, t.opts.s2sPreserveAccessTier.Get(), t.opts.dryrun, t.opts.dryrunJobPartOrderHandler)
}

func isResourceDirectory(ctx context.Context, res common.ResourceString, loc common.Location, serviceClient *common.ServiceClient, isSource bool, trailingDotOption common.TrailingDotOption) bool {
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
