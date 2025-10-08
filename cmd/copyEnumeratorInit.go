package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"

	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type BucketToContainerNameResolver interface {
	ResolveName(bucketName string) (string, error)
}

func (cca *CookedCopyCmdArgs) validateSourceDir(traverser ResourceTraverser) error {
	var err error
	// Ensure we're only copying a directory under valid conditions
	cca.IsSourceDir, err = traverser.IsDirectory(true)
	if cca.IsSourceDir &&
		!cca.Recursive && // Copies the folder & everything under it
		!cca.StripTopDir { // Copies only everything under it
		// todo: dir only transfer, also todo: support syncing the root folder's acls on sync.
		return errors.New("cannot use directory as source without --recursive or a trailing wildcard (/*)")
	}
	// check if error is file not found - if it is then we need to make sure it's not a wild card
	if err != nil && strings.EqualFold(err.Error(), common.FILE_NOT_FOUND) && !cca.StripTopDir {
		return err
	}
	return nil
}

func (cca *CookedCopyCmdArgs) initEnumerator(jobPartOrder common.CopyJobPartOrderRequest, srcCredInfo common.CredentialInfo, ctx context.Context) (*CopyEnumerator, error) {
	var traverser ResourceTraverser
	var err error
	jobPartOrder.FileAttributes = common.FileTransferAttributes{
		TrailingDot: cca.trailingDot,
	}
	jobPartOrder.CpkOptions = cca.CpkOptions
	jobPartOrder.PreservePermissions = cca.preservePermissions
	jobPartOrder.PreserveInfo = cca.preserveInfo
	// We set preservePOSIXProperties if the customer has explicitly asked for this in transfer or if it is just a Posix-property only transfer
	jobPartOrder.PreservePOSIXProperties = cca.preservePOSIXProperties || (cca.ForceWrite == common.EOverwriteOption.PosixProperties())

	// Infer on download so that we get LMT and MD5 on files download
	// On S2S transfers the following rules apply:
	// If preserve properties is enabled, but get properties in backend is disabled, turn it on
	// If source change validation is enabled on files to remote, turn it on (consider a separate flag entirely?)
	getRemoteProperties := cca.ForceWrite == common.EOverwriteOption.IfSourceNewer() ||
		(cca.FromTo.From().IsFile() && !cca.FromTo.To().IsRemote()) || // If it's a download, we still need LMT and MD5 from files.
		(cca.FromTo.From().IsFile() &&
			cca.FromTo.To().IsRemote() && (cca.s2sSourceChangeValidation || cca.IncludeAfter != nil || cca.IncludeBefore != nil)) || // If S2S from File to *, and sourceChangeValidation is enabled, we get properties so that we have LMTs. Likewise, if we are using includeAfter or includeBefore, which require LMTs.
		(cca.FromTo.From().IsRemote() && cca.FromTo.To().IsRemote() && cca.s2sPreserveProperties.Value() && !cca.s2sGetPropertiesInBackend) // If S2S and preserve properties AND get properties in backend is on, turn this off, as properties will be obtained in the backend.
	jobPartOrder.S2SGetPropertiesInBackend = cca.s2sPreserveProperties.Value() && !getRemoteProperties && cca.s2sGetPropertiesInBackend // Infer GetProperties if GetPropertiesInBackend is enabled.
	jobPartOrder.S2SSourceChangeValidation = cca.s2sSourceChangeValidation
	jobPartOrder.DestLengthValidation = cca.CheckLength
	jobPartOrder.S2SInvalidMetadataHandleOption = cca.s2sInvalidMetadataHandleOption
	jobPartOrder.S2SPreserveBlobTags = cca.S2sPreserveBlobTags

	dest := cca.FromTo.To()
	traverser, err = InitResourceTraverser(cca.Source, cca.FromTo.From(), ctx, InitResourceTraverserOptions{
		DestResourceType: &dest,

		Credential: &srcCredInfo,

		ListOfFiles:      cca.ListOfFilesChannel,
		ListOfVersionIDs: cca.ListOfVersionIDsChannel,

		CpkOptions: cca.CpkOptions,

		PreservePermissions: cca.preservePermissions,
		SymlinkHandling:     cca.SymlinkHandling,
		PermanentDelete:     cca.permanentDeleteOption,
		SyncHashType:        common.ESyncHashType.None(),
		TrailingDotOption:   cca.trailingDot,

		Recursive:               cca.Recursive,
		GetPropertiesInFrontend: getRemoteProperties,
		IncludeDirectoryStubs:   cca.IncludeDirectoryStubs,
		PreserveBlobTags:        cca.S2sPreserveBlobTags,
		StripTopDir:             cca.StripTopDir,

		ExcludeContainers: cca.excludeContainer,
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

	err = cca.validateSourceDir(traverser)
	if err != nil {
		return nil, err
	}

	// Check if the destination is a directory to correctly decide where our files land
	isDestDir := cca.isDestDirectory(cca.Destination, ctx)
	if cca.ListOfVersionIDsChannel != nil && (!(cca.FromTo == common.EFromTo.BlobLocal() || cca.FromTo == common.EFromTo.BlobTrash()) || cca.IsSourceDir || !isDestDir) {
		log.Fatalf("Either source is not a blob or destination is not a local folder")
	}
	srcLevel, err := DetermineLocationLevel(cca.Source.Value, cca.FromTo.From(), true)

	if err != nil {
		return nil, err
	}

	dstLevel, err := DetermineLocationLevel(cca.Destination.Value, cca.FromTo.To(), false)

	if err != nil {
		return nil, err
	}

	// Disallow list-of-files and include-path on service-level traversal due to a major bug
	// TODO: Fix the bug.
	//       Two primary issues exist with the list-of-files implementation:
	//       1) Account name doesn't get trimmed from the path
	//       2) List-of-files is not considered an account traverser; therefore containers don't get made.
	//       Resolve these two issues and service-level list-of-files/include-path will work
	if cca.ListOfFilesChannel != nil && srcLevel == ELocationLevel.Service() {
		return nil, errors.New("cannot combine list-of-files or include-path with account traversal")
	}

	if (srcLevel == ELocationLevel.Object() || cca.FromTo.From().IsLocal()) && dstLevel == ELocationLevel.Service() {
		return nil, errors.New("cannot transfer individual files/folders to the root of a service. Add a container or directory to the destination URL")
	}

	if srcLevel == ELocationLevel.Container() && dstLevel == ELocationLevel.Service() && !cca.asSubdir {
		return nil, errors.New("cannot use --as-subdir=false with a service level destination")
	}

	// When copying a container directly to a container, strip the top directory, unless we're attempting to persist permissions.
	if srcLevel == ELocationLevel.Container() && dstLevel == ELocationLevel.Container() && cca.FromTo.From().IsRemote() && cca.FromTo.To().IsRemote() {
		if cca.preservePermissions.IsTruthy() {
			// if we're preserving permissions, we need to keep the top directory, but with container->container, we don't need to add the container name to the path.
			// asSubdir is a better option than stripTopDir as stripTopDir disincludes the root.
			cca.asSubdir = false
		} else {
			cca.StripTopDir = true
		}
	}

	// Create a Remote resource resolver
	// Giving it nothing to work with as new names will be added as we traverse.
	var containerResolver BucketToContainerNameResolver
	containerResolver = NewS3BucketNameToAzureResourcesResolver(nil)
	if cca.FromTo == common.EFromTo.GCPBlob() {
		containerResolver = NewGCPBucketNameToAzureResourcesResolver(nil)
	}
	existingContainers := make(map[string]bool)
	var logDstContainerCreateFailureOnce sync.Once
	seenFailedContainers := make(map[string]bool) // Create map of already failed container conversions so we don't log a million items just for one container.

	dstContainerName := ""
	// Extract the existing destination container name
	if cca.FromTo.To().IsRemote() {
		dstContainerName, err = GetContainerName(cca.Destination.Value, cca.FromTo.To())

		if err != nil {
			return nil, err
		}

		// only create the destination container in S2S scenarios
		if cca.FromTo.From().IsRemote() && dstContainerName != "" { // if the destination has a explicit container name
			// Attempt to create the container. If we fail, fail silently.
			err = cca.createDstContainer(dstContainerName, cca.Destination, ctx, existingContainers, common.ELogLevel.None())
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
		} else if cca.FromTo.From().IsRemote() { // if the destination has implicit container names
			if acctTraverser, ok := traverser.(AccountTraverser); ok && dstLevel == ELocationLevel.Service() {
				containers, err := acctTraverser.listContainers()

				if err != nil {
					return nil, fmt.Errorf("failed to list containers: %w", err)
				}

				// Resolve all container names up front.
				// If we were to resolve on-the-fly, then name order would affect the results inconsistently.
				if cca.FromTo == common.EFromTo.S3Blob() {
					containerResolver = NewS3BucketNameToAzureResourcesResolver(containers)
				} else if cca.FromTo == common.EFromTo.GCPBlob() {
					containerResolver = NewGCPBucketNameToAzureResourcesResolver(containers)
				}

				for _, v := range containers {
					bucketName, err := containerResolver.ResolveName(v)

					if err != nil {
						// Silently ignore the failure; it'll get logged later.
						continue
					}

					err = cca.createDstContainer(bucketName, cca.Destination, ctx, existingContainers, common.ELogLevel.None())
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
							glcm.Warn("Failed to create one or more destination container(s). Your transfers may still succeed if the container already exists.")
						})
						common.LogToJobLogWithPrefix(fmt.Sprintf("failed to initialize destination container %s; the transfer will continue (but be wary it may fail).", bucketName), common.LogWarning)
						common.LogToJobLogWithPrefix(fmt.Sprintf("Error %v", err), common.LogDebug)
						seenFailedContainers[bucketName] = true
					}
				}
			} else {
				cName, err := GetContainerName(cca.Source.Value, cca.FromTo.From())

				if err != nil || cName == "" {
					// this will probably never be reached
					return nil, fmt.Errorf("failed to get container name from source (is it formatted correctly?)")
				}
				resName, err := containerResolver.ResolveName(cName)

				if err == nil {
					err = cca.createDstContainer(resName, cca.Destination, ctx, existingContainers, common.ELogLevel.None())
					// For file share,if the share does not exist, azcopy will fail, prompting the customer to create
					// the share manually with the required quota and settings.
					if fileerror.HasCode(err, fileerror.ShareNotFound) {
						return nil, fmt.Errorf("the destination file share %s does not exist; please create it manually with the required quota and settings before running the copy —refer to https://learn.microsoft.com/en-us/azure/storage/files/storage-how-to-create-file-share?tabs=azure-portal for SMB or https://learn.microsoft.com/en-us/azure/storage/files/storage-files-quick-create-use-linux for NFS.", dstContainerName)
					}

					if _, ok := seenFailedContainers[dstContainerName]; err != nil && jobsAdmin.JobsAdmin != nil && !ok {
						logDstContainerCreateFailureOnce.Do(func() {
							glcm.Warn("Failed to create one or more destination container(s). Your transfers may still succeed if the container already exists.")
						})
						common.LogToJobLogWithPrefix(fmt.Sprintf("failed to initialize destination container %s; the transfer will continue (but be wary it may fail).", resName), common.LogWarning)
						common.LogToJobLogWithPrefix(fmt.Sprintf("Error %v", err), common.LogDebug)
						seenFailedContainers[dstContainerName] = true
					}
				}
			}
		}
	}

	filters := cca.InitModularFilters()

	// decide our folder transfer strategy
	var message string
	jobPartOrder.Fpo, message = NewFolderPropertyOption(cca.FromTo, cca.Recursive, cca.StripTopDir, filters, cca.preserveInfo,
		cca.preservePermissions.IsTruthy(), cca.preservePOSIXProperties, strings.EqualFold(cca.Destination.Value, common.Dev_Null), cca.IncludeDirectoryStubs)
	if !cca.dryrunMode {
		glcm.Info(message)
	}
	common.LogToJobLogWithPrefix(message, common.LogInfo)

	processor := func(object StoredObject) error {
		// Start by resolving the name and creating the container
		if object.ContainerName != "" {
			// set up the destination container name.
			cName := dstContainerName
			// if a destination container name is not specified OR copying service to container/folder, append the src container name.
			if cName == "" || (srcLevel == ELocationLevel.Service() && dstLevel > ELocationLevel.Service()) {
				cName, err = containerResolver.ResolveName(object.ContainerName)

				if err != nil {
					if _, ok := seenFailedContainers[object.ContainerName]; !ok {
						WarnStdoutAndScanningLog(fmt.Sprintf("failed to add transfers from container %s as it has an invalid name. Please manually transfer from this container to one with a valid name.", object.ContainerName))
						seenFailedContainers[object.ContainerName] = true
					}
					return nil
				}

				object.DstContainerName = cName
			}
		}

		// If above the service level, we already know the container name and don't need to supply it to makeEscapedRelativePath
		if srcLevel != ELocationLevel.Service() {
			object.ContainerName = ""

			// When copying directly TO a container or object from a container, don't drop under a sub directory
			if dstLevel >= ELocationLevel.Container() {
				object.DstContainerName = ""
			}
		}

		srcRelPath := cca.MakeEscapedRelativePath(true, isDestDir, cca.asSubdir, object)
		dstRelPath := cca.MakeEscapedRelativePath(false, isDestDir, cca.asSubdir, object)

		transfer, shouldSendToSte := object.ToNewCopyTransfer(cca.autoDecompress && cca.FromTo.IsDownload(), srcRelPath, dstRelPath, cca.s2sPreserveAccessTier.Value(), jobPartOrder.Fpo, cca.SymlinkHandling, cca.hardlinks)
		if !cca.S2sPreserveBlobTags {
			transfer.BlobTags = cca.blobTagsMap
		}

		if cca.dryrunMode && shouldSendToSte {
			glcm.Dryrun(func(format common.OutputFormat) string {
				src := common.GenerateFullPath(cca.Source.Value, srcRelPath)
				dst := common.GenerateFullPath(cca.Destination.Value, dstRelPath)

				switch format {
				case common.EOutputFormat.Json():
					tx := DryrunTransfer{
						EntityType:  transfer.EntityType,
						BlobType:    common.FromBlobType(transfer.BlobType),
						FromTo:      cca.FromTo,
						Source:      src,
						Destination: dst,
						SourceSize:  &transfer.SourceSize,
						HttpHeaders: blob.HTTPHeaders{
							BlobCacheControl:       &transfer.CacheControl,
							BlobContentDisposition: &transfer.ContentDisposition,
							BlobContentEncoding:    &transfer.ContentEncoding,
							BlobContentLanguage:    &transfer.ContentLanguage,
							BlobContentMD5:         transfer.ContentMD5,
							BlobContentType:        &transfer.ContentType,
						},
						Metadata:     transfer.Metadata,
						BlobTier:     &transfer.BlobTier,
						BlobVersion:  &transfer.BlobVersionID,
						BlobTags:     transfer.BlobTags,
						BlobSnapshot: &transfer.BlobSnapshotID,
					}

					buf, _ := json.Marshal(tx)
					return string(buf)
				default:
					return fmt.Sprintf("DRYRUN: copy %v to %v",
						src, dst)
				}
			})
			return nil
		}

		if shouldSendToSte {
			return addTransfer(&jobPartOrder, transfer, cca)
		}
		return nil
	}
	finalizer := func() error {
		return dispatchFinalPart(&jobPartOrder, cca)
	}

	return NewCopyEnumerator(traverser, filters, processor, finalizer), nil
}

// This is condensed down into an individual function as we don't end up reusing the destination traverser at all.
// This is just for the directory check.
func (cca *CookedCopyCmdArgs) isDestDirectory(dst common.ResourceString, ctx context.Context) bool {
	var err error
	dstCredInfo := common.CredentialInfo{}

	if ctx == nil {
		return false
	}

	if dstCredInfo, _, err = GetCredentialInfoForLocation(ctx, cca.FromTo.To(), cca.Destination, false, cca.CpkOptions); err != nil {
		return false
	}

	rt, err := InitResourceTraverser(dst, cca.FromTo.To(), ctx, InitResourceTraverserOptions{
		Credential: &dstCredInfo,

		ListOfVersionIDs: cca.ListOfVersionIDsChannel,

		PreservePermissions: cca.preservePermissions,

		StripTopDir:       cca.StripTopDir,
		TrailingDotOption: cca.trailingDot,

		ExcludeContainers: cca.excludeContainer,
		HardlinkHandling:  cca.hardlinks,
	})

	if err != nil {
		return false
	}

	isDir, _ := rt.IsDirectory(false)
	return isDir
}

// Initialize the modular filters outside of copy to increase readability.
func (cca *CookedCopyCmdArgs) InitModularFilters() []ObjectFilter {
	filters := make([]ObjectFilter, 0) // same as []ObjectFilter{} under the hood

	if cca.IncludeBefore != nil {
		filters = append(filters, &IncludeBeforeDateFilter{Threshold: *cca.IncludeBefore})
	}

	if cca.IncludeAfter != nil {
		filters = append(filters, &IncludeAfterDateFilter{Threshold: *cca.IncludeAfter})
	}

	if len(cca.IncludePatterns) != 0 {
		filters = append(filters, &IncludeFilter{patterns: cca.IncludePatterns}) // TODO should this call buildIncludeFilters?
	}

	if len(cca.ExcludePatterns) != 0 {
		for _, v := range cca.ExcludePatterns {
			filters = append(filters, &excludeFilter{pattern: v})
		}
	}

	// include-path is not a filter, therefore it does not get handled here.
	// Check up in cook() around the list-of-files implementation as include-path gets included in the same way.

	if len(cca.ExcludePathPatterns) != 0 {
		for _, v := range cca.ExcludePathPatterns {
			filters = append(filters, &excludeFilter{pattern: v, targetsPath: true})
		}
	}

	if len(cca.includeRegex) != 0 {
		filters = append(filters, &regexFilter{patterns: cca.includeRegex, isIncluded: true})
	}

	if len(cca.excludeRegex) != 0 {
		filters = append(filters, &regexFilter{patterns: cca.excludeRegex, isIncluded: false})
	}

	if len(cca.excludeBlobType) != 0 {
		excludeSet := map[blob.BlobType]bool{}

		for _, v := range cca.excludeBlobType {
			excludeSet[v] = true
		}

		filters = append(filters, &excludeBlobTypeFilter{blobTypes: excludeSet})
	}

	if len(cca.IncludeFileAttributes) != 0 {
		filters = append(filters, buildAttrFilters(cca.IncludeFileAttributes, cca.Source.ValueLocal(), true)...)
	}

	if len(cca.ExcludeFileAttributes) != 0 {
		filters = append(filters, buildAttrFilters(cca.ExcludeFileAttributes, cca.Source.ValueLocal(), false)...)
	}

	// finally, log any search prefix computed from these
	if prefixFilter := FilterSet(filters).GetEnumerationPreFilter(cca.Recursive); prefixFilter != "" {
		common.LogToJobLogWithPrefix("Search prefix, which may be used to optimize scanning, is: "+prefixFilter, common.LogInfo) // "May be used" because we don't know here which enumerators will use it
	}

	switch cca.permanentDeleteOption {
	case common.EPermanentDeleteOption.Snapshots():
		filters = append(filters, &permDeleteFilter{deleteSnapshots: true})
	case common.EPermanentDeleteOption.Versions():
		filters = append(filters, &permDeleteFilter{deleteVersions: true})
	case common.EPermanentDeleteOption.SnapshotsAndVersions():
		filters = append(filters, &permDeleteFilter{deleteSnapshots: true, deleteVersions: true})
	}

	return filters
}

func (cca *CookedCopyCmdArgs) createDstContainer(containerName string, dstWithSAS common.ResourceString, parentCtx context.Context, existingContainers map[string]bool, logLevel common.LogLevel) (err error) {
	if _, ok := existingContainers[containerName]; ok {
		return
	}
	existingContainers[containerName] = true

	var dstCredInfo common.CredentialInfo
	// 3minutes is enough time to list properties of a container, and create new if it does not exist.
	ctx, cancel := context.WithTimeout(parentCtx, time.Minute*3)
	defer cancel()
	if dstCredInfo, _, err = GetCredentialInfoForLocation(ctx, cca.FromTo.To(), cca.Destination, false, cca.CpkOptions); err != nil {
		return err
	}

	var reauthTok *common.ScopedAuthenticator
	if at, ok := dstCredInfo.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok {
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		reauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}

	options := createClientOptions(
		common.LogLevelOverrideLogger{ // override our log level here
			ILoggerResetable:  common.AzcopyCurrentJobLogger,
			MinimumLevelToLog: common.Iff(LogLevel == common.ELogLevel.Debug(), common.ELogLevel.Debug(), common.ELogLevel.None()),
		}, nil, reauthTok)

	sc, err := common.GetServiceClientForLocation(
		cca.FromTo.To(),
		dstWithSAS,
		dstCredInfo.CredentialType,
		dstCredInfo.OAuthTokenInfo.TokenCredential,
		&options,
		nil, // trailingDot is not required when creating a share
	)

	if err != nil {
		return err
	}

	// Because the only use-cases for createDstContainer will be on service-level S2S and service-level download
	// We only need to create "containers" on local and blob.
	// TODO: Reduce code dupe somehow
	switch cca.FromTo.To() {
	case common.ELocation.Local():
		err = os.MkdirAll(common.GenerateFullPath(cca.Destination.ValueLocal(), containerName), os.ModeDir|os.ModePerm)
	case common.ELocation.Blob():
		bsc, _ := sc.BlobServiceClient()
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
		fsc, _ := sc.FileServiceClient()
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
		dsc, _ := sc.DatalakeServiceClient()
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
		panic(fmt.Sprintf("cannot create a destination container at location %s.", cca.FromTo.To()))
	}
	return
}

// Because some invalid characters weren't being properly encoded by url.PathEscape, we're going to instead manually encode them.
var encodedInvalidCharacters = map[rune]string{
	'<':  "%3C",
	'>':  "%3E",
	'\\': "%5C",
	'/':  "%2F",
	':':  "%3A",
	'"':  "%22",
	'|':  "%7C",
	'?':  "%3F",
	'*':  "%2A",
}

var reverseEncodedChars = map[string]rune{
	"%3C": '<',
	"%3E": '>',
	"%5C": '\\',
	"%2F": '/',
	"%3A": ':',
	"%22": '"',
	"%7C": '|',
	"%3F": '?',
	"%2A": '*',
}

func pathEncodeRules(path string, fromTo common.FromTo, disableAutoDecoding bool, source bool) string {
	var loc common.Location

	if source {
		loc = fromTo.From()
	} else {
		loc = fromTo.To()
	}
	pathParts := strings.Split(path, common.AZCOPY_PATH_SEPARATOR_STRING)

	// If downloading on Windows or uploading to files, encode unsafe characters.
	if (loc == common.ELocation.Local() && !source && runtime.GOOS == "windows") ||
		(!source && (loc == common.ELocation.File() || loc == common.ELocation.FileNFS())) {
		// invalidChars := `<>\/:"|?*` + string(0x00)

		for k, c := range encodedInvalidCharacters {
			for part, p := range pathParts {
				pathParts[part] = strings.ReplaceAll(p, string(k), c)
			}
		}

		// If uploading from Windows or downloading from files, decode unsafe chars if user enables decoding
	} else if ((!source && fromTo.From() == common.ELocation.Local() && runtime.GOOS == "windows") ||
		(!source && (fromTo.From() == common.ELocation.File() || fromTo.From() == common.ELocation.FileNFS()))) && !disableAutoDecoding {

		for encoded, c := range reverseEncodedChars {
			for k, p := range pathParts {
				pathParts[k] = strings.ReplaceAll(p, encoded, string(c))
			}
		}
	}

	if loc.IsRemote() {
		for k, p := range pathParts {
			pathParts[k] = url.PathEscape(p)
		}
	}

	path = strings.Join(pathParts, "/")
	return path
}

func (cca *CookedCopyCmdArgs) MakeEscapedRelativePath(source bool, dstIsDir bool, asSubdir bool, object StoredObject) (relativePath string) {
	// write straight to /dev/null, do not determine a indirect path
	if !source && cca.Destination.Value == common.Dev_Null {
		return "" // ignore path encode rules
	}

	if object.relativePath == "\x00" { // Short circuit, our relative path is requesting root/
		return "\x00"
	}

	// source is a EXACT path to the file
	if object.isSingleSourceFile() {
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
				if len(object.blobVersionID) > 0 {
					processedVID = strings.ReplaceAll(object.blobVersionID, ":", "-") + "-"
				}
				relativePath += "/" + processedVID + object.name
			} else {
				relativePath = ""
			}
		}

		return pathEncodeRules(relativePath, cca.FromTo, cca.disableAutoDecoding, source)
	}

	// If it's out here, the object is contained in a folder, or was found via a wildcard, or object.isSourceRootFolder == true
	if object.isSourceRootFolder() {
		relativePath = "" // otherwise we get "/" from the line below, and that breaks some clients, e.g. blobFS
	} else {
		relativePath = "/" + strings.Replace(object.relativePath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
	}

	if common.Iff(source, object.ContainerName, object.DstContainerName) != "" {
		relativePath = `/` + common.Iff(source, object.ContainerName, object.DstContainerName) + relativePath
	} else if !source && !cca.StripTopDir && cca.asSubdir { // Avoid doing this where the root is shared or renamed.
		// We ONLY need to do this adjustment to the destination.
		// The source SAS has already been removed. No need to convert it to a URL or whatever.
		// Save to a directory
		rootDir := filepath.Base(cca.Source.Value)

		/* In windows, when a user tries to copy whole volume (eg. D:\),  the upload destination
		will contains "//"" in the files/directories names because of rootDir = "\" prefix.
		(e.g. D:\file.txt will end up as //file.txt).
		Following code will get volume name from source and add volume name as prefix in rootDir
		*/
		if runtime.GOOS == "windows" && rootDir == `\` {
			rootDir = filepath.VolumeName(common.ToShortPath(cca.Source.Value))
		}

		if cca.FromTo.From().IsRemote() {
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

	return pathEncodeRules(relativePath, cca.FromTo, cca.disableAutoDecoding, source)
}

// we assume that preserveSmbPermissions and preserveSmbInfo have already been validated, such that they are only true if both resource types support them
func NewFolderPropertyOption(fromTo common.FromTo, recursive, stripTopDir bool, filters []ObjectFilter, preserveSmbInfo,
	preservePermissions, preservePosixProperties, isDstNull, includeDirectoryStubs bool) (common.FolderPropertyOption, string) {

	getSuffix := func(willProcess bool) string {
		willProcessString := common.Iff(willProcess, "will be processed", "will not be processed")

		template := ". For the same reason, %s defined on folders %s"
		switch {
		case preservePermissions && preserveSmbInfo:
			return fmt.Sprintf(template, "properties and permissions", willProcessString)
		case preserveSmbInfo:
			return fmt.Sprintf(template, "properties", willProcessString)
		case preservePermissions:
			return fmt.Sprintf(template, "permissions", willProcessString)
		default:
			return "" // no preserve flags set, so we have nothing to say about them
		}
	}

	bothFolderAware := (fromTo.AreBothFolderAware() || preservePosixProperties || preservePermissions || includeDirectoryStubs) && !isDstNull
	isRemoveFromFolderAware := fromTo == common.EFromTo.FileTrash()
	if bothFolderAware || isRemoveFromFolderAware {
		if !recursive {
			return common.EFolderPropertiesOption.NoFolders(), // doesn't make sense to move folders when not recursive. E.g. if invoked with /* and WITHOUT recursive
				"Any empty folders will not be processed, because --recursive was not specified" +
					getSuffix(false)
		}

		// check filters. Otherwise, if filter was say --include-pattern *.txt, we would transfer properties
		// (but not contents) for every directory that contained NO text files.  Could make heaps of empty directories
		// at the destination.
		filtersOK := true
		for _, f := range filters {
			if f.AppliesOnlyToFiles() {
				filtersOK = false // we have a least one filter that doesn't apply to folders
			}
		}
		if !filtersOK {
			return common.EFolderPropertiesOption.NoFolders(),
				"Any empty folders will not be processed, because a file-focused filter is applied" +
					getSuffix(false)
		}

		message := "Any empty folders will be processed, because source and destination both support folders"
		if isRemoveFromFolderAware {
			message = "Any empty folders will be processed, because deletion is from a folder-aware location"
		}
		message += getSuffix(true)
		if stripTopDir {
			return common.EFolderPropertiesOption.AllFoldersExceptRoot(), message
		}
		return common.EFolderPropertiesOption.AllFolders(), message
	}

	return common.EFolderPropertiesOption.NoFolders(),
		"Any empty folders will not be processed, because source and/or destination doesn't have full folder support" +
			getSuffix(false)

}
