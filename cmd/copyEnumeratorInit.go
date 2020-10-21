package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
)

func (cca *cookedCopyCmdArgs) initEnumerator(jobPartOrder common.CopyJobPartOrderRequest, ctx context.Context) (*copyEnumerator, error) {
	var traverser resourceTraverser

	srcCredInfo := common.CredentialInfo{}
	var isPublic bool
	var err error

	if srcCredInfo, isPublic, err = getCredentialInfoForLocation(ctx, cca.fromTo.From(), cca.source.Value, cca.source.SAS, true); err != nil {
		return nil, err
		// If S2S and source takes OAuthToken as its cred type (OR) source takes anonymous as its cred type, but it's not public and there's no SAS
	} else if cca.fromTo.From().IsRemote() && cca.fromTo.To().IsRemote() &&
		(srcCredInfo.CredentialType == common.ECredentialType.OAuthToken() ||
			(srcCredInfo.CredentialType == common.ECredentialType.Anonymous() && !isPublic && cca.source.SAS == "")) {
		// TODO: Generate a SAS token if it's blob -> *
		return nil, errors.New("a SAS token (or S3 access key) is required as a part of the source in S2S transfers, unless the source is a public resource")
	}

	jobPartOrder.PreserveSMBPermissions = cca.preserveSMBPermissions
	jobPartOrder.PreserveSMBInfo = cca.preserveSMBInfo

	// Infer on download so that we get LMT and MD5 on files download
	// On S2S transfers the following rules apply:
	// If preserve properties is enabled, but get properties in backend is disabled, turn it on
	// If source change validation is enabled on files to remote, turn it on (consider a separate flag entirely?)
	getRemoteProperties := cca.forceWrite == common.EOverwriteOption.IfSourceNewer() ||
		(cca.fromTo.From() == common.ELocation.File() && !cca.fromTo.To().IsRemote()) || // If download, we still need LMT and MD5 from files.
		(cca.fromTo.From() == common.ELocation.File() && cca.fromTo.To().IsRemote() && (cca.s2sSourceChangeValidation || cca.includeAfter != nil)) || // If S2S from File to *, and sourceChangeValidation is enabled, we get properties so that we have LMTs. Likewise if we are using includeAfter, which requires LMTs.
		(cca.fromTo.From().IsRemote() && cca.fromTo.To().IsRemote() && cca.s2sPreserveProperties && !cca.s2sGetPropertiesInBackend) // If S2S and preserve properties AND get properties in backend is on, turn this off, as properties will be obtained in the backend.
	jobPartOrder.S2SGetPropertiesInBackend = cca.s2sPreserveProperties && !getRemoteProperties && cca.s2sGetPropertiesInBackend // Infer GetProperties if GetPropertiesInBackend is enabled.
	jobPartOrder.S2SSourceChangeValidation = cca.s2sSourceChangeValidation
	jobPartOrder.DestLengthValidation = cca.CheckLength
	jobPartOrder.S2SInvalidMetadataHandleOption = cca.s2sInvalidMetadataHandleOption

	traverser, err = initResourceTraverser(cca.source, cca.fromTo.From(), &ctx, &srcCredInfo, &cca.followSymlinks, cca.listOfFilesChannel, cca.recursive, getRemoteProperties, cca.includeDirectoryStubs, func(common.EntityType) {}, cca.listOfVersionIDs)

	if err != nil {
		return nil, err
	}

	// Ensure we're only copying from a directory with a trailing wildcard or recursive.
	isSourceDir := traverser.isDirectory(true)
	if isSourceDir && !cca.recursive && !cca.stripTopDir {
		return nil, errors.New("cannot use directory as source without --recursive or a trailing wildcard (/*)")
	}

	// Check if the destination is a directory so we can correctly decide where our files land
	isDestDir := cca.isDestDirectory(cca.destination, &ctx)
	if cca.listOfVersionIDs != nil && (!(cca.fromTo == common.EFromTo.BlobLocal() || cca.fromTo == common.EFromTo.BlobTrash()) || isSourceDir || !isDestDir) {
		log.Fatalf("Either source is not a blob or destination is not a local folder")
	}
	srcLevel, err := determineLocationLevel(cca.source.Value, cca.fromTo.From(), true)

	if err != nil {
		return nil, err
	}

	dstLevel, err := determineLocationLevel(cca.destination.Value, cca.fromTo.To(), false)

	if err != nil {
		return nil, err
	}

	// Disallow list-of-files and include-path on service-level traversal due to a major bug
	// TODO: Fix the bug.
	//       Two primary issues exist with the list-of-files implementation:
	//       1) Account name doesn't get trimmed from the path
	//       2) List-of-files is not considered an account traverser; therefore containers don't get made.
	//       Resolve these two issues and service-level list-of-files/include-path will work
	if cca.listOfFilesChannel != nil && srcLevel == ELocationLevel.Service() {
		return nil, errors.New("cannot combine list-of-files or include-path with account traversal")
	}

	if (srcLevel == ELocationLevel.Object() || cca.fromTo.From().IsLocal()) && dstLevel == ELocationLevel.Service() {
		return nil, errors.New("cannot transfer individual files/folders to the root of a service. Add a container or directory to the destination URL")
	}

	// When copying a container directly to a container, strip the top directory
	if srcLevel == ELocationLevel.Container() && dstLevel == ELocationLevel.Container() && cca.fromTo.From().IsRemote() && cca.fromTo.To().IsRemote() {
		cca.stripTopDir = true
	}

	// Create a S3 bucket resolver
	// Giving it nothing to work with as new names will be added as we traverse.
	var containerResolver = NewS3BucketNameToAzureResourcesResolver(nil)
	existingContainers := make(map[string]bool)
	var logDstContainerCreateFailureOnce sync.Once
	seenFailedContainers := make(map[string]bool) // Create map of already failed container conversions so we don't log a million items just for one container.

	dstContainerName := ""
	// Extract the existing destination container name
	if cca.fromTo.To().IsRemote() {
		dstContainerName, err = GetContainerName(cca.destination.Value, cca.fromTo.To())

		if err != nil {
			return nil, err
		}

		// only create the destination container in S2S scenarios
		if cca.fromTo.From().IsRemote() && dstContainerName != "" { // if the destination has a explicit container name
			// Attempt to create the container. If we fail, fail silently.
			err = cca.createDstContainer(dstContainerName, cca.destination, ctx, existingContainers)

			// check against seenFailedContainers so we don't spam the job log with initialization failed errors
			if _, ok := seenFailedContainers[dstContainerName]; err != nil && ste.JobsAdmin != nil && !ok {
				logDstContainerCreateFailureOnce.Do(func() {
					glcm.Info("Failed to create one or more destination container(s). Your transfers may still succeed if the container already exists.")
				})
				ste.JobsAdmin.LogToJobLog(fmt.Sprintf("failed to initialize destination container %s; the transfer will continue (but be wary it may fail): %s", dstContainerName, err), pipeline.LogWarning)
				seenFailedContainers[dstContainerName] = true
			}
		} else if cca.fromTo.From().IsRemote() { // if the destination has implicit container names
			if acctTraverser, ok := traverser.(accountTraverser); ok && dstLevel == ELocationLevel.Service() {
				containers, err := acctTraverser.listContainers()

				if err != nil {
					return nil, fmt.Errorf("failed to list containers: %s", err)
				}

				// Resolve all container names up front.
				// If we were to resolve on-the-fly, then name order would affect the results inconsistently.
				containerResolver = NewS3BucketNameToAzureResourcesResolver(containers)

				for _, v := range containers {
					bucketName, err := containerResolver.ResolveName(v)

					if err != nil {
						// Silently ignore the failure; it'll get logged later.
						continue
					}

					err = cca.createDstContainer(bucketName, cca.destination, ctx, existingContainers)

					// if JobsAdmin is nil, we're probably in testing mode.
					// As a result, container creation failures are expected as we don't give the SAS tokens adequate permissions.
					// check against seenFailedContainers so we don't spam the job log with initialization failed errors
					if _, ok := seenFailedContainers[bucketName]; err != nil && ste.JobsAdmin != nil && !ok {
						logDstContainerCreateFailureOnce.Do(func() {
							glcm.Info("Failed to create one or more destination container(s). Your transfers may still succeed if the container already exists.")
						})
						ste.JobsAdmin.LogToJobLog(fmt.Sprintf("failed to initialize destination container %s; the transfer will continue (but be wary it may fail): %s", bucketName, err), pipeline.LogWarning)
						seenFailedContainers[bucketName] = true
					}
				}
			} else {
				cName, err := GetContainerName(cca.source.Value, cca.fromTo.From())

				if err != nil || cName == "" {
					// this will probably never be reached
					return nil, fmt.Errorf("failed to get container name from source (is it formatted correctly?)")
				}

				resName, err := containerResolver.ResolveName(cName)

				if err == nil {
					err = cca.createDstContainer(resName, cca.destination, ctx, existingContainers)

					if _, ok := seenFailedContainers[dstContainerName]; err != nil && ste.JobsAdmin != nil && !ok {
						logDstContainerCreateFailureOnce.Do(func() {
							glcm.Info("Failed to create one or more destination container(s). Your transfers may still succeed if the container already exists.")
						})
						ste.JobsAdmin.LogToJobLog(fmt.Sprintf("failed to initialize destination container %s; the transfer will continue (but be wary it may fail): %s", dstContainerName, err), pipeline.LogWarning)
						seenFailedContainers[dstContainerName] = true
					}
				}
			}
		}
	}

	filters := cca.initModularFilters()

	// decide our folder transfer strategy
	var message string
	jobPartOrder.Fpo, message = newFolderPropertyOption(cca.fromTo, cca.recursive, cca.stripTopDir, filters, cca.preserveSMBInfo, cca.preserveSMBPermissions.IsTruthy())
	glcm.Info(message)
	if ste.JobsAdmin != nil {
		ste.JobsAdmin.LogToJobLog(message, pipeline.LogInfo)
	}

	processor := func(object storedObject) error {
		// Start by resolving the name and creating the container
		if object.containerName != "" {
			// set up the destination container name.
			cName := dstContainerName
			// if a destination container name is not specified OR copying service to container/folder, append the src container name.
			if cName == "" || (srcLevel == ELocationLevel.Service() && dstLevel > ELocationLevel.Service()) {
				cName, err = containerResolver.ResolveName(object.containerName)

				if err != nil {
					if _, ok := seenFailedContainers[object.containerName]; !ok {
						WarnStdoutAndJobLog(fmt.Sprintf("failed to add transfers from container %s as it has an invalid name. Please manually transfer from this container to one with a valid name.", object.containerName))
						seenFailedContainers[object.containerName] = true
					}
					return nil
				}

				object.dstContainerName = cName
			}
		}

		// If above the service level, we already know the container name and don't need to supply it to makeEscapedRelativePath
		if srcLevel != ELocationLevel.Service() {
			object.containerName = ""

			// When copying directly TO a container or object from a container, don't drop under a sub directory
			if dstLevel >= ELocationLevel.Container() {
				object.dstContainerName = ""
			}
		}

		srcRelPath := cca.makeEscapedRelativePath(true, isDestDir, object)
		dstRelPath := cca.makeEscapedRelativePath(false, isDestDir, object)

		transfer, shouldSendToSte := object.ToNewCopyTransfer(
			cca.autoDecompress && cca.fromTo.IsDownload(),
			srcRelPath, dstRelPath,
			cca.s2sPreserveAccessTier,
			jobPartOrder.Fpo,
		)

		if shouldSendToSte {
			return addTransfer(&jobPartOrder, transfer, cca)
		} else {
			return nil
		}
	}
	finalizer := func() error {
		return dispatchFinalPart(&jobPartOrder, cca)
	}

	return newCopyEnumerator(traverser, filters, processor, finalizer), nil
}

// This is condensed down into an individual function as we don't end up re-using the destination traverser at all.
// This is just for the directory check.
func (cca *cookedCopyCmdArgs) isDestDirectory(dst common.ResourceString, ctx *context.Context) bool {
	var err error
	dstCredInfo := common.CredentialInfo{}

	if ctx == nil {
		return false
	}

	if dstCredInfo, _, err = getCredentialInfoForLocation(*ctx, cca.fromTo.To(), cca.destination.Value, cca.destination.SAS, false); err != nil {
		return false
	}

	rt, err := initResourceTraverser(dst, cca.fromTo.To(), ctx, &dstCredInfo, nil, nil, false, false, false, func(common.EntityType) {}, cca.listOfVersionIDs)

	if err != nil {
		return false
	}

	return rt.isDirectory(false)
}

// Initialize the modular filters outside of copy to increase readability.
func (cca *cookedCopyCmdArgs) initModularFilters() []objectFilter {
	filters := make([]objectFilter, 0) // same as []objectFilter{} under the hood

	if cca.includeAfter != nil {
		filters = append(filters, &includeAfterDateFilter{threshold: *cca.includeAfter})
	}

	if len(cca.includePatterns) != 0 {
		filters = append(filters, &includeFilter{patterns: cca.includePatterns}) // TODO should this call buildIncludeFilters?
	}

	if len(cca.excludePatterns) != 0 {
		for _, v := range cca.excludePatterns {
			filters = append(filters, &excludeFilter{pattern: v})
		}
	}

	// include-path is not a filter, therefore it does not get handled here.
	// Check up in cook() around the list-of-files implementation as include-path gets included in the same way.

	if len(cca.excludePathPatterns) != 0 {
		for _, v := range cca.excludePathPatterns {
			filters = append(filters, &excludeFilter{pattern: v, targetsPath: true})
		}
	}

	if len(cca.excludeBlobType) != 0 {
		excludeSet := map[azblob.BlobType]bool{}

		for _, v := range cca.excludeBlobType {
			excludeSet[v] = true
		}

		filters = append(filters, &excludeBlobTypeFilter{blobTypes: excludeSet})
	}

	if len(cca.includeFileAttributes) != 0 {
		filters = append(filters, buildAttrFilters(cca.includeFileAttributes, cca.source.ValueLocal(), true)...)
	}

	if len(cca.excludeFileAttributes) != 0 {
		filters = append(filters, buildAttrFilters(cca.excludeFileAttributes, cca.source.ValueLocal(), false)...)
	}

	// finally, log any search prefix computed from these
	if ste.JobsAdmin != nil {
		if prefixFilter := filterSet(filters).GetEnumerationPreFilter(cca.recursive); prefixFilter != "" {
			ste.JobsAdmin.LogToJobLog("Search prefix, which may be used to optimize scanning, is: "+prefixFilter, pipeline.LogInfo) // "May be used" because we don't know here which enumerators will use it
		}
	}

	return filters
}

func (cca *cookedCopyCmdArgs) createDstContainer(containerName string, dstWithSAS common.ResourceString, ctx context.Context, existingContainers map[string]bool) (err error) {
	if _, ok := existingContainers[containerName]; ok {
		return
	}
	existingContainers[containerName] = true

	dstCredInfo := common.CredentialInfo{}

	if dstCredInfo, _, err = getCredentialInfoForLocation(ctx, cca.fromTo.To(), cca.destination.Value, cca.destination.SAS, false); err != nil {
		return err
	}

	dstPipeline, err := initPipeline(ctx, cca.fromTo.To(), dstCredInfo)
	if err != nil {
		return
	}

	// Because the only use-cases for createDstContainer will be on service-level S2S and service-level download
	// We only need to create "containers" on local and blob.
	// TODO: Reduce code dupe somehow
	switch cca.fromTo.To() {
	case common.ELocation.Local():
		err = os.MkdirAll(common.GenerateFullPath(cca.destination.ValueLocal(), containerName), os.ModeDir|os.ModePerm)
	case common.ELocation.Blob():
		accountRoot, err := GetAccountRoot(dstWithSAS, cca.fromTo.To())

		if err != nil {
			return err
		}

		dstURL, err := url.Parse(accountRoot)

		if err != nil {
			return err
		}

		bsu := azblob.NewServiceURL(*dstURL, dstPipeline)
		bcu := bsu.NewContainerURL(containerName)
		_, err = bcu.GetProperties(ctx, azblob.LeaseAccessConditions{})

		if err == nil {
			return err // Container already exists, return gracefully
		}

		_, err = bcu.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)

		if stgErr, ok := err.(azblob.StorageError); ok {
			if stgErr.ServiceCode() != azblob.ServiceCodeContainerAlreadyExists {
				return err
			}
		} else {
			return err
		}
	case common.ELocation.File():
		// Grab the account root and parse it as a URL
		accountRoot, err := GetAccountRoot(dstWithSAS, cca.fromTo.To())

		if err != nil {
			return err
		}

		dstURL, err := url.Parse(accountRoot)

		if err != nil {
			return err
		}

		fsu := azfile.NewServiceURL(*dstURL, dstPipeline)
		shareURL := fsu.NewShareURL(containerName)
		_, err = shareURL.GetProperties(ctx)

		if err == nil {
			return err
		}

		// Create a destination share with the default service quota
		// TODO: Create a flag for the quota
		_, err = shareURL.Create(ctx, azfile.Metadata{}, 0)

		if stgErr, ok := err.(azfile.StorageError); ok {
			if stgErr.ServiceCode() != azfile.ServiceCodeShareAlreadyExists {
				return err
			}
		} else {
			return err
		}
	default:
		panic(fmt.Sprintf("cannot create a destination container at location %s.", cca.fromTo.To()))
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

func pathEncodeRules(path string, fromTo common.FromTo, source bool) string {
	loc := common.ELocation.Unknown()

	if source {
		loc = fromTo.From()
	} else {
		loc = fromTo.To()
	}
	pathParts := strings.Split(path, common.AZCOPY_PATH_SEPARATOR_STRING)

	// If downloading on Windows or uploading to files, encode unsafe characters.
	if (loc == common.ELocation.Local() && !source && runtime.GOOS == "windows") || (!source && loc == common.ELocation.File()) {
		// invalidChars := `<>\/:"|?*` + string(0x00)

		for k, c := range encodedInvalidCharacters {
			for part, p := range pathParts {
				pathParts[part] = strings.ReplaceAll(p, string(k), c)
			}
		}

		// If uploading from Windows or downloading from files, decode unsafe chars
	} else if (!source && fromTo.From() == common.ELocation.Local() && runtime.GOOS == "windows") || (!source && fromTo.From() == common.ELocation.File()) {

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

func (cca *cookedCopyCmdArgs) makeEscapedRelativePath(source bool, dstIsDir bool, object storedObject) (relativePath string) {
	// write straight to /dev/null, do not determine a indirect path
	if !source && cca.destination.Value == common.Dev_Null {
		return "" // ignore path encode rules
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

		return pathEncodeRules(relativePath, cca.fromTo, source)
	}

	// If it's out here, the object is contained in a folder, or was found via a wildcard, or object.isSourceRootFolder == true

	if object.isSourceRootFolder() {
		relativePath = "" // otherwise we get "/" from the line below, and that breaks some clients, e.g. blobFS
	} else {
		relativePath = "/" + strings.Replace(object.relativePath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
	}

	if common.IffString(source, object.containerName, object.dstContainerName) != "" {
		relativePath = `/` + common.IffString(source, object.containerName, object.dstContainerName) + relativePath
	} else if !source && !cca.stripTopDir {
		// We ONLY need to do this adjustment to the destination.
		// The source SAS has already been removed. No need to convert it to a URL or whatever.
		// Save to a directory
		rootDir := filepath.Base(cca.source.Value)

		/* In windows, when a user tries to copy whole volume (eg. D:\),  the upload destination
		will contains "//"" in the files/directories names because of rootDir = "\" prefix.
		(e.g. D:\file.txt will end up as //file.txt).
		Following code will get volume name from source and add volume name as prefix in rootDir
		*/
		if runtime.GOOS == "windows" && rootDir == `\` {
			rootDir = filepath.VolumeName(common.ToShortPath(cca.source.Value))
		}

		if cca.fromTo.From().IsRemote() {
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

	return pathEncodeRules(relativePath, cca.fromTo, source)
}

// we assume that preserveSmbPermissions and preserveSmbInfo have already been validated, such that they are only true if both resource types support them
func newFolderPropertyOption(fromTo common.FromTo, recursive bool, stripTopDir bool, filters []objectFilter, preserveSmbInfo, preserveSmbPermissions bool) (common.FolderPropertyOption, string) {

	getSuffix := func(willProcess bool) string {
		willProcessString := common.IffString(willProcess, "will be processed", "will not be processed")

		template := ". For the same reason, %s defined on folders %s"
		switch {
		case preserveSmbPermissions && preserveSmbInfo:
			return fmt.Sprintf(template, "properties and permissions", willProcessString)
		case preserveSmbInfo:
			return fmt.Sprintf(template, "properties", willProcessString)
		case preserveSmbPermissions:
			return fmt.Sprintf(template, "permissions", willProcessString)
		default:
			return "" // no preserve flags set, so we have nothing to say about them
		}
	}

	bothFolderAware := fromTo.AreBothFolderAware()
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
			if f.appliesOnlyToFiles() {
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
		} else {
			return common.EFolderPropertiesOption.AllFolders(), message
		}
	}

	return common.EFolderPropertiesOption.NoFolders(),
		"Any empty folders will not be processed, because source and/or destination doesn't have full folder support" +
			getSuffix(false)

}
