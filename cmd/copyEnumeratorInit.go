package cmd

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

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
)

func (cca *cookedCopyCmdArgs) initEnumerator(jobPartOrder common.CopyJobPartOrderRequest, ctx context.Context) (*copyEnumerator, error) {
	var traverser resourceTraverser

	dst := cca.destination
	src := cca.source

	// Append SAS tokens if necessary
	if cca.destinationSAS != "" {
		destURL, err := url.Parse(dst)

		if err != nil {
			return nil, err
		}

		destURL = copyHandlerUtil{}.appendQueryParamToUrl(destURL, cca.destinationSAS)
		dst = destURL.String()
	}

	if cca.sourceSAS != "" {
		srcURL, err := url.Parse(src)

		if err != nil {
			return nil, err
		}

		srcURL = copyHandlerUtil{}.appendQueryParamToUrl(srcURL, cca.sourceSAS)
		src = srcURL.String()
	}

	var err error
	srcCredInfo := common.CredentialInfo{}

	if srcCredInfo, err = getCredentialInfoForLocation(ctx, cca.fromTo.From(), cca.source, cca.sourceSAS, true); err != nil {
		return nil, err
	} else if cca.fromTo.From() != common.ELocation.Local() && cca.fromTo.To() != common.ELocation.Local() && srcCredInfo.CredentialType == common.ECredentialType.OAuthToken() {
		// TODO: Generate a SAS token if it's blob -> *
		return nil, errors.New("a SAS token (or S3 access key) is required as a part of the source in S2S transfers")
	}

	traverser, err = initResourceTraverser(src, cca.fromTo.From(), &ctx, &srcCredInfo, &cca.followSymlinks, cca.listOfFilesChannel, cca.recursive, func() {})
	if err != nil {
		return nil, err
	}

	// Ensure we're only copying from a directory with a trailing wildcard or recursive.
	isSourceDir := traverser.isDirectory(true)
	if isSourceDir && !cca.recursive && !cca.stripTopDir {
		return nil, errors.New("cannot use directory as source without --recursive or trailing wildcard (/*)")
	}

	// Check if the destination is a directory so we can correctly decide where our files land
	isDestDir := cca.isDestDirectory(dst, &ctx)

	// Create a S3 bucket resolver
	// Giving it nothing to work with as new names will be added as we traverse.
	s3BucketResolver := NewS3BucketNameToAzureResourcesResolver(make([]string, 0))

	srcLevel, err := determineLocationLevel(cca.source, cca.fromTo.From(), true)

	if err != nil {
		return nil, err
	}

	dstLevel, err := determineLocationLevel(cca.destination, cca.fromTo.To(), false)

	if err != nil {
		return nil, err
	}

	if srcLevel == 2 && dstLevel == 0 {
		return nil, errors.New("cannot transfer files/folders to a service")
	}

	// When copying a container directly to a container, strip the top directory
	if srcLevel == ELocationLevel.Container() && dstLevel == ELocationLevel.Container() && cca.fromTo.From() != common.ELocation.Local() && cca.fromTo.To() != common.ELocation.Local() {
		cca.stripTopDir = true
	}

	var logDstContainerCreateFailureOnce sync.Once
	filters := cca.initModularFilters()
	processor := func(object storedObject) error {
		// Start by resolving the name and creating the container
		if object.containerName != "" && cca.fromTo.From() != common.ELocation.Local() && cca.fromTo.To() != common.ELocation.Local() {
			cName, err := s3BucketResolver.ResolveName(object.containerName)

			if err != nil {
				glcm.Error(err.Error())
				return errors.New("failed to add transfers from service, some of the buckets have invalid names for Azure. " +
					"Please exclude the invalid buckets in service to service copy, and copy them using bucket to container/share/filesystem copy " +
					"with customized destination name after the service to service copy finished")
			}

			object.dstContainerName = cName
			err = cca.createDstContainer(cName, dst, ctx)

			// if JobsAdmin is nil, we're probably in testing mode.
			// As a result, container creation failures are expected as we don't give the SAS tokens adequate permissions.
			if err != nil && ste.JobsAdmin != nil {
				logDstContainerCreateFailureOnce.Do(func() {
					glcm.Info("Failed to create one or more destination container(s). Your transfers may still succeed.")
				})
				ste.JobsAdmin.LogToJobLog(fmt.Sprintf("failed to initialize destination container %s; the transfer will continue (but be wary it may fail): %s", cName, err))
			}
		}

		// If above the service level, we already know the container name and don't need to supply it to makeEscapedRelativePath
		if srcLevel != ELocationLevel.Service() {
			object.containerName = ""
		}

		// When copying directly TO a container from a container, don't drop under a sub directory
		if dstLevel >= ELocationLevel.Container() {
			object.dstContainerName = ""
		}

		srcRelPath := cca.makeEscapedRelativePath(true, isDestDir, object)
		dstRelPath := cca.makeEscapedRelativePath(false, isDestDir, object)

		transfer := common.CopyTransfer{
			Source:           srcRelPath,
			Destination:      dstRelPath,
			LastModifiedTime: object.lastModifiedTime,
			SourceSize:       object.size,
			BlobType:         object.blobType,
		}

		if cca.s2sPreserveAccessTier {
			transfer.BlobTier = object.blobAccessTier
		}

		return addTransfer(&jobPartOrder, transfer, cca)
	}
	finalizer := func() error {
		if len(jobPartOrder.Transfers) == 0 {
			return errors.New("cannot find source to upload")
		}

		return dispatchFinalPart(&jobPartOrder, cca)
	}

	return newCopyEnumerator(traverser, filters, processor, finalizer), nil
}

// This is condensed down into an individual function as we don't end up re-using the destination traverser at all.
// This is just for the directory check.
func (cca *cookedCopyCmdArgs) isDestDirectory(dst string, ctx *context.Context) bool {
	var err error
	dstCredInfo := common.CredentialInfo{}

	if ctx == nil {
		return false
	}

	if dstCredInfo, err = getCredentialInfoForLocation(*ctx, cca.fromTo.To(), cca.destination, cca.destinationSAS, true); err != nil {
		return false
	}

	rt, err := initResourceTraverser(dst, cca.fromTo.To(), ctx, &dstCredInfo, nil, nil, false, func() {})

	if err != nil {
		return false
	}

	return rt.isDirectory(false)
}

// Initialize the modular filters outside of copy to increase readability.
func (cca *cookedCopyCmdArgs) initModularFilters() []objectFilter {
	filters := make([]objectFilter, 0) // same as []objectFilter{} under the hood

	if len(cca.includePatterns) != 0 {
		filters = append(filters, &includeFilter{patterns: cca.includePatterns})
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
		filters = append(filters, buildAttrFilters(cca.includeFileAttributes, cca.source, true)...)
	}

	if len(cca.excludeFileAttributes) != 0 {
		filters = append(filters, buildAttrFilters(cca.excludeFileAttributes, cca.source, false)...)
	}

	return filters
}

var existingContainers = map[string]bool{}

func (cca *cookedCopyCmdArgs) createDstContainer(containerName, dstWithSAS string, ctx context.Context) (err error) {
	if _, ok := existingContainers[containerName]; ok {
		return
	}
	existingContainers[containerName] = true

	dstCredInfo := common.CredentialInfo{}

	if dstCredInfo, err = getCredentialInfoForLocation(ctx, cca.fromTo.To(), cca.destination, cca.destinationSAS, false); err != nil {
		return err
	}

	dstPipeline, err := initPipeline(ctx, cca.fromTo.To(), dstCredInfo)
	if err != nil {
		return
	}

	// Because the only use-cases for createDstContainer will be on service-level S2S and service-level download
	// We only need to create "containers" on local and blob.
	switch cca.fromTo.To() {
	case common.ELocation.Local():
		err = os.MkdirAll(filepath.Join(cca.destination, containerName), os.ModeDir|os.ModePerm)
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
	}

	return
}

func (cca *cookedCopyCmdArgs) makeEscapedRelativePath(source bool, dstIsDir bool, object storedObject) (relativePath string) {
	var pathEncodeRules = func(path string) string {
		loc := common.ELocation.Unknown()

		if source {
			loc = cca.fromTo.From()
		} else {
			loc = cca.fromTo.To()
		}
		pathParts := strings.Split(path, common.AZCOPY_PATH_SEPARATOR_STRING)

		// If downloading on Windows or uploading to files, encode unsafe characters.
		if (loc == common.ELocation.Local() && !source && runtime.GOOS == "windows") || (!source && loc == common.ELocation.File()) {
			invalidChars := `<>\/:"|?*` + string(0x00)

			for _, c := range strings.Split(invalidChars, "") {
				for k, p := range pathParts {
					pathParts[k] = strings.ReplaceAll(p, c, url.PathEscape(c))
				}
			}

			// If uploading from Windows or downloading from files, decode unsafe chars
		} else if (!source && cca.fromTo.From() == common.ELocation.Local() && runtime.GOOS == "windows") && (!source && cca.fromTo.From() == common.ELocation.File()) {
			invalidChars := `<>\/:"|?*` + string(0x00)

			for _, c := range strings.Split(invalidChars, "") {
				for k, p := range pathParts {
					pathParts[k] = strings.ReplaceAll(p, url.PathEscape(c), c)
				}
			}
		}

		if loc != common.ELocation.Local() {
			for k, p := range pathParts {
				pathParts[k] = url.PathEscape(p)
			}
		}

		path = strings.Join(pathParts, "/")
		return path
	}

	// source is a EXACT path to the file.
	if object.relativePath == "" {
		// If we're finding an object from the source, it returns "" if it's already got it.
		// If we're finding an object on the destination and we get "", we need to hand it the object name (if it's pointing to a folder)
		if source {
			relativePath = ""
		} else {
			if dstIsDir {
				relativePath = "/" + object.name
			} else {
				relativePath = ""
			}
		}

		return pathEncodeRules(relativePath)
	}

	// If it's out here, the object is contained in a folder, or was found via a wildcard.

	relativePath = "/" + strings.Replace(object.relativePath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

	if common.IffString(source, object.containerName, object.dstContainerName) != "" {
		relativePath = `/` + common.IffString(source, object.containerName, object.dstContainerName) + relativePath
	} else if !source && !cca.stripTopDir {
		// We ONLY need to do this adjustment to the destination.
		// The source SAS has already been removed. No need to convert it to a URL or whatever.
		// Save to a directory
		rootDir := filepath.Base(cca.source)

		if cca.fromTo.From() != common.ELocation.Local() {
			ueRootDir, err := url.PathUnescape(rootDir)

			// Realistically, err should never not be nil here.
			if err == nil {
				rootDir = ueRootDir
			}
		}

		relativePath = "/" + rootDir + relativePath
	}

	return pathEncodeRules(relativePath)
}
