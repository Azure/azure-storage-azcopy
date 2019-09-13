package cmd

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/Azure/azure-storage-azcopy/common"
)

func (cca *cookedCopyCmdArgs) initEnumerator(jobPartOrder common.CopyJobPartOrderRequest, ctx context.Context) (*copyEnumerator, error) {
	var traverser resourceTraverser

	dst, err := appendSASIfNecessary(cca.destination, cca.destinationSAS)
	if err != nil {
		return nil, err
	}

	src, err := appendSASIfNecessary(cca.source, cca.sourceSAS)
	if err != nil {
		return nil, err
	}

	// TODO: in download refactor, handle trailing wildcard properly here.
	// As is, we don't cut it off at the moment,
	// and we also don't properly handle the "source points to contents" scenario aside from on local, which waives it through wildcard support.
	traverser, err = initResourceTraverser(src, cca.fromTo.From(), &ctx, &cca.credentialInfo, &cca.followSymlinks, cca.listOfFilesChannel, cca.recursive, func() {})
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

	filters := cca.initModularFilters()
	processor := func(object storedObject) error {
		src := cca.makeEscapedRelativePath(true, isDestDir, object)
		dst := cca.makeEscapedRelativePath(false, isDestDir, object)

		transfer := common.CopyTransfer{
			Source:           src,
			Destination:      dst,
			LastModifiedTime: object.lastModifiedTime,
			SourceSize:       object.size,
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
	rt, err := initResourceTraverser(dst, cca.fromTo.To(), ctx, &cca.credentialInfo, nil, nil, false, func() {})

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

	if !source && !cca.stripTopDir {
		// We ONLY need to do this adjustment to the destination.
		// The source SAS has already been removed. No need to convert it to a URL or whatever.
		// Save to a directory
		relativePath = "/" + filepath.Base(cca.source) + relativePath
	}

	return pathEncodeRules(relativePath)
}
