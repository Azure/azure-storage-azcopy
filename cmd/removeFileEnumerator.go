package cmd

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

type removeFileEnumerator common.CopyJobPartOrderRequest

// enumerate accepts an URL (with or without *) pointing to file/directory for enumerate, processe and remove.
// The method supports two general cases:
// Case 1: End with star, means remove files with specified prefix.
// directory/fprefix*
// directory/* (this expression is transferred to remove from directory, means remove all files in a directory.)
// Case 2: Not end with star, means remove a single file or a directory.
// directory/dir
// directory/file
func (e *removeFileEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	// Init params.
	util := copyHandlerUtil{}
	p := azfile.NewPipeline(
		azfile.NewAnonymousCredential(),
		azfile.PipelineOptions{
			Retry: azfile.RetryOptions{
				Policy:        azfile.RetryPolicyExponential,
				MaxTries:      ste.UploadMaxTries,
				TryTimeout:    ste.UploadTryTimeout,
				RetryDelay:    ste.UploadRetryDelay,
				MaxRetryDelay: ste.UploadMaxRetryDelay,
			},
			Telemetry: azfile.TelemetryOptions{
				Value: common.UserAgent,
			},
		})
	ctx := context.TODO()                                               // Ensure correct context is used
	cookedSourceURLString := util.replaceBackSlashWithSlash(cca.source) // Replace back slash with slash, otherwise url.Parse would encode the back slash.

	// Attempt to parse the source url.
	sourceURL, err := url.Parse(cookedSourceURLString)
	if err != nil {
		return fmt.Errorf("cannot parse source URL")
	}
	// append the sas at the end of query params.
	sourceURL = util.appendQueryParamToUrl(sourceURL, cca.sourceSAS)

	// Validate the source url.
	numOfStartInURLPath := util.numOfWildcardInURL(*sourceURL)
	if numOfStartInURLPath > 1 || (numOfStartInURLPath == 1 && !strings.HasSuffix(sourceURL.Path, "*")) {
		return fmt.Errorf("only support prefix matching (e.g: fileprefix*), or exact matching")
	}
	doPrefixSearch := numOfStartInURLPath == 1

	// For prefix search, only support file name matching in file prefix's parent dir level.
	if cca.recursive && doPrefixSearch {
		return fmt.Errorf("only support file name matching in file prefix's parent dir level, prefix matching with recursive mode is not supported currently for Azure file remove")
	}

	// Get the DirectoryURL or FileURL to be later used for listing.
	dirURL, fileURL, fileProperties, ok := util.getDeepestDirOrFileURLFromString(ctx, *sourceURL, p)

	if !ok {
		return fmt.Errorf("cannot find accessible file or base directory with specified sourceURLString")
	}

	// Check if source URL is in directory/* expression, and transfer it to remove from directory if the express is directory/*.
	if hasEquivalentDirectoryURL, equivalentURL := util.hasEquivalentDirectoryURL(*sourceURL); hasEquivalentDirectoryURL {
		*sourceURL = equivalentURL
		doPrefixSearch = false
	}

	if doPrefixSearch { // Case 1: Do prefix search, the file pattern would be [AnyLetter]+\*
		// The destination must be a directory, otherwise we don't know where to put the files.
		if !util.isPathALocalDirectory(cca.destination) {
			return fmt.Errorf("the destination must be an existing directory in this remove scenario")
		}

		// If there is * it's matching a file (like pattern matching)
		// get the search prefix to query the service
		searchPrefix := util.getPossibleFileNameFromURL(sourceURL.Path)
		if searchPrefix == "" {
			panic("invalid state, searchPrefix should not be emtpy in do prefix search.")
		}
		searchPrefix = searchPrefix[:len(searchPrefix)-1] // strip away the * at the end

		// Perform list files and directories, note only files would be matched and transferred in prefix search.
		for marker := (azfile.Marker{}); marker.NotDone(); {
			// Look for all files that start with the prefix.
			lResp, err := dirURL.ListFilesAndDirectoriesSegment(ctx, marker, azfile.ListFilesAndDirectoriesOptions{Prefix: searchPrefix})
			if err != nil {
				return err
			}

			// Process the files returned in this result segment.
			for _, fileInfo := range lResp.FileItems {
				f := dirURL.NewFileURL(fileInfo.Name)
				_, err := f.GetProperties(ctx) // TODO: the cost is high while otherwise we cannot get the last modified time. As Azure file's PM description, list might get more valuable file properties later, optimize the logic after the change...
				if err != nil {
					return err
				}
				fUrl := util.stripSASFromFileShareUrl(f.URL())
				e.addTransfer(common.CopyTransfer{
					Source:     fUrl.String(),
					SourceSize: fileInfo.Properties.ContentLength}, cca)
			}

			marker = lResp.NextMarker
		}

		err = e.dispatchFinalPart(cca)
		if err != nil {
			return err
		}

	} else { // Case 2: remove a single file or a directory.

		if fileURL != nil { // Single file.
			sUrl := util.stripSASFromFileShareUrl(*sourceURL)
			e.addTransfer(
				common.CopyTransfer{
					Source:     sUrl.String(),
					SourceSize: fileProperties.ContentLength(),
				}, cca)

		} else { // Directory.
			dirStack := &directoryStack{}
			dirStack.Push(*dirURL)
			for currentDirURL, ok := dirStack.Pop(); ok; currentDirURL, ok = dirStack.Pop() {
				// Perform list files and directories.
				for marker := (azfile.Marker{}); marker.NotDone(); {
					lResp, err := currentDirURL.ListFilesAndDirectoriesSegment(ctx, marker, azfile.ListFilesAndDirectoriesOptions{})
					if err != nil {
						return fmt.Errorf("cannot list files for remove")
					}

					// Process the files returned in this segment.
					for _, fileInfo := range lResp.FileItems {
						f := currentDirURL.NewFileURL(fileInfo.Name)
						_, err := f.GetProperties(ctx) // TODO: the cost is high while otherwise we cannot get the last modified time. As Azure file's PM description, list might get more valuable file properties later, optimize the logic after the change...
						if err != nil {
							return err
						}
						fUrl := util.stripSASFromFileShareUrl(f.URL())
						e.addTransfer(
							common.CopyTransfer{
								Source:     fUrl.String(),
								SourceSize: fileInfo.Properties.ContentLength}, cca)
					}

					// If recursive is turned on, add sub directories.
					if cca.recursive {
						for _, dirInfo := range lResp.DirectoryItems {
							d := currentDirURL.NewDirectoryURL(dirInfo.Name)
							dirStack.Push(d)
						}
					}
					marker = lResp.NextMarker
				}
			}
		}

		err = e.dispatchFinalPart(cca)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *removeFileEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, cca)
}

func (e *removeFileEnumerator) dispatchFinalPart(cca *cookedCopyCmdArgs) error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e), cca)
}

// TODO: Optimize for resource consumption cases. Can change to DFS with recursive method simply.
// Temporarily keep this implementation as discussion.
type directoryStack []azfile.DirectoryURL

func (s *directoryStack) Push(d azfile.DirectoryURL) {
	*s = append(*s, d)
}

func (s *directoryStack) Pop() (*azfile.DirectoryURL, bool) {
	l := len(*s)

	if l == 0 {
		return nil, false
	} else {
		e := (*s)[l-1]
		*s = (*s)[:l-1]
		return &e, true
	}
}
