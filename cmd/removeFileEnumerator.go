package cmd

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

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
func (e *removeFileEnumerator) enumerate(sourceURLString string, isRecursiveOn bool, destinationPath string,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {

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
		})
	ctx := context.TODO()                                                    // Ensure correct context is used
	cookedSourceURLString := util.replaceBackSlashWithSlash(sourceURLString) // Replace back slash with slash, otherwise url.Parse would encode the back slash.

	// Attempt to parse the source url.
	sourceURL, err := url.Parse(cookedSourceURLString)
	if err != nil {
		return fmt.Errorf("cannot parse source URL")
	}

	// Validate the source url.
	numOfStartInURLPath := util.numOfStarInUrl(sourceURL.Path)
	if numOfStartInURLPath > 1 || (numOfStartInURLPath == 1 && !strings.HasSuffix(sourceURL.Path, "*")) {
		return fmt.Errorf("only support prefix matching (e.g: fileprefix*), or exact matching")
	}
	doPrefixSearch := numOfStartInURLPath == 1

	// For prefix search, only support file name matching in file prefix's parent dir level.
	if isRecursiveOn && doPrefixSearch {
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
		if !util.isPathDirectory(destinationPath) {
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
			for _, fileInfo := range lResp.Files {
				f := dirURL.NewFileURL(fileInfo.Name)
				_, err := f.GetProperties(ctx) // TODO: the cost is high while otherwise we cannot get the last modified time. As Azure file's PM description, list might get more valuable file properties later, optimize the logic after the change...
				if err != nil {
					return err
				}

				e.addTransfer(common.CopyTransfer{
					Source:           f.String(),
					SourceSize:       fileInfo.Properties.ContentLength},
					wg,
					waitUntilJobCompletion)
			}

			marker = lResp.NextMarker
		}

		err = e.dispatchFinalPart()
		if err != nil {
			return err
		}

	} else { // Case 2: remove a single file or a directory.

		if fileURL != nil { // Single file.
			e.addTransfer(
				common.CopyTransfer{
					Source:           sourceURL.String(),
					SourceSize:       fileProperties.ContentLength(),
				},
				wg,
				waitUntilJobCompletion)

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
					for _, fileInfo := range lResp.Files {
						f := currentDirURL.NewFileURL(fileInfo.Name)
						_, err := f.GetProperties(ctx) // TODO: the cost is high while otherwise we cannot get the last modified time. As Azure file's PM description, list might get more valuable file properties later, optimize the logic after the change...
						if err != nil {
							return err
						}

						e.addTransfer(
							common.CopyTransfer{
								Source:           f.String(),
								SourceSize:       fileInfo.Properties.ContentLength},
							wg,
							waitUntilJobCompletion)
					}

					// If recursive is turned on, add sub directories.
					if isRecursiveOn {
						for _, dirInfo := range lResp.Directories {
							d := currentDirURL.NewDirectoryURL(dirInfo.Name)
							dirStack.Push(d)
						}
					}
					marker = lResp.NextMarker
				}
			}
		}

		err = e.dispatchFinalPart()
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *removeFileEnumerator) addTransfer(transfer common.CopyTransfer, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, wg, waitUntilJobCompletion)
}

func (e *removeFileEnumerator) dispatchFinalPart() error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e))
}

func (e *removeFileEnumerator) partNum() common.PartNumber {
	return e.PartNum
}

