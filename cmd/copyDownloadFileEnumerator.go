package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

type copyDownloadFileEnumerator common.CopyJobPartOrderRequest

// Support two general cases:
// 1. End with star, means download a file with specified prefix, if directory\*, means download the files under the directory.
// directory/fprefix*
// directory/dirprefix*
// directory/* (--recursive)
// 2. Not end with star, means download a single file or a directory.
// directory/dir
// directory/file
// this function accepts a url (with or without *) to files for download and processes them
func (e *copyDownloadFileEnumerator) enumerate(sourceURLString string, isRecursiveOn bool, destinationPath string,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	util := copyHandlerUtil{}
	p := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})

	// attempt to parse the source url
	sourceURL, err := url.Parse(sourceURLString)
	if err != nil {
		return fmt.Errorf("cannot parse source URL")
	}

	// validate the source url
	numOfStartInURLPath := util.numOfStarInUrl(sourceURL.Path)
	if numOfStartInURLPath > 1 || (numOfStartInURLPath == 1 && !strings.HasSuffix(sourceURL.Path, "*")) {
		return fmt.Errorf("only support prefix matching (e.g: fileprefix*), or exact matching")
	}
	doPrefixSearch := numOfStartInURLPath == 1

	ctx := context.Background() // use default background context

	// get the DirectoryURL or FileURL to be used later for listing
	dirURL, fileURL, fileProperties, ok := util.getDeepestDirOrFileURLFromString(ctx, *sourceURL, p)

	if !ok {
		return fmt.Errorf("cannot find accessible file or base directory with specified sourceURLString")
	}

	// Support two general cases:
	// 1. End with star, means download a file with specified prefix, if directory\*, means download the files under the directory.
	// directory/fprefix*
	// directory/dirprefix*
	// directory/* (--recursive)
	// 2. Not end with star, means download a single file or a directory.
	// directory/dir
	// directory/file
	if isDirectoryStarExpress, equivalentURL := util.isDirectoryStarExpression(*sourceURL); isDirectoryStarExpress {
		*sourceURL = equivalentURL
		doPrefixSearch = true
	}

	if doPrefixSearch { // Do prefix search, the file pattern would be [AnyLetter]+\*
		// the destination must be a directory, otherwise we don't know where to put the files
		if !util.isPathDirectory(destinationPath) {
			return fmt.Errorf("the destination must be an existing directory in this download scenario")
		}

		// If there is * it's matching a file (like pattern matching)
		// get the search prefix to query the service
		searchPrefix := util.getPossibleFileNameFromURL(sourceURL.Path)
		searchPrefix = searchPrefix[:len(searchPrefix)-1] // strip away the * at the end

		// perform a list dir
		for marker := (azfile.Marker{}); marker.NotDone(); {
			// look for all files that start with the prefix
			lResp, err := dirURL.ListFilesAndDirectoriesSegment(ctx, marker, azfile.ListFilesAndDirectoriesOptions{Prefix: searchPrefix})
			if err != nil {
				return err
			}

			// Process the files returned in this result segment (if the segment is empty, the loop body won't execute)
			for _, fileInfo := range lResp.Files {
				f := dirURL.NewFileURL(fileInfo.Name)
				gResp, err := f.GetProperties(ctx) // TODO: the cost is high wile otherwise we cannot get the last modified time...
				if err != nil {
					return err
				}

				e.addTransfer(common.CopyTransfer{
					Source:           f.String(),
					Destination:      util.generateLocalPath(destinationPath, fileInfo.Name),
					LastModifiedTime: gResp.LastModified(), // TODO: As Azure file's PM description, list might get more valuable file properties, currently we need fetch again...
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

	} else {
		if fileURL != nil { // single file case
			var singleFileDestinationPath string
			if util.isPathDirectory(destinationPath) {
				singleFileDestinationPath = util.generateLocalPath(destinationPath, util.getPossibleFileNameFromURL(sourceURL.Path))
			} else {
				singleFileDestinationPath = destinationPath
			}

			e.addTransfer(
				common.CopyTransfer{
					Source:           sourceURL.String(),
					Destination:      singleFileDestinationPath,
					LastModifiedTime: fileProperties.LastModified(),
					SourceSize:       fileProperties.ContentLength(),
				},
				wg,
				waitUntilJobCompletion)
		} else { // directory case
			// the destination must be a directory, otherwise we don't know where to put the files
			if !util.isPathDirectory(destinationPath) {
				return errors.New("the destination must be an existing directory in this download scenario")
			}

			dirStack := &directoryStack{}
			dirStack.Push(*dirURL)
			rootDirPath := "/" + azfile.NewFileURLParts(dirURL.URL()).DirectoryOrFilePath // TODO: finialize after DirectoryOrFilePath possible change

			for currentDirURL, ok := dirStack.Pop(); ok; currentDirURL, ok = dirStack.Pop() {
				// perform a list files and directories
				for marker := (azfile.Marker{}); marker.NotDone(); {
					lResp, err := currentDirURL.ListFilesAndDirectoriesSegment(context.TODO(), marker, azfile.ListFilesAndDirectoriesOptions{})
					if err != nil {
						return errors.New("cannot list files for download")
					}

					// process the files returned in this result segment (if the segment is empty, the loop body won't execute)
					for _, fileInfo := range lResp.Files {
						f := currentDirURL.NewFileURL(fileInfo.Name)
						gResp, err := f.GetProperties(ctx) // TODO: the cost is high wile otherwise we cannot get the last modified time...
						if err != nil {
							return err
						}

						currentFilePath := "/" + azfile.NewFileURLParts(f.URL()).DirectoryOrFilePath // TODO: finialize after DirectoryOrFilePath possible change

						e.addTransfer(
							common.CopyTransfer{
								Source:           f.String(),
								Destination:      util.generateLocalPath(destinationPath, util.getRelativePath(rootDirPath, currentFilePath)),
								LastModifiedTime: gResp.LastModified(),
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

func (e *copyDownloadFileEnumerator) addTransfer(transfer common.CopyTransfer, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, wg, waitUntilJobCompletion)
}

func (e *copyDownloadFileEnumerator) dispatchFinalPart() error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e))
}

// TODO: consider about resource consumption cases, would better refactor with space control manner
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
