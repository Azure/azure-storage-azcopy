package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"path/filepath"
)

type copyDownloadBlobEnumerator common.CopyJobPartOrderRequest

func (e *copyDownloadBlobEnumerator) enumerate1(sourceUrlString string, isRecursiveOn bool, destinationPath string,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	util := copyHandlerUtil{}

	p := azblob.NewPipeline(
		azblob.NewAnonymousCredential(),
		azblob.PipelineOptions{
			Retry: azblob.RetryOptions{
				Policy:        azblob.RetryPolicyExponential,
				MaxTries:      ste.UploadMaxTries,
				TryTimeout:    ste.UploadTryTimeout,
				RetryDelay:    ste.UploadRetryDelay,
				MaxRetryDelay: ste.UploadMaxRetryDelay,
			},
		})

	// attempt to parse the source url
	sourceUrl, err := url.Parse(sourceUrlString)
	if err != nil {
		return errors.New("cannot parse source URL")
	}

	// get the blob parts
	blobUrlParts := azblob.NewBlobURLParts(*sourceUrl)

	// first check if source blob exists
	blobUrl := azblob.NewBlobURL(*sourceUrl, p)
	blobProperties, err := blobUrl.GetProperties(context.Background(), azblob.BlobAccessConditions{})

	//TODO: Examples
	// if the single blob exists, download it
	if err == nil {

		// for a single blob, the destination can either be a file or a directory
		var singleBlobDestinationPath string
		if util.isPathDirectory(destinationPath) {
			blobNameFromUrl := util.blobNameFromUrl(blobUrlParts)
			// check for special characters and get blobName without special character.
			blobNameFromUrl = util.blobPathWOSpecialCharacters(blobNameFromUrl)
			singleBlobDestinationPath = util.generateLocalPath(destinationPath, blobNameFromUrl)
		} else {
			singleBlobDestinationPath = destinationPath
		}

		e.addTransfer(common.CopyTransfer{
			Source:           sourceUrl.String(),
			Destination:      singleBlobDestinationPath,
			LastModifiedTime: blobProperties.LastModified(),
			SourceSize:       blobProperties.ContentLength(),
		}, wg, waitUntilJobCompletion)

		return nil
	}


	// the destination must be a directory, otherwise we don't know where to put the files
	if !util.isPathDirectory(destinationPath) {
		return errors.New("the destination must be an existing directory in this download scenario")
	}

	literalContainerUrl := util.getContainerUrl(blobUrlParts)
	containerUrl := azblob.NewContainerURL(literalContainerUrl, p)

	searchPrefix, blobNamePattern := util.searchPrefixFromUrl(blobUrlParts)


	// perform a list blob
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		listBlob, err := containerUrl.ListBlobsFlatSegment(context.Background(), marker,
			azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}, Prefix: searchPrefix})
		if err != nil {
			return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Blobs.Blob {
			// If the blob is not valid as per the conditions mentioned in the
			// api doesBlobRepresentAFolder, then skip the blob.
			if util.doesBlobRepresentAFolder(blobInfo) {
				continue
			}
			// TODO: add a function to perform the match
			matched, err := filepath.Match(blobNamePattern, blobInfo.Name)
			if err != nil {
				panic(err)
			}
			if !matched {
				continue
			}

			blobRelativePath := util.getRelativePath(searchPrefix, blobInfo.Name, "/")
			// check for the special character in blob relative path and get path without special character.
			blobRelativePath = util.blobPathWOSpecialCharacters(blobRelativePath)
			e.addTransfer(common.CopyTransfer{
				Source:           util.createBlobUrlFromContainer(blobUrlParts, blobInfo.Name),
				Destination:      util.generateLocalPath(destinationPath, blobRelativePath),
				LastModifiedTime: blobInfo.Properties.LastModified,
				SourceSize:       *blobInfo.Properties.ContentLength},
				wg,
				waitUntilJobCompletion)
		}

		marker = listBlob.NextMarker
		//err = e.dispatchPart(false)
		if err != nil {
			return err
		}
	}
	return nil
}

// this function accepts a url (with or without *) to blobs for download and processes them
func (e *copyDownloadBlobEnumerator) enumerate(sourceUrlString string, isRecursiveOn bool, destinationPath string,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	util := copyHandlerUtil{}

	p := azblob.NewPipeline(
		azblob.NewAnonymousCredential(),
		azblob.PipelineOptions{
			Retry: azblob.RetryOptions{
				Policy:        azblob.RetryPolicyExponential,
				MaxTries:      ste.UploadMaxTries,
				TryTimeout:    ste.UploadTryTimeout,
				RetryDelay:    ste.UploadRetryDelay,
				MaxRetryDelay: ste.UploadMaxRetryDelay,
			},
		})

	// attempt to parse the source url
	sourceUrl, err := url.Parse(sourceUrlString)
	if err != nil {
		return errors.New("cannot parse source URL")
	}

	// check if the given url is a container
	if util.urlIsContainerOrShare(sourceUrl) {
		// if path ends with a /, then append *
		if strings.HasSuffix(sourceUrl.Path, "/") {
			sourceUrl.Path += "*"
		} else { // if path is just /container_name, '/*' needs to be appended
			sourceUrl.Path += "/*"
		}
	}

	// get the container url to be used later for listing
	blobUrlParts := azblob.NewBlobURLParts(*sourceUrl)
	literalContainerUrl := util.getContainerUrl(blobUrlParts)
	containerUrl := azblob.NewContainerURL(literalContainerUrl, p)

	numOfStarInUrlPath := util.numOfStarInUrl(sourceUrl.Path)
	if numOfStarInUrlPath == 1 { // prefix search

		// the * must be at the end of the path
		if strings.LastIndex(sourceUrl.Path, "*") != len(sourceUrl.Path)-1 {
			return errors.New("the * in the source URL must be at the end of the path")
		}

		// the destination must be a directory, otherwise we don't know where to put the files
		if !util.isPathDirectory(destinationPath) {
			return errors.New("the destination must be an existing directory in this download scenario")
		}

		// get the search prefix to query the service
		searchPrefix := util.blobNameFromUrl(blobUrlParts)
		searchPrefix = searchPrefix[:len(searchPrefix)-1] // strip away the * at the end

		closestVirtualDirectory := util.getLastVirtualDirectoryFromPath(searchPrefix)

		// strip away the leading / in the closest virtual directory
		if len(closestVirtualDirectory) > 0 && closestVirtualDirectory[0:1] == "/" {
			closestVirtualDirectory = closestVirtualDirectory[1:]
		}

		// perform a list blob
		for marker := (azblob.Marker{}); marker.NotDone(); {
			// look for all blobs that start with the prefix
			listBlob, err := containerUrl.ListBlobsFlatSegment(context.TODO(), marker,
				azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}, Prefix: searchPrefix})
			if err != nil {
				return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
			}

			// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
			for _, blobInfo := range listBlob.Blobs.Blob {
				// If the blob is not valid as per the conditions mentioned in the
				// api doesBlobRepresentAFolder, then skip the blob.
				if !util.doesBlobRepresentAFolder(blobInfo) {
					continue
				}
				blobNameAfterPrefix := blobInfo.Name[len(closestVirtualDirectory):]
				if !isRecursiveOn && strings.Contains(blobNameAfterPrefix, "/") {
					continue
				}

				// check for special characters and get the blob without special characters.
				blobNameAfterPrefix = util.blobPathWOSpecialCharacters(blobNameAfterPrefix)
				e.addTransfer(common.CopyTransfer{
					Source:           util.createBlobUrlFromContainer(blobUrlParts, blobInfo.Name),
					Destination:      util.generateLocalPath(destinationPath, blobNameAfterPrefix),
					LastModifiedTime: blobInfo.Properties.LastModified,
					SourceSize:       *blobInfo.Properties.ContentLength},
					wg, waitUntilJobCompletion)
			}

			marker = listBlob.NextMarker
			if err != nil {
				return err
			}
		}

		err = e.dispatchFinalPart()
		if err != nil {
			return err
		}

	} else if numOfStarInUrlPath == 0 { // no prefix search
		// see if source blob exists
		blobUrl := azblob.NewBlobURL(*sourceUrl, p)
		blobProperties, err := blobUrl.GetProperties(context.Background(), azblob.BlobAccessConditions{})

		// for a single blob, the destination can either be a file or a directory
		var singleBlobDestinationPath string
		if util.isPathDirectory(destinationPath) {
			blobNameFromUrl := util.blobNameFromUrl(blobUrlParts)
			// check for special characters and get blobName without special character.
			blobNameFromUrl = util.blobPathWOSpecialCharacters(blobNameFromUrl)
			singleBlobDestinationPath = util.generateLocalPath(destinationPath, blobNameFromUrl)
		} else {
			singleBlobDestinationPath = destinationPath
		}

		// if the single blob exists, download it
		if err == nil {
			e.addTransfer(common.CopyTransfer{
				Source:           sourceUrl.String(),
				Destination:      singleBlobDestinationPath,
				LastModifiedTime: blobProperties.LastModified(),
				SourceSize:       blobProperties.ContentLength(),
			}, wg, waitUntilJobCompletion)

			//err = e.dispatchPart(false)
			if err != nil {
				return err
			}
		} else if err != nil && !isRecursiveOn {
			return errors.New("cannot get source blob properties, make sure it exists, for virtual directory download please use --recursive")
		}

		// if recursive happens to be turned on, then we will attempt to download a virtual directory
		if isRecursiveOn {
			// recursively download everything that is under the given path, that is a virtual directory
			searchPrefix := util.blobNameFromUrl(blobUrlParts)

			// if the user did not specify / at the end of the virtual directory, add it before doing the prefix search
			if strings.LastIndex(searchPrefix, "/") != len(searchPrefix)-1 {
				searchPrefix += "/"
			}

			// the destination must be a directory, otherwise we don't know where to put the files
			if !util.isPathDirectory(destinationPath) {
				return errors.New("the destination must be an existing directory in this download scenario")
			}

			// perform a list blob
			for marker := (azblob.Marker{}); marker.NotDone(); {
				// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
				listBlob, err := containerUrl.ListBlobsFlatSegment(context.Background(), marker,
					azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}, Prefix: searchPrefix})
				if err != nil {
					return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
				}

				// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
				for _, blobInfo := range listBlob.Blobs.Blob {
					// If the blob is not valid as per the conditions mentioned in the
					// api doesBlobRepresentAFolder, then skip the blob.
					if !util.doesBlobRepresentAFolder(blobInfo) {
						continue
					}
					blobRelativePath := util.getRelativePath(searchPrefix, blobInfo.Name, "/")
					// check for the special character in blob relative path and get path without special character.
					blobRelativePath = util.blobPathWOSpecialCharacters(blobRelativePath)
					e.addTransfer(common.CopyTransfer{
						Source:           util.createBlobUrlFromContainer(blobUrlParts, blobInfo.Name),
						Destination:      util.generateLocalPath(destinationPath, blobRelativePath),
						LastModifiedTime: blobInfo.Properties.LastModified,
						SourceSize:       *blobInfo.Properties.ContentLength},
						wg,
						waitUntilJobCompletion)
				}

				marker = listBlob.NextMarker
				//err = e.dispatchPart(false)
				if err != nil {
					return err
				}
			}
		}

		err = e.dispatchFinalPart()
		if err != nil {
			return err
		}

	} else { // more than one * is not supported
		return errors.New("only one * is allowed in the source URL")
	}
	return nil
}

func (e *copyDownloadBlobEnumerator) addTransfer(transfer common.CopyTransfer, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, wg, waitUntilJobCompletion)
}

func (e *copyDownloadBlobEnumerator) dispatchFinalPart() error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e))
}

func (e *copyDownloadBlobEnumerator) partNum() common.PartNumber {
	return e.PartNum
}
