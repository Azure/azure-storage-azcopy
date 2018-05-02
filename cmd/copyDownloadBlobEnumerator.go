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
)

type copyDownloadBlobEnumerator common.CopyJobPartOrderRequest

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
	literalContainerUrl := util.getContainerURLFromString(*sourceUrl)
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
		searchPrefix := util.getBlobNameFromURL(sourceUrl.Path)
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
				azblob.ListBlobsSegmentOptions{Prefix: searchPrefix})
			if err != nil {
				return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
			}

			// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
			for _, blobInfo := range listBlob.Blobs.Blob {
				blobNameAfterPrefix := blobInfo.Name[len(closestVirtualDirectory):]
				if !isRecursiveOn && strings.Contains(blobNameAfterPrefix, "/") {
					continue
				}

				e.addTransfer(common.CopyTransfer{
					Source:           util.generateBlobUrl(literalContainerUrl, blobInfo.Name),
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
			singleBlobDestinationPath = util.generateLocalPath(destinationPath, util.getBlobNameFromURL(sourceUrl.Path))
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
			searchPrefix := util.getBlobNameFromURL(sourceUrl.Path)

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
					azblob.ListBlobsSegmentOptions{Prefix: searchPrefix})
				if err != nil {
					return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
				}

				// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
				for _, blobInfo := range listBlob.Blobs.Blob {
					e.addTransfer(common.CopyTransfer{
						Source:           util.generateBlobUrl(literalContainerUrl, blobInfo.Name),
						Destination:      util.generateLocalPath(destinationPath, util.getRelativePath(searchPrefix, blobInfo.Name, "/")),
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

/////////////////////////////////////////////////////////////////////////////////////
// TODO: Following are dup code during involve file, please double check.
/////////////////////////////////////////////////////////////////////////////////////

// // accept a new transfer, simply add to the list of transfers and wait for the dispatch call to send the order
// func (e *copyDownloadBlobEnumerator) addTransfer(transfer common.CopyTransfer, wg *sync.WaitGroup,
// 	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
// 	e.Transfers = append(e.Transfers, transfer)

// 	if len(e.Transfers) == NumOfFilesPerUploadJobPart {
// 		resp := common.CopyJobPartOrderResponse{}
// 		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(e), &resp)

// 		if !resp.JobStarted {
// 			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
// 		}

// 		// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
// 		if e.PartNum == 0 {
// 			// adding go routine to the wait group.
// 			wg.Add(1)
// 			go waitUntilJobCompletion(e.JobID, wg)
// 		}
// 		e.Transfers = []common.CopyTransfer{}
// 		e.PartNum++
// 	}
// 	return nil
// }

// // send the current list of transfer to the STE
// func (e *copyDownloadBlobEnumerator) dispatchPart(isFinalPart bool) error {
// 	// if the job is empty, throw an error
// 	if !isFinalPart && len(e.Transfers) == 0 {
// 		return errors.New("cannot initiate empty job, please make sure source is not empty")
// 	}

// 	e.IsFinalPart = isFinalPart
// 	var resp common.CopyJobPartOrderResponse
// 	Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(e), &resp)

// 	if !resp.JobStarted {
// 		return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
// 	}

// 	// empty the transfers and increment part number count
// 	e.Transfers = []common.CopyTransfer{}
// 	if !isFinalPart {
// 		// part number needs to incremented only when the part is not the final part.
// 		e.PartNum++
// 	}
// 	return nil
// }
