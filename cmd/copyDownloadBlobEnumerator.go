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

func (e *copyDownloadBlobEnumerator) enumerate(sourceUrlString string, isRecursiveOn bool, destinationPath string,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	util := copyHandlerUtil{}

	// Create Pipeline to Get the Blob Properties or List Blob Segment
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
			Telemetry: azblob.TelemetryOptions{
				Value: common.UserAgent,
			},
		})

	// attempt to parse the source url
	sourceUrl, err := url.Parse(sourceUrlString)
	if err != nil {
		return errors.New("cannot parse source URL")
	}

	// get the blob parts
	blobUrlParts := azblob.NewBlobURLParts(*sourceUrl)

	// First Check if source blob exists
	// This check is in place to avoid listing of the blobs and matching the given blob against it
	// For example given source is https://<container>/a?<query-params> and there exists other blobs aa and aab
	// Listing the blobs with prefix /a will list other blob as well
	blobUrl := azblob.NewBlobURL(*sourceUrl, p)
	blobProperties, err := blobUrl.GetProperties(context.Background(), azblob.BlobAccessConditions{})

	// If the source blob exists, then queue transfer and return
	// Example: https://<container>/<blob>?<query-params>
	if err == nil {
		// For a single blob, destination provided can be either a directory or file.
		// If the destination is directory, then name of blob is preserved
		// If the destination is file, then blob will be downloaded as the given file name
		// Example1: Downloading https://<container>/a?<query-params> to directory C:\\Users\\User1
		// will download the blob as C:\\Users\\User1\\a
		// Example2: Downloading https://<container>/a?<query-params> to directory C:\\Users\\User1\\b
		// (b is not a directory) will download blob as C:\\Users\\User1\\b
		var blobLocalPath string
		if util.isPathALocalDirectory(destinationPath) {
			blobNameFromUrl := util.blobNameFromUrl(blobUrlParts)
			// check for special characters and get blobName without special character.
			blobNameFromUrl = util.blobPathWOSpecialCharacters(blobNameFromUrl)
			blobLocalPath = util.generateLocalPath(destinationPath, blobNameFromUrl)
		} else {
			blobLocalPath = destinationPath
		}
		// Add the transfer to CopyJobPartOrderRequest
		e.addTransfer(common.CopyTransfer{
			Source:           sourceUrl.String(),
			Destination:      blobLocalPath,
			LastModifiedTime: blobProperties.LastModified(),
			SourceSize:       blobProperties.ContentLength(),
		}, wg, waitUntilJobCompletion)
		// only one transfer for this Job, dispatch the JobPart
		err := e.dispatchFinalPart()
		if err != nil {
			return err
		}
		return nil
	}
	// Since the given source url doesn't represent an existing blob
	// it is either a container or a virtual directory, so it need to be
	// downloaded to an existing directory
	// Check if the given destination path is a directory or not.
	if !util.isPathALocalDirectory(destinationPath) {
		return errors.New("the destination must be an existing directory in this download scenario")
	}

	literalContainerUrl := util.getContainerUrl(blobUrlParts)
	containerUrl := azblob.NewContainerURL(literalContainerUrl, p)

	// If the files to be downloaded are mentioned in the include flag
	// Download the blobs or virtual directory mentioned with the include flag
	if len(e.Include) > 0 {
		for blob, _ := range e.Include {
			// Get the blobUrl by appending the blob name to the given source Url
			// blobName is the name after the container in the appended blobUrl
			blobUrl, blobName := util.appendBlobNameToUrl(blobUrlParts, blob)
			if blob[len(blob)-1] != '/' {
				// If there is no separator at the end of blobName, then it is consider to be a blob
				// For Example src = https://<container-name>?<sig> include = "file1.txt"
				// blobUrl = https://<container-name>/file1.txt?<sig> ; blobName = file1.txt
				bUrl := azblob.NewBlobURL(blobUrl, p)
				bProperties, err := bUrl.GetProperties(context.TODO(), azblob.BlobAccessConditions{})
				if err != nil {
					return fmt.Errorf("invalid blob name %s passed in include flag", blob)
				}
				// check for special characters and get blobName without special character.
				blobName = util.blobPathWOSpecialCharacters(blobName)
				blobLocalPath := util.generateLocalPath(destinationPath, blobName)
				e.addTransfer(common.CopyTransfer{
					Source:           blobUrl.String(),
					Destination:      blobLocalPath,
					LastModifiedTime: bProperties.LastModified(),
					SourceSize:       bProperties.ContentLength(),
				}, wg, waitUntilJobCompletion)
			} else {
				// If there is a separator at the end of blobName, then it is consider to be a virtual directory in the container
				// all blobs inside this virtual directory needs to downloaded
				// For Example: src = https://<container-name>?<sig> include = "dir1/"
				// blobName = dir1/  searchPrefix = dir1/
				// all blob starting with dir1/ will be listed
				searchPrefix := blobName
				pattern := "*"
				// perform a list blob with search prefix
				for marker := (azblob.Marker{}); marker.NotDone(); {
					// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
					listBlob, err := containerUrl.ListBlobsFlatSegment(context.Background(), marker,
						azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}, Prefix: searchPrefix})
					if err != nil {
						return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
					}

					// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
					for _, blobInfo := range listBlob.Blobs.Blob {
						// If the blob represents a folder as per the conditions mentioned in the
						// api doesBlobRepresentAFolder, then skip the blob.
						if util.doesBlobRepresentAFolder(blobInfo) {
							continue
						}
						// If the blobName doesn't matches the blob name pattern, then blob is not included
						// queued for transfer
						if !util.blobNameMatchesThePattern(pattern, blobInfo.Name) {
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
				}
			}
		}
		// dispatch the JobPart as Final Part of the Job
		err = e.dispatchFinalPart()
		if err != nil {
			return err
		}
		return nil
	}

	// If some blobs are mentioned with exclude flag
	// Iterate through each blob and append to the source url passed.
	// The blob name after appending to the source url is stored in the map
	// For Example: src = https://<container-name/dir?<sig> exclude ="file.txt"
	// blobNameToExclude will be dir/file.txt
	if len(e.Exclude) > 0 {
		destinationBlobName := blobUrlParts.BlobName
		if len(destinationBlobName) > 0 && destinationBlobName[len(destinationBlobName)-1] != '/' {
			destinationBlobName += "/"
		}
		for blob, index := range e.Exclude {
			blobNameToExclude := destinationBlobName + blob
			// If the blob name passed with the exclude flag is a virtual directory
			// Append * at the end of the blobNameToExclude so blobNameToExclude matches
			// the name of all blob inside the virtual dir
			// For Example: src = https://<container-name/dir?<sig> exclude ="dir/"
			// blobNameToExclude will be "dir/*"
			if blobNameToExclude[len(blobNameToExclude)-1] == '/' {
				blobNameToExclude += "*"
			}
			delete(e.Exclude, blob)
			e.Exclude[blobNameToExclude] = index
		}
	}

	// searchPrefix is the used in listing blob inside a container
	// all the blob listed should have the searchPrefix as the prefix
	// blobNamePattern represents the regular expression which the blobName should Match
	searchPrefix, blobNamePattern := util.searchPrefixFromUrl(blobUrlParts)

	// If blobNamePattern is "*", means that all the contents inside the given source url needs to be downloaded
	// It means that source url provided is either a container or a virtual directory
	// All the blobs inside a container or virtual directory will be downloaded only when the recursive flag is set to true
	if blobNamePattern == "*" && !isRecursiveOn {
		return fmt.Errorf("cannot download the enitre container / virtual directory. Please use recursive flag for this download scenario")
	}
	// perform a list blob with search prefix
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		listBlob, err := containerUrl.ListBlobsFlatSegment(context.Background(), marker,
			azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}, Prefix: searchPrefix})
		if err != nil {
			return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Blobs.Blob {
			// If the blob represents a folder as per the conditions mentioned in the
			// api doesBlobRepresentAFolder, then skip the blob.
			if util.doesBlobRepresentAFolder(blobInfo) {
				continue
			}
			// If the blobName doesn't matches the blob name pattern, then blob is not included
			// queued for transfer
			if !util.blobNameMatchesThePattern(blobNamePattern, blobInfo.Name) {
				continue
			}
			if util.resourceShouldBeExcluded(e.Exclude, blobInfo.Name) {
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
	}
	// If part number is 0 && number of transfer queued is 0
	// it means that no job part has been dispatched and there are no
	// transfer in Job to dispatch a JobPart. 
	if e.PartNum == 0  && len(e.Transfers) == 0 {
		return fmt.Errorf("no transfer queued to download. Please verify the source / destination")
	}
	// dispatch the JobPart as Final Part of the Job
	err = e.dispatchFinalPart()
	if err != nil {
		return err
	}
	return nil
}

// this function accepts a url (with or without *) to blobs for download and processes them
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
			Telemetry: azblob.TelemetryOptions{
				Value: common.UserAgent,
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
		if !util.isPathALocalDirectory(destinationPath) {
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
		if util.isPathALocalDirectory(destinationPath) {
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
			if !util.isPathALocalDirectory(destinationPath) {
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
