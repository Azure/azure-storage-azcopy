package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

type copyDownloadBlobEnumerator common.CopyJobPartOrderRequest

func (e *copyDownloadBlobEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	util := copyHandlerUtil{}

	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	// Create Pipeline to Get the Blob Properties or List Blob Segment
	p, err := createBlobPipeline(ctx, e.CredentialInfo)
	if err != nil {
		return err
	}

	// attempt to parse the source url
	sourceUrl, err := url.Parse(cca.src)
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
	blobProperties, err := blobUrl.GetProperties(ctx, azblob.BlobAccessConditions{})

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
		if util.isPathALocalDirectory(cca.dst) {
			blobNameFromUrl := util.blobNameFromUrl(blobUrlParts)
			// check for special characters and get blobName without special character.
			blobNameFromUrl = util.blobPathWOSpecialCharacters(blobNameFromUrl)
			blobLocalPath = util.generateLocalPath(cca.dst, blobNameFromUrl)
		} else {
			blobLocalPath = cca.dst
		}
		// Add the transfer to CopyJobPartOrderRequest
		e.addTransfer(common.CopyTransfer{
			Source:           sourceUrl.String(),
			Destination:      blobLocalPath,
			LastModifiedTime: blobProperties.LastModified(),
			SourceSize:       blobProperties.ContentLength(),
		}, cca)
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
	if !util.isPathALocalDirectory(cca.dst) {
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
				// For Example cca.src = https://<container-name>?<sig> include = "file1.txt"
				// blobUrl = https://<container-name>/file1.txt?<sig> ; blobName = file1.txt
				bUrl := azblob.NewBlobURL(blobUrl, p)
				bProperties, err := bUrl.GetProperties(ctx, azblob.BlobAccessConditions{})
				if err != nil {
					return fmt.Errorf("invalid blob name %s passed in include flag", blob)
				}
				// check for special characters and get blobName without special character.
				blobName = util.blobPathWOSpecialCharacters(blobName)
				blobLocalPath := util.generateLocalPath(cca.dst, blobName)
				e.addTransfer(common.CopyTransfer{
					Source:           blobUrl.String(),
					Destination:      blobLocalPath,
					LastModifiedTime: bProperties.LastModified(),
					SourceSize:       bProperties.ContentLength(),
				}, cca)
			} else {
				// If there is a separator at the end of blobName, then it is consider to be a virtual directory in the container
				// all blobs inside this virtual directory needs to downloaded
				// For Example: cca.src = https://<container-name>?<sig> include = "dir1/"
				// blobName = dir1/  searchPrefix = dir1/
				// all blob starting with dir1/ will be listed
				searchPrefix := blobName
				pattern := "*"
				// perform a list blob with search prefix
				for marker := (azblob.Marker{}); marker.NotDone(); {
					// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
					listBlob, err := containerUrl.ListBlobsFlatSegment(ctx, marker,
						azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}, Prefix: searchPrefix})
					if err != nil {
						return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
					}

					// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
					for _, blobInfo := range listBlob.Segment.BlobItems {
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
							Destination:      util.generateLocalPath(cca.dst, blobRelativePath),
							LastModifiedTime: blobInfo.Properties.LastModified,
							SourceSize:       *blobInfo.Properties.ContentLength}, cca)
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
	// For Example: cca.src = https://<container-name/dir?<sig> exclude ="file.txt"
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
			// For Example: cca.src = https://<container-name/dir?<sig> exclude ="dir/"
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
	if blobNamePattern == "*" && !cca.recursive {
		return fmt.Errorf("cannot download the enitre container / virtual directory. Please use recursive flag for this download scenario")
	}
	// perform a list blob with search prefix
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		listBlob, err := containerUrl.ListBlobsFlatSegment(ctx, marker,
			azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}, Prefix: searchPrefix})
		if err != nil {
			return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
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
				Destination:      util.generateLocalPath(cca.dst, blobRelativePath),
				LastModifiedTime: blobInfo.Properties.LastModified,
				SourceSize:       *blobInfo.Properties.ContentLength}, cca)
		}
		marker = listBlob.NextMarker
	}
	// If part number is 0 && number of transfer queued is 0
	// it means that no job part has been dispatched and there are no
	// transfer in Job to dispatch a JobPart.
	if e.PartNum == 0 && len(e.Transfers) == 0 {
		return fmt.Errorf("no transfer queued to download. Please verify the source / destination")
	}
	// dispatch the JobPart as Final Part of the Job
	err = e.dispatchFinalPart()
	if err != nil {
		return err
	}
	return nil
}

func (e *copyDownloadBlobEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, cca)
}

func (e *copyDownloadBlobEnumerator) dispatchFinalPart() error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e))
}

func (e *copyDownloadBlobEnumerator) partNum() common.PartNumber {
	return e.PartNum
}
