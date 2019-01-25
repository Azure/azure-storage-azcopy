package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
)

type removeBlobEnumerator common.CopyJobPartOrderRequest

func (e *removeBlobEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	util := copyHandlerUtil{}

	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	// Create Pipeline to Get the Blob Properties or List Blob Segment
	p, err := createBlobPipeline(ctx, e.CredentialInfo)
	if err != nil {
		return err
	}

	// attempt to parse the source url
	sourceURL, err := url.Parse(cca.source)
	if err != nil {
		return errors.New("cannot parse source URL")
	}
	// append the sas at the end of query params.
	sourceURL = util.appendQueryParamToUrl(sourceURL, cca.sourceSAS)

	// get the blob parts
	blobUrlParts := azblob.NewBlobURLParts(*sourceURL) // TODO: keep blobUrlPart temporarily, it should be removed and further refactored.
	blobURLPartsExtension := blobURLPartsExtension{blobUrlParts}

	// First Check if source blob exists
	// This check is in place to avoid listing of the blobs and matching the given blob against it
	// For example given source is https://<container>/a?<query-params> and there exists other blobs aa and aab
	// Listing the blobs with prefix /a will list other blob as well
	blobUrl := azblob.NewBlobURL(*sourceURL, p)
	blobProperties, err := blobUrl.GetProperties(ctx, azblob.BlobAccessConditions{})

	// If the source blob exists, then queue transfer for deletion and return
	// Example: https://<container>/<blob>?<query-params>
	if err == nil {
		e.addTransfer(common.CopyTransfer{
			Source:     util.stripSASFromBlobUrl(*sourceURL).String(),
			SourceSize: blobProperties.ContentLength(),
		}, cca)
		// only one transfer for this Job, dispatch the JobPart
		err := e.dispatchFinalPart()
		if err != nil {
			return err
		}
		return nil
	}

	// save the container Url in order to list the blobs further
	literalContainerURL := util.getContainerUrl(blobUrlParts)
	containerURL := azblob.NewContainerURL(literalContainerURL, p)

	// searchPrefix is the used in listing blob inside a container
	// all the blob listed should have the searchPrefix as the prefix
	// blobNamePattern represents the regular expression which the blobName should Match
	// For Example: cca.src = https://<container-name>/user-1?<sig> searchPrefix = user-1/
	// For Example: cca.src = https://<container-name>/user-1/file*?<sig> searchPrefix = user-1/file
	searchPrefix, blobNamePattern, _ := blobURLPartsExtension.searchPrefixFromBlobURL()

	// If blobNamePattern is "*", means that all the contents inside the given source url needs to be downloaded
	// It means that source url provided is either a container or a virtual directory
	// All the blobs inside a container or virtual directory will be downloaded only when the recursive flag is set to true
	if blobNamePattern == "*" && !cca.recursive {
		return fmt.Errorf("cannot download the enitre container / virtual directory. Please use --recursive flag")
	}

	// perform a list blob with search prefix
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker,
			azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}, Prefix: searchPrefix})
		if err != nil {
			return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			// If the blob represents a folder as per the conditions mentioned in the
			// api doesBlobRepresentAFolder, then skip the blob.
			if util.doesBlobRepresentAFolder(blobInfo.Metadata) {
				continue
			}
			// If the blobName doesn't matches the blob name pattern, then blob is not included
			// queued for transfer
			if !util.blobNameMatchesThePattern(blobNamePattern, blobInfo.Name) {
				continue
			}

			e.addTransfer(common.CopyTransfer{
				Source:     util.stripSASFromBlobUrl(util.createBlobUrlFromContainer(blobUrlParts, blobInfo.Name)).String(),
				SourceSize: *blobInfo.Properties.ContentLength}, cca)
		}
		marker = listBlob.NextMarker
	}
	// dispatch the JobPart as Final Part of the Job
	err = e.dispatchFinalPart()
	if err != nil {
		return err
	}
	return nil
}

// accept a new transfer, simply add to the list of transfers and wait for the dispatch call to send the order
func (e *removeBlobEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, cca)
}

// send the current list of transfer to the STE
func (e *removeBlobEnumerator) dispatchFinalPart() error {
	// if the job is empty, throw an error
	if len(e.Transfers) == 0 {
		return errors.New("cannot initiate empty job, please make sure source is not empty or is a valid source")
	}

	e.IsFinalPart = true
	var resp common.CopyJobPartOrderResponse
	Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(e), &resp)

	if !resp.JobStarted {
		return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
	}
	return nil
}
