package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
)

type removeBlobEnumerator common.CopyJobPartOrderRequest

func (e *removeBlobEnumerator) enumerate(sourceUrlString string, isRecursiveOn bool, destinationPath string,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	util := copyHandlerUtil{}

	// Create Pipeline to Get the Blob Properties or List Blob Segment
	p := ste.NewBlobPipeline(azblob.NewAnonymousCredential(),
		azblob.PipelineOptions{
			Telemetry: azblob.TelemetryOptions{Value: "azcopy-V2"},
		},
		ste.XferRetryOptions{
			Policy:        0,
			MaxTries:      ste.UploadMaxTries,
			TryTimeout:    ste.UploadTryTimeout,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay,
		}, nil)

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

	// If the source blob exists, then queue transfer for deletion and return
	// Example: https://<container>/<blob>?<query-params>
	if err == nil {
		e.addTransfer(common.CopyTransfer{
			Source:           sourceUrl.String(),
			SourceSize:       blobProperties.ContentLength(),
		}, wg, waitUntilJobCompletion)
		// only one transfer for this Job, dispatch the JobPart
		err := e.dispatchFinalPart()
		if err != nil{
			return err
		}
		return nil
	}

	// save the container Url in order to list the blobs further
	literalContainerUrl := util.getContainerUrl(blobUrlParts)
	containerUrl := azblob.NewContainerURL(literalContainerUrl, p)

	// searchPrefix is the used in listing blob inside a container
	// all the blob listed should have the searchPrefix as the prefix
	// blobNamePattern represents the regular expression which the blobName should Match
	// For Example: src = https://<container-name>/user-1?<sig> searchPrefix = user-1/
	// For Example: src = https://<container-name>/user-1/file*?<sig> searchPrefix = user-1/file
	searchPrefix, blobNamePattern := util.searchPrefixFromUrl(blobUrlParts)

	// If blobNamePattern is "*", means that all the contents inside the given source url needs to be downloaded
	// It means that source url provided is either a container or a virtual directory
	// All the blobs inside a container or virtual directory will be downloaded only when the recursive flag is set to true
	if blobNamePattern == "*" && !isRecursiveOn{
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
			if !util.blobNameMatchesThePattern(blobNamePattern, blobInfo.Name){
				continue
			}

			e.addTransfer(common.CopyTransfer{
				Source:           util.createBlobUrlFromContainer(blobUrlParts, blobInfo.Name),
				SourceSize:       *blobInfo.Properties.ContentLength},
				wg,
				waitUntilJobCompletion)
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
func (e *removeBlobEnumerator) addTransfer(transfer common.CopyTransfer, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, wg, waitUntilJobCompletion)
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

func (e *removeBlobEnumerator) partNum() common.PartNumber {
	return e.PartNum
}
