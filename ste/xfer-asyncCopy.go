package ste

import (
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	"net/http"
	"net/url"
	"strings"
)

func AsyncCopy(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer pacer) {
	// If the transfer was cancelled, then reporting transfer as done and increasing the bytes transferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	// schedule the work as a chunk, so it will run on the main goroutine pool, instead of the
	// smaller "transfer initiation pool", where this code runs.
	id := common.NewChunkID(jptm.Info().Source, 0, 0)
	cf := createChunkFunc(true, jptm, id, func() {
		to := jptm.FromTo()
		switch to.From() {
		case common.ELocation.Blob():
			asyncCopyBlob(jptm, p)
		case common.ELocation.File():
			asyncCopyFile(jptm, p)
		default:
			panic("Attempting async copy on invalid location: " + to.From().String())
		}
	})
	jptm.ScheduleChunks(cf)
}

func asyncCopyBlob(jptm IJobPartTransferMgr, p pipeline.Pipeline) {
	info := jptm.Info()
	// Get the source blob url of blob to set properties on
	u, _ := url.Parse(info.Source)
	srcBlobURL := azblob.NewBlobURL(*u, p)
	u, _ = url.Parse(info.Destination)
	dstBlobURL := azblob.NewBlobURL(*u, p)

	// Internal function which checks the transfer status and logs the msg respectively.
	// Sets the transfer status and Reports Transfer as Done.
	// Internal function is created to avoid redundancy of the above steps from several places in the api.
	transferDone := func(status common.TransferStatus, err error) {
		if status == common.ETransferStatus.Failed() {
			jptm.LogError(info.Source, "ASYNC COPY FAILED with error: ", err)
		} else {
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("ASYNC COPY SUCCESSFUL: %s", strings.Split(info.Destination, "?")[0]))
		}

		jptm.SetStatus(status)
		jptm.ReportTransferDone()
	}

	ll, err := dstBlobURL.StartCopyFromURL(jptm.Context(), srcBlobURL.URL(), azblob.Metadata{}, azblob.ModifiedAccessConditions{}, azblob.BlobAccessConditions{}, azblob.AccessTierNone, nil)

	jptm.Log(pipeline.LogWarning, "ISHAAN: "+string(ll.CopyStatus())+string(ll.StatusCode()))
	fmt.Println(string(ll.CopyStatus()))
	fmt.Println(ll.StatusCode()) // TODO tiverma remove this

	if err != nil {
		if strErr, ok := err.(azblob.StorageError); ok {
			// TODO: Do we need to add more conditions? Won't happen on some snapshots and versions. Check documentation

			// If the status code was 403, it means there was an authentication error, and we exit.
			// User can resume the job if completely ordered with a new sas.
			if strErr.Response().StatusCode == http.StatusForbidden {
				errMsg := fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error())
				jptm.Log(pipeline.LogError, errMsg)
				common.GetLifecycleMgr().Error(errMsg)
			}
		}
		// in all other cases, make the transfer as failed
		transferDone(common.ETransferStatus.Failed(), err)
	} else {
		transferDone(common.ETransferStatus.Success(), nil)
	}
}

func asyncCopyFile(jptm IJobPartTransferMgr, p pipeline.Pipeline) {
	info := jptm.Info()
	// Get the source blob url of blob to set properties on
	u, _ := url.Parse(info.Source)
	srcFileURL := azfile.NewFileURL(*u, p)
	u, _ = url.Parse(info.Destination)
	dstFileURL := azfile.NewFileURL(*u, p)

	// Internal function which checks the transfer status and logs the msg respectively.
	// Sets the transfer status and Reports Transfer as Done.
	// Internal function is created to avoid redundancy of the above steps from several places in the api.
	transferDone := func(status common.TransferStatus, err error) {
		// TODO tiverma change this and add 'pending'
		if status == common.ETransferStatus.Failed() {
			jptm.LogError(info.Source, "ASYNC COPY FAILED with error: ", err)
		} else {
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("ASYNC COPY SUCCESSFUL: %s", strings.Split(info.Destination, "?")[0]))
		}

		jptm.SetStatus(status)
		jptm.ReportTransferDone()
	}

	_, err := dstFileURL.StartCopy(jptm.Context(), srcFileURL.URL(), azfile.Metadata{})

	if err != nil {
		if strErr, ok := err.(azblob.StorageError); ok {
			// TODO: Do we need to add more conditions? Won't happen on some snapshots and versions. Check documentation

			// If the status code was 403, it means there was an authentication error, and we exit.
			// User can resume the job if completely ordered with a new sas.
			if strErr.Response().StatusCode == http.StatusForbidden {
				errMsg := fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error())
				jptm.Log(pipeline.LogError, errMsg)
				common.GetLifecycleMgr().Error(errMsg)
			}
		}
		// in all other cases, make the transfer as failed
		transferDone(common.ETransferStatus.Failed(), err)
	} else {
		transferDone(common.ETransferStatus.Success(), nil)
	}
}

//resp, _ := containerURL.ListBlobsFlatSegment(ctx, Marker{},
//ListBlobsSegmentOptions{Details: BlobListingDetails{Snapshots: true}})

//func (s *aztestsSuite) TestBlobAbortCopyInProgress(c *chk.C) {
