package ste

import (
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	"net/http"
	"net/url"
	"strings"
)

func SetProperties(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer pacer) {
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
			setPropertiesBlob(jptm, p)
		case common.ELocation.BlobFS():
			setPropertiesBlobFS(jptm, p)
		case common.ELocation.File():
			setPropertiesFile(jptm, p)
		default:
			panic("Attempting set-properties on invalid location: " + to.From().String())
		}
	})
	jptm.ScheduleChunks(cf)
}

func setPropertiesBlob(jptm IJobPartTransferMgr, p pipeline.Pipeline) {
	info := jptm.Info()
	// Get the source blob url of blob to set properties on
	u, _ := url.Parse(info.Source)
	srcBlobURL := azblob.NewBlobURL(*u, p)

	// Internal function which checks the transfer status and logs the msg respectively.
	// Sets the transfer status and Reports Transfer as Done.
	// Internal function is created to avoid redundancy of the above steps from several places in the api.
	transferDone := func(status common.TransferStatus, err error) {
		if status == common.ETransferStatus.Failed() {
			jptm.LogError(info.Source, "SET-PROPERTIES ERROR ", err)
		} else {
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("SET-PROPERTIES SUCCESSFUL: %s", strings.Split(info.Destination, "?")[0]))
		}

		jptm.SetStatus(status)
		jptm.ReportTransferDone()
	}

	rehydratePriority := jptm.Info().RehydratePriority
	fmt.Println("Rehydrate priority unused- " + rehydratePriority) //this line is a personal reminder and will be removed when https://github.com/Azure/azure-storage-blob-go/pull/319 is merged
	blockBlobTier, pageBlobTier := jptm.BlobTiers()
	srcBlobType := jptm.Info().SrcBlobType
	SetPropertiesAPIOption := jptm.SetPropertiesAPIOption()

	if SetPropertiesAPIOption.ShouldTransferTier() {
		var err error = nil
		switch srcBlobType {
		case azblob.BlobBlockBlob:
			if blockBlobTier.ToAccessTierType() != azblob.AccessTierNone {
				_, err = srcBlobURL.SetTier(jptm.Context(), blockBlobTier.ToAccessTierType(), azblob.LeaseAccessConditions{})
			}
		case azblob.BlobPageBlob:
			// todo check if works
			if pageBlobTier.ToAccessTierType() != azblob.AccessTierNone {
				_, err = srcBlobURL.SetTier(jptm.Context(), pageBlobTier.ToAccessTierType(), azblob.LeaseAccessConditions{})
			}
		}
		//todo add more options like priority etc.
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

	if SetPropertiesAPIOption.ShouldTransferMetaData() {
		panic("Not Supported to be setting metadata yet")
	}
}

func setPropertiesBlobFS(jptm IJobPartTransferMgr, p pipeline.Pipeline) {
	info := jptm.Info()
	// Get the source blob url of blob to delete
	u, _ := url.Parse(info.Source)
	srcBlobURL := azbfs.NewFileURL(*u, p)

	// Internal function which checks the transfer status and logs the msg respectively.
	// Sets the transfer status and Report Transfer as Done.
	// Internal function is created to avoid redundancy of the above steps from several places in the api.
	transferDone := func(status common.TransferStatus, err error) {
		if status == common.ETransferStatus.Failed() {
			jptm.LogError(info.Source, "SET-PROPERTIES ERROR ", err)
		} else {
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("SET-PROPERTIES SUCCESSFUL: %s", strings.Split(info.Destination, "?")[0]))
		}

		jptm.SetStatus(status)
		jptm.ReportTransferDone()
	}

	_, err := srcBlobURL.SetAccessControl(jptm.Context(), azbfs.BlobFSAccessControl{})
	if err != nil {
		if strErr, ok := err.(azblob.StorageError); ok {
			// TODO: Do we need to add more conditions?

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

func setPropertiesFile(jptm IJobPartTransferMgr, p pipeline.Pipeline) {
	info := jptm.Info()
	u, _ := url.Parse(info.Source)
	srcFileURL := azfile.NewFileURL(*u, p)

	// Internal function which checks the transfer status and logs the msg respectively.
	// Sets the transfer status and Report Transfer as Done.
	// Internal function is created to avoid redundancy of the above steps from several places in the api.
	transferDone := func(status common.TransferStatus, err error) {
		if status == common.ETransferStatus.Failed() {
			jptm.LogError(info.Source, "SET-PROPERTIES ERROR ", err)
		} else {
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("SET-PROPERTIES SUCCESSFUL: %s", strings.Split(info.Destination, "?")[0]))
		}

		jptm.SetStatus(status)
		jptm.ReportTransferDone()
	}

	_, err := srcFileURL.SetMetadata(jptm.Context(), info.SrcMetadata.ToAzFileMetadata())
	if err != nil {
		if strErr, ok := err.(azblob.StorageError); ok {
			// TODO: Do we need to add more conditions?

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
