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

//var explainedSkippedRemoveOnce sync.Once

func getBlockBlobTierString(tier common.BlockBlobTier) azblob.AccessTierType {
	switch tier {
	case common.EBlockBlobTier.Hot():
		return azblob.AccessTierHot
	case common.EBlockBlobTier.Cool():
		return azblob.AccessTierCool
	case common.EBlockBlobTier.Archive():
		return azblob.AccessTierArchive
	case common.EBlockBlobTier.None():
		panic("trying to set tier to none")
	default:
		panic("invalid tier type")
	}
}

func getPageBlobTierString(tier common.PageBlobTier) azblob.AccessTierType {
	switch tier {
	case common.EPageBlobTier.P10():
		return azblob.AccessTierP10
	case common.EPageBlobTier.P15():
		return azblob.AccessTierP15
	case common.EPageBlobTier.P20():
		return azblob.AccessTierP20
	case common.EPageBlobTier.P30():
		return azblob.AccessTierP30
	case common.EPageBlobTier.P4():
		return azblob.AccessTierP4
	case common.EPageBlobTier.P40():
		return azblob.AccessTierP40
	case common.EPageBlobTier.P50():
		return azblob.AccessTierP50
	case common.EPageBlobTier.P6():
		return azblob.AccessTierP6
	case common.EPageBlobTier.None():
		panic("trying to set tier to none")
	default:
		panic("Invalid tier type")
	}
}
func SetProperties(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer pacer) {
	// If the transfer was cancelled, then reporting transfer as done and increasing the bytes transferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	// schedule the work as a chunk, so it will run on the main goroutine pool, instead of the
	// smaller "transfer initiation pool", where this code runs.
	id := common.NewChunkID(jptm.Info().Source, 0, 0)
	cf := createChunkFunc(true, jptm, id, func() { //TODO t-iverma should done status be set? The job is async
		to := jptm.FromTo()
		switch to.From() {
		case common.ELocation.Blob():
			setPropertiesBlob(jptm, p)
		case common.ELocation.BlobFS():
			setPropertiesBlobFS(jptm, p)
		case common.ELocation.File():
			setPropertiesFile(jptm, p)
		default:
			panic("Shouldn't have happened")
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

	blockBlobTier, pageBlobTier := jptm.BlobTiers()
	srcBlobType := jptm.Info().SrcBlobType
	_ = srcBlobType
	SetPropertiesAPIOption := jptm.SetPropertiesAPIOption()
	switch SetPropertiesAPIOption {
	case common.ESetPropertiesAPIOption.SetTier():
		var err error = nil
		switch srcBlobType {
		case azblob.BlobBlockBlob:
			_, err = srcBlobURL.SetTier(jptm.Context(), getBlockBlobTierString(blockBlobTier), azblob.LeaseAccessConditions{})
		case azblob.BlobPageBlob:
			_, err = srcBlobURL.SetTier(jptm.Context(), getPageBlobTierString(pageBlobTier), azblob.LeaseAccessConditions{})
		default:
			panic("Invalid blob type")
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
	case common.ESetPropertiesAPIOption.SetMetaData(), common.ESetPropertiesAPIOption.SetTierAndMetaData():
		panic("Not Supported to be setting metadata yet")
	default:
		jptm.Log(pipeline.LogInfo, "No properties were changed because common.ESetPropertiesAPIOption.SetTierAndMetaData() was set to none")
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
