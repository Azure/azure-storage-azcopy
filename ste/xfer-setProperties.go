package ste

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func SetProperties(jptm IJobPartTransferMgr, _ pacer) {
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
			setPropertiesBlob(jptm)
		case common.ELocation.BlobFS():
			setPropertiesBlobFS(jptm)
		case common.ELocation.File():
			setPropertiesFile(jptm)
		default:
			panic("Attempting set-properties on invalid location: " + to.From().String())
		}
	})
	jptm.ScheduleChunks(cf)
}

func setPropertiesBlob(jptm IJobPartTransferMgr) {
	info := jptm.Info()
	// Internal function which checks the transfer status and logs the msg respectively.
	// Sets the transfer status and Reports Transfer as Done.
	// Internal function is created to avoid redundancy of the above steps from several places in the api.
	transferDone := func(status common.TransferStatus, err error) {
		if status == common.ETransferStatus.Failed() {
			jptm.LogError(info.Source, "SET-PROPERTIES FAILED with error: ", err)
		} else {
			jptm.Log(common.LogInfo, fmt.Sprintf("SET-PROPERTIES SUCCESSFUL: %s", strings.Split(info.Destination, "?")[0]))
		}

		jptm.SetStatus(status)
		jptm.ResetSourceSize() // sets source size to 0 (made to be used by setProperties command to make number of bytes transferred = 0)
		jptm.ReportTransferDone()
	}

	bsc, err := jptm.SrcServiceClient().BlobServiceClient()
	if err != nil {
		transferDone(common.ETransferStatus.Failed(), err)
		return
	}

	srcBlobClient := bsc.NewContainerClient(jptm.Info().SrcContainer).NewBlobClient(info.SrcFilePath)

	PropertiesToTransfer := jptm.PropertiesToTransfer()
	_, metadata, blobTags, _ := jptm.ResourceDstData(nil)

	if PropertiesToTransfer.ShouldTransferTier() {
		rehydratePriority := info.RehydratePriority
		blockBlobTier, pageBlobTier := jptm.BlobTiers()

		var err error = nil
		if jptm.Info().SrcBlobType == blob.BlobTypeBlockBlob && blockBlobTier != common.EBlockBlobTier.None() && ValidateTier(jptm, to.Ptr(blockBlobTier.ToAccessTierType()), srcBlobClient, jptm.Context(), true) {
			_, err = srcBlobClient.SetTier(jptm.Context(), blockBlobTier.ToAccessTierType(),
				&blob.SetTierOptions{RehydratePriority: &rehydratePriority})
		}
		// cannot return true for >1, therefore only one of these will run
		if jptm.Info().SrcBlobType == blob.BlobTypePageBlob && pageBlobTier != common.EPageBlobTier.None() && ValidateTier(jptm, to.Ptr(pageBlobTier.ToAccessTierType()), srcBlobClient, jptm.Context(), true) {
			_, err = srcBlobClient.SetTier(jptm.Context(), pageBlobTier.ToAccessTierType(),
				&blob.SetTierOptions{RehydratePriority: &rehydratePriority})
		}

		if err != nil {
			errorHandlerForXferSetProperties(err, jptm, transferDone)
			return
		}
		// don't mark it a success just yet, because more properties might need to be changed
	}

	if PropertiesToTransfer.ShouldTransferMetaData() {
		_, err := srcBlobClient.SetMetadata(jptm.Context(), metadata, nil)
		//TODO the canonical thing in this is changing key value to upper case. How to go around it?
		if err != nil {
			errorHandlerForXferSetProperties(err, jptm, transferDone)
			return
		}
	}
	if PropertiesToTransfer.ShouldTransferBlobTags() {
		_, err := srcBlobClient.SetTags(jptm.Context(), blobTags, nil)
		if err != nil {
			errorHandlerForXferSetProperties(err, jptm, transferDone)
			return
		}
	}
	// marking it a successful flow, as no property has resulted in err != nil
	transferDone(common.ETransferStatus.Success(), nil)
}

func setPropertiesBlobFS(jptm IJobPartTransferMgr) {
	info := jptm.Info()
	// Internal function which checks the transfer status and logs the msg respectively.
	// Sets the transfer status and Report Transfer as Done.
	// Internal function is created to avoid redundancy of the above steps from several places in the api.
	transferDone := func(status common.TransferStatus, err error) {
		if status == common.ETransferStatus.Failed() {
			jptm.LogError(info.Source, "SET-PROPERTIES ERROR ", err)
		} else {
			jptm.Log(common.LogInfo, fmt.Sprintf("SET-PROPERTIES SUCCESSFUL: %s", strings.Split(info.Destination, "?")[0]))
		}

		jptm.SetStatus(status)
		jptm.ResetSourceSize() // sets source size to 0 (made to be used by setProperties command to make number of bytes transferred = 0)
		jptm.ReportTransferDone()
	}

	bsc, err := jptm.SrcServiceClient().BlobServiceClient()
	if err != nil {
		transferDone(common.ETransferStatus.Failed(), err)
		return
	}

	srcBlobClient := bsc.NewContainerClient(info.SrcContainer).NewBlobClient(info.SrcFilePath)

	PropertiesToTransfer := jptm.PropertiesToTransfer()
	_, metadata, blobTags, _ := jptm.ResourceDstData(nil)

	if PropertiesToTransfer.ShouldTransferTier() {
		rehydratePriority := info.RehydratePriority
		_, pageBlobTier := jptm.BlobTiers()
		var err error = nil
		if ValidateTier(jptm, to.Ptr(pageBlobTier.ToAccessTierType()), srcBlobClient, jptm.Context(), false) {
			_, err = srcBlobClient.SetTier(jptm.Context(), pageBlobTier.ToAccessTierType(),
				&blob.SetTierOptions{RehydratePriority: &rehydratePriority})
		}

		if err != nil {
			errorHandlerForXferSetProperties(err, jptm, transferDone)
			return
		}
		// don't mark it a success just yet, because more properties might need to be changed
	}

	if PropertiesToTransfer.ShouldTransferMetaData() {
		_, err := srcBlobClient.SetMetadata(jptm.Context(), metadata, nil)
		if err != nil {
			errorHandlerForXferSetProperties(err, jptm, transferDone)
			return
		}
	}
	if PropertiesToTransfer.ShouldTransferBlobTags() {
		_, err := srcBlobClient.SetTags(jptm.Context(), blobTags, nil)
		if err != nil {
			errorHandlerForXferSetProperties(err, jptm, transferDone)
			return
		}
	}

	// marking it a successful flow, as no property has resulted in err != nil
	transferDone(common.ETransferStatus.Success(), nil)
}

func setPropertiesFile(jptm IJobPartTransferMgr) {
	info := jptm.Info()
	// Internal function which checks the transfer status and logs the msg respectively.
	// Sets the transfer status and Report Transfer as Done.
	// Internal function is created to avoid redundancy of the above steps from several places in the api.
	transferDone := func(status common.TransferStatus, err error) {
		if status == common.ETransferStatus.Failed() {
			jptm.LogError(info.Source, "SET-PROPERTIES ERROR ", err)
		} else {
			jptm.Log(common.LogInfo, fmt.Sprintf("SET-PROPERTIES SUCCESSFUL: %s", strings.Split(info.Destination, "?")[0]))
		}

		jptm.SetStatus(status)
		jptm.ResetSourceSize() // sets source size to 0 (made to be used by setProperties command to make number of bytes transferred = 0)
		jptm.ReportTransferDone()
	}

	s, err := jptm.SrcServiceClient().FileServiceClient()
	if err != nil {
		transferDone(common.ETransferStatus.Failed(), err)
		return
	}

	srcFileClient := s.NewShareClient(jptm.Info().SrcContainer).NewRootDirectoryClient().NewFileClient(jptm.Info().SrcFilePath)
	PropertiesToTransfer := jptm.PropertiesToTransfer()
	_, metadata, _, _ := jptm.ResourceDstData(nil)

	if PropertiesToTransfer.ShouldTransferTier() {
		// this case should have been picked up by front end and given error (changing tier is not available for File Storage)
		err := fmt.Errorf("trying to change tier of file")
		transferDone(common.ETransferStatus.Failed(), err)
	}
	if PropertiesToTransfer.ShouldTransferMetaData() {
		_, err := srcFileClient.SetMetadata(jptm.Context(), &file.SetMetadataOptions{Metadata: metadata})
		if err != nil {
			errorHandlerForXferSetProperties(err, jptm, transferDone)
			return
		}
	}
	// TAGS NOT AVAILABLE FOR FILES
	transferDone(common.ETransferStatus.Success(), nil)
}

func errorHandlerForXferSetProperties(err error, jptm IJobPartTransferMgr, transferDone func(status common.TransferStatus, err error)) {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) && respErr.StatusCode == http.StatusForbidden {
		// If the status code was 403, it means there was an authentication error, and we exit.
		// User can resume the job if completely ordered with a new sas.
		errMsg := fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error())
		jptm.Log(common.LogError, errMsg)
		common.GetLifecycleMgr().OnError(errMsg)
		// TODO : Migrate on azfile
	}

	// in all other cases, make the transfer as failed
	transferDone(common.ETransferStatus.Failed(), err)
}
