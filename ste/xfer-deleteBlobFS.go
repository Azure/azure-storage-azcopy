package ste

import (
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"

	"net/http"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var logBlobFSDeleteWarnOnce = &sync.Once{}

const blobFSDeleteWarning = "Displayed file count will be either 1 or based upon list-of-files entries, and thus inaccurate, as deletes are performed recursively service-side."

func DeleteHNSResource(jptm IJobPartTransferMgr, pacer pacer) {
	// If the transfer was cancelled, then report the transfer as done.
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	logBlobFSDeleteWarnOnce.Do(func() {
		jptm.Log(common.LogWarning, blobFSDeleteWarning)
		common.GetLifecycleMgr().OnInfo(blobFSDeleteWarning)
	})

	// schedule the transfer as a chunk, so it will run on the main goroutine pool
	id := common.NewChunkID(jptm.Info().Source, 0, 0)
	cf := createChunkFunc(true, jptm, id, func() {
		doDeleteHNSResource(jptm)
	})
	jptm.ScheduleChunks(cf)
}

func doDeleteHNSResource(jptm IJobPartTransferMgr) {
	ctx := jptm.Context()
	info := jptm.Info()

	// parsing should not fail, we've made it this far
	datalakeURLParts, err := azdatalake.ParseURL(info.Source)
	if err != nil {
		panic("sanity check: HNS source URI did not parse.")
	}

	recursive := info.BlobFSRecursiveDelete

	transferDone := func(err error) {
		status := common.ETransferStatus.Success()
		if err != nil {
			var respErr *azcore.ResponseError
			if errors.As(err, &respErr) {
				if respErr.StatusCode == http.StatusNotFound {
					// if the delete failed with err 404, i.e resource not found, then mark the transfer as success.
					status = common.ETransferStatus.Success()
				}
				// If the status code was 403, it means there was an authentication error and we exit.
				// User can resume the job if completely ordered with a new sas.
				if respErr.StatusCode == http.StatusForbidden {
					errMsg := fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error())
					jptm.Log(common.LogError, errMsg)
					common.GetLifecycleMgr().Error(errMsg)
				} else {
					// in all other cases, make the transfer as failed
					status = common.ETransferStatus.Failed()
				}
			}
		}

		if status == common.ETransferStatus.Failed() {
			jptm.LogError(info.Source, "DELETE ERROR ", err)
		} else {
			jptm.Log(common.LogInfo, fmt.Sprintf("DELETE SUCCESSFUL: %s", strings.Split(info.Source, "?")[0]))
		}

		jptm.SetStatus(status)
		jptm.ReportTransferDone()
	}

	//fsClient := common.CreateFilesystemClient(info.Source, jptm.CredentialInfo(), jptm.CredentialOpOptions(), jptm.ClientOptions())
	s, err := jptm.SrcServiceClient().DatalakeServiceClient()
	if err != nil {
		transferDone(err)
		return
	}

	c := s.NewFileSystemClient(jptm.Info().SrcContainer)

	// Deleting a filesystem
	if datalakeURLParts.PathName == "" {
		_, err := c.Delete(ctx, nil)
		transferDone(err)
		return
	}

	// Check if the source is a file or directory
	directoryClient := c.NewDirectoryClient(info.SrcFilePath)
	props, err := directoryClient.GetProperties(ctx, nil)
	if err != nil {
		transferDone(err)
		return
	}

	resourceType := common.IffNotNil(props.ResourceType, "")
	if strings.EqualFold(resourceType, "file") {
		fileClient := c.NewFileClient(info.SrcFilePath)

		_, err := fileClient.Delete(ctx, nil)
		transferDone(err)
	} else {
		// Remove the directory
		recursiveContext := common.WithRecursive(ctx, recursive)
		_, err := directoryClient.Delete(recursiveContext, nil)
		transferDone(err)
	}
}
