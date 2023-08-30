package ste

import (
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"net/url"
	"strings"
	"sync"
)

var logBlobFSDeleteWarnOnce = &sync.Once{}
const blobFSDeleteWarning = "Displayed file count will be either 1 or based upon list-of-files entries, and thus inaccurate, as deletes are performed recursively service-side."

func DeleteHNSResource(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer pacer) {
	// If the transfer was cancelled, then report the transfer as done.
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	logBlobFSDeleteWarnOnce.Do(func() {
		jptm.Log(common.LogWarning, blobFSDeleteWarning)
		common.GetLifecycleMgr().Info(blobFSDeleteWarning)
	})

	// schedule the transfer as a chunk, so it will run on the main goroutine pool
	id := common.NewChunkID(jptm.Info().Source, 0, 0)
	cf := createChunkFunc(true, jptm, id, func() {
		doDeleteHNSResource(jptm, p)
	})
	jptm.ScheduleChunks(cf)
}

func doDeleteHNSResource(jptm IJobPartTransferMgr, p pipeline.Pipeline) {
	ctx := jptm.Context()
	info := jptm.Info()

	// parsing should not fail, we've made it this far
	u, err := url.Parse(info.Source)
	if err != nil {
		panic("sanity check: HNS source URI did not parse.")
	}

	recursive := info.BlobFSRecursiveDelete

	transferDone := func(err error) {
		status := common.ETransferStatus.Success()
		if err != nil {
			status = common.ETransferStatus.Failed()
		}

		if status == common.ETransferStatus.Failed() {
			jptm.LogError(info.Source, "DELETE ERROR ", err)
		} else {
			jptm.Log(common.LogInfo, fmt.Sprintf("DELETE SUCCESSFUL: %s", strings.Split(info.Source, "?")[0]))
		}

		jptm.SetStatus(status)
		jptm.ReportTransferDone()
	}


	urlParts := azbfs.NewBfsURLParts(*u)

	// Deleting a filesystem
	if urlParts.DirectoryOrFilePath == "" {
		fsURL := azbfs.NewFileSystemURL(*u, p)

		_, err := fsURL.Delete(ctx)
		transferDone(err)
		return
	}

	// Check if the source is a file or directory
	directoryURL := azbfs.NewDirectoryURL(*u, p)
	props, err := directoryURL.GetProperties(ctx)
	if err != nil {
		transferDone(err)
		return
	}

	if strings.EqualFold(props.XMsResourceType(), "file") {
		fileURL := directoryURL.NewFileUrl()

		_, err := fileURL.Delete(ctx)
		transferDone(err)
	} else {
		// Remove the directory
		marker := ""
		for {
			removeResp, err := directoryURL.Delete(ctx, &marker, recursive)
			if err != nil {
				transferDone(err)
				return
			}

			// Update continuation for next call
			marker = removeResp.XMsContinuation()

			// Break if finished
			if marker == "" {
				break
			}
		}

		transferDone(err)
	}
}