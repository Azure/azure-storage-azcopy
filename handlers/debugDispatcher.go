package handlers

import (
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	tm "github.com/buger/goterm"
	"math"
)

type coordinatorScheduleFunc func(*common.CopyJobPartOrderRequest)

func generateCoordinatorScheduleFunc() coordinatorScheduleFunc {
	//time.Sleep(time.Second * 2)

	return func(jobPartOrder *common.CopyJobPartOrderRequest) {
		sendJobPartOrderToSTE(jobPartOrder)
	}
}

func sendJobPartOrderToSTE(payload *common.CopyJobPartOrderRequest) {
	url := "http://localhost:1337"
	httpClient := common.NewHttpClient(url)

	responseBytes := httpClient.Send("copy", payload)

	// If the request is not valid or it is not processed by transfer engine, it does not returns Http StatusAccepted
	if len(responseBytes) == 0 {
		return
	}
}

func fetchJobStatus(jobId string) string {
	url := "http://localhost:1337"
	client := common.NewHttpClient(url)

	lsCommand := common.ListRequest{JobId: jobId, ExpectedTransferStatus: math.MaxUint32}

	responseBytes := client.Send("list", lsCommand)

	if len(responseBytes) == 0 {
		return ""
	}
	var summary common.JobProgressSummary
	json.Unmarshal(responseBytes, &summary)

	tm.Clear()
	tm.MoveCursor(1, 1)

	fmt.Println("----------------- Progress Summary for JobId ", jobId, "------------------")
	tm.Println("Total Number of Transfers: ", summary.TotalNumberOfTransfers)
	tm.Println("Total Number of Transfers Completed: ", summary.TotalNumberofTransferCompleted)
	tm.Println("Total Number of Transfers Failed: ", summary.TotalNumberofFailedTransfer)
	tm.Println("Job order fully received: ", summary.CompleteJobOrdered)

	//tm.Println(tm.Background(tm.Color(tm.Bold(fmt.Sprintf("Job Progress: %d %%", summary.PercentageProgress)), tm.WHITE), tm.GREEN))
	//tm.Println(tm.Background(tm.Color(tm.Bold(fmt.Sprintf("Realtime Throughput: %f MB/s", summary.ThroughputInBytesPerSeconds/1024/1024)), tm.WHITE), tm.BLUE))

	tm.Println(fmt.Sprintf("Job Progress: %d %%", summary.PercentageProgress))
	tm.Println(fmt.Sprintf("Realtime Throughput: %f MB/s", summary.ThroughputInBytesPerSeconds/1024/1024))

	for index := 0; index < len(summary.FailedTransfers); index++ {
		message := fmt.Sprintf("transfer-%d	source: %s	destination: %s", index, summary.FailedTransfers[index].Src, summary.FailedTransfers[index].Dst)
		fmt.Println(message)
	}
	tm.Flush()

	return summary.JobStatus
}
