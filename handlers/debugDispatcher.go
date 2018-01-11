package handlers

import (
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-azcopy/common"
	"encoding/json"
	"fmt"
	"runtime"
	"time"
	"net/http"
	"bytes"
	"io/ioutil"
	"errors"
)

type coordinatorScheduleFunc func(*common.CopyJobPartOrder)

func generateCoordinatorScheduleFunc() coordinatorScheduleFunc{
	//coordinatorChannel, execEngineChannels := ste.InitializedChannels()
	//ste.InitializeExecutionEngine(execEngineChannels)
	//runtime.GOMAXPROCS(4)
	ste.InitializeSTE()

	return func(jobPartOrder *common.CopyJobPartOrder) {
		order, _ := json.MarshalIndent(jobPartOrder, "", "  ")
		fmt.Println("=============================================================")
		fmt.Println("The following job part order was generated:")
		fmt.Println(string(order))
		sendUploadRequestToSTE(order)
	}
}

func sendUploadRequestToSTE(payload []byte) {
	fmt.Println("Sending Upload Request TO STE")
	url := "http://localhost:1337"

	res, err := http.Post(url, "application/json; charset=utf-8", bytes.NewBuffer(payload))
	if err != nil {
		panic(err)
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println("Response to request", res.Status, " ", body)
}


func fetchJobStatus(jobId string) (common.Status){
	url := "http://localhost:1337"
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil{
		panic(err)
	}
	q := req.URL.Query()
	q.Add("type", "JobStatus")
	q.Add("GUID", jobId)
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil{
		panic(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		fmt.Println("request failed with status ", resp.Status)
		panic(err)
	}

	defer resp.Body.Close()
	body, err:= ioutil.ReadAll(resp.Body)
	if err != nil{
		panic(err)
	}
	var summary common.JobProgressSummary
	json.Unmarshal(body, &summary)
	fmt.Println("-----------------Progress Summary for JobId ", jobId," ------------------")
	fmt.Println("Total Number of Transfer ", summary.TotalNumberOfTransfer)
	fmt.Println("Total Number of Transfer Completed ", summary.TotalNumberofTransferCompleted)
	fmt.Println("Total Number of Transfer Failed ", summary.TotalNumberofFailedTransfer)
	fmt.Println("Has the final part been ordered ", summary.CompleteJobOrdered)
	fmt.Println("Progress of Job in terms of Perecentage ", summary.PercentageProgress)
	for index := 0; index < len(summary.FailedTransfers); index++ {
		message := fmt.Sprintf("transfer-%d	source: %s	destination: %s", index, summary.FailedTransfers[index].Src, summary.FailedTransfers[index].Dst)
		fmt.Println(message)
	}
	return summary.JobStatus
}