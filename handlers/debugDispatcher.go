package handlers

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"encoding/json"
	"fmt"
	"net/http"
	"bytes"
	"io/ioutil"
	"github.com/Azure/azure-storage-azcopy/ste"
	"math"
)

type coordinatorScheduleFunc func(*common.CopyJobPartOrder)

func generateCoordinatorScheduleFunc() coordinatorScheduleFunc{
	//coordinatorChannel, execEngineChannels := ste.InitializedChannels()
	//ste.InitializeExecutionEngine(execEngineChannels)
	//runtime.GOMAXPROCS(4)
	go ste.InitializeSTE()

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
	lsCommand := common.ListJobPartsTransfers{JobId:common.JobID(jobId),ExpectedTransferStatus:math.MaxUint8}
	lsCommandMarshalled, err := json.Marshal(lsCommand)
	if err != nil{
		panic(err)
	}
	q := req.URL.Query()
	q.Add("command", string(lsCommandMarshalled))
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
	return PrintJobProgressSummary(body, jobId)
}