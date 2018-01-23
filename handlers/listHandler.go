package handlers

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"net/http"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"errors"
	"github.com/Azure/azure-storage-azcopy/ste"
)

// handles the list command
// dispatches the list order to the storage engine
func HandleListCommand(commandLineInput common.ListCmdArgsAndFlags) {
	listOrder := common.ListJobPartsTransfers{}
	listOrder.JobId =  common.JobID(commandLineInput.JobId)
	listOrder.ExpectedTransferStatus = common.TransferStatusStringToStatusCode(commandLineInput.TransferStatus)
	go ste.InitializeSTE()
	marshalledCommand , err := json.Marshal(listOrder)
	if err != nil{
		panic(err)
	}
	url := "http://localhost:1337"
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil{
		panic(err)
	}
	q := req.URL.Query()
	q.Add("command", string(marshalledCommand))
	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)
	if err != nil{
		panic(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		fmt.Println("request failed with status ", resp.Status)
		panic(errors.New(fmt.Sprintf("request failed with status %s", resp.Status)))
	}

	defer resp.Body.Close()
	body, err:= ioutil.ReadAll(resp.Body)
	if err != nil{
		panic(err)
	}
	if listOrder.JobId == ""{
		PrintExistingJobIds(body)
	}else if commandLineInput.TransferStatus == "" {
		PrintJobProgressSummary(body, commandLineInput.JobId)
	}else{
		PrintJobTransfers(body, commandLineInput.JobId)
	}
}


func PrintExistingJobIds(data []byte){
	var jobs common.ExistingJobDetails
	err := json.Unmarshal(data, &jobs)
	if err != nil{
		panic(err)
	}
	fmt.Println("Existing Jobs ")
	for index := 0; index < len(jobs.JobIds); index++{
		fmt.Println(jobs.JobIds[index])
	}
}

func PrintJobTransfers(data []byte, jobId string){
	var transfers common.TransfersStatus
	err := json.Unmarshal(data, &transfers)
	if err != nil{
		panic(err)
	}
	fmt.Println(fmt.Sprintf("----------- Transfers for JobId %s -----------", jobId))
	for index := 0; index < len(transfers.Status); index++{
		fmt.Println(fmt.Sprintf("transfer source: %s destination: %s status %s", transfers.Status[index].Src, transfers.Status[index].Dst,
																common.TransferStatusCodeToString(transfers.Status[index].TransferStatus)))
	}
}

func PrintJobProgressSummary(summaryData []byte, jobId string) (status common.Status){
	var summary common.JobProgressSummary
	err := json.Unmarshal(summaryData, &summary)
	if err != nil{
		panic(errors.New(fmt.Sprintf("error unmarshaling the progress summary. Failed with error %s", err.Error())))
		return
	}
	fmt.Println(fmt.Sprintf("--------------- Progress Summary for Job %s ---------------", jobId))
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