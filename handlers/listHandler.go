// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"math"
)

// handles the list command
// dispatches the list order to theZiyi Wang storage engine
func HandleListCommand(commandLineInput common.ListCmdArgsAndFlags) {
	listOrder := common.ListRequest{}

	// checking if the jobId passed is valid or not
	if commandLineInput.JobId != "" {
		listOrder.JobId = common.JobID(common.ParseUUID(commandLineInput.JobId)
	} else {
		listOrder.JobId = ""
	}

	// if the expected status is given by User, then it is converted to the respective Transfer status code
	if commandLineInput.OfStatus != "" {
		listOrder.ExpectedTransferStatus = common.TransferStatusStringToCode(commandLineInput.OfStatus)
	} else {
		// if the expected status is not given by user, it is set to 255
		listOrder.ExpectedTransferStatus = math.MaxUint32
	}
	// converted the list order command to json byte array

	url := "http://localhost:1337"
	httpClient := common.NewHttpClient(url)

	responseBytes := httpClient.Send("list", listOrder)

	// If the request is not valid or it is not processed by transfer engine, it does not returns Http StatusAccepted
	if len(responseBytes) == 0 {
		return
	}

	// list Order command requested the list of existing jobs
	if commandLineInput.JobId == "" {
		PrintExistingJobIds(responseBytes)
	} else if commandLineInput.OfStatus == "" { //list Order command requested the progress summary of an existing job
		PrintJobProgressSummary(responseBytes, commandLineInput.JobId)
	} else { //list Order command requested the list of specific transfer of an existing job
		PrintJobTransfers(responseBytes, commandLineInput.JobId)
	}
}

// PrintExistingJobIds prints the response of listOrder command when listOrder command requested the list of existing jobs
func PrintExistingJobIds(data []byte) {
	var jobs common.ExistingJobDetails
	err := json.Unmarshal(data, &jobs)
	if err != nil {
		panic(err)
	}
	fmt.Println("Existing Jobs ")
	for index := 0; index < len(jobs.JobIds); index++ {
		fmt.Println(jobs.JobIds[index].String())
	}
}

// PrintJobTransfers prints the response of listOrder command when list Order command requested the list of specific transfer of an existing job
func PrintJobTransfers(data []byte, jobId string) {
	var transfers common.TransfersDetail
	err := json.Unmarshal(data, &transfers)
	if err != nil {
		panic(err)
	}
	fmt.Println(fmt.Sprintf("----------- Transfers for JobId %s -----------", jobId))
	for index := 0; index < len(transfers.Details); index++ {
		fmt.Println(fmt.Sprintf("transfer--> source: %s destination: %s status %s", transfers.Details[index].Src, transfers.Details[index].Dst,
			transfers.Details[index].TransferStatus))
	}
}

// PrintJobProgressSummary prints the response of listOrder command when listOrder command requested the progress summary of an existing job
func PrintJobProgressSummary(summaryData []byte, jobId string) {
	var summary common.JobProgressSummary
	err := json.Unmarshal(summaryData, &summary)
	if err != nil {
		panic(fmt.Errorf("error unmarshaling the progress summary. Failed with error %s", err.Error()))
		return
	}
	fmt.Println(fmt.Sprintf("--------------- Progress Summary for Job %s ---------------", jobId))
	fmt.Println("Total Number of Transfer ", summary.TotalNumberOfTransfers)
	fmt.Println("Total Number of Transfer Completed ", summary.TotalNumberofTransferCompleted)
	fmt.Println("Total Number of Transfer Failed ", summary.TotalNumberofFailedTransfer)
	fmt.Println("Has the final part been ordered ", summary.CompleteJobOrdered)
	fmt.Println("Progress of Job in terms of Perecentage ", summary.PercentageProgress)
	for index := 0; index < len(summary.FailedTransfers); index++ {
		message := fmt.Sprintf("transfer-%d	source: %s	destination: %s", index, summary.FailedTransfers[index].Src, summary.FailedTransfers[index].Dst)
		fmt.Println(message)
	}
}
