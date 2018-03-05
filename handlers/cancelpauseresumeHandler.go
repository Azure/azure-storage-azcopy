package handlers

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"encoding/json"
)

// handles the cancel command
// dispatches the cancel Job order to the storage engine
func HandleCancelCommand(jobIdString string) {
	// parsing the given JobId to validate its format correctness
	jobId, err := common.ParseUUID(jobIdString)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobId")
		return
	}

	responseBytes, _ := common.Rpc("cancel", jobId)

	var cancelJobResponse common.CancelPauseResumeResponse
	err = json.Unmarshal(responseBytes, &cancelJobResponse)
	if err != nil{
		panic(err)
	}
	if !cancelJobResponse.CancelledPauseResumed {
		fmt.Println(fmt.Sprintf("job cannot be cancelled because %s", cancelJobResponse.ErrorMsg))
		return
	}
	fmt.Println(fmt.Sprintf("Job %s cancelled successfully", jobId))
}

// handles the pause command
// dispatches the pause Job order to the storage engine
func HandlePauseCommand(jobIdString string) {

	// parsing the given JobId to validate its format correctness
	jobId, err := common.ParseUUID(jobIdString)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobId")
		return
	}

	responseBytes, _ := common.Rpc("pause", jobId)

	var pauseJobResponse common.CancelPauseResumeResponse
	err = json.Unmarshal(responseBytes, &pauseJobResponse)
	if err != nil{
		panic(err)
	}
	if !pauseJobResponse.CancelledPauseResumed {
		fmt.Println(fmt.Sprintf("job cannot be paused because %s", pauseJobResponse.ErrorMsg))
		return
	}
	fmt.Println(fmt.Sprintf("Job %s paused successfully", jobId))
}

// handles the resume command
// dispatches the resume Job order to the storage engine
func HandleResumeCommand(jobIdString string) {
	// parsing the given JobId to validate its format correctness
	jobId, err := common.ParseUUID(jobIdString)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobId")
		return
	}

	responseBytes, _ := common.Rpc("resume", jobId)
	var resumeJobResponse common.CancelPauseResumeResponse

	err = json.Unmarshal(responseBytes, &resumeJobResponse)
	if err != nil{
		panic(err)
	}
	if !resumeJobResponse.CancelledPauseResumed{
		fmt.Println(fmt.Sprintf("job cannot be resumed because %s", resumeJobResponse.ErrorMsg))
		return
	}
	fmt.Println(fmt.Sprintf("Job %s resume successfully", jobId))
}
