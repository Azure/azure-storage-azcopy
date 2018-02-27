package handlers

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
)

// handles the cancel command
// dispatches the cancel Job order to the storage engine
func HandleCancelCommand(jobIdString string) {
	url := "http://localhost:1337"

	// parsing the given JobId to validate its format correctness
	jobId, err := common.ParseUUID(jobIdString)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobId")
		return
	}

	httpClient := common.NewHttpClient(url)

	responseBytes := httpClient.Send("cancel", jobId)

	// If the request is not valid or it is not processed by transfer engine, it does not returns Http StatusAccepted
	if len(responseBytes) == 0 {
		return
	}
	fmt.Println(fmt.Sprintf("Job %s cancelled successfully", jobId))
}

// handles the pause command
// dispatches the pause Job order to the storage engine
func HandlePauseCommand(jobIdString string) {
	url := "http://localhost:1337"
	client := common.NewHttpClient(url)

	// parsing the given JobId to validate its format correctness
	jobId, err := common.ParseUUID(jobIdString)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobId")
		return
	}

	responseBytes := client.Send("pause", jobId)

	// If the request is not valid or it is not processed by transfer engine, it does not returns Http StatusAccepted
	if len(responseBytes) == 0 {
		return
	}

	fmt.Println(fmt.Sprintf("Job %s paused successfully", jobId))
}

// handles the resume command
// dispatches the resume Job order to the storage engine
func HandleResumeCommand(jobIdString string) {
	url := "http://localhost:1337"
	client := common.NewHttpClient(url)

	// parsing the given JobId to validate its format correctness
	jobId, err := common.ParseUUID(jobIdString)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobId")
		return
	}

	responseBytes := client.Send("resume", jobId)

	// If the request is not valid or it is not processed by transfer engine, it does not returns Http StatusAccepted
	if len(responseBytes) == 0 {
		return
	}

	fmt.Println(fmt.Sprintf("Job %s resume successfully", jobId))
}
