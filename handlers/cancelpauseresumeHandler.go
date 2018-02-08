package handlers

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"io/ioutil"
	"net/http"
	"encoding/json"
)

// handles the cancel command
// dispatches the cancel Job order to the storage engine
func HandleCancelCommand(jobIdString string) {
	url := "http://localhost:1337"
	client := &http.Client{}

	jobId, err := common.ParseUUID(jobIdString)
	if err != nil{
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobId")
		return
	}

	marshaledJobId, err := json.Marshal(jobId)
	if err != nil{
		fmt.Println("error marshalling the jobId ", jobIdString)
		return
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	q := req.URL.Query()
	// Type defines the type of GET request processed by the transfer engine
	q.Add("Type", "cancel")
	// command defines the actual list command serialized to byte array
	q.Add("jobId", string(marshaledJobId))
	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	// If the request is not valid or it is not processed by transfer engine, it does not returns Http StatusAccepted
	if resp.StatusCode != http.StatusAccepted {
		errorMessage := fmt.Sprintf("request failed with status %s and message %s", resp.Status, string(body))
		fmt.Println(errorMessage)
		return
	}
	fmt.Println(fmt.Sprintf("Job %s cancelled successfully", jobId))
}

// handles the pause command
// dispatches the pause Job order to the storage engine
func HandlePauseCommand(jobIdString string) {
	url := "http://localhost:1337"
	client := &http.Client{}

	jobId, err := common.ParseUUID(jobIdString)
	if err != nil{
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobId")
		return
	}

	marshaledJobId, err := json.Marshal(jobId)
	if err != nil{
		fmt.Println("error marshalling the jobId ", jobIdString)
		return
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	q := req.URL.Query()
	// Type defines the type of GET request processed by the transfer engine
	q.Add("Type", "pause")
	// command defines the actual list command serialized to byte array
	q.Add("jobId", string(marshaledJobId))
	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	// If the request is not valid or it is not processed by transfer engine, it does not returns Http StatusAccepted
	if resp.StatusCode != http.StatusAccepted {
		errorMessage := fmt.Sprintf("request failed with status %s and message %s", resp.Status, string(body))
		fmt.Println(errorMessage)
		return
	}
	fmt.Println(fmt.Sprintf("Job %s paused successfully", jobId))
}

// handles the resume command
// dispatches the resume Job order to the storage engine
func HandleResumeCommand(jobIdString string) {
	url := "http://localhost:1337"
	client := &http.Client{}

	jobId, err := common.ParseUUID(jobIdString)
	if err != nil{
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobId")
		return
	}

	marshaledJobId, err := json.Marshal(jobId)
	if err != nil{
		fmt.Println("error marshalling the jobId ", jobIdString)
		return
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	q := req.URL.Query()
	// Type defines the type of GET request processed by the transfer engine
	q.Add("Type", "resume")
	// command defines the actual list command serialized to byte array
	q.Add("jobId", string(marshaledJobId))
	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	// If the request is not valid or it is not processed by transfer engine, it does not returns Http StatusAccepted
	if resp.StatusCode != http.StatusAccepted {
		errorMessage := fmt.Sprintf("request failed with status %s and message %s", resp.Status, string(body))
		fmt.Println(errorMessage)
		return
	}
	fmt.Println(fmt.Sprintf("Job %s resume successfully", jobId))
}
