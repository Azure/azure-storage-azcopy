package handlers

import (
	"net/http"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"io/ioutil"
)

// handles the cancel command
// dispatches the cancel Job order to the storage engine
func HandleCancelCommand(jobId common.JobID) {
	url := "http://localhost:1337"
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	q := req.URL.Query()
	// Type defines the type of GET request processed by the transfer engine
	q.Add("Type", "cancel")
	// command defines the actual list command serialized to byte array
	q.Add("jobId", string(jobId))
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
func HandlePauseCommand(jobId common.JobID) {
	url := "http://localhost:1337"
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	q := req.URL.Query()
	// Type defines the type of GET request processed by the transfer engine
	q.Add("Type", "pause")
	// command defines the actual list command serialized to byte array
	q.Add("jobId", string(jobId))
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
func HandleResumeCommand(jobId common.JobID) {
	url := "http://localhost:1337"
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	q := req.URL.Query()
	// Type defines the type of GET request processed by the transfer engine
	q.Add("Type", "resume")
	// command defines the actual list command serialized to byte array
	q.Add("jobId", string(jobId))
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