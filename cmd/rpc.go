package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
)

// Global singleton for sending RPC requests from the frontend to the STE
var Rpc = func(cmd common.RpcCmd, request interface{}, response interface{}) {
	err := inprocSend(cmd, request, response)
	//err := NewHttpClient("").send(cmd, request, response)
	if err != nil {
		panic(err)
	}
}

// Send method on HttpClient sends the data passed in the interface for given command type to the client url
func inprocSend(rpcCmd common.RpcCmd, requestData interface{}, responseData interface{}) error {
	// waiting for JobsAdmin to initialize before the request are send to transfer engine.
	select {
	case <-ste.JobsAdminInitialized:
		break
	default:
		time.Sleep(time.Millisecond * 500)
	}

	switch rpcCmd {
	case common.ERpcCmd.CopyJobPartOrder():
		*(responseData.(*common.CopyJobPartOrderResponse)) = ste.ExecuteNewCopyJobPartOrder(*requestData.(*common.CopyJobPartOrderRequest))

	case common.ERpcCmd.ListJobs():
		*(responseData.(*common.ListJobsResponse)) = ste.ListJobs()

	case common.ERpcCmd.ListJobSummary():
		*(responseData.(*common.ListJobSummaryResponse)) = ste.GetJobSummary(*requestData.(*common.JobID))

	case common.ERpcCmd.ListJobTransfers():
		*(responseData.(*common.ListJobTransfersResponse)) = ste.ListJobTransfers(requestData.(common.ListJobTransfersRequest))

	case common.ERpcCmd.PauseJob():
		responseData = ste.CancelPauseJobOrder(requestData.(common.JobID), common.EJobStatus.Paused())

	case common.ERpcCmd.CancelJob():
		*(responseData.(*common.CancelPauseResumeResponse)) = ste.CancelPauseJobOrder(requestData.(common.JobID), common.EJobStatus.Cancelling())

	case common.ERpcCmd.ResumeJob():
		*(responseData.(*common.CancelPauseResumeResponse)) = ste.ResumeJobOrder(*requestData.(*common.ResumeJobRequest))

	default:
		panic(fmt.Errorf("Unrecognized RpcCmd: %q", rpcCmd.String()))
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// NewHttpClient returns the instance of struct containing an instance of http.client and url
func NewHttpClient(url string) *HTTPClient {
	return &HTTPClient{
		client: &http.Client{},
		url:    url,
	}
}

// todo : use url in case of string
type HTTPClient struct {
	client *http.Client
	url    string
}

// Send method on HttpClient sends the data passed in the interface for given command type to the client url
func (httpClient *HTTPClient) send(rpcCmd common.RpcCmd, requestData interface{}, responseData interface{}) error {
	// Create HTTP request with command in query parameter & request data as JSON payload
	requestJson, err := json.Marshal(requestData)
	if err != nil {
		return fmt.Errorf("error marshalling request payload for command type %q", rpcCmd.String())
	}
	request, err := http.NewRequest("POST", httpClient.url, bytes.NewReader(requestJson))
	// adding the commandType as a query param
	q := request.URL.Query()
	q.Add("commandType", rpcCmd.String())
	request.URL.RawQuery = q.Encode()

	response, err := httpClient.client.Do(request)
	if err != nil {
		return err
	}

	// Read response data, deserialie it and return it (via out responseData parameter) & error
	responseJson, err := ioutil.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return fmt.Errorf("error reading response for the request")
	}
	if err = json.Unmarshal(responseJson, responseData); err != nil {
		panic(err)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
