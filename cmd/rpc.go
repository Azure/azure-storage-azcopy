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

package cmd

import (
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

// Global singleton for sending RPC requests from the frontend to the STE
var Rpc = func(cmd common.RpcCmd, request interface{}, response interface{}) {
	err := inprocSend(cmd, request, response)
	common.PanicIfErr(err)
}

// Send method on HttpClient sends the data passed in the interface for given command type to the client url
func inprocSend(rpcCmd common.RpcCmd, requestData interface{}, responseData interface{}) error {
	switch rpcCmd {
	case common.ERpcCmd.CopyJobPartOrder():
		*(responseData.(*common.CopyJobPartOrderResponse)) = jobsAdmin.ExecuteNewCopyJobPartOrder(*requestData.(*common.CopyJobPartOrderRequest))

	case common.ERpcCmd.GetJobLCMWrapper():
		*(responseData.(*common.LifecycleMgr)) = jobsAdmin.GetJobLCMWrapper(*requestData.(*common.JobID))

	case common.ERpcCmd.ListJobs():
		*(responseData.(*common.ListJobsResponse)) = jobsAdmin.ListJobs(requestData.(common.JobStatus))

	case common.ERpcCmd.ListJobSummary():
		*(responseData.(*common.ListJobSummaryResponse)) = jobsAdmin.GetJobSummary(*requestData.(*common.JobID))

	case common.ERpcCmd.ListJobTransfers():
		*(responseData.(*common.ListJobTransfersResponse)) = jobsAdmin.ListJobTransfers(requestData.(common.ListJobTransfersRequest))

	case common.ERpcCmd.PauseJob():
		*(responseData.(*common.CancelPauseResumeResponse)) = jobsAdmin.CancelPauseJobOrder(requestData.(common.JobID), common.EJobStatus.Paused())

	case common.ERpcCmd.CancelJob():
		*(responseData.(*common.CancelPauseResumeResponse)) = jobsAdmin.CancelPauseJobOrder(requestData.(common.JobID), common.EJobStatus.Cancelling())

	case common.ERpcCmd.ResumeJob():
		*(responseData.(*common.CancelPauseResumeResponse)) = jobsAdmin.ResumeJobOrder(*requestData.(*common.ResumeJobRequest))

	case common.ERpcCmd.GetJobDetails():
		*(responseData.(*common.GetJobDetailsResponse)) = jobsAdmin.GetJobDetails(*requestData.(*common.GetJobDetailsRequest))

	default:
		panic(fmt.Errorf("unrecognized RpcCmd: %q", rpcCmd.String()))
	}
	return nil
}
