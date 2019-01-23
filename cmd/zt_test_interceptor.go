package cmd

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"time"
)

// the interceptor gathers/saves the job part orders for validation
type interceptor struct {
	transfers   []common.CopyTransfer
	lastRequest interface{}
}

func (i *interceptor) intercept(cmd common.RpcCmd, request interface{}, response interface{}) {
	switch cmd {
	case common.ERpcCmd.CopyJobPartOrder():
		// cache the transfers
		copyRequest := *request.(*common.CopyJobPartOrderRequest)
		i.transfers = append(i.transfers, copyRequest.Transfers...)
		i.lastRequest = request

		// mock the result
		*(response.(*common.CopyJobPartOrderResponse)) = common.CopyJobPartOrderResponse{JobStarted: true}

	case common.ERpcCmd.ListSyncJobSummary():
		copyRequest := *request.(*common.CopyJobPartOrderRequest)

		// fake the result saying that job is already completed
		// doing so relies on the mockedLifecycleManager not quitting the application
		*(response.(*common.ListSyncJobSummaryResponse)) = common.ListSyncJobSummaryResponse{
			Timestamp:          time.Now().UTC(),
			JobID:              copyRequest.JobID,
			ErrorMsg:           "",
			JobStatus:          common.EJobStatus.Completed(),
			CompleteJobOrdered: true,
			FailedTransfers:    []common.TransferDetail{},
		}
	case common.ERpcCmd.ListJobs():
	case common.ERpcCmd.ListJobSummary():
	case common.ERpcCmd.ListJobTransfers():
	case common.ERpcCmd.PauseJob():
	case common.ERpcCmd.CancelJob():
	case common.ERpcCmd.ResumeJob():
	case common.ERpcCmd.GetJobFromTo():
		fallthrough
	default:
		panic("RPC mock not implemented")
	}
}

func (i *interceptor) init() {
	// mock out the lifecycle manager so that it can no longer terminate the application
	glcm = mockedLifecycleManager{}
}

func (i *interceptor) reset() {
	i.transfers = make([]common.CopyTransfer, 0)
	i.lastRequest = nil
}

// this lifecycle manager substitute does not perform any action
type mockedLifecycleManager struct{}

func (mockedLifecycleManager) Progress(string)                                          {}
func (mockedLifecycleManager) Info(string)                                              {}
func (mockedLifecycleManager) Prompt(string) string                                     { return "" }
func (mockedLifecycleManager) Exit(string, common.ExitCode)                             {}
func (mockedLifecycleManager) Error(string)                                             {}
func (mockedLifecycleManager) SurrenderControl()                                        {}
func (mockedLifecycleManager) InitiateProgressReporting(common.WorkController, bool)    {}
func (mockedLifecycleManager) GetEnvironmentVariable(common.EnvironmentVariable) string { return "" }
