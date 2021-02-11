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
	"os"

	"github.com/Azure/azure-storage-azcopy/common"
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
		if len(i.transfers) != 0 || !copyRequest.IsFinalPart {
			*(response.(*common.CopyJobPartOrderResponse)) = common.CopyJobPartOrderResponse{JobStarted: true}
		} else {
			*(response.(*common.CopyJobPartOrderResponse)) = common.CopyJobPartOrderResponse{JobStarted: false, ErrorMsg: common.ECopyJobPartOrderErrorType.NoTransfersScheduledErr()}
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
	glcm = &mockedLifecycleManager{
		infoLog: make(chan string, 5000),
	}
}

func (i *interceptor) reset() {
	i.transfers = make([]common.CopyTransfer, 0)
	i.lastRequest = nil
}

// this lifecycle manager substitute does not perform any action
type mockedLifecycleManager struct {
	infoLog     chan string
	errorLog    chan string
	progressLog chan string
	exitLog     chan string
}

func (m *mockedLifecycleManager) Progress(o common.OutputBuilder) {
	select {
	case m.progressLog <- o(common.EOutputFormat.Text()):
	default:
	}
}
func (*mockedLifecycleManager) Init(common.OutputBuilder) {}
func (m *mockedLifecycleManager) Info(msg string) {
	select {
	case m.infoLog <- msg:
	default:
	}
}
func (*mockedLifecycleManager) Prompt(message string, details common.PromptDetails) common.ResponseOption {
	return common.EResponseOption.Default()
}
func (m *mockedLifecycleManager) Exit(o common.OutputBuilder, e common.ExitCode) {
	select {
	case m.exitLog <- o(common.EOutputFormat.Text()):
	default:
	}
}
func (m *mockedLifecycleManager) Error(msg string) {
	select {
	case m.errorLog <- msg:
	default:
	}
}
func (*mockedLifecycleManager) SurrenderControl()                               {}
func (*mockedLifecycleManager) RegisterCloseFunc(func())                        {}
func (mockedLifecycleManager) AllowReinitiateProgressReporting()                {}
func (*mockedLifecycleManager) InitiateProgressReporting(common.WorkController) {}
func (*mockedLifecycleManager) ClearEnvironmentVariable(env common.EnvironmentVariable) {
	_ = os.Setenv(env.Name, "")
}
func (*mockedLifecycleManager) GetEnvironmentVariable(env common.EnvironmentVariable) string {
	value := os.Getenv(env.Name)
	if value == "" {
		return env.DefaultValue
	}
	return value
}
func (*mockedLifecycleManager) SetOutputFormat(common.OutputFormat) {}
func (*mockedLifecycleManager) EnableInputWatcher()                 {}
func (*mockedLifecycleManager) EnableCancelFromStdIn()              {}
func (*mockedLifecycleManager) AddUserAgentPrefix(userAgent string) string {
	return userAgent
}

func (*mockedLifecycleManager) E2EAwaitContinue() {
	// not implemented in mocked version
}

func (*mockedLifecycleManager) E2EAwaitAllowOpenFiles() {
	// not implemented in mocked version
}

func (*mockedLifecycleManager) E2EEnableAwaitAllowOpenFiles(_ bool) {
	// not implemented in mocked version
}

func (*mockedLifecycleManager) GatherAllLogs(channel chan string) (result []string) {
	close(channel)
	for line := range channel {
		fmt.Println(line)
		result = append(result, line)
	}

	return
}

type dummyProcessor struct {
	record []storedObject
}

func (d *dummyProcessor) process(storedObject storedObject) (err error) {
	d.record = append(d.record, storedObject)
	return
}

func (d *dummyProcessor) countFilesOnly() int {
	n := 0
	for _, x := range d.record {
		if x.entityType == common.EEntityType.File() {
			n++
		}
	}
	return n
}
