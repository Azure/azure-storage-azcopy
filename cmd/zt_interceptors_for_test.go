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
)

// the interceptor gathers/saves the job part orders for validation
type interceptor struct {
	transfers []common.CopyTransfer
}

func (i *interceptor) intercept(copyRequest common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
	// cache the transfers
	i.transfers = append(i.transfers, copyRequest.Transfers.List...)

	// mock the result
	if len(i.transfers) != 0 || !copyRequest.IsFinalPart {
		return common.CopyJobPartOrderResponse{JobStarted: true}
	} else {
		return common.CopyJobPartOrderResponse{JobStarted: false, ErrorMsg: common.ECopyJobPartOrderErrorType.NoTransfersScheduledErr()}
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
}

// this lifecycle manager substitute does not perform any action
type mockedLifecycleManager struct {
	infoLog      chan string
	warnLog      chan string
	errorLog     chan string
	progressLog  chan string
	exitLog      chan string
	dryrunLog    chan string
	outputFormat common.OutputFormat
}

func (m *mockedLifecycleManager) ReportAllJobPartsDone() {
}

func (m *mockedLifecycleManager) SetOutputVerbosity(mode common.OutputVerbosity) {
}

func (m *mockedLifecycleManager) Progress(o common.OutputBuilder) {
	select {
	case m.progressLog <- o(common.EOutputFormat.Text()):
	default:
	}
}
func (*mockedLifecycleManager) OnStart(ctx common.JobContext) {}
func (m *mockedLifecycleManager) Info(msg string) {
	select {
	case m.infoLog <- msg:
	default:
	}
}
func (m *mockedLifecycleManager) Warn(msg string) {
	select {
	case m.warnLog <- msg:
	default:
	}
}
func (m *mockedLifecycleManager) Dryrun(o common.OutputBuilder) {
	select {
	case m.dryrunLog <- o(m.outputFormat):
	default:
	}
}
func (m *mockedLifecycleManager) Output(o common.OutputBuilder, e common.OutputMessageType) {
	select {
	case m.infoLog <- o(m.outputFormat):
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
func (m *mockedLifecycleManager) SetOutputFormat(format common.OutputFormat) {
	m.outputFormat = format
}
func (*mockedLifecycleManager) EnableInputWatcher()    {}
func (*mockedLifecycleManager) EnableCancelFromStdIn() {}

func (*mockedLifecycleManager) SetForceLogging() {}

func (*mockedLifecycleManager) IsForceLoggingDisabled() bool {
	return false
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

func (*mockedLifecycleManager) MsgHandlerChannel() <-chan *common.LCMMsg {
	return nil
}

type dummyProcessor struct {
	record []StoredObject
}

func (d *dummyProcessor) process(storedObject StoredObject) (err error) {
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
