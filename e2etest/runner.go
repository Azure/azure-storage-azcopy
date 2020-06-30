// Copyright © Microsoft <wastore@microsoft.com>
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

package e2etest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
)

// encapsulates the interaction with the AzCopy instance that is being tested
// the flag names should be captured here so that in case they change, only 1 place needs to be updated
type TestRunner struct {
	flags map[string]string
}

func newTestRunner() TestRunner {
	return TestRunner{flags: make(map[string]string)}
}

var isLaunchedByDebugger = func() bool {
	// gops executable must be in the path. See https://github.com/google/gops
	gopsOut, err := exec.Command("gops", strconv.Itoa(os.Getppid())).Output()
	if err == nil && strings.Contains(string(gopsOut), "\\dlv.exe") {
		// our parent process is (probably) the Delve debugger
		return true
	}
	return false
}()

func (t *TestRunner) SetAllFlags(p params) {
	set := func(key string, value interface{}, dflt interface{}, formats ...string) {
		if value == dflt {
			return // nothing to do. The flag is not supposed to be set
		}

		format := "%v"
		if len(formats) > 0 {
			format = formats[0]
		}

		t.flags[key] = fmt.Sprintf(format, value)
	}

	set("recursive", p.recursive, false, "%t")
	set("include-path", p.includePath, "")
	set("include-after", p.includeAfter, "")
	set("cap-mbps", p.capMbps, 0)
}

func (t *TestRunner) computeArgs() []string {
	args := make([]string, 0)
	for key, value := range t.flags {
		args = append(args, fmt.Sprintf("--%s=%s", key, value))
	}

	return append(args, "--output-type=json")
}

// execCommandWithOutput replaces Go's exec.Command().Output, but appends an extra parameter and
// breaks up the c.Run() call into its component parts. Both changes are to assist debugging
func (t *TestRunner) execDebuggableWithOutput(name string, args []string) ([]byte, error) {
	debug := isLaunchedByDebugger
	if debug {
		args = append(args, "--await-continue")
	}
	c := exec.Command(name, args...)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	stdin, err := c.StdinPipe()
	if err != nil {
		return make([]byte, 0), err
	}

	c.Stdout = &stdout
	c.Stderr = &stderr

	//instead of err := c.Run(), we do the following
	runErr := c.Start()
	if runErr == nil {
		if debug {
			beginAzCopyDebugging(stdin)
		}
		runErr = c.Wait()
	}

	// back to normal exec.Cmd.Output() processing
	if runErr != nil {
		if ee, ok := runErr.(*exec.ExitError); ok {
			ee.Stderr = stderr.Bytes()
		}
	}
	return stdout.Bytes(), runErr
}

func (t *TestRunner) ExecuteCopyOrSyncCommand(operation Operation, src, dst string) (CopyOrSyncCommandResult, error) {
	verb := ""
	switch operation {
	case eOperation.Copy():
		verb = "copy"
	case eOperation.Sync():
		verb = "sync"
	default:
		panic("unsupported operation type")
	}

	args := append([]string{verb, src, dst}, t.computeArgs()...)
	out, err := t.execDebuggableWithOutput(GlobalInputManager{}.GetExecutablePath(), args)
	if err != nil {
		return CopyOrSyncCommandResult{}, err
	}

	return newCopyOrSyncCommandResult(string(out)), nil
}

func (t *TestRunner) SetTransferStatusFlag(value string) {
	t.flags["with-status"] = value
}

func (t *TestRunner) ExecuteJobsShowCommand(jobID common.JobID) (JobsShowCommandResult, error) {
	args := append([]string{"jobs", "show", jobID.String()}, t.computeArgs()...)
	out, err := exec.Command(GlobalInputManager{}.GetExecutablePath(), args...).Output()
	if err != nil {
		return JobsShowCommandResult{}, err
	}

	return newJobsShowCommandResult(string(out)), nil
}

type CopyOrSyncCommandResult struct {
	jobID       common.JobID
	finalStatus common.ListSyncJobSummaryResponse
}

func newCopyOrSyncCommandResult(rawOutput string) CopyOrSyncCommandResult {
	lines := strings.Split(rawOutput, "\n")

	// parse out the final status
	// -2 because the last line is empty
	finalLine := lines[len(lines)-2]
	finalMsg := common.JsonOutputTemplate{}
	err := json.Unmarshal([]byte(finalLine), &finalMsg)
	if err != nil {
		panic(err)
	}

	jobSummary := common.ListSyncJobSummaryResponse{} // this is a superset of ListJobSummaryResponse, so works for both copy and sync
	err = json.Unmarshal([]byte(finalMsg.MessageContent), &jobSummary)
	if err != nil {
		panic(err)
	}

	return CopyOrSyncCommandResult{jobID: jobSummary.JobID, finalStatus: jobSummary}
}

func (c *CopyOrSyncCommandResult) GetTransferList(status common.TransferStatus) []common.TransferDetail {
	runner := newTestRunner()
	runner.SetTransferStatusFlag(status.String())

	// invoke AzCopy to get the status from the plan files
	result, err := runner.ExecuteJobsShowCommand(c.jobID)
	if err != nil {
		panic(err)
	}

	return result.transfers
}

type JobsShowCommandResult struct {
	jobID     common.JobID
	transfers []common.TransferDetail
}

func newJobsShowCommandResult(rawOutput string) JobsShowCommandResult {
	lines := strings.Split(rawOutput, "\n")

	// parse out the final status
	// -2 because the last line is empty
	finalLine := lines[len(lines)-2]
	finalMsg := common.JsonOutputTemplate{}
	err := json.Unmarshal([]byte(finalLine), &finalMsg)
	if err != nil {
		panic(err)
	}

	listTransfersResponse := common.ListJobTransfersResponse{}
	err = json.Unmarshal([]byte(finalMsg.MessageContent), &listTransfersResponse)
	if err != nil {
		panic(err)
	}

	return JobsShowCommandResult{jobID: listTransfersResponse.JobID, transfers: listTransfersResponse.Details}
}
