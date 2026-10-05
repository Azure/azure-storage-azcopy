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

package cmd

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

type jobsListJSONLifecycleManager struct {
	mockedLifecycleManager
	output string
}

func (m *jobsListJSONLifecycleManager) Exit(builder common.OutputBuilder, _ common.ExitCode) {
	if builder != nil {
		m.output = builder(common.EOutputFormat.Json())
	}
}

func TestJobsListJSONPreservesStartTime(t *testing.T) {
	a := assert.New(t)
	originalLCM := glcm
	t.Cleanup(func() { glcm = originalLCM })
	lcm := &jobsListJSONLifecycleManager{}
	glcm = lcm

	startTime := time.Unix(1700000000, 123456789)
	jobID := common.NewJobID()
	err := PrintExistingJobIds(azcopy.ListJobsResponse{Details: []azcopy.JobDetail{{
		JobID:     jobID,
		StartTime: startTime,
		Status:    common.EJobStatus.Completed(),
		Command:   "copy source destination",
	}}})
	a.NoError(err)

	var response common.ListJobsResponse
	a.NoError(json.Unmarshal([]byte(lcm.output), &response))
	if a.Len(response.JobIDDetails, 1) {
		a.Equal(jobID, response.JobIDDetails[0].JobId)
		a.Equal(startTime.UnixNano(), response.JobIDDetails[0].StartTime)
		a.Equal(common.EJobStatus.Completed(), response.JobIDDetails[0].JobStatus)
		a.Equal("copy source destination", response.JobIDDetails[0].CommandString)
	}
}

func TestJobsManagementCommandsRegistered(t *testing.T) {
	registered := make(map[string]bool)
	for _, command := range jobsCmd.Commands() {
		registered[command.Name()] = true
	}
	for _, name := range []string{"clean", "list", "remove", "show"} {
		assert.True(t, registered[name], "jobs %s must be registered", name)
	}
}
