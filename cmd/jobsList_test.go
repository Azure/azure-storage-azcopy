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
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSortJobs(t *testing.T) {
	a := assert.New(t)
	// setup
	job2 := common.JobIDDetails{
		JobId:         common.NewJobID(),
		StartTime:     time.Now().UnixNano(),
		CommandString: "dummy2",
	}

	// sleep for a bit so that the time stamp is different
	time.Sleep(time.Millisecond)
	job1 := common.JobIDDetails{
		JobId:         common.NewJobID(),
		StartTime:     time.Now().UnixNano(),
		CommandString: "dummy1",
	}

	// sleep for a bit so that the time stamp is different
	time.Sleep(time.Millisecond)
	job0 := common.JobIDDetails{
		JobId:         common.NewJobID(),
		StartTime:     time.Now().UnixNano(),
		CommandString: "dummy0",
	}
	jobsList := []common.JobIDDetails{job2, job1, job0}

	// act
	sortJobs(jobsList)

	// verify
	a.Equal(job0, jobsList[0])
	a.Equal(job1, jobsList[1])
	a.Equal(job2, jobsList[2])
}