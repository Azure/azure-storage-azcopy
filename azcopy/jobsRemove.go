// Copyright © 2025 Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

type RemoveJobOptions struct {
	JobID common.JobID
}

type RemoveJobResult struct {
	Count int // Number of files cleaned
}

// RemoveJob removes a job with the specified JobID.
func (c Client) RemoveJob(opts RemoveJobOptions) (result RemoveJobResult, err error) {
	result = RemoveJobResult{}
	if opts.JobID.IsEmpty() {
		return result, errors.New("remove job requires the JobID")
	}
	result.Count, err = jobsAdmin.RemoveSingleJobFiles(opts.JobID)
	if err != nil {
		return result, fmt.Errorf("failed to remove log and job plan files for job %s due to error: %w", opts.JobID, err)
	}
	return result, nil
}
