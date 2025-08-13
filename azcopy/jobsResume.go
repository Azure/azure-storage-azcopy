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
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

type ResumeJobOptions struct {
	JobID common.JobID

	SourceSAS      string
	DestinationSAS string
}

// ResumeJob resumes a job with the specified JobID.
func (c *Client) ResumeJob(opts ResumeJobOptions) (err error) {

	if opts.JobID.IsEmpty() {
		return errors.New("resume job requires the JobID")
	}
	c.CurrentJobID = opts.JobID
	timeAtPrestart := time.Now()
	common.AzcopyCurrentJobLogger = common.NewJobLogger(c.CurrentJobID, c.logLevel, common.LogPathFolder, "")
	common.AzcopyCurrentJobLogger.OpenLog()

	c.EnumerationParallelism, c.EnumerationParallelStatFiles = jobsAdmin.JobsAdmin.GetConcurrencySettings()
	// Log a clear ISO 8601-formatted start time, so it can be read and use in the --include-after parameter
	// Subtract a few seconds, to ensure that this date DEFINITELY falls before the LMT of any file changed while this
	// job is running. I.e. using this later with --include-after is _guaranteed_ to pick up all files that changed during
	// or after this job
	adjustedTime := timeAtPrestart.Add(-5 * time.Second)
	startTimeMessage := fmt.Sprintf("ISO 8601 START TIME: to copy files that changed before or after this job started, use the parameter --%s=%s or --%s=%s",
		common.IncludeBeforeFlagName, FormatAsUTC(adjustedTime),
		common.IncludeAfterFlagName, FormatAsUTC(adjustedTime))
	common.LogToJobLogWithPrefix(startTimeMessage, common.LogInfo)

	// TODO (gapra): Implement the logic to resume a job.
	return nil
}
