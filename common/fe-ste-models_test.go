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

package common_test

import (
	"github.com/Azure/azure-storage-azcopy/common"
	chk "gopkg.in/check.v1"
)

type feSteModelsTestSuite struct{}

var _ = chk.Suite(&feSteModelsTestSuite{})

func (s *feSteModelsTestSuite) TestEnhanceJobStatusInfo(c *chk.C) {
	status := common.EJobStatus

	status = status.EnhanceJobStatusInfo(true, true, true)
	c.Assert(status, chk.Equals, common.EJobStatus.CompletedWithErrorsAndSkipped())

	status = status.EnhanceJobStatusInfo(true, true, false)
	c.Assert(status, chk.Equals, common.EJobStatus.CompletedWithErrorsAndSkipped())

	status = status.EnhanceJobStatusInfo(true, false, true)
	c.Assert(status, chk.Equals, common.EJobStatus.CompletedWithSkipped())

	status = status.EnhanceJobStatusInfo(true, false, false)
	c.Assert(status, chk.Equals, common.EJobStatus.CompletedWithSkipped())

	status = status.EnhanceJobStatusInfo(false, true, true)
	c.Assert(status, chk.Equals, common.EJobStatus.CompletedWithErrors())

	status = status.EnhanceJobStatusInfo(false, true, false)
	c.Assert(status, chk.Equals, common.EJobStatus.Failed())

	status = status.EnhanceJobStatusInfo(false, false, true)
	c.Assert(status, chk.Equals, common.EJobStatus.Completed())

	// No-op if all are false
	status = status.EnhanceJobStatusInfo(false, false, false)
	c.Assert(status, chk.Equals, common.EJobStatus.Completed())
}

func (s *feSteModelsTestSuite) TestIsJobDone(c *chk.C) {
	status := common.EJobStatus.InProgress()
	c.Assert(status.IsJobDone(), chk.Equals, false)

	status = status.Paused()
	c.Assert(status.IsJobDone(), chk.Equals, false)

	status = status.Cancelling()
	c.Assert(status.IsJobDone(), chk.Equals, false)

	status = status.Cancelled()
	c.Assert(status.IsJobDone(), chk.Equals, true)

	status = status.Completed()
	c.Assert(status.IsJobDone(), chk.Equals, true)

	status = status.CompletedWithErrors()
	c.Assert(status.IsJobDone(), chk.Equals, true)

	status = status.CompletedWithSkipped()
	c.Assert(status.IsJobDone(), chk.Equals, true)

	status = status.CompletedWithErrors()
	c.Assert(status.IsJobDone(), chk.Equals, true)

	status = status.CompletedWithErrorsAndSkipped()
	c.Assert(status.IsJobDone(), chk.Equals, true)

	status = status.Failed()
	c.Assert(status.IsJobDone(), chk.Equals, true)
}