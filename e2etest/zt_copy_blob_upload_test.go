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

package e2etest

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/Azure/azure-storage-azcopy/common"

	chk "gopkg.in/check.v1"
)

// represents a set of source files, including what we expect shoud happen to them
type sourceFiles struct {
	// names of files that we expect to be transferred
	shouldTransfer []string

	// names of files that we expect to be found by the enumeration
	shouldIgnore []string

	// names of files that we expect to  fail with error
	shouldFail []string

	// names of files that we expect to be skipped to an overwrite setting
	shouldSkip []string
}

func (s *cmdIntegrationSuite) TestIncludeDir(c *chk.C) {
	sourceContents := sourceFiles{
		shouldIgnore: []string{
			"filea",
			"fileb",
			"filec",
			"sub/filea",
			"sub/fileb",
			"sub/filec",
			"sub/somethingelse/subsub/filex", // should not be included because sub/subsub is not contiguous here
			"othersub/sub/subsub/filey",      // should not be included because sub/subsub is not at root here
		},
		shouldTransfer: []string{
			"sub/subsub/filea",
			"sub/subsub/fileb",
			"sub/subsub/filec",
		},
	}

	r := RunCopy(copyParams{
		source:      newSourceLocalDir(sourceContents),
		dest:        newDestContainer(EAccountType.Standard()),
		recursive:   true,
		includePath: "sub/subsub"})
	defer r.Cleanup()

	r.ValidateCopyTransfersAreScheduled()

	// note the object returned by RunCopy represents the complete state of the test run. It includes
	// - information about the source and dest (including which have been created and
	//   therefore need to be cleaned up in the deferred cleanup)
	// - information about the results of the test run, including any error, any caputured output and logs etc
	// - Validate methods to validate those results. By default, every ValidateXXX method includes validation that
	//   the transfer succeded. That can be turned off with something like like r.ExpectJobStatus(...Failed) before
	//   calling a Validate method
	//
	// In addition to the RunCopy method here, there would be RunSync, RunList etc.
}
