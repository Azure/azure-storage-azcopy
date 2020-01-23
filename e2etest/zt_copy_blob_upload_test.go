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

type sourceSpecifier interface{} // TODO
type destSpecifier interface{}   // TODO

func newSourceLocalDir(x interface{}) interface{} { // todo
	return nil
}

func newDestContainer(x interface{}) interface{} { // todo
	return nil
}

type copyParams struct {
	source      sourceSpecifier
	dest        destSpecifier
	recursive   bool
	includePath string
}

type testState struct {
}

func (t testState) cleanup() {

}

func (t testState) validateCopyTransfersAreScheduled() {

}

func runCopy(p copyParams) testState {
	return testState{}
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

	r := runCopy(copyParams{
		source:      newSourceLocalDir(sourceContents),
		dest:        newDestContainer(EAccountType.Standard()),
		recursive:   true,
		includePath: "sub/subsub"})
	defer r.cleanup()

	r.validateCopyTransfersAreScheduled()

	// note the object returned by RunCopy represents the complete state of the test run. It includes
	// - information about the source and dest (including which have been created and
	//   therefore need to be cleaned up in the deferred cleanup)
	// - information about the results of the test run, including any error, any caputured output and logs etc
	// - Validate methods to validate those results. By default, every ValidateXXX method includes validation that
	//   the transfer succeded. That can be turned off with something like like r.ExpectJobStatus(...Failed) before
	//   calling a Validate method
	//
	// In addition to the RunCopy method here, there would be RunSync, RunList etc.
	//
	// To specify files with certain sizes, best notation I can find is to just use something like "filename:20K"
	// i.e. choose a separator, and make our own syntax for what comes after it, and have our setup code process
	// it accordingly. (We lose the ability to put that separator into test filenames - with this test harness/helper method -
	// but for the rare tests where we need that I think we can just write unit-style test directly against the relevant classes)
}
