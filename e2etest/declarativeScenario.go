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
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"os"
	"path"
	"path/filepath"
)

// E.g. if we have enumerationSuite/TestFooBar/Copy-LocalBlob the scenario is "Copy-LocalBlob"
// A scenario is treated as a sub-test by our declarative test runner
type scenario struct {

	// scenario config properties as provided by user
	subtestName string
	operation   Operation
	validate    Validate
	fromTo      common.FromTo
	p           params
	hs          *hooks
	fs          testFiles
	a           asserter

	stripTopDir bool // TODO: figure out how we'll control and use this

	// internal declarative runner state
	state scenarioState // TODO: does this really need to be a separate struct?
}

type scenarioState struct {
	source resourceManager
	dest   resourceManager
	result *CopyOrSyncCommandResult
}

// Run runs one test scenario
func (s *scenario) Run() {
	defer s.cleanup()

	// setup
	s.assignSourceAndDest() // what/where are they
	s.state.source.setup(s.a, s.fs, true)
	s.state.dest.setup(s.a, s.fs, false)
	s.prepareParams()

	// execute
	s.runAzCopy()

	// check
	// TODO: which options to we want to expose here, and is eValidate the right way to do so? Or do we just need a boolean, validateContent?
	s.validateTransfers()
	if s.validate&eValidate.Content() == eValidate.Content() {
		s.validateContent()
	}
}

func (s *scenario) assignSourceAndDest() {
	createTestResource := func(loc common.Location) resourceManager {
		// TODO: handle account to account (multi-container) scenarios
		switch loc {
		case common.ELocation.Local():
			return &resourceLocal{}
		case common.ELocation.File():
			return &resourceAzureFileShare{accountType: EAccountType.Standard()}
		case common.ELocation.Blob():
			// TODO: handle the multi-container (whole account) scenario
			// TODO: handle wider variety of account types
			return &resourceBlobContainer{accountType: EAccountType.Standard()}
		case common.ELocation.BlobFS():
			s.a.Error("Not implementd yet for blob FS")
			return &resourceDummy{}
		case common.ELocation.S3():
			s.a.Error("Not implementd yet for S3")
			return &resourceDummy{}
		default:
			panic(fmt.Sprintf("location type '%s' is not yet supported in declarative tests", loc))
		}
	}

	s.state.source = createTestResource(s.fromTo.From())
	s.state.dest = createTestResource(s.fromTo.To())
}

func (s *scenario) prepareParams() {
	// todo: mess with hooks
}

func (s *scenario) runAzCopy() {
	r := newTestRunner()
	r.SetAllFlags(s.p)
	const useSas = true // TODO: support other auth options (see params of RunTest)
	result, err := r.ExecuteCopyOrSyncCommand(
		s.operation,
		s.state.source.getParam(s.stripTopDir, useSas),
		s.state.dest.getParam(false, useSas))
	s.a.AssertNoErr(err, "running AzCopy")
	s.state.result = &result
}

func (s *scenario) validateTransfers() {

	if s.p.deleteDestination != common.EDeleteDestination.False() {
		// TODO: implement deleteDestinationValidation
		panic("validation of deleteDestination behaviour is not yet implemented in the declarative test runner")
	}

	isSrcEncoded := s.fromTo.From().IsRemote() // TODO: is this right, reviewers?
	isDstEncoded := s.fromTo.To().IsRemote()   // TODO: is this right, reviewers?
	srcRoot := s.state.source.getParam(false, false)
	dstRoot := s.state.dest.getParam(false, false)

	// do we expect folder transfers
	expectFolders := s.fromTo.From().IsFolderAware() &&
		s.fromTo.To().IsFolderAware() &&
		s.p.allowsFolderTransfers()

	// compute dest, taking into account our stripToDir rules
	areBothContainerLike := s.state.source.isContainerLike() && s.state.dest.isContainerLike()
	if s.stripTopDir || areBothContainerLike {
		// noop
	} else if s.fromTo.From().IsLocal() {
		dstRoot = fmt.Sprintf("%s%c%s", dstRoot, os.PathSeparator, filepath.Base(srcRoot))
	} else {
		dstRoot = fmt.Sprintf("%s/%s", dstRoot, path.Base(srcRoot))
	}

	// test the sets of files in the various statuses
	for _, statusToTest := range []common.TransferStatus{
		common.ETransferStatus.Success(),
		common.ETransferStatus.Failed(),
		// TODO: testing of skipped is implicit, in that they are created at the source, but don't exist in Success or Failed lists
		//       Is that OK? (Not sure what to do if it's not, because azcopy jobs show, apparently doesn't offer us a way to get the skipped list)
	} {
		expectedTransfers := s.fs.getForStatus(statusToTest, expectFolders)
		actualTransfers, err := s.state.result.GetTransferList(statusToTest)
		s.a.AssertNoErr(err)

		Validator{}.ValidateCopyTransfersAreScheduled(s.a, isSrcEncoded, isDstEncoded, srcRoot, dstRoot, expectedTransfers, actualTransfers)
		// TODO: how are we going to validate folder transfers????
	}

	// TODO: for failures, consider validating the failure messages (for which we have expected values, in s.fs; but don't currently have a good way to get
	//    the actual values from the test run
}

func (s *scenario) validateContent() {
	panic("not implemented yet")
}

func (s *scenario) cleanup() {
	if s.state.source != nil {
		s.state.source.cleanup(s.a)
	}
	if s.state.dest != nil {
		s.state.dest.cleanup(s.a)
	}
}
