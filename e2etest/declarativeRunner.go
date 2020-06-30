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
	"testing"
)

// This declarative test runner adds a layer on top of e2etest/base. The added layer allows us to test in a declarative style,
// saying what to do, but not how to do it.
// In particular, it lets one test cover a range of different source/dest types, and even cover both sync and copy.
// See first test in zt_enumeration for an annotated example.

// TODO:
//     account types (std, prem etc)
//     account-to-account (e.g. multiple containers, copying whole account)
//     specifying "strip top dir"
//     copying to/from things that are not the share root/container root

// A note on test frameworks.
// We are just using GoLang's own Testing package.
// Why aren't we using gocheck (gopkg.in/check.v1) as we did for older unit tests?
// Because gocheck doesn't seem to have, or expose, any concept of sub-tests. But we want: suite/test/subtest
// (subtest = scenario in our wording below).
// Why aren't we using stretchr/testify/suite? Because it appears from the code there that tests (and subtests) within a suite cannot be parallelized
// (Since suite.SetT() manipulates shared state).

// RunScenarios is the key entry point for declarative testing.
// It constructs and executes scenarios (subtest in Go-speak), according to its parameters, and checks their results
func RunScenarios(
	t *testing.T,
	operations Operation,
	testFromTo TestFromTo,
	validate Validate, // TODO: do we really want the test author to have to nominate which validation should happen?  Pros: better perf of tests. Cons: they have to tell us, and if they tell us wrong test may not test what they think it tests
	_ interface{}, // TODO, blockBLobsOnly or specifc/all blob types
	_ interface{}, // TODO, default auth type only, or specific/all auth types
	p params,
	hs *hooks,
	fs testFiles,
	// TODO: do we need something here to explicitly say that we expect success or failure? For now, we are just inferring that from the elements of sourceFiles
) {
	suiteName, testName := getTestName()

	// construct all the scenarios
	scenarios := make([]scenario, 0, 16)
	for _, op := range operations.getValues() {
		for _, fromTo := range testFromTo.getValues(op) {
			// Create unique name for generating container names
			uniqueScenarioName := fmt.Sprintf("%s-%s-%c-%c%c", suiteName, testName, op.String()[0], fromTo.From().String()[0], fromTo.To().String()[0])
			// Subtest name is not globally unique (it doesn't need to be) but it is more human-readable
			subtestName := fmt.Sprintf("%s-%s", op, fromTo)
			s := scenario{
				subtestName: subtestName,
				operation:   op,
				fromTo:      fromTo,
				validate:    validate,
				p:           p, // copies them, because they are a struct. This is what we need, since the may be morphed while running
				hs:          hs,
				fs:          fs,
				a:           &testingAsserter{t, uniqueScenarioName}, // it's a bit ugly passing the scenario name in here, in a "context"-like way, but it works
				stripTopDir: false,                                   // TODO: how will we set this?
			}

			scenarios = append(scenarios, s)
		}
	}

	// run them in parallel if not debugging, but sequentially (for easier debugging) if a debugger is attached
	parallel := !isLaunchedByDebugger // this only works if gops.exe is on your path. See azcopyDebugHelper.go for instructions.
	for _, s := range scenarios {
		sen := s // capture to separate var inside the loop, for the parallel case
		// use t.Run to get proper sub-test support
		t.Run(s.subtestName, func(t *testing.T) {
			if parallel {
				t.Parallel() // tell testing that it can run stuff in parallel with us
			}
			sen.Run()
		})
	}
}

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
	state scenarioState
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
		actualTransfers := s.state.result.GetTransferList(statusToTest)

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
