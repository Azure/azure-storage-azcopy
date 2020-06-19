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
	chk "gopkg.in/check.v1"
	"sync"
)

// This declarative test runner adds a layer on top of e2etest/base. The added layer allows us to test in a declarative style,
// saying what to do, but not how to do it.
// In particular, it lets one test cover a range of different source/dest types, and even cover both sync and copy.
// See first test in zt_enumeration for an annotated example.

// TODO:
//     account types (std, prem etc)
//     account-to-account (e.g. multiple containers, copying whole account)

// RunTests is the key entry point for declarative testing.
// It constructs and executes tests, according to its parameters, and checks their results
func RunTests(
	c *chk.C,
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
			scenarioName := fmt.Sprintf("%s.%s[%s,%s]", suiteName, testName, op, fromTo)
			s := scenario{
				c:            c,
				scenarioName: scenarioName,
				operation:    op,
				fromTo:       fromTo,
				validate:     validate,
				p:            p, // copies them, because they are a struct. This is what we need, since the may be morphed while running
				hs:           hs,
				fs:           fs,
				a:            &scenarioAsserter{c, scenarioName},
			}

			scenarios = append(scenarios, s)
		}
	}

	// run them in parallel
	// TODO: is this really how we want to do this?
	wg := &sync.WaitGroup{}
	wg.Add(len(scenarios))
	for _, s := range scenarios {
		sen := s // capture to separate var inside the loop
		go func() {
			defer wg.Done()

			sen.Run()
		}()
	}
	wg.Wait() // TODO: do we want some kind of timeout here (and how does one even do that with WaitGroups anyway?)
}

type scenario struct {

	// scenario config properties as provided by user
	c            *chk.C
	scenarioName string
	operation    Operation
	validate     Validate
	fromTo       common.FromTo
	p            params
	hs           *hooks
	fs           testFiles
	a            asserter

	// internal declarative runner state
	state scenarioState
}

type scenarioState struct {
	source resourceManager
	dest   resourceManager
}

// TODO: any better names for this?
// a source or destination
type resourceManager interface {

	// setup creates and initializes a test resource appropriate for the given test files
	setup(a asserter, fs testFiles, isSource bool)

	// cleanup gets rid of everything that setup created
	// (Takes no param, because the resourceManager is expected to track its own state. E.g. "what did I make")
	cleanup(a asserter)
}

// Run runs one test scenario
func (s *scenario) Run() {
	defer s.cleanup()

	s.logStart()

	// setup
	s.assignSourceAndDest() // what/where are they
	s.state.source.setup(s.a, s.fs, true)
	s.state.dest.setup(s.a, s.fs, false)
	s.prepareParams()

	// execute
	s.runAzCopy()

	// check
	s.validateTransfers()
	if s.validate&eValidate.Content() == eValidate.Content() {
		s.validateContent()
	}
}

func (s *scenario) logStart() {
	s.c.Logf("Start scenario: %s", s.scenarioName)
}

func (s *scenario) logWarning(where string, err error) {
	s.c.Logf("warning in %s: %s %v", s.scenarioName, where, err)
}

func (s *scenario) assignSourceAndDest() {
	createTestResource := func(loc common.Location) resourceManager {
		// TODO: handle account to account (multi-container) scenarios
		switch loc {
		case common.ELocation.Local():
			return &resourceLocal{}
		case common.ELocation.File():
			return &resourceAzureFiles{accountType: EAccountType.Standard()}
		case common.ELocation.Blob():
			// TODO: handle the multi-container (whole account) scenario
			// TODO: handle wider variety of account types
			return &resourceBlobContainer{accountType: EAccountType.Standard()}
		case common.ELocation.BlobFS():
			s.c.Error("Not implementd yet for blob FS")
			return &resourceDummy{}
		case common.ELocation.S3():
			s.c.Error("Not implementd yet for S3")
			return &resourceDummy{}
		default:
			panic(fmt.Sprintf("location type '%s' is not yet supported in declarative tests", loc))
		}
	}

	s.state.source = createTestResource(s.fromTo.From())
	s.state.dest = createTestResource(s.fromTo.To())
}

func (s *scenario) prepareParams() {

}

func (s *scenario) runAzCopy() {

}

func (s *scenario) validateTransfers() {

}

func (s *scenario) validateContent() {

}

func (s *scenario) cleanup() {
	if s.state.source != nil {
		s.state.source.cleanup(s.a)
	}
	if s.state.dest != nil {
		s.state.dest.cleanup(s.a)
	}
}
