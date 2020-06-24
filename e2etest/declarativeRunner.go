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
	"testing"
)

// This declarative test runner adds a layer on top of e2etest/base. The added layer allows us to test in a declarative style,
// saying what to do, but not how to do it.
// In particular, it lets one test cover a range of different source/dest types, and even cover both sync and copy.
// See first test in zt_enumeration for an annotated example.

// A note on test frameworks.
// We are just using GoLang's own Testing package.
// Why aren't we using gocheck (gopkg.in/check.v1) as we did for older unit tests?
// Because gocheck doesn't seem to have, or expose, any concept of sub-tests. But we want: suite/test/subtest
// (subtest = scenario in our wording below).
// Why aren't we using stretchr/testify/suite? Because it appears from the code there that tests (and subtests) within a suite cannot be parallelized
// (Since suite.SetT() manipulates shared state), but we might want to parallelize tests within a suite.

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
	suiteName, testName := getTestName(t)

	// construct all the scenarios
	scenarios := make([]scenario, 0, 16)
	for _, op := range operations.getValues() {
		for _, fromTo := range testFromTo.getValues(op) {
			// Create unique name for generating container names
			compactScenarioName := fmt.Sprintf("%.4s-%s-%c-%c%c", suiteName, testName, op.String()[0], fromTo.From().String()[0], fromTo.To().String()[0])
			fullScenarioName := fmt.Sprintf("%s.%s.%s-%s", suiteName, testName, op.String(), fromTo.String())
			// Sub-test name is not globally unique (it doesn't need to be) but it is more human-readable
			subtestName := fmt.Sprintf("%s-%s", op, fromTo)

			s := scenario{
				subtestName: subtestName,
				operation:   op,
				fromTo:      fromTo,
				validate:    validate,
				p:           p, // copies them, because they are a struct. This is what we need, since the may be morphed while running
				hs:          hs,
				fs:          fs,
				a: &testingAsserter{
					t:                   t,
					compactScenarioName: compactScenarioName,
					fullScenarioName:    fullScenarioName},
				stripTopDir: false, // TODO: how will we set this?
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
