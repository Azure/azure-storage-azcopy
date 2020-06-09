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

// RunTests is the key entry point for declarative testing.
// It constructs and executes tests, according to its parameters, and checks their results
func RunTests(
	c *chk.C,
	operations Operation,
	testFromTo TestFromTo,
	_ interface{}, // TODO, blockBLobsOnly or specifc/all blob types
	_ interface{}, // TODO, default auth type only, or specific/all auth types
	p params,
	hs *hooks,
	fs testFiles,
	// TODO: do we need something here to explicitly say that we expect success or failure? For now, we are just inferring that from the elements of sourceFiles
) {
	// log the overall test that we are running, in a concise form (each scenario will be logged later)
	c.Log(fmt.Sprintf("Running %s for %s", operations, testFromTo))

	// construct all the scenarios
	scenarios := make([]scenario, 0, 16)
	for _, op := range operations.getValues() {
		for _, fromTo := range testFromTo.getValues(op) {
			s := scenario{
				c:         c,
				operation: op,
				fromTo:    fromTo,
				p:         p, // copies them, because they are a struct. This is what we need, since the may be morphed while running
				hs:        hs,
				fs:        fs,
			}

			scenarios = append(scenarios, s)
		}
	}

	// run them in parallel
	// TODO: is this really how we want to do this?
	wg := &sync.WaitGroup{}
	for _, s := range scenarios {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Run()
		}()
	}
	wg.Wait() // TODO: do we want some kind of timeout here (and how does one even do that with WaitGroups anyway?)
}

type scenario struct {
	c         *chk.C
	operation Operation
	fromTo    common.FromTo
	p         params
	hs        *hooks
	fs        testFiles
}

// Run runs one test scenario
func (s *scenario) Run() {
	s.log()

	// TODO: add implementation here! ;-)
	s.c.Succeed()
}

func (s *scenario) log() {
	s.c.Log("wombat")
}
