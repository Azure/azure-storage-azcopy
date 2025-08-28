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
	"os"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// This declarative test runner adds a layer on top of e2etest/base. The added layer allows us to test in a declarative style,
// saying what to do, but not how to do it.
// In particular, it lets one test cover a range of different source/dest types, and even cover both sync and copy.
// See first test in zt_enumeration for an annotated example.

var validCredTypesPerLocation = map[common.Location][]common.CredentialType{
	common.ELocation.Unknown(): {common.ECredentialType.Unknown(), common.ECredentialType.Anonymous(), common.ECredentialType.OAuthToken()}, // Delete!
	common.ELocation.FileSMB(): {common.ECredentialType.Anonymous(), common.ECredentialType.OAuthToken()},
	common.ELocation.Blob():    {common.ECredentialType.Anonymous(), common.ECredentialType.OAuthToken(), common.ECredentialType.MDOAuthToken()},
	common.ELocation.BlobFS():  {common.ECredentialType.Anonymous(), common.ECredentialType.OAuthToken()}, // todo: currently, account key auth isn't even supported in e2e tests.
	common.ELocation.Local():   {common.ECredentialType.Anonymous()},
	common.ELocation.Pipe():    {common.ECredentialType.Anonymous()},
	common.ELocation.S3():      {common.ECredentialType.S3AccessKey()},
	common.ELocation.GCP():     {common.ECredentialType.GoogleAppCredentials()},
	common.ELocation.FileNFS(): {common.ECredentialType.Anonymous(), common.ECredentialType.OAuthToken()},
}

var allCredentialTypes []common.CredentialType = nil

var oAuthOnly = []common.CredentialType{common.ECredentialType.OAuthToken()}
var anonymousAuthOnly = []common.CredentialType{common.ECredentialType.Anonymous()}

func getValidCredCombinationsForFromTo(fromTo common.FromTo, requestedCredentialTypesSrc, requestedCredentialTypesDst []common.CredentialType, accountTypes []AccountType) [][2]common.CredentialType {
	output := make([][2]common.CredentialType, 0)

	credIsRequested := func(cType common.CredentialType, dst bool) bool {
		if (dst && requestedCredentialTypesDst == nil) || (!dst && requestedCredentialTypesSrc == nil) {
			return true
		}

		toSearch := requestedCredentialTypesSrc
		if dst {
			toSearch = requestedCredentialTypesDst
		}

		for _, v := range toSearch {
			if v == cType {
				return true
			}
		}

		return false
	}

	// determine source types
	var sourceTypes []common.CredentialType
	if fromTo.IsS2S() && (fromTo != common.EFromTo.BlobBlob() && fromTo != common.EFromTo.BlobFile() && fromTo != common.EFromTo.FileFile()) {
		// source must always be anonymous-- no exceptions until OAuth over S2S is introduced.
		sourceTypes = []common.CredentialType{common.ECredentialType.Anonymous()}
	} else {
		sourceTypes = validCredTypesPerLocation[fromTo.From()]
	}

	for _, srcCredType := range sourceTypes {
		if srcCredType == common.ECredentialType.MDOAuthToken() && accountTypes[0] != EAccountType.OAuthManagedDisk() && accountTypes[0] != EAccountType.ManagedDiskSnapshotOAuth() {
			continue // invalid selection
		}

		for _, dstCredType := range validCredTypesPerLocation[fromTo.To()] {
			if dstCredType == common.ECredentialType.MDOAuthToken() && accountTypes[1] != EAccountType.OAuthManagedDisk() && accountTypes[0] != EAccountType.ManagedDiskSnapshotOAuth() {
				continue // invalid selection
			}

			// make sure the user asked for this.
			if !(credIsRequested(srcCredType, false) && credIsRequested(dstCredType, true)) {
				continue
			}

			output = append(output, [2]common.CredentialType{srcCredType, dstCredType})
		}
	}

	return output
}

// RunScenarios is the key entry point for declarative testing.
// It constructs and executes scenarios (subtest in Go-speak), according to its parameters, and checks their results
func RunScenarios(
	t *testing.T,
	operations Operation,
	testFromTo TestFromTo,
	validate Validate, // TODO: do we really want the test author to have to nominate which validation should happen?  Pros: better perf of tests. Cons: they have to tell us, and if they tell us wrong test may not test what they think it tests
	// _ interface{}, // TODO if we want it??, blockBlobsOnly or specific/all blob types

	// It would be a pain to list out every combo by hand,
	// In addition to the fact that not every credential type is sensible.
	// Thus, the E2E framework takes in a requested set of credential types, and applies them where sensible.
	// This allows you to make tests use OAuth only, SAS only, etc.
	requestedCredentialTypesSrc []common.CredentialType,
	requestedCredentialTypesDst []common.CredentialType,
	p params,
	hs *hooks,
	fs testFiles,
	// TODO: do we need something here to explicitly say that we expect success or failure? For now, we are just inferring that from the elements of sourceFiles
	destAccountType AccountType,
	srcAccountType AccountType,
	scenarioSuffix string) {
	// enable this if we want parents in parallel: t.Parallel()

	suiteName, testName := getTestName(t)
	if suiteName == "" {
		t.Errorf("To group our tests cleanly, our test names should be of the form: TestXxx_Yyy..., where Xxx matches one of the words in the (underscore-separated) name of the containing file. '%s' does not follow that rule",
			t.Name())
	}

	// construct all the scenarios
	scenarios := make([]scenario, 0)
	for _, op := range operations.getValues() {
		if op == eOperation.Resume() || op == eOperation.Cancel() {
			continue
		}

		seenFromTos := make(map[common.FromTo]bool)
		fromTos := testFromTo.getValues(op)

		for _, fromTo := range fromTos {
			// dedupe the scenarios
			if _, ok := seenFromTos[fromTo]; ok {
				continue
			}
			seenFromTos[fromTo] = true

			credentialTypes := getValidCredCombinationsForFromTo(fromTo, requestedCredentialTypesSrc, requestedCredentialTypesDst, []AccountType{srcAccountType, destAccountType})

			for _, credTypes := range credentialTypes {
				// Create unique name for generating container names
				compactScenarioName := fmt.Sprintf("%.4s-%s-%c-%c%c", suiteName, testName, op.String()[0], fromTo.From().String()[0], fromTo.To().String()[0])
				fullScenarioName := fmt.Sprintf("%s.%s.%s-%s%s", suiteName, testName, op.String(), fromTo.From().String(), fromTo.To().String())
				// Sub-test name is not globally unique (it doesn't need to be) but it is more human-readable
				subtestName := fmt.Sprintf("%s-%s", op, fromTo)

				hsToUse := hooks{}
				if hs != nil {
					hsToUse = *hs
				}

				if scenarioSuffix != "" {
					subtestName += "-" + scenarioSuffix
				}

				usedSrc, usedDst := srcAccountType, destAccountType
				if fromTo.From() == common.ELocation.BlobFS() {
					// switch to an account made for dfs
					usedSrc = EAccountType.HierarchicalNamespaceEnabled()
				}

				if fromTo.To() == common.ELocation.BlobFS() {
					// switch to an account made for dfs
					usedDst = EAccountType.HierarchicalNamespaceEnabled()
				}

				s := scenario{
					srcAccountType:      usedSrc,
					destAccountType:     usedDst,
					subtestName:         subtestName,
					compactScenarioName: compactScenarioName,
					fullScenarioName:    fullScenarioName,
					operation:           op,
					fromTo:              fromTo,
					credTypes:           credTypes,
					validate:            validate,
					p:                   p, // copies them, because they are a struct. This is what we need, since they may be morphed while running
					hs:                  hsToUse,
					fs:                  fs.DeepCopy(),
					needResume:          operations&eOperation.Resume() != 0,
					needCancel:          operations&eOperation.Cancel() != 0,
					stripTopDir:         p.stripTopDir,
				}

				scenarios = append(scenarios, s)
			}
		}
	}

	logErr := logTestSummary(suiteName, testName, operations.includes(eOperation.Copy()), operations.includes(eOperation.Sync()), testFromTo, len(scenarios))
	if logErr != nil {
		t.Errorf("Error logging to test summary file: %s", logErr)
	}

	// run them in parallel if not debugging, but sequentially (for easier debugging) if a debugger is attached
	parallel := !isLaunchedByDebugger && !p.disableParallelTesting // this only works if gops.exe is on your path. See azcopyDebugHelper.go for instructions.
	for _, s := range scenarios {
		// use t.Run to get proper sub-test support
		t.Run(s.subtestName, func(t *testing.T) {
			sen := s // capture to separate var inside the loop, for the parallel case
			credNames := fmt.Sprintf("%s-%s", s.credTypes[0].String(), s.credTypes[1].String())

			t.Run(credNames, func(t *testing.T) {
				if parallel {
					t.Parallel() // tell testing that it can run stuff in parallel with us
				}
				// set asserter now (and only now), since before this point we don't have the right "t"
				sen.a = &testingAsserter{
					t:                   t,
					fullScenarioName:    sen.fullScenarioName,
					compactScenarioName: sen.compactScenarioName,
				}

				if hs != nil {
					sen.runHook(hs.beforeTestRun)
				}

				sen.Run()
			})
		})
	}
}

var testSummaryLogName string

func init() {
	path := GlobalInputManager{}.TestSummaryLogPath()
	if path == "" {
		return
	}

	fmt.Printf("Creating/replacing test summary log file at '%s'\n", path)
	_ = os.Remove(path)
	testSummaryLogName = path
	_ = logTestHeaders()
}

func logTestHeaders() error {
	return logToSummaryFile("PseudoSuite,Test,Copy,Sync,TestFromTo,ScenarioCount")
}

func logTestSummary(suite, test string, forCopy, forSync bool, testFromTo TestFromTo, scenarioCount int) error {
	return logToSummaryFile(fmt.Sprintf("%s,%s,%t,%t,%s,%d", suite, test, forCopy, forSync, testFromTo.String(), scenarioCount))
}

// this might be useful for helping us to understand what our tests cover, and the differences in AzCopy functionality
// in terms of which things are supported in both copy and sync and which in one only.
// Is a csv file, for ease of importing into Excel
func logToSummaryFile(s string) error {
	if testSummaryLogName == "" {
		return nil
	}

	f, err := os.OpenFile(testSummaryLogName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteString(s + "\n")
	return err
}
