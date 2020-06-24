package e2etest

import (
	"flag"
	"strings"
	"testing"
)

// TODO soon:
//    stripTopDir
//    think about "decode unsafe dst characters no Windows" comment in validator.go
//    With asserter, we get the call stack from the point of the asserter method, not the caller...
//    .... so we get the equivalent of true != false (again!)

// TODO:
//     account types (std, prem etc)
//     account-to-account (e.g. multiple containers, copying whole account)
//     specifying "strip top dir"
//     copying to/from things that are not the share root/container root

// Think about suites.  Don't want to use testify, because it doesn't support parallization within a suite (and doesn't support subtests)
// But we want something that the IDE will recognise, and allow us to run individual tests or the suite. I think this will work
func RunFileAsSuite(t *testing.T) {
	f := flag.Lookup("test.run")
	if f == nil {
		t.Skip("Cannot determine whether suite-based running was requested, so skipping it")
		return
	}
	suiteRunningRequested := strings.HasPrefix(strings.TrimPrefix("^", f.Value.String()), "TestSuite") // the regex is looking for things starting with TestSuite
	if !suiteRunningRequested {
		// skip if not explictly requested to run, else we'll end up running every test twice - once directly
		// from the test runner, and once as a child of the suite.
		t.Skip("test.run parameter did not request test(s) named TestSuite..., so skipping suite-based runner")
	}

	// TODO:
	//   use a syncOnce to scan for all Test(t *testing.T) methods, excluding those that beging with TestSUite, and group them by file
	//   then find out which file the calling method/test is in, and then run as subtests all the ones from that file
	//

	// Document that it can be used like this:
	//func TestSuiteEnumeration(t *testing.T) {
	//		RunFileAsSuite(t)
	//	}
}

// TODO: right now, as soon as one scenario fails in a test,
//    we stop executing them, and won't execute any other scearios
//    in that test.  Should we change this? It could be a bit difficult,
//    but might be possible with the Check vs Assert and Fail method in gocheck (chk)

// TODO: consider how to add tests to cover the following

// Running these tests doesn't ensure that AZCOPY_E2E_EXECUTABLE_PATH points to an UP TO DATE build of the app.
// Could we somehow make it do that?  (e.g. AZCOPY_E2E_AUTO_BUILD=true makes it call go build in the directory of  AZCOPY_E2E_EXECUTABLE_PATH,
// but only once for when the test suite is run. Not once PER TEST!

// Piping (we don't have any piping tests in the new suite yet)

// --cap-mbps
// page blob auto pacer
// exclusiveStringMap (usage of it, by the STE)
// Resume
// Operations other than copy and sync
// Account-to-account (i.e. whole of account) copies (since right now we only support single-container copies in the declarative tests)

// unit tests, of the test code, to add
//   Read and Seek on common.RandomDataGenerator
