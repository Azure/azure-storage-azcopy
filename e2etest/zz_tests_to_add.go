package e2etest

// TODO soon:
//
//   Next framework _use_ tasks
//   	More filter tests
//      The resource manager support for S3 and BlobFS and (who will do this one) GCP?
//
//   Next framework _development_ tasks
//      Change expectation lists from string to interface{}
//  	Content preservation tests
//		Properties preservation tests
//
//   Suggested near-term goal:
//		Complete the following suites:
//			Change detection (done)
//			Filters
//			preserve content
//			preserve properties
//
//  Leave for later:
//		Capabilities:
//			Page and Append Blobs
//          Different account types (should be easy to add tho)
//          Whole-account-to-whole account (e.g. copy all containers, or all S3 buckets)
//			Different auth types
//		These suites:
//  	  Overwrite  (how will we process the prompts? Just through t.execDebuggableWithOutput's afterstart parameter? Requires us to assume what the prompt will be, and that there will be only 1 prompt - so can only test teh "all" answers with this mechansim. But may be able to extend it.
//        Sync (special stuff that's unique to sync)
//        Preserve names (special chars, escaping etc)
//        Managed disks (our special case logic or uploading managed disks)
//        Logging
//		  Error handing (e.g. deleting files that fail part way through) and Resume
//
//   To think about:
//    stripTopDir
//    copying to/from things that are not the share root/container root
//    think about "decode unsafe dst characters no Windows" comment in validator.go
//    Add a timeout to all executions of AzCopy
//    Is our cleanup reliable enough, eg. after stopping the test harness during debugging? No, it doesn't always seem to cleanup in those cases. Can we fix that?
//    Given this is a new suite for an existing app, how do we make sure each test really is testing what we think its testing
//    See other notes below, in this file

// ----------------- additional unstructured notes below this point -----

//
// TODO: document the following re test frameworks, and support for suites specifically:
//// A note on test frameworks.
//// We are just using GoLang's own Testing package.
//// Why aren't we using gocheck (gopkg.in/check.v1) as we did for older unit tests?
//// Because gocheck doesn't seem to have, or expose, any concept of sub-tests. But we want: suite/test/subtest
//// (subtest = scenario in our wording below).
//// Why aren't we using stretchr/testify/suite? Because it appears from the code there that tests (and subtests) within a suite cannot be parallelized
//// (Since suite.SetT() manipulates shared state), but we might want to parallelize tests within a suite.
/// We need subetsts so we can report the pass/fail the state of each sceario separately
/// Things we can't do:
/// 1. Make our own suite approach like testify does (where tests are methods rather than func) This is easy to implement, but because GoLand doesn't know about it, it becomes impossible to
///    invoke individual tests from the IDE.
/// 2. Use normal "testing" style funcs but somehow group them automatically. Can't group automatiecally, because even tho we can get their names in a TestMain method (via
///    a little relfection on the M parameter, we can't tell which file each one is defined it. We just get their name and their func object.  And we have no (easy) way
///    of doing symbol table lookups or similar to get the file for the func.
///
/// SO we are just adopting the convention of including a prefix in the name, so that they sort sensible.  So we'll just have a two-level structure,
/// but it will look like this MySuite_MyTest/scenario

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
