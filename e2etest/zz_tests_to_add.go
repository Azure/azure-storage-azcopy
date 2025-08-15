package e2etest

// TODO soon:
//
//   Next framework _use_ tasks
//   	In progress: More filter tests
//      Flesh out attribute support, in usages of objectProperties.smbAttributes, so that we can create, and verify, tests with these
//         (right now, tests that use these will fail, because they lack the necessary code to retrieve them, from the destination (and set the at the source)
//         isn't there.  See commented code marked TODO: nakulkar-msft
//      The resource manager support for S3 and BlobFS and (who will do this one) GCP?
//
//   Next framework _development_ tasks
//      DONE Change expectation lists from string to interface{}
//  	DONE at framework level. But still have todos for getting/setting some properties) Properties preservation tests
//      DONE in draft form A resume test
//
//   Discussion points
//     How to use it outside AzCopy? (Maybe let it mature a little first, then abstract out the running of AzCopy)
//     Hacks/workarounds?
//    		The beforeOpenFirstFile hook is a bit ugly, but will probably have to do.
//          AT some stage, all TODOs need to be reviewed, and actioned or remove
//     And see gaps, below
//     Need to hook it into build
//     Need to gradually remove python tests, but only remove those that are covered by the new suite.
//     Also need to remove the "unit" tests in cmd that actually hit the service. Probably they can all be covered by the new suite.
//     What do we want to do about the recursive flag?  Just leave it how it is now, set to a specific value for each test?
//        Or, do we want to have a feature where we can tell the framework to also run additional non-recursive scenarios?  We shouldn't have to write any more
//        test code, because the framework could just automatically know "Oh, I'm doing the non-recursive case. Anything in "shouldTransfer" which
//        is not at root level should be treated as if its in "shouldIgnore".
//
//  Framework gaps
//		IMPORTANT Creating remote files more quickly (or at least in parallel). Right now, it takes too long to do the setup for tests with non-trivial file sizes
//      Putting content in all our remote test files (done for blob, but not for others, and for some tests content is needed)
//      Content preservation verification. Content preservation tests. Will need a way, in resourceManager, to ask it for some proof of what the content of a specific file is.
//          Maybe a getSha256Hash method? (I'm suggesting that hashing type, since MD5 can misleading. Just downloading the MD5 from storage doesn't
//          prove that the file has that content.  Using Sha256 makes it very clear that we need to download the blob and hash it ourselves)
//  Less important framework gaps:
//      Verifying the failure messages, from shouldFail are actually present in the log
//      Responding to prompts, for testing overwrite.  Part of the implementation could come from chToStdin that is supported by the method that runs AzCopy.
//         Not 100% sure how to get the stdout back out in real time. That may be a little trickier.
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
//  	  Overwrite  (how will we process the prompts? Just through t.execDebuggableWithOutput's afterstart parameter? Requires us to assume what the prompt will be, and that there will be only 1 prompt - so can only test the "all" answers with this mechanism. But may be able to extend it.
//        Sync (special stuff that's unique to sync)
//        Preserve names (special chars, escaping etc)
//        Managed disks (our special case logic or uploading managed disks)
//        Logging
//		  OnError handing (e.g. deleting files that fail part way through) and Resume
//		Refactor (maybe):
//        The e2etest package into a set of packages - e.g. separate the test framework from the tests?  But, do we really need this?
//
//   To think about:
//
//    stripTopDir (we need to test cases with it true (i.e;. trailing /*) on the source and false
//    Add a test that uses lots of filters in combination (we once found a bug, in manual testing with combining include-path with other filters).
//      See EnumerationTestMatrix.xls in the Teams channel files list, for some possible examples of combined tests.
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
///    a little reflection on the M parameter, we can't tell which file each one is defined it. We just get their name and their func object.  And we have no (easy) way
///    of doing symbol table lookups or similar to get the file for the func.
///
/// SO we are just adopting the convention of including a prefix in the name, so that they sort sensible.  So we'll just have a two-level structure,
/// but it will look like this MySuite_MyTest/scenario

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
