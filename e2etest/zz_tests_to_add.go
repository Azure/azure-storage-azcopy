package e2etest

// TODO: right now, as soon as one scenario fails in a test,
//    we stop executing them, and won't execute any other scearios
//    in that test.  Should we change this? It could be a bit difficult,
//    but might be possible with the Check vs Assert and Fail method in gocheck (chk)

// TODO: consider how to add tests to cover the following

// Running these tests doesn't ensure that AZCOPY_E2E_EXECUTABLE_PATH points to an UP TO DATE build of the app.
// Could we somehow make it do that?

// Piping (we don't have any piping tests in the new suite yet)

// --cap-mbps
// page blob auto pacer
// exclusiveStringMap (usage of it, by the STE)
// Resume
// Operations other than copy and sync
// Account-to-account (i.e. whole of account) copies (since right now we only support single-container copies in the declarative tests)

// unit tests, of the test code, to add
//   Read and Seek on common.RandomDataGenerator
