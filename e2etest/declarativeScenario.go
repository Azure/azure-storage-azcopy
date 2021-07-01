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
	"crypto/md5"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// E.g. if we have enumerationSuite/TestFooBar/Copy-LocalBlob the scenario is "Copy-LocalBlob"
// A scenario is treated as a sub-test by our declarative test runner
type scenario struct {

	// scenario config properties
	subtestName         string
	compactScenarioName string
	fullScenarioName    string
	operation           Operation
	validate            Validate
	fromTo              common.FromTo
	p                   params
	hs                  hooks
	fs                  testFiles

	stripTopDir bool // TODO: figure out how we'll control and use this

	// internal declarative runner state
	a          asserter
	state      scenarioState // TODO: does this really need to be a separate struct?
	needResume bool
	chToStdin  chan string
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
	s.state.source.createLocation(s.a, s)
	s.state.dest.createLocation(s.a, s)
	s.state.source.createFiles(s.a, s, true)
	s.state.dest.createFiles(s.a, s, false)
	if s.a.Failed() {
		return // setup failed. No point in running the test
	}

	// call pre-run hook
	if !s.runHook(s.hs.beforeRunJob) {
		return
	}

	// execute
	s.runAzCopy()
	if s.a.Failed() {
		return // execution failed. No point in running validation
	}

	// resume if needed
	if s.needResume {
		// TODO: create a method something like runAzCopy, but which does a resume, based on the job id returned inside runAzCopy
		// s.a.Error("Resume support is not built yet")

	}
	if s.a.Failed() {
		return // resume failed. No point in running validation
	}

	// check
	s.validateTransferStates()
	if s.a.Failed() {
		return // no point in doing more validation
	}
	s.validateProperties()
	if s.a.Failed() {
		return // no point in doing more validation
	}
	if s.validate == eValidate.AutoPlusContent() {
		s.validateContent()
	}
}

func (s *scenario) runHook(h hookFunc) bool {
	if h == nil {
		return true //nothing to do. So "successful"
	}

	// run the hook, passing ourself in as the implementation of hookHelper interface
	h(s)

	return !s.a.Failed() // was successful if the test state did not become "failed" while the hook ran
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
		case common.ELocation.Unknown():
			return &resourceDummy{}
		default:
			panic(fmt.Sprintf("location type '%s' is not yet supported in declarative tests", loc))
		}
	}

	s.state.source = createTestResource(s.fromTo.From())
	s.state.dest = createTestResource(s.fromTo.To())
}

func (s *scenario) runAzCopy() {
	s.chToStdin = make(chan string) // unubuffered seems the most predictable for our usages
	defer close(s.chToStdin)

	r := newTestRunner()
	r.SetAllFlags(s.p)

	// use the general-purpose "after start" mechanism, provided by execDebuggableWithOutput,
	// for the _specific_ purpose of running beforeOpenFirstFile, if that hook exists.
	afterStart := func() string { return "" }
	if s.hs.beforeOpenFirstFile != nil {
		r.SetAwaitOpenFlag() // tell AzCopy to wait for "open" on stdin before opening any files
		afterStart = func() string {
			time.Sleep(2 * time.Second) // give AzCopy a moment to initialize it's monitoring of stdin
			s.hs.beforeOpenFirstFile(s)
			return "open" // send open to AzCopy's stdin
		}
	}

	// run AzCopy
	const useSas = true // TODO: support other auth options (see params of RunTest)
	result, wasClean, err := r.ExecuteAzCopyCommand(
		s.operation,
		s.state.source.getParam(s.stripTopDir, useSas),
		s.state.dest.getParam(false, useSas),
		afterStart, s.chToStdin)

	if !wasClean {
		s.a.AssertNoErr(err, "running AzCopy")
	}

	s.state.result = &result
}
func (s *scenario) validateRemove() {
	removedFiles := s.fs.toTestObjects(s.fs.shouldTransfer, false)
	props := s.state.source.getAllProperties(s.a)
	if len(removedFiles) != len(props) {
		s.a.Failed()
	}
	for _, removeFile := range removedFiles {
		if _, ok := props[removeFile.name]; !ok {
			s.a.Failed()
		}
	}
}
func (s *scenario) validateTransferStates() {
	if s.operation == eOperation.Remove() {
		s.validateRemove()
		return
	}

	if s.p.deleteDestination != common.EDeleteDestination.False() {
		// TODO: implement deleteDestinationValidation
		panic("validation of deleteDestination behaviour is not yet implemented in the declarative test runner")
	}

	isSrcEncoded := s.fromTo.From().IsRemote() // TODO: is this right, reviewers?
	isDstEncoded := s.fromTo.To().IsRemote()   // TODO: is this right, reviewers?
	srcRoot, dstRoot, expectFolders, expectRootFolder, _ := s.getTransferInfo()

	// test the sets of files in the various statuses
	for _, statusToTest := range []common.TransferStatus{
		common.ETransferStatus.Success(),
		common.ETransferStatus.Failed(),
		// TODO: testing of skipped is implicit, in that they are created at the source, but don't exist in Success or Failed lists
		//       Is that OK? (Not sure what to do if it's not, because azcopy jobs show, apparently doesn't offer us a way to get the skipped list)
	} {
		expectedTransfers := s.fs.getForStatus(statusToTest, expectFolders, expectRootFolder)
		actualTransfers, err := s.state.result.GetTransferList(statusToTest)
		s.a.AssertNoErr(err)

		Validator{}.ValidateCopyTransfersAreScheduled(s.a, isSrcEncoded, isDstEncoded, srcRoot, dstRoot, expectedTransfers, actualTransfers, statusToTest)
		// TODO: how are we going to validate folder transfers????
	}

	// TODO: for failures, consider validating the failure messages (for which we have expected values, in s.fs; but don't currently have a good way to get
	//    the actual values from the test run
}

func (s *scenario) getTransferInfo() (srcRoot string, dstRoot string, expectFolders bool, expectedRootFolder bool, addedDirAtDest string) {
	srcRoot = s.state.source.getParam(false, false)
	dstRoot = s.state.dest.getParam(false, false)

	// do we expect folder transfers
	expectFolders = s.fromTo.From().IsFolderAware() &&
		s.fromTo.To().IsFolderAware() &&
		s.p.allowsFolderTransfers()
	expectRootFolder := expectFolders

	// compute dest, taking into account our stripToDir rules
	addedDirAtDest = ""
	areBothContainerLike := s.state.source.isContainerLike() && s.state.dest.isContainerLike()
	if s.stripTopDir || s.operation == eOperation.Sync() || areBothContainerLike {
		// Sync always acts like stripTopDir is true.
		// For copies between two container-like locations, we don't expect the root directory to be transferred, regardless of stripTopDir.
		// Yes, this is arguably inconsistent. But its the way its always been, and it does seem to match user expectations for copies
		// of that kind.
		expectRootFolder = false
	} else if s.fromTo.From().IsLocal() {
		addedDirAtDest = filepath.Base(srcRoot)
		dstRoot = fmt.Sprintf("%s%c%s", dstRoot, os.PathSeparator, addedDirAtDest)
	} else {
		addedDirAtDest = path.Base(srcRoot)
		dstRoot = fmt.Sprintf("%s/%s", dstRoot, addedDirAtDest)
	}

	if s.fromTo.From() == common.ELocation.Local() {
		srcRoot = common.ToExtendedPath(srcRoot)
	}
	if s.fromTo.To() == common.ELocation.Local() {
		dstRoot = common.ToExtendedPath(dstRoot)
	}

	return srcRoot, dstRoot, expectFolders, expectRootFolder, addedDirAtDest
}

func (s *scenario) validateProperties() {
	destPropsRetrieved := false
	var destProps map[string]*objectProperties // map of all files, and their properties, that now exist at the destination

	_, _, expectFolders, expectRootFolder, addedDirAtDest := s.getTransferInfo()

	// for everything that should have been transferred, verify that any expected properties have been transferred to the destination
	expectedFilesAndFolders := s.fs.getForStatus(common.ETransferStatus.Success(), expectFolders, expectRootFolder)
	for _, f := range expectedFilesAndFolders {
		expected := f.verificationProperties // use verificationProperties (i.e. what we expect) NOT creationProperties (what we made at the source). They won't ALWAYS be the same
		if expected == nil {
			// nothing to verify
			continue
		}
		if !destPropsRetrieved {
			// only incur the IO cost of getting dest properties if we have at least one thing to verify
			destPropsRetrieved = true
			destProps = s.state.dest.getAllProperties(s.a)
		}

		destName := fixSlashes(path.Join(addedDirAtDest, f.name), s.fromTo.To())
		actual, ok := destProps[destName]
		if !ok {
			// this shouldn't happen, because we only run if validateTransferStates passed, but check anyway
			// TODO: JohnR: need to remove the strip top dir prefix from the map (and normalize the delimiters)
			//    since currently uploads fail here
			var rawPaths []string
			for rawPath, _ := range destProps {
				rawPaths = append(rawPaths, rawPath)
			}
			s.a.Error(fmt.Sprintf("could not find expected file %s in keys %v", destName, rawPaths))

			return
		}

		// validate all the different things
		s.validateMetadata(expected.nameValueMetadata, actual.nameValueMetadata)
		s.validateBlobTags(expected.blobTags, actual.blobTags)
		s.validateContentHeaders(expected.contentHeaders, actual.contentHeaders)
		s.validateCreateTime(expected.creationTime, actual.creationTime)
		s.validateLastWriteTime(expected.lastWriteTime, actual.lastWriteTime)
		s.validateCPKByScope(expected.cpkScopeInfo, actual.cpkScopeInfo)
		s.validateCPKByValue(expected.cpkInfo, actual.cpkInfo)
		if expected.smbPermissionsSddl != nil {
			s.a.Error("validateProperties does not yet support the properties you are using")
			// TODO: nakulkar-msft it will be necessary to validate all of these
		}
	}
}

func (s *scenario) validateContent() {
	_, _, expectFolders, expectRootFolder, addedDirAtDest := s.getTransferInfo()

	// for everything that should have been transferred, verify that any expected properties have been transferred to the destination
	expectedFilesAndFolders := s.fs.getForStatus(common.ETransferStatus.Success(), expectFolders, expectRootFolder)
	for _, f := range expectedFilesAndFolders {
		if f.creationProperties.contentHeaders == nil {
			s.a.Failed()
		}
		if !f.isFolder() {
			expectedContentMD5 := f.creationProperties.contentHeaders.contentMD5
			resourceRelPath := fixSlashes(path.Join(addedDirAtDest, f.name), s.fromTo.To())
			actualContent := s.state.dest.downloadContent(s.a, downloadContentOptions{
				resourceRelPath: resourceRelPath,
				downloadBlobContentOptions: downloadBlobContentOptions{
					cpkInfo:      common.GetCpkInfo(s.p.cpkByValue),
					cpkScopeInfo: common.GetCpkScopeInfo(s.p.cpkByName),
				},
			})
			actualContentMD5 := md5.Sum(actualContent)
			s.a.Assert(expectedContentMD5, equals(), actualContentMD5[:], "Content MD5 validation failed")

			// We don't need to check the content md5 of all the remote resources. Checking for just one entity should do.
			return
		}
	}
}

//// Individual property validation routines

func (s *scenario) validateMetadata(expected, actual map[string]string) {
	s.a.Assert(len(expected), equals(), len(actual), "Both should have same number of metadata entries")
	for key := range expected {
		exValue := expected[key]
		actualValue, ok := actual[key]
		s.a.Assert(ok, equals(), true, fmt.Sprintf("expect key '%s' to be found in destination metadata", key))
		if ok {
			s.a.Assert(exValue, equals(), actualValue, fmt.Sprintf("Expect value for key '%s' to be '%s' but found '%s'", key, exValue, actualValue))
		}
	}
}

func (s *scenario) validateCPKByScope(expected, actual *common.CpkScopeInfo) {
	if expected == nil && actual == nil {
		return
	}
	if expected == nil || actual == nil {
		s.a.Failed()
		return
	}
	s.a.Assert(expected.EncryptionScope, equals(), actual.EncryptionScope,
		fmt.Sprintf("Expected encryption scope is: '%v' but found: '%v'", expected.EncryptionScope, actual.EncryptionScope))
}

func (s *scenario) validateCPKByValue(expected, actual *common.CpkInfo) {
	if expected == nil && actual == nil {
		return
	}
	if expected == nil || actual == nil {
		s.a.Failed()
		return
	}

	s.a.Assert(expected.EncryptionKeySha256, equals(), actual.EncryptionKeySha256,
		fmt.Sprintf("Expected encryption scope is: '%v' but found: '%v'", expected.EncryptionKeySha256, actual.EncryptionKeySha256))
}

// Validate blob tags
func (s *scenario) validateBlobTags(expected, actual common.BlobTags) {
	s.a.Assert(len(expected), equals(), len(actual), "Both should have same number of tags")
	for k, v := range expected {
		exKey := url.QueryEscape(k)
		exValue := url.QueryEscape(v)

		actualValue, ok := actual[exKey]
		s.a.Assert(ok, equals(), true, fmt.Sprintf("expect key '%s' to be found in destination metadata", exKey))
		if ok {
			s.a.Assert(exValue, equals(), actualValue, fmt.Sprintf("Expect value for key '%s' to be '%s' but found '%s'", exKey, exValue, actualValue))
		}
	}
}

func (s *scenario) validateContentHeaders(expected, actual *contentHeaders) {
	if expected == nil {
		return
	}

	if expected.cacheControl != nil {
		s.a.Assert(expected.cacheControl, equals(), actual.cacheControl,
			fmt.Sprintf("Content cache control mismatch: Expected %v, obtained %v", *expected.cacheControl, *actual.cacheControl))
	}

	if expected.contentDisposition != nil {
		s.a.Assert(expected.contentDisposition, equals(), actual.contentDisposition,
			fmt.Sprintf("Content disposition mismatch: Expected %v, obtained %v", *expected.contentDisposition, *actual.contentDisposition))
	}

	if expected.contentEncoding != nil {
		s.a.Assert(expected.contentEncoding, equals(), actual.contentEncoding,
			fmt.Sprintf("Content encoding mismatch: Expected %v, obtained %v", *expected.contentEncoding, *actual.contentEncoding))
	}

	if expected.contentLanguage != nil {
		s.a.Assert(expected.contentLanguage, equals(), actual.contentLanguage,
			fmt.Sprintf("Content language mismatch: Expected %v, obtained %v", *expected.contentLanguage, *actual.contentLanguage))
	}

	if expected.contentType != nil {
		s.a.Assert(expected.contentType, equals(), actual.contentType,
			fmt.Sprintf("Content type mismatch: Expected %v, obtained %v", *expected.contentType, *actual.contentType))
	}

	// content md5 is verified at a different place when validation method is eValidate.AutoPlusContent()
}

func (s *scenario) validateCreateTime(expected, actual *time.Time) {
	if expected == nil {
		// These properties were not explicitly stated for verification
		return
	}
	s.a.Assert(expected, equals(), actual, fmt.Sprintf("Create time mismatch: Expected %v, obtained %v",
		expected, actual))
}

func (s *scenario) validateLastWriteTime(expected, actual *time.Time) {
	if expected == nil {
		// These properties were not explicitly stated for verification
		return
	}
	s.a.Assert(expected, equals(), actual, fmt.Sprintf("Create time mismatch: Expected %v, obtained %v",
		expected, actual))
}

func (s *scenario) validateSMBAttrs(expected, actual *uint32) {
	if expected == nil {
		// These properties were not explicitly stated for verification
		return
	}
	s.a.Assert(expected, equals(), actual, fmt.Sprintf("SMB Attrs mismatch: Expected %v, obtained, %v",
		expected, actual))
}

func (s *scenario) cleanup() {

	if s.a.Failed() && (GlobalInputManager{}.KeepFailedData()) {
		return // don't clean up. Leave the failed data so the dev can investigate the failure
	}

	if s.state.source != nil {
		s.state.source.cleanup(s.a)
	}
	if s.state.dest != nil {
		s.state.dest.cleanup(s.a)
	}
}

/// support the hookHelper functions. These are use by our hooks to modify the state, or resources, of the running test

func (s *scenario) FromTo() common.FromTo {
	return s.fromTo
}

func (s *scenario) Operation() Operation {
	return s.operation
}

func (s *scenario) GetModifiableParameters() *params {
	return &s.p
}

func (s *scenario) GetTestFiles() testFiles {
	return s.fs
}

func (s *scenario) CreateFiles(fs testFiles, atSource bool) {
	s.fs = fs
	if atSource {
		s.state.source.createFiles(s.a, s, true)
	} else {
		s.state.dest.createFiles(s.a, s, false)
	}
}

func (s *scenario) CreateSourceSnapshot() {
	s.state.source.createSourceSnapshot(s.a)
}

func (s *scenario) CancelAndResume() {
	s.a.Assert(s.p.cancelFromStdin, equals(), true, "cancelFromStdin must be set in parameters, to use CancelAndResume")
	s.needResume = true
	s.chToStdin <- "cancel"
}

func (s *scenario) SkipTest() {
	s.a.Skip("Skipping test")
}
