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
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/sddl"
)

// E.g. if we have enumerationSuite/TestFooBar/Copy-LocalBlob the scenario is "Copy-LocalBlob"
// A scenario is treated as a subtest by our declarative test runner
type scenario struct {
	// scenario config properties
	srcAccountType      AccountType
	destAccountType     AccountType
	subtestName         string
	compactScenarioName string
	fullScenarioName    string
	operation           Operation
	validate            Validate
	fromTo              common.FromTo
	credTypes           [2]common.CredentialType
	p                   params
	hs                  hooks
	fs                  testFiles

	stripTopDir bool // TODO: figure out how we'll control and use this

	// internal declarative runner state
	a          asserter
	state      scenarioState // TODO: does this really need to be a separate struct?
	needResume bool
	needCancel bool
	chToStdin  chan string
}

type scenarioState struct {
	source resourceManager
	dest   resourceManager
	result *CopyOrSyncCommandResult
}

// Run runs one test scenario
func (s *scenario) Run() {
	defer func() { // catch a test panicking
		if err := recover(); err != nil {
			s.a.Error(fmt.Sprintf("Test panicked: %v\n%s", err, debug.Stack()))
		}
	}()
	defer s.cleanup()

	// setup runner
	azcopyDir, err := os.MkdirTemp("", "")
	if err != nil {
		s.a.Error(err.Error())
		return
	}
	azcopyRan := false
	defer func() {
		if os.Getenv("AZCOPY_E2E_LOG_OUTPUT") == "" {
			s.a.Assert(os.RemoveAll(azcopyDir), equals(), nil)
			return // no need, just delete logdir
		}

		err := os.MkdirAll(os.Getenv("AZCOPY_E2E_LOG_OUTPUT"), os.ModePerm|os.ModeDir)
		if err != nil {
			s.a.Assert(err, equals(), nil)
			return
		}
		if azcopyRan && s.a.Failed() {
			s.uploadLogs(azcopyDir)
			s.a.(*testingAsserter).t.Log("uploaded logs for job " + s.state.result.jobID.String() + " as an artifact")
		}
	}()

	// setup scenario
	// First, validate the accounts make sense for the source/dests
	if s.srcAccountType.IsBlobOnly() {
		s.a.Assert(true, equals(), s.fromTo.From() == common.ELocation.Blob() || s.fromTo.From() == common.ELocation.BlobFS())
	}

	if s.destAccountType.IsManagedDisk() {
		s.a.Assert(s.destAccountType, notEquals(), EAccountType.StdManagedDisk(), "Upload is not supported in MD testing yet")
		s.a.Assert(s.destAccountType, notEquals(), EAccountType.OAuthManagedDisk(), "Upload is not supported in MD testing yet")
		s.a.Assert(s.destAccountType, notEquals(), EAccountType.LargeManagedDisk(), "Upload is not supported in MD testing yet")
		s.a.Assert(s.destAccountType, notEquals(), EAccountType.ManagedDiskSnapshot(), "Cannot upload to a MD snapshot")
		s.a.Assert(s.destAccountType, notEquals(), EAccountType.ManagedDiskSnapshotOAuth(), "Cannot upload to a MD snapshot")
		s.a.Assert(s.destAccountType, notEquals(), EAccountType.LargeManagedDiskSnapshot(), "Cannot upload to a MD snapshot")
		s.a.Assert(true, equals(), s.fromTo.From() == common.ELocation.Blob() || s.fromTo.From() == common.ELocation.BlobFS())
	}

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
	azcopyRan = true
	s.runAzCopy(azcopyDir)
	if s.a.Failed() {
		return // execution failed. No point in running validation
	}

	// resume if needed
	if s.needResume {
		tx, err := s.state.result.GetTransferList(common.ETransferStatus.Cancelled(), azcopyDir)
		s.a.AssertNoErr(err, "Failed to get transfer list for Cancelled")
		s.a.Assert(len(tx), equals(), len(s.p.debugSkipFiles), "Job cancel didn't completely work")

		if !s.runHook(s.hs.beforeResumeHook) {
			return
		}

		s.resumeAzCopy(azcopyDir)
	}
	if s.a.Failed() {
		return // resume failed. No point in running validation
	}

	// cancel if needed
	if s.needCancel {
		s.cancelAzCopy(azcopyDir)
	}
	if s.a.Failed() {
		return // resume failed. No point in running validation
	}

	// check
	s.validateTransferStates(azcopyDir)
	if s.a.Failed() {
		return // no point in doing more validation
	}

	if !s.p.destNull {
		s.validateProperties()
		if s.a.Failed() {
			return // no point in doing more validation
		}

		if s.validate&eValidate.AutoPlusContent() != 0 {
			s.validateContent()
		}
	}

	s.runHook(s.hs.afterValidation)
}

func (s *scenario) uploadLogs(logDir string) {
	if s.state.result == nil || os.Getenv("AZCOPY_E2E_LOG_OUTPUT") == "" {
		return // nothing to upload
	}
	// sometimes, the log dir cannot be copied because the destination is on another drive. So, we'll copy the files instead by hand.
	files, err := os.ReadDir(logDir)
	s.a.AssertNoErr(err, "Failed to read log dir")
	jobId := ""
	for _, file := range files { // first, find the job ID
		if strings.HasSuffix(file.Name(), ".log") {
			jobId = strings.TrimSuffix(strings.TrimSuffix(file.Name(), "-scanning"), ".log")
			break
		}
	}

	// Create the destination log directory
	destLogDir := filepath.Join(os.Getenv("AZCOPY_E2E_LOG_OUTPUT"), jobId)
	err = os.MkdirAll(destLogDir, os.ModePerm|os.ModeDir)
	s.a.AssertNoErr(err, "Failed to create log dir")

	// Copy the files by hand
	err = filepath.WalkDir(logDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relPath := strings.TrimPrefix(path, logDir)
		if d.IsDir() {
			err = os.MkdirAll(filepath.Join(destLogDir, relPath), os.ModePerm|os.ModeDir)
			return err
		}

		// copy the file
		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}

		destFile, err := os.Create(filepath.Join(destLogDir, relPath))
		if err != nil {
			return err
		}

		defer srcFile.Close()
		defer destFile.Close()

		_, err = io.Copy(destFile, srcFile)
		if err != nil {
			return err
		}

		return err
	})
	s.a.AssertNoErr(err, "Failed to copy log files")
}

func (s *scenario) runHook(h hookFunc) bool {
	if h == nil {
		return true // nothing to do. So "successful"
	}

	// run the hook, passing ourself in as the implementation of hookHelper interface
	h(s)

	return !s.a.Failed() // was successful if the test state did not become "failed" while the hook ran
}

func (s *scenario) assignSourceAndDest() {
	createTestResource := func(loc common.Location, isSourceAcc bool) resourceManager {
		accType := s.srcAccountType
		if !isSourceAcc {
			accType = s.destAccountType
		}

		// TODO: handle account to account (multi-container) scenarios
		switch loc {
		case common.ELocation.Local():
			return &resourceLocal{common.Iff[string](s.p.destNull && !isSourceAcc, common.Dev_Null, "")}
		case common.ELocation.File():
			return &resourceAzureFileShare{accountType: accType}
		case common.ELocation.Blob(), common.ELocation.BlobFS():
			// TODO: handle the multi-container (whole account) scenario
			// TODO: handle wider variety of account types
			if accType.IsManagedDisk() {
				mdCfg, err := GlobalInputManager{}.GetMDConfig(accType)
				s.a.AssertNoErr(err)
				return &resourceManagedDisk{config: *mdCfg}
			}

			return &resourceBlobContainer{accountType: accType, isBlobFS: loc == common.ELocation.BlobFS()}
		case common.ELocation.S3():
			s.a.Error("Not implemented yet for S3")
			return &resourceDummy{}
		case common.ELocation.Unknown():
			return &resourceDummy{}
		default:
			panic(fmt.Sprintf("location type '%s' is not yet supported in declarative tests", loc))
		}
	}

	s.state.source = createTestResource(s.fromTo.From(), true)
	s.state.dest = createTestResource(s.fromTo.To(), false)
}

func (s *scenario) runAzCopy(logDirectory string) {
	s.chToStdin = make(chan string) // unubuffered seems the most predictable for our usages
	defer close(s.chToStdin)

	tf := s.GetTestFiles()

	r := newTestRunner()
	r.SetAllFlags(s)

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

	needsSAS := func(credType common.CredentialType) bool {
		return credType == common.ECredentialType.Anonymous() || credType == common.ECredentialType.MDOAuthToken()
	}

	needsFromTo := s.destAccountType == EAccountType.Azurite() || s.srcAccountType == EAccountType.Azurite()

	var destObjTarget objectTarget
	if tf.destTarget != "" {
		destObjTarget.objectName = tf.destTarget
	} else if tf.objectTarget.objectName != "" &&
		// Object target must have no list of versions.
		(len(tf.objectTarget.versions) == 0 || (len(tf.objectTarget.versions) == 1 && !tf.objectTarget.singleVersionList)) {
		destObjTarget.objectName = tf.objectTarget.objectName
	}

	// run AzCopy
	result, wasClean, err := r.ExecuteAzCopyCommand(
		s.operation,
		s.state.source.getParam(s.a, s.stripTopDir, needsSAS(s.credTypes[0]), tf.objectTarget),
		s.state.dest.getParam(s.a, false, needsSAS(s.credTypes[1]), destObjTarget),
		s.credTypes[0].IsAzureOAuth() || s.credTypes[1].IsAzureOAuth(), // needsOAuth
		s.p.AutoLoginType,
		needsFromTo,
		s.fromTo,
		afterStart, s.chToStdin, logDirectory)

	if !wasClean {
		s.a.AssertNoErr(err, "running AzCopy")
	}

	// Generally, a cancellation is done when auth fails.
	if result.finalStatus.JobStatus == common.EJobStatus.Cancelled() {
		for _, v := range result.finalStatus.FailedTransfers {
			if v.ErrorCode == 403 {
				s.a.Error("Job " + result.jobID.String() + " authorization failed, perhaps SPN auth or the SAS token is bad?. Error message: " + v.ErrorMessage)
			}
		}
	}

	s.state.result = &result
}

func (s *scenario) cancelAzCopy(logDir string) {
	r := newTestRunner()
	s.operation = eOperation.Cancel()
	r.SetAllFlags(s)

	afterStart := func() string { return "" }
	result, wasClean, err := r.ExecuteAzCopyCommand(
		eOperation.Cancel(),
		s.state.result.jobID.String(),
		"",
		false,
		"",
		false,
		s.fromTo,
		afterStart,
		s.chToStdin,
		logDir,
	)

	if !wasClean {
		s.a.AssertNoErr(err, "running AzCopy")
	}

	s.state.result = &result
}

func (s *scenario) resumeAzCopy(logDir string) {
	s.chToStdin = make(chan string) // unubuffered seems the most predictable for our usages
	defer close(s.chToStdin)

	r := newTestRunner()
	if sas := s.state.source.getSAS(); s.GetTestFiles().sourcePublic == nil && sas != "" {
		r.flags["source-sas"] = strings.TrimPrefix(sas, "?")
	}
	if sas := s.state.dest.getSAS(); sas != "" {
		r.flags["destination-sas"] = strings.TrimPrefix(sas, "?")
	}

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

	result, wasClean, err := r.ExecuteAzCopyCommand(
		eOperation.Resume(),
		s.state.result.jobID.String(),
		"",
		s.credTypes[0].IsAzureOAuth() || s.credTypes[1].IsAzureOAuth(),
		s.p.AutoLoginType,
		false,
		s.fromTo,
		afterStart,
		s.chToStdin,
		logDir,
	)

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
func (s *scenario) validateTransferStates(azcopyDir string) {
	if s.operation == eOperation.Remove() {
		s.validateRemove()
		return
	}

	if s.operation == eOperation.Benchmark() {
		// TODO: Benchmark validation will occur in new e2e test framework. For now the goal is to test that AzCopy doesn't crash.
		return
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
		expectedTransfers := s.fs.getForStatus(s, statusToTest, expectFolders, expectRootFolder)
		actualTransfers, err := s.state.result.GetTransferList(statusToTest, azcopyDir)
		s.a.AssertNoErr(err)

		Validator{}.ValidateCopyTransfersAreScheduled(s, isSrcEncoded, isDstEncoded, srcRoot, dstRoot, expectedTransfers, actualTransfers, statusToTest, expectFolders)
		// TODO: how are we going to validate folder transfers????
	}

	// TODO: for failures, consider validating the failure messages (for which we have expected values, in s.fs; but don't currently have a good way to get
	//    the actual values from the test run
}

func (s *scenario) getTransferInfo() (srcRoot string, dstRoot string, expectFolders bool, expectedRootFolder bool, addedDirAtDest string) {
	srcRoot = s.state.source.getParam(s.a, false, false, objectTarget{})
	dstRoot = s.state.dest.getParam(s.a, false, false, objectTarget{})

	srcBase := filepath.Base(srcRoot)
	srcRootURL, err := url.Parse(srcRoot)
	if err == nil {
		srcBase, _ = trimBaseSnapshotDetails(s.a, srcRootURL, s.fromTo.From(), s.srcAccountType)
		srcBase = filepath.Base(srcBase)
	}

	// do we expect folder transfers
	expectFolders = (s.fromTo.From().IsFolderAware() &&
		s.fromTo.To().IsFolderAware() &&
		s.p.allowsFolderTransfers()) ||
		(s.p.preserveSMBPermissions && s.FromTo().From().SupportsHnsACLs() && s.FromTo().To().SupportsHnsACLs()) ||
		(s.p.preservePOSIXProperties && (s.FromTo() == common.EFromTo.LocalBlob() || s.FromTo() == common.EFromTo.BlobBlob() || s.FromTo() == common.EFromTo.BlobLocal()))
	expectRootFolder := expectFolders

	// compute dest, taking into account our stripToDir rules
	addedDirAtDest = ""
	areBothContainerLike := s.state.source.isContainerLike() && s.state.dest.isContainerLike() && !s.p.preserveSMBPermissions // There are no permission-compatible sources and destinations that do not feature support for root folder perms anymore*

	tf := s.GetTestFiles()
	if s.stripTopDir || s.operation == eOperation.Sync() || areBothContainerLike {
		// Sync always acts like stripTopDir is true.
		// For copies between two container-like locations, we don't expect the root directory to be transferred, regardless of stripTopDir.
		// Yes, this is arguably inconsistent. But its the way its always been, and it does seem to match user expectations for copies
		// of that kind.
		expectRootFolder = false
	} else if expectRootFolder && s.fromTo == common.EFromTo.BlobLocal() && s.destAccountType != EAccountType.HierarchicalNamespaceEnabled() && tf.objectTarget.objectName == "" {
		expectRootFolder = false // we can only persist the root folder if it's a subfolder of the container on Blob.

		if tf.objectTarget.objectName == "" && tf.destTarget == "" {
			addedDirAtDest = path.Base(srcRoot)
		} else if tf.destTarget != "" {
			addedDirAtDest = tf.destTarget
		}
		dstRoot = fmt.Sprintf("%s/%s", dstRoot, addedDirAtDest)
	} else if s.fromTo.From().IsLocal() {
		if tf.objectTarget.objectName == "" && tf.destTarget == "" {
			addedDirAtDest = srcBase
		} else if tf.destTarget != "" {
			addedDirAtDest = tf.destTarget
		}
		dstRoot = fmt.Sprintf("%s%c%s", dstRoot, os.PathSeparator, addedDirAtDest)
	} else if s.state.source.isContainerLike() && s.state.dest.isContainerLike() && s.p.preserveSMBPermissions {
		// Preserving permissions includes the root folder, but for container-container, we don't expect any added folder name.
		expectRootFolder = true
	} else {
		if tf.objectTarget.objectName == "" && tf.destTarget == "" {
			addedDirAtDest = srcBase
		} else if tf.destTarget != "" {
			addedDirAtDest = tf.destTarget
		}
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
	expectedFilesAndFolders := s.fs.getForStatus(s, common.ETransferStatus.Success(), expectFolders, expectRootFolder)
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

		var destName string
		if addedDirAtDest == "" {
			destName = f.name
		} else if f.name == "" {
			destName = addedDirAtDest
		} else {
			destName = addedDirAtDest + "/" + f.name
		}
		destName = fixSlashes(destName, s.fromTo.To())
		actual, ok := destProps[destName]
		if !ok {
			// this shouldn't happen, because we only run if validateTransferStates passed, but check anyway
			// TODO: JohnR: need to remove the strip top dir prefix from the map (and normalize the delimiters)
			//    since currently uploads fail here
			var rawPaths []string
			for rawPath := range destProps {
				rawPaths = append(rawPaths, rawPath)
			}
			s.a.Error(fmt.Sprintf("could not find expected file %s in keys %v", destName, rawPaths))

			return
		}

		// validate all the different things
		s.validatePOSIXProperties(f, actual.nameValueMetadata)
		s.validateSymlink(f, actual.nameValueMetadata)
		s.validateMetadata(f, expected.nameValueMetadata, actual.nameValueMetadata)
		s.validateBlobTags(expected.blobTags, actual.blobTags)
		s.validateContentHeaders(expected.contentHeaders, actual.contentHeaders)
		s.validateCreateTime(expected.creationTime, actual.creationTime)
		s.validateLastWriteTime(expected.lastWriteTime, actual.lastWriteTime)
		s.validateCPKByScope(expected.cpkScopeInfo, actual.cpkScopeInfo)
		s.validateCPKByValue(expected.cpkInfo, actual.cpkInfo)
		s.validateADLSACLs(f.name, expected.adlsPermissionsACL, actual.adlsPermissionsACL)
		if expected.smbPermissionsSddl != nil {
			if actual.smbPermissionsSddl == nil {
				s.a.Error("Expected a SDDL on file " + destName + ", but none was found")
			} else {
				s.validateSMBPermissionsByValue(*expected.smbPermissionsSddl, *actual.smbPermissionsSddl, destName)
			}
		}
	}
}

func (s *scenario) validateSMBPermissionsByValue(expected, actual string, objName string) {
	expectedSDDL, err := sddl.ParseSDDL(expected)
	s.a.AssertNoErr(err)

	actualSDDL, err := sddl.ParseSDDL(actual)
	s.a.AssertNoErr(err)

	s.a.Assert(expectedSDDL.Compare(actualSDDL), equals(), true)
}

func (s *scenario) validateContent() {
	_, _, expectFolders, expectRootFolder, addedDirAtDest := s.getTransferInfo()

	// for everything that should have been transferred, verify that any expected properties have been transferred to the destination
	expectedFilesAndFolders := s.fs.getForStatus(s, common.ETransferStatus.Success(), expectFolders, expectRootFolder)
	for _, f := range expectedFilesAndFolders {
		if f.creationProperties.contentHeaders == nil {
			s.a.Failed()
		}
		if f.hasContentToValidate() {
			expectedContentMD5 := f.creationProperties.contentHeaders.contentMD5
			var destName string
			if addedDirAtDest == "" {
				destName = f.name
			} else if f.name == "" {
				destName = addedDirAtDest
			} else {
				destName = addedDirAtDest + "/" + f.name
			}
			destName = fixSlashes(destName, s.fromTo.To())
			actualContent := s.state.dest.downloadContent(s.a, downloadContentOptions{
				resourceRelPath: destName,
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

func (s *scenario) validatePOSIXProperties(f *testObject, metadata map[string]*string) {
	if !s.p.preservePOSIXProperties {
		return
	}

	_, _, _, _, addedDirAtDest := s.getTransferInfo()

	var adapter common.UnixStatAdapter
	switch s.fromTo.To() {
	case common.ELocation.Local():
		adapter = osScenarioHelper{}.GetUnixStatAdapterForFile(s.a, filepath.Join(s.state.dest.(*resourceLocal).dirPath, addedDirAtDest, f.name))
	case common.ELocation.Blob():
		var err error
		adapter, err = common.ReadStatFromMetadata(metadata, 0)
		s.a.AssertNoErr(err, "reading stat from metadata")
	}

	s.a.Assert(f.verificationProperties.posixProperties.EquivalentToStatAdapter(adapter), equals(), "", "POSIX properties were mismatched")
}

func (s *scenario) validateSymlink(f *testObject, metadata map[string]*string) {
	c := s.GetAsserter()

	prepareSymlinkForComparison := func(oldName string) string {
		switch s.fromTo {
		case common.EFromTo.LocalBlob():
			source := s.state.source.(*resourceLocal)

			return strings.TrimPrefix(oldName, source.dirPath+common.OS_PATH_SEPARATOR)
		case common.EFromTo.BlobLocal():
			dest := s.state.dest.(*resourceLocal)
			_, _, _, _, addedDirAtDest := s.getTransferInfo()

			return strings.TrimPrefix(oldName, path.Join(dest.dirPath, addedDirAtDest)+common.OS_PATH_SEPARATOR)
		case common.EFromTo.BlobBlob():
			return oldName // no adjustment necessary
		default:
			c.Error("Symlink persistence is only available on Local<->Blob->Blob")
			return ""
		}
	}

	if f.verificationProperties.entityType == common.EEntityType.Symlink() {
		c.Assert(s.p.symlinkHandling, equals(), common.ESymlinkHandlingType.Preserve()) // we should only be doing this if we're persisting symlinks

		dest := s.GetDestination()
		_, _, _, _, addedDirAtDest := s.getTransferInfo()
		switch s.fromTo.To() {
		case common.ELocation.Local():
			symlinkDest := path.Join(dest.(*resourceLocal).dirPath, addedDirAtDest, f.name)
			stat, err := os.Lstat(symlinkDest)
			c.AssertNoErr(err)
			c.Assert(stat.Mode()&os.ModeSymlink, equals(), os.ModeSymlink, "the file is not a symlink")

			oldName, err := os.Readlink(symlinkDest)
			c.AssertNoErr(err)
			c.Assert(prepareSymlinkForComparison(oldName), equals(), *f.verificationProperties.symlinkTarget)
		case common.ELocation.Blob():
			val, ok := metadata[common.POSIXSymlinkMeta]
			c.Assert(ok, equals(), true)
			c.Assert(*val, equals(), "true")

			content := dest.downloadContent(c, downloadContentOptions{
				resourceRelPath: fixSlashes(path.Join(addedDirAtDest, f.name), common.ELocation.Blob()),
				downloadBlobContentOptions: downloadBlobContentOptions{
					cpkInfo:      common.GetCpkInfo(s.p.cpkByValue),
					cpkScopeInfo: common.GetCpkScopeInfo(s.p.cpkByName),
				},
			})

			c.Assert(prepareSymlinkForComparison(string(content)), equals(), *f.verificationProperties.symlinkTarget)
		default:
			c.Error("Cannot validate symlink from endpoint other than local/blob")
		}
	}
}

func metadataWithProperCasing(original map[string]*string) map[string]*string {
	result := make(map[string]*string)
	for k, v := range original {
		result[strings.ToLower(k)] = v
	}
	return result
}

// // Individual property validation routines
func (s *scenario) validateMetadata(f *testObject, expected, actual map[string]*string) {
	cased := metadataWithProperCasing(actual)

	for _, v := range common.AllLinuxProperties { // properties are evaluated elsewhere
		delete(expected, v)
		delete(cased, v)
	}

	s.a.Assert(len(cased), equals(), len(expected), "Both should have same number of metadata entries")

	for key := range expected {
		exValue := expected[key]
		actualValue, ok := cased[key]
		s.a.Assert(ok, equals(), true, fmt.Sprintf("%s: expect key '%s' to be found in destination metadata", f.name, key))
		if ok {
			s.a.Assert(exValue, equals(), actualValue, fmt.Sprintf("%s: Expect value for key '%s' to be '%s' but found '%s'", f.name, key, *exValue, *actualValue))
		}
	}
}

func (s *scenario) validateADLSACLs(name string, expected, actual *string) {
	if expected == nil { // Don't test when we don't want to
		return
	}

	if actual == nil {
		e, a := *expected, "nil"
		if actual != nil {
			a = *actual
		}

		s.a.Assert(true, equals(), false, fmt.Sprintf("for object %s: If expected ACLs are nonzero, actual must be nonzero and equal (expected: %s actual: %s)", name, e, a))
		return
	}

	s.a.Assert(expected, equals(), actual, fmt.Sprintf("for object %s: Expected Gen 2 ACL: %s but found: %s", name, *expected, *actual))
}

func (s *scenario) validateCPKByScope(expected, actual *blob.CPKScopeInfo) {
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

func (s *scenario) validateCPKByValue(expected, actual *blob.CPKInfo) {
	if expected == nil && actual == nil {
		return
	}
	if expected == nil || actual == nil {
		s.a.Failed()
		return
	}

	s.a.Assert(expected.EncryptionKeySHA256, equals(), actual.EncryptionKeySHA256,
		fmt.Sprintf("Expected encryption scope is: '%v' but found: '%v'", expected.EncryptionKeySHA256, actual.EncryptionKeySHA256))
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

// / support the hookHelper functions. These are use by our hooks to modify the state, or resources, of the running test

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

func (s *scenario) SetTestFiles(fs testFiles) {
	s.fs = fs
}

func (s *scenario) CreateFiles(fs testFiles, atSource bool, setTestFiles bool, createSourceFilesAtDest bool) {
	original := s.fs
	s.fs = fs
	if atSource {
		s.state.source.createFiles(s.a, s, true)
	} else {
		s.state.dest.createFiles(s.a, s, createSourceFilesAtDest)
	}

	if !setTestFiles {
		s.fs = original
	}
}

func (s *scenario) CreateFile(f *testObject, atSource bool) {
	if atSource {
		s.state.source.createFile(s.a, f, s, atSource)
	} else {
		s.state.dest.createFile(s.a, f, s, atSource)
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

func (s *scenario) GetAsserter() asserter {
	return s.a
}

func (s *scenario) GetDestination() resourceManager {
	return s.state.dest
}

func (s *scenario) GetSource() resourceManager {
	return s.state.source
}
