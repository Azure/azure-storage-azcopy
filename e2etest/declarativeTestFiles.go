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
	"github.com/Azure/azure-storage-azcopy/cmd"
	"github.com/Azure/azure-storage-azcopy/common"
	"math"
	"strings"
	"time"
)

///////////////

type contentHeaders struct {
	// TODO: do we really need/want the fields here to be individually nillable? For Blob, at least, setting these is an all-or-nothing proposition anyway, so maybe there's little need for individual nillablity
	cacheControl       *string
	contentDisposition *string
	contentEncoding    *string
	contentLanguage    *string
	contentType        *string
	contentMD5         []byte
}

// The full set of properties, dates, info etc, that we can potentially preserve for a file or folder
// This is exposed to the declarativeResourceManagers, to create/check the objects.
// All field are pointers or interfaces to make them nil-able. Nil means "unspecified".
type objectProperties struct {
	isFolder           bool // if false, the object is a file
	size               *int64
	contentHeaders     *contentHeaders
	nameValueMetadata  map[string]string
	creationTime       *time.Time
	lastWriteTime      *time.Time
	smbAttributes      *string
	smbPermissionsSddl *string
}

// returns op.size, if present, else defaultSize
func (op objectProperties) sizeBytes(a asserter, defaultSize string) int {
	if op.size != nil {
		if *op.size > math.MaxInt32 {
			a.Error(fmt.Sprintf("unsupported size: %d", *op.size))
			return 0
		}
		return int(*op.size)
	}

	longSize, err := cmd.ParseSizeString(defaultSize, "testFiles.size")
	if longSize < math.MaxInt32 {
		a.AssertNoErr(err)
		return int(longSize)
	}
	a.Error("unsupported size: " + defaultSize)
	return 0
}

// a file or folder. Create these with the f() and folder() functions
type testObject struct {
	name                   string
	expectedFailureMessage string // the failure message that we expect to see in the log for this file/folder (only populated for expected failures)

	// info to be used at creation time. Usually, creationInfo and and verificationInfo will be the same
	// I.e. we expect the creation properties to be preserved. But, for flexibility, they can be set to something different.
	creationProperties objectProperties
	// info to be used at verification time
	verificationProperties objectProperties
}

func (t *testObject) isFolder() bool {
	if t.creationProperties.isFolder != t.verificationProperties.isFolder {
		panic("isFolder properties are misconfigured")
	}
	return t.creationProperties.isFolder
}

func (t *testObject) isRootFolder() bool {
	return t.name == "" && t.isFolder()
}

// This interface is implemented by types that provide extra information about test files
// It is to be used ONLY as parameters to the f() and folder() methods.
// It is not used in other parts of the code, since the other parts use the testObject instances that are created
// from
type withPropertyProvider interface {
	appliesToCreation() bool
	appliesToVerification() bool

	createObjectProperties() *objectProperties
}

type expectedFailureProvider interface {
	expectedFailure() string
}

// Define a file, in the expectations lists on a testFiles struct.
// (Note, if you are not going to supply any parameters other than the name, you can just use a string literal in the list
// instead of calling this function).
// Provide properties by including one or more objects that implement withPropertyProvider.
// Typically, that will just be done like this: f("foo", with{<set properties here>})
// For advanced cases, you can use verifyOnly instead of the normal "with".  The normal "with" applies to both creation
// and verification.
// You can also add withFailureMessage{"message"} to files that are expected to fail, to specify what the expected
// failure message will be in the log.
// And withStubMetadata{} to supply the metadata that indicates that an object is a directory stub.
func f(n string, properties ...withPropertyProvider) *testObject {
	haveCreationProperties := false
	haveVerificationProperties := false

	result := &testObject{name: n}
	for _, p := range properties {

		// special case for specifying expected failure message
		if efp, ok := p.(expectedFailureProvider); ok {
			if p.createObjectProperties() != nil {
				panic("a withPropertyProvider that implements expectedFailureProvider should not provide any objectProperties. It ONLY specifies the failure message.")
			}
			result.expectedFailureMessage = efp.expectedFailure()
			continue
		}

		// normal case. Not that normally the same p will return true for both p.appliesToCreation and p.appliesToVerification
		const mustBeOne = "there must be only one withPropertyProvider that specifies the %s properties.  You can't mix 'with{...}' and 'with%sOnly{...}. But you can mix 'withCreationOnly{...}' and 'withVerificationOnly{...}"
		if p.appliesToCreation() {
			if haveCreationProperties {
				panic(fmt.Sprintf(mustBeOne, "creation", "Creation"))
			}
			haveCreationProperties = true
			objProps := p.createObjectProperties()
			result.creationProperties = *objProps
		}

		if p.appliesToVerification() {
			if haveVerificationProperties {
				panic(fmt.Sprintf(mustBeOne, "verification", "Verification"))
			}
			haveVerificationProperties = true
			objProps := p.createObjectProperties()
			result.verificationProperties = *objProps
		}
	}

	return result
}

// define a folder, in the expectations lists on a testFiles struct
func folder(n string, properties ...withPropertyProvider) *testObject {
	name := strings.TrimLeft(n, "/")
	result := f(name, properties...)

	// isFolder is at properties level, not testObject level, because we need it at properties level when reading
	// the properties back from the destination (where we don't read testObjects, we just read objectProperties)
	result.creationProperties.isFolder = true
	result.verificationProperties.isFolder = true

	return result
}

//////////

// Represents a set of source files, including what we expect should happen to them
// Our expectations, e.g. success or failure, are represented by whether we put each file into
// "shouldTransfer", "shouldFail" etc.
type testFiles struct {
	defaultSize string // how big should the files be? Applies to those files that don't specify individual sizes. Uses the same K, M, G suffixes as benchmark mode's size-per-file

	// The files/folders that we expect to be transferred. Elements of the list must be strings or testObject's.
	// A string can be used if no properties need to be specified.
	// Folders included here are ignored by the verification code when we are not transferring between folder-aware
	// locations.
	shouldTransfer []interface{}

	// the files/folders that we expect NOT to be found by the enumeration. See comments on shouldTransfer
	shouldIgnore []interface{}

	// the files/folders that we expect to  fail with error (unlike the other fields, this one is composite object instead of just a filename
	shouldFail []interface{}

	// files/folders that we expect to be skipped due to an overwrite setting
	shouldSkip []interface{}
}

func (tf testFiles) cloneShouldTransfers() testFiles {
	return testFiles{
		defaultSize:    tf.defaultSize,
		shouldTransfer: tf.shouldTransfer,
	}
}

func (tf testFiles) cloneAll() testFiles {
	clone := tf
	return clone
}

// takes a mixed list of (potentially) strings and testObjects, and returns them all as test objects
// TODO: do we want to continue supporting plain strings in the expectation file lists (for convenience of test coders)
//   or force them to use f() for every file?
func (_ *testFiles) toTestObjects(rawList []interface{}, isFail bool) []*testObject {
	result := make([]*testObject, 0, len(rawList))
	for _, r := range rawList {
		if asTestObject, ok := r.(*testObject); ok {
			if asTestObject.expectedFailureMessage != "" && !isFail {
				panic("expected failures are only allowed in the shouldFail list. They are not allowed for other test files")
			}
			result = append(result, asTestObject)
		} else if asString, ok := r.(string); ok {
			result = append(result, &testObject{name: asString})
		} else {
			panic("testFiles lists may contain only strings and testObjects. Create your test objects with the f() and folder() functions")
		}
	}
	return result
}

func (tf *testFiles) allObjects(isSource bool) []*testObject {
	if isSource {
		result := make([]*testObject, 0)
		result = append(result, tf.toTestObjects(tf.shouldTransfer, false)...)
		result = append(result, tf.toTestObjects(tf.shouldIgnore, false)...) // these must be present at the source. Enumeration filters are expected to skip them
		result = append(result, tf.toTestObjects(tf.shouldSkip, false)...)   // these must be present at the source. Overwrite processing is expected to skip them
		result = append(result, tf.toTestObjects(tf.shouldFail, true)...)    // these must also be present at the source. Their transferring is expected to fail
		return result
	} else {
		// destination only needs the things that overwrite will skip
		return tf.toTestObjects(tf.shouldSkip, false)
	}
}

func (tf *testFiles) getForStatus(status common.TransferStatus, expectFolders bool, expectRootFolder bool) []*testObject {
	shouldInclude := func(f *testObject) bool {
		if !f.isFolder() {
			return true
		}

		if expectFolders {
			if f.isRootFolder() {
				return expectRootFolder
			} else {
				return true
			}
		}
		return false
	}

	result := make([]*testObject, 0)
	switch status {
	case common.ETransferStatus.Success():
		for _, f := range tf.toTestObjects(tf.shouldTransfer, false) {
			if shouldInclude(f) {
				result = append(result, f)
			}
		}
	case common.ETransferStatus.Failed():
		for _, f := range tf.toTestObjects(tf.shouldFail, true) {
			if shouldInclude(f) {
				result = append(result, f)
			}
		}
	default:
		panic("unsupported status type")
	}
	return result
}
