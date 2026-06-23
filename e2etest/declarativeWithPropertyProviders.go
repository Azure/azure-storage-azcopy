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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// All the structs in this file have names starting with "with", to make the readability flow when they are used
// as parameters to the f() or folder() functions.

// This is the main property provider, and the only one most tests will ever need.
// For ease of use, and conciseness in the tests, the members of this struct are NOT pointers.
// Instead, default values in these structs are mapped to nils, inside the createObjectProperties method.
type with struct {
	size string // uses our standard K, M, G suffix

	symlinkTarget string

	posixProperties objectUnixStatContainer

	cacheControl       string
	contentDisposition string
	contentEncoding    string
	contentLanguage    string
	contentType        string
	contentMD5         []byte

	nameValueMetadata map[string]*string
	blobTags          string
	blobType          common.BlobType
	// blobVersions is a list of strings defining the data stored inside the object's body.
	// These versions are treated as a key, as well, and correspond to the version IDs Azure assigns.
	blobVersions       uint
	lastWriteTime      time.Time
	creationTime       time.Time
	smbAttributes      uint32
	smbPermissionsSddl string
	adlsPermissionsACL string
	cpkByName          string
	cpkByValue         bool
}

func (with) appliesToCreation() bool {
	return true
}

func (with) appliesToVerification() bool {
	return true
}

// maps non-nillable fields (which are easy to create in the tests) to nillable ones, which have clearer meaning in
// the resourceManagers.
func (w with) createObjectProperties() *objectProperties {
	result := &objectProperties{}
	populated := false

	ensureContentPropsExist := func() {
		if result.contentHeaders == nil {
			result.contentHeaders = &contentHeaders{}
		}
	}

	if w.size != "" {
		populated = true
		longSize, err := cmd.ParseSizeString(w.size, "with.size")
		common.PanicIfErr(err) // TODO: any better option?
		result.size = &longSize
	}

	// content headers
	if w.symlinkTarget != "" {
		populated = true
		result.symlinkTarget = &w.symlinkTarget
	}
	if w.cacheControl != "" {
		populated = true
		ensureContentPropsExist()
		result.contentHeaders.cacheControl = &w.cacheControl
	}
	if w.contentDisposition != "" {
		populated = true
		ensureContentPropsExist()
		result.contentHeaders.contentDisposition = &w.contentDisposition
	}
	if w.contentEncoding != "" {
		populated = true
		ensureContentPropsExist()
		result.contentHeaders.contentEncoding = &w.contentEncoding
	}
	if w.contentLanguage != "" {
		populated = true
		ensureContentPropsExist()
		result.contentHeaders.contentLanguage = &w.contentLanguage
	}
	if w.contentMD5 != nil {
		populated = true
		ensureContentPropsExist()
		result.contentHeaders.contentMD5 = w.contentMD5
	}
	if w.contentType != "" {
		populated = true
		ensureContentPropsExist()
		result.contentHeaders.contentType = &w.contentType
	}

	// other properties
	if w.nameValueMetadata != nil {
		populated = true
		result.nameValueMetadata = w.nameValueMetadata
	}

	if w.blobTags != "" {
		populated = true
		result.blobTags = common.ToCommonBlobTagsMap(w.blobTags)
	}
	if w.blobType != common.EBlobType.Detect() {
		populated = true
		result.blobType = w.blobType
	}
	if w.blobVersions > 0 {
		populated = true
		result.blobVersions = &w.blobVersions
	}
	if w.lastWriteTime != (time.Time{}) {
		populated = true
		result.lastWriteTime = &w.lastWriteTime
	}
	if w.creationTime != (time.Time{}) {
		populated = true
		result.creationTime = &w.creationTime
	}
	if w.smbAttributes != 0 {
		populated = true
		result.smbAttributes = &w.smbAttributes
	}
	if w.smbPermissionsSddl != "" {
		populated = true
		result.smbPermissionsSddl = &w.smbPermissionsSddl
	}
	if w.adlsPermissionsACL != "" {
		populated = true
		result.adlsPermissionsACL = &w.adlsPermissionsACL
	}
	if !w.posixProperties.Empty() {
		populated = true
		result.posixProperties = &w.posixProperties
	}

	if w.cpkByName != "" {
		populated = true
		cpkScopeInfo := common.GetCpkScopeInfo(w.cpkByName)
		result.cpkScopeInfo = cpkScopeInfo
	}

	if w.cpkByValue {
		populated = true
		cpkInfo, _ := common.GetCpkInfo(w.cpkByValue)
		result.cpkInfo = cpkInfo
	}

	if populated {
		return result
	} else {
		return nil // this gives consumers a shortcut way to know "there is no validation to do here", and so avoid expensive network calls to get destination properties when there is no validation needed
	}
}

////

// use createOnly if you want to define properties that should be used when creating an object, but not
// used when verifying the state of the transferred object. Generally you'll have no use for this.
// Just use "with", and the test framework will do the right thing.
type createOnly struct {
	with
}

func (createOnly) appliesToVerification() bool {
	return false
}

////

// Use verifyOnly if you need to specify some properties that should NOT be applied to the file when it is created,
// but should be present on it after) the transfer
type verifyOnly struct {
	with
}

func (verifyOnly) appliesToCreation() bool {
	return false
}

////

// use withDirStubMetadata to say that file should be created with metadata that says its a directory stub, and it should have zero size
type withDirStubMetadata struct{}

func (withDirStubMetadata) appliesToCreation() bool {
	return true
}

func (withDirStubMetadata) appliesToVerification() bool {
	return true // since IF we ever do move these stubs, we expect them to retain their stub metadata
}

func (withDirStubMetadata) createObjectProperties() *objectProperties {
	m := map[string]*string{"hdi_isfolder": to.Ptr("true")} // special flag that says this file is a stub
	size := int64(0)
	return &objectProperties{
		size:              &size,
		nameValueMetadata: m,
	}
}

////

////

// use withError ONLY on files in the shouldFail section.
// It allows you to say what the error should be
// TODO: as at 1 July 2020, we are not actually validating these.  Should we? It could be nice.  If we don't,
//
//	remove this type and its usages, and the expectedFailureProvider interface
type withError struct {
	msg string
}

func (withError) appliesToCreation() bool {
	return false
}

func (withError) appliesToVerification() bool {
	return false
}

func (withError) createObjectProperties() *objectProperties {
	return nil // implementing withPropertyProvider is just to trick the type system into letting us pass this to f() and folder(). Our implementation doesn't DO anything
}

func (w withError) expectedFailure() string {
	return w.msg
}
