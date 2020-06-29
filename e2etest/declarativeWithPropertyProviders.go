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
	"github.com/Azure/azure-storage-azcopy/cmd"
	"github.com/Azure/azure-storage-azcopy/common"
	"time"
)

// All the structs in this file have names starting with "with", to make the readability flow when they are used
// as parameters to the f() or folder() functions.

// This is the main property provider, and the only one most tests will ever need.
// For ease of use, and conciseness in the tests, the members of this struct are NOT pointers.
// Instead, default values in these structs are mapped to nils, inside the createObjectProperties method.
type with struct {
	size string // uses our standard K, M, G suffix

	cacheControl       string
	contentDisposition string
	contentEncoding    string
	contentLanguage    string
	contentType        string
	contentMD5         []byte

	nameValueMetadata  map[string]string
	lastWriteTime      time.Time
	creationTime       time.Time
	smbAttributes      string // TODO: should this be uint or a custom struct? (that knows how to convert to/from textual, Storage and Windows representations?)
	smbPermissionsSddl string
}

func (with) appliesToCreation() bool {
	return true
}

func (with) appliesToVerification() bool {
	return true
}

// maps non-nillable fields (which are easy to create in the tests) to nillable ones, which have clearer meaning in
// the resourceManagers
func (w with) createObjectProperties() *objectProperties {
	result := &objectProperties{}

	ensureContentPropsExist := func() {
		if result.contentHeaders == nil {
			result.contentHeaders = &contentHeaders{}
		}
	}

	if w.size != "" {
		longSize, err := cmd.ParseSizeString(w.size, "with.size")
		common.PanicIfErr(err) // TODO: any better option?
		result.size = &longSize
	}

	// content headers
	if w.cacheControl != "" {
		ensureContentPropsExist()
		result.contentHeaders.cacheControl = &w.cacheControl
	}
	if w.contentDisposition != "" {
		ensureContentPropsExist()
		result.contentHeaders.contentDisposition = &w.contentDisposition
	}
	if w.contentEncoding != "" {
		ensureContentPropsExist()
		result.contentHeaders.contentEncoding = &w.contentEncoding
	}
	if w.contentLanguage != "" {
		ensureContentPropsExist()
		result.contentHeaders.contentLanguage = &w.contentLanguage
	}
	if w.contentMD5 != nil {
		ensureContentPropsExist()
		result.contentHeaders.contentMD5 = w.contentMD5
	}
	if w.contentType != "" {
		ensureContentPropsExist()
		result.contentHeaders.contentType = &w.contentType
	}

	// other properties
	if w.nameValueMetadata != nil {
		result.nameValueMetadata = w.nameValueMetadata
	}
	if w.lastWriteTime != (time.Time{}) {
		result.lastWriteTime = &w.lastWriteTime
	}
	if w.creationTime != (time.Time{}) {
		result.creationTime = &w.creationTime
	}
	if w.smbAttributes != "" {
		result.smbAttributes = &w.smbAttributes
	}
	if w.smbPermissionsSddl != "" {
		result.smbPermissionsSddl = &w.smbPermissionsSddl
	}

	return result
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
// but should be present on it afte) the transfer
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
	m := map[string]string{"hdi_isfolder": "true"} // special flag that says this file is a stub
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
//   remove this type and its usages, and the expectedFailureProvider interface
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
