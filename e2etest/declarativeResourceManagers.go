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
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	"net/url"
	"os"
)

func assertNoStripTopDir(stripTopDir bool) {
	if stripTopDir {
		panic("support for stripTopDir is not yet implemented here") // when implemented, resourceManagers should return /* in the right part of the string
	}
}

// TODO: any better names for this?
// a source or destination
type resourceManager interface {

	// setup creates and initializes a test resource appropriate for the given test files
	setup(a asserter, fs testFiles, isSource bool)

	// cleanup gets rid of everything that setup created
	// (Takes no param, because the resourceManager is expected to track its own state. E.g. "what did I make")
	cleanup(a asserter)

	// gets the azCopy command line param that represents the resource.  withSas is ignored when not applicable
	getParam(stripTopDir bool, withSas bool) string

	// isContainerLike returns true if the resource is a top-level cloud-based resource (e.g. a container, a File Share, etc)
	isContainerLike() bool
}

///////////////

type resourceLocal struct {
	dirPath string
}

func (r *resourceLocal) setup(a asserter, fs testFiles, isSource bool) {
	r.dirPath = TestResourceFactory{}.CreateLocalDirectory(a)

	size, err := fs.defaultSizeBytes()
	a.AssertNoErr("get size", err)

	scenarioHelper{}.generateLocalFilesFromList(a, r.dirPath, fs.allNames(isSource), size)
}

func (r *resourceLocal) cleanup(_ asserter) {
	if r.dirPath != "" {
		_ = os.RemoveAll(r.dirPath)
	}
}

func (r *resourceLocal) getParam(stripTopDir bool, _ bool) string {
	assertNoStripTopDir(stripTopDir)
	return r.dirPath
}

func (r *resourceLocal) isContainerLike() bool {
	return false
}

///////

type resourceBlobContainer struct {
	accountType  AccountType
	containerURL *azblob.ContainerURL
	rawSasURL    *url.URL
}

func (r *resourceBlobContainer) setup(a asserter, fs testFiles, isSource bool) {
	cu, _, rawSasURL := TestResourceFactory{}.CreateNewContainer(a, r.accountType)
	r.containerURL = &cu
	r.rawSasURL = &rawSasURL

	size, err := fs.defaultSizeBytes()
	a.AssertNoErr("get size", err)

	scenarioHelper{}.generateBlobsFromList(a, *r.containerURL, fs.allNames(isSource), size)
}

func (r *resourceBlobContainer) cleanup(a asserter) {
	if r.containerURL != nil {
		deleteContainer(a, *r.containerURL)
	}
}

func (r *resourceBlobContainer) getParam(stripTopDir bool, useSas bool) string {
	assertNoStripTopDir(stripTopDir)
	if useSas {
		return r.rawSasURL.String()
	} else {
		return r.containerURL.String()
	}
}

func (r *resourceBlobContainer) isContainerLike() bool {
	return true
}

/////

type resourceAzureFileShare struct {
	accountType AccountType
	shareURL    *azfile.ShareURL
	rawSasURL   *url.URL
}

func (r *resourceAzureFileShare) setup(a asserter, fs testFiles, isSource bool) {
	su, _, rawSasURL := TestResourceFactory{}.CreateNewFileShare(a, EAccountType.Standard())
	r.shareURL = &su
	r.rawSasURL = &rawSasURL

	size, err := fs.defaultSizeBytes()
	a.AssertNoErr("get size", err)

	scenarioHelper{}.generateAzureFilesFromList(a, *r.shareURL, fs.allNames(isSource), size)
}

func (r *resourceAzureFileShare) cleanup(a asserter) {
	if r.shareURL != nil {
		deleteShare(a, *r.shareURL)
	}
}

func (r *resourceAzureFileShare) getParam(stripTopDir bool, useSas bool) string {
	assertNoStripTopDir(stripTopDir)
	if !useSas {
		panic("SAS is currently the only supported auth type for Azure Files")
	}
	return r.rawSasURL.String()
}

func (r *resourceAzureFileShare) isContainerLike() bool {
	return true
}

////

type resourceDummy struct{}

func (r *resourceDummy) setup(a asserter, fs testFiles, isSource bool) {

}

func (r *resourceDummy) cleanup(_ asserter) {
}

func (r *resourceDummy) getParam(stripTopDir bool, _ bool) string {
	assertNoStripTopDir(stripTopDir)
	return "foobar"
}

func (r *resourceDummy) isContainerLike() bool {
	return false
}
