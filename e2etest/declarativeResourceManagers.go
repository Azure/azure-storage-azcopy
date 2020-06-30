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
// a source or destination. We need one of these for each of Blob, Azure Files, BlobFS, S3, Local disk etc.
type resourceManager interface {

	// creates an empty container/share/directory etc
	createLocation(a asserter)

	// creates the test files in the location. Implementers can assume that createLocation has been called first.
	// This method may be called multiple times, in which case it should overwrite any like-named files that are already there.
	// (e.g. when test need to create files with later modification dates, they will trigger a second call to this)
	createFiles(a asserter, fs testFiles, isSource bool)

	// Gets the names and properties of all files (and, if applicable, folders) that exist.
	// Used for verification
	getAllProperties(a asserter) map[string]*objectProperties

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

func (r *resourceLocal) createLocation(a asserter) {
	r.dirPath = TestResourceFactory{}.CreateLocalDirectory(a)
}

func (r *resourceLocal) createFiles(a asserter, fs testFiles, isSource bool) {
	scenarioHelper{}.generateLocalFilesFromList(a, r.dirPath, fs.allObjects(isSource), fs.defaultSize)
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

func (r *resourceLocal) getAllProperties(a asserter) map[string]*objectProperties {
	return scenarioHelper{}.enumerateLocalProperties(a, r.dirPath)
}

///////

type resourceBlobContainer struct {
	accountType  AccountType
	containerURL *azblob.ContainerURL
	rawSasURL    *url.URL
}

func (r *resourceBlobContainer) createLocation(a asserter) {
	cu, _, rawSasURL := TestResourceFactory{}.CreateNewContainer(a, r.accountType)
	r.containerURL = &cu
	r.rawSasURL = &rawSasURL
}

func (r *resourceBlobContainer) createFiles(a asserter, fs testFiles, isSource bool) {
	scenarioHelper{}.generateBlobsFromList(a, *r.containerURL, fs.allObjects(isSource), fs.defaultSize)
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

func (r *resourceBlobContainer) getAllProperties(a asserter) map[string]*objectProperties {
	return scenarioHelper{}.enumerateContainerBlobProperties(a, *r.containerURL)
}

/////

type resourceAzureFileShare struct {
	accountType AccountType
	shareURL    *azfile.ShareURL
	rawSasURL   *url.URL
}

func (r *resourceAzureFileShare) createLocation(a asserter) {
	su, _, rawSasURL := TestResourceFactory{}.CreateNewFileShare(a, EAccountType.Standard())
	r.shareURL = &su
	r.rawSasURL = &rawSasURL
}

func (r *resourceAzureFileShare) createFiles(a asserter, fs testFiles, isSource bool) {
	scenarioHelper{}.generateAzureFilesFromList(a, *r.shareURL, fs.allObjects(isSource), fs.defaultSize)
}

func (r *resourceAzureFileShare) cleanup(a asserter) {
	if r.shareURL != nil {
		deleteShare(a, *r.shareURL)
	}
}

func (r *resourceAzureFileShare) getParam(stripTopDir bool, useSas bool) string {
	assertNoStripTopDir(stripTopDir)
	if useSas {
		return r.rawSasURL.String()
	} else {
		return r.shareURL.String()
	}
}

func (r *resourceAzureFileShare) isContainerLike() bool {
	return true
}

func (r *resourceAzureFileShare) getAllProperties(a asserter) map[string]*objectProperties {
	return scenarioHelper{}.enumerateShareFileProperties(a, *r.shareURL)
}

////

type resourceDummy struct{}

func (r *resourceDummy) createLocation(a asserter) {

}

func (r *resourceDummy) createFiles(a asserter, fs testFiles, isSource bool) {

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

func (r *resourceDummy) getAllProperties(a asserter) map[string]*objectProperties {
	panic("not impelmented")
}
