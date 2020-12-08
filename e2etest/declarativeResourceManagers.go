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
	createLocation(a asserter, s *scenario)

	// creates the test files in the location. Implementers can assume that createLocation has been called first.
	// This method may be called multiple times, in which case it should overwrite any like-named files that are already there.
	// (e.g. when test need to create files with later modification dates, they will trigger a second call to this)
	createFiles(a asserter, fs testFiles, isSource bool)

	// Gets the names and properties of all files (and, if applicable, folders) that exist.
	// Used for verification
	getAllProperties(a asserter) map[string]*objectProperties

	// Download
	downloadContent(a asserter, resourceRelPath string) []byte

	// cleanup gets rid of everything that setup created
	// (Takes no param, because the resourceManager is expected to track its own state. E.g. "what did I make")
	cleanup(a asserter)

	// gets the azCopy command line param that represents the resource.  withSas is ignored when not applicable
	getParam(stripTopDir bool, withSas bool) string

	// isContainerLike returns true if the resource is a top-level cloud-based resource (e.g. a container, a File Share, etc)
	isContainerLike() bool

	// appendSourcePath appends a path to creates absolute path
	appendSourcePath(string, bool)

	// create a snapshot for the source, and use it for the job
	createSourceSnapshot(a asserter)
}

///////////////

type resourceLocal struct {
	dirPath string
}

func (r *resourceLocal) createLocation(a asserter, s *scenario) {
	r.dirPath = TestResourceFactory{}.CreateLocalDirectory(a)
	if s.GetModifiableParameters().relativeSourcePath != "" {
		r.appendSourcePath(s.GetModifiableParameters().relativeSourcePath, true)
	}
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

func (r *resourceLocal) appendSourcePath(filePath string, _ bool) {
	r.dirPath += "/" + filePath
}

func (r *resourceLocal) getAllProperties(a asserter) map[string]*objectProperties {
	return scenarioHelper{}.enumerateLocalProperties(a, r.dirPath)
}

func (r *resourceLocal) downloadContent(a asserter, resourceRelPath string) []byte {
	//return scenarioHelper{}.enumerateLocalProperties(a, r.dirPath)
	panic("Not Implemented")
}

func (r *resourceLocal) createSourceSnapshot(a asserter) {
	panic("Not Implemented")
}

///////

type resourceBlobContainer struct {
	accountType  AccountType
	containerURL *azblob.ContainerURL
	rawSasURL    *url.URL
}

func (r *resourceBlobContainer) createLocation(a asserter, s *scenario) {
	cu, _, rawSasURL := TestResourceFactory{}.CreateNewContainer(a, r.accountType)
	r.containerURL = &cu
	r.rawSasURL = &rawSasURL
	if s.GetModifiableParameters().relativeSourcePath != "" {
		r.appendSourcePath(s.GetModifiableParameters().relativeSourcePath, true)
	}
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

func (r *resourceBlobContainer) appendSourcePath(filePath string, useSas bool) {
	if useSas {
		r.rawSasURL.Path += "/" + filePath
	}
}

func (r *resourceBlobContainer) getAllProperties(a asserter) map[string]*objectProperties {
	return scenarioHelper{}.enumerateContainerBlobProperties(a, *r.containerURL)
}

func (r *resourceBlobContainer) downloadContent(a asserter, resourceRelPath string) []byte {
	return scenarioHelper{}.downloadBlobContent(a, *r.containerURL, resourceRelPath)
}

func (r *resourceBlobContainer) createSourceSnapshot(a asserter) {
	panic("Not Implemented")
}

/////

type resourceAzureFileShare struct {
	accountType AccountType
	shareURL    *azfile.ShareURL // // TODO: Either eliminate SDK URLs from ResourceManager or provide means to edit it (File SDK) for which pipeline is required
	rawSasURL   *url.URL
	snapshotID  string // optional, use a snapshot as the location instead
}

func (r *resourceAzureFileShare) createLocation(a asserter, s *scenario) {
	su, _, rawSasURL := TestResourceFactory{}.CreateNewFileShare(a, EAccountType.Standard())
	r.shareURL = &su
	r.rawSasURL = &rawSasURL
	if s.GetModifiableParameters().relativeSourcePath != "" {
		r.appendSourcePath(s.GetModifiableParameters().relativeSourcePath, true)
	}
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
	var param url.URL
	if useSas {
		param = *r.rawSasURL
	} else {
		param = r.shareURL.URL()
	}

	// append the snapshot ID if present
	if r.snapshotID != "" {
		parts := azfile.NewFileURLParts(param)
		parts.ShareSnapshot = r.snapshotID
		param = parts.URL()
	}

	return param.String()
}

func (r *resourceAzureFileShare) isContainerLike() bool {
	return true
}

func (r *resourceAzureFileShare) appendSourcePath(filePath string, useSas bool) {
	if useSas {
		r.rawSasURL.Path += "/" + filePath
	}
}

func (r *resourceAzureFileShare) getAllProperties(a asserter) map[string]*objectProperties {
	return scenarioHelper{}.enumerateShareFileProperties(a, *r.shareURL)
}

func (r *resourceAzureFileShare) downloadContent(a asserter, resourceRelPath string) []byte {
	return scenarioHelper{}.downloadFileContent(a, *r.shareURL, resourceRelPath)
}

func (r *resourceAzureFileShare) createSourceSnapshot(a asserter) {
	r.snapshotID = TestResourceFactory{}.CreateNewFileShareSnapshot(a, *r.shareURL)
}

////

type resourceDummy struct{}

func (r *resourceDummy) createLocation(a asserter, s *scenario) {

}

func (r *resourceDummy) createFiles(a asserter, fs testFiles, isSource bool) {

}

func (r *resourceDummy) cleanup(_ asserter) {
}

func (r *resourceDummy) getParam(stripTopDir bool, _ bool) string {
	assertNoStripTopDir(stripTopDir)
	return ""
}

func (r *resourceDummy) isContainerLike() bool {
	return false
}

func (r *resourceDummy) getAllProperties(a asserter) map[string]*objectProperties {
	panic("not impelmented")
}

func (r *resourceDummy) downloadContent(a asserter, _ string) []byte {
	return make([]byte, 0)
}

func (r *resourceDummy) appendSourcePath(_ string, _ bool) {
}

func (r *resourceDummy) createSourceSnapshot(a asserter) {}
