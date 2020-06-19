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
	"os"
)

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

///////

type resourceBlobContainer struct {
	accountType  AccountType
	containerURL *azblob.ContainerURL
}

func (r *resourceBlobContainer) setup(a asserter, fs testFiles, isSource bool) {
	cu, _, _ := TestResourceFactory{}.CreateNewContainer(a, r.accountType)
	r.containerURL = &cu

	size, err := fs.defaultSizeBytes()
	a.AssertNoErr("get size", err)

	scenarioHelper{}.generateBlobsFromList(a, *r.containerURL, fs.allNames(isSource), size)
}

func (r *resourceBlobContainer) cleanup(a asserter) {
	if r.containerURL != nil {
		deleteContainer(a, *r.containerURL)
	}
}

/////

type resourceAzureFiles struct {
	accountType AccountType
	shareURL    *azfile.ShareURL
}

func (r *resourceAzureFiles) setup(a asserter, fs testFiles, isSource bool) {
	su, _ := TestResourceFactory{}.CreateNewFileShare(a, EAccountType.Standard())
	r.shareURL = &su

	size, err := fs.defaultSizeBytes()
	a.AssertNoErr("get size", err)

	scenarioHelper{}.generateAzureFilesFromList(a, *r.shareURL, fs.allNames(isSource), size)
}

func (r *resourceAzureFiles) cleanup(a asserter) {
	if r.shareURL != nil {
		deleteShare(a, *r.shareURL)
	}
}

////

type resourceDummy struct{}

func (r *resourceDummy) setup(a asserter, fs testFiles, isSource bool) {

}

func (r *resourceDummy) cleanup(_ asserter) {
}
