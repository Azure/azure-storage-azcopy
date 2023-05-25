// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package cmd

import (
	"context"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

func TestLocalDeleter(t *testing.T) {
	a := assert.New(t)
	// set up the local file
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)
	dstFileName := "extraFile.txt"
	scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, []string{dstFileName})

	// construct the cooked input to simulate user input
	cca := &cookedSyncCmdArgs{
		destination:       newLocalRes(dstDirName),
		deleteDestination: common.EDeleteDestination.True(),
	}

	// set up local deleter
	deleter := newSyncLocalDeleteProcessor(cca, common.EFolderPropertiesOption.NoFolders())

	// validate that the file still exists
	_, err := os.Stat(filepath.Join(dstDirName, dstFileName))
	a.Nil(err)

	// exercise the deleter
	err = deleter.removeImmediately(StoredObject{relativePath: dstFileName})
	a.Nil(err)

	// validate that the file no longer exists
	_, err = os.Stat(filepath.Join(dstDirName, dstFileName))
	a.NotNil(err)
}

func TestBlobDeleter(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	blobName := "extraBlob.pdf"

	// set up the blob to delete
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	scenarioHelper{}.generateBlobsFromList(a, containerURL, []string{blobName}, blockBlobDefaultData)

	// validate that the blob exists
	blobURL := containerURL.NewBlobURL(blobName)
	_, err := blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	a.Nil(err)

	// construct the cooked input to simulate user input
	rawContainerURL := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	cca := &cookedSyncCmdArgs{
		destination:       newRemoteRes(rawContainerURL.String()),
		credentialInfo:    common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()},
		deleteDestination: common.EDeleteDestination.True(),
		fromTo:            common.EFromTo.LocalBlob(),
	}

	// set up the blob deleter
	deleter, err := newSyncDeleteProcessor(cca, common.EFolderPropertiesOption.NoFolders())
	a.Nil(err)

	// exercise the deleter
	err = deleter.removeImmediately(StoredObject{relativePath: blobName})
	a.Nil(err)

	// validate that the blob was deleted
	_, err = blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	a.NotNil(err)
}

func TestFileDeleter(t *testing.T) {
	a := assert.New(t)
	fsu := getFSU()
	fileName := "extraFile.pdf"

	// set up the file to delete
	shareURL, shareName := createNewAzureShare(a, fsu)
	defer deleteShare(a, shareURL)
	scenarioHelper{}.generateAzureFilesFromList(a, shareURL, []string{fileName})

	// validate that the file exists
	fileURL := shareURL.NewRootDirectoryURL().NewFileURL(fileName)
	_, err := fileURL.GetProperties(context.Background())
	a.Nil(err)

	// construct the cooked input to simulate user input
	rawShareSAS := scenarioHelper{}.getRawShareURLWithSAS(a, shareName)
	cca := &cookedSyncCmdArgs{
		destination:       newRemoteRes(rawShareSAS.String()),
		credentialInfo:    common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()},
		deleteDestination: common.EDeleteDestination.True(),
		fromTo:            common.EFromTo.FileFile(),
	}

	// set up the file deleter
	deleter, err := newSyncDeleteProcessor(cca, common.EFolderPropertiesOption.NoFolders())
	a.Nil(err)

	// exercise the deleter
	err = deleter.removeImmediately(StoredObject{relativePath: fileName})
	a.Nil(err)

	// validate that the file was deleted
	_, err = fileURL.GetProperties(context.Background())
	a.NotNil(err)
}