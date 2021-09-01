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
	"os"
	"path/filepath"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
)

type syncProcessorSuite struct{}

var _ = chk.Suite(&syncProcessorSuite{})

func (s *syncProcessorSuite) TestLocalDeleter(c *chk.C) {
	// set up the local file
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)
	dstFileName := "extraFile.txt"
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, []string{dstFileName})

	// construct the cooked input to simulate user input
	cca := &cookedSyncCmdArgs{
		destination:       newLocalRes(dstDirName),
		deleteDestination: common.EDeleteDestination.True(),
	}

	// set up local deleter
	deleter := newSyncLocalDeleteProcessor(cca)

	// validate that the file still exists
	_, err := os.Stat(filepath.Join(dstDirName, dstFileName))
	c.Assert(err, chk.IsNil)

	// exercise the deleter
	err = deleter.removeImmediately(StoredObject{relativePath: dstFileName})
	c.Assert(err, chk.IsNil)

	// validate that the file no longer exists
	_, err = os.Stat(filepath.Join(dstDirName, dstFileName))
	c.Assert(err, chk.NotNil)
}

func (s *syncProcessorSuite) TestBlobDeleter(c *chk.C) {
	bsu := getBSU()
	blobName := "extraBlob.pdf"

	// set up the blob to delete
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	scenarioHelper{}.generateBlobsFromList(c, containerURL, []string{blobName}, blockBlobDefaultData)

	// validate that the blob exists
	blobURL := containerURL.NewBlobURL(blobName)
	_, err := blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	c.Assert(err, chk.IsNil)

	// construct the cooked input to simulate user input
	rawContainerURL := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	cca := &cookedSyncCmdArgs{
		destination:       newRemoteRes(rawContainerURL.String()),
		credentialInfo:    common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()},
		deleteDestination: common.EDeleteDestination.True(),
		fromTo:            common.EFromTo.LocalBlob(),
	}

	// set up the blob deleter
	deleter, err := newSyncDeleteProcessor(cca)
	c.Assert(err, chk.IsNil)

	// exercise the deleter
	err = deleter.removeImmediately(StoredObject{relativePath: blobName})
	c.Assert(err, chk.IsNil)

	// validate that the blob was deleted
	_, err = blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	c.Assert(err, chk.NotNil)
}

func (s *syncProcessorSuite) TestFileDeleter(c *chk.C) {
	fsu := getFSU()
	fileName := "extraFile.pdf"

	// set up the file to delete
	shareURL, shareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, shareURL)
	scenarioHelper{}.generateAzureFilesFromList(c, shareURL, []string{fileName})

	// validate that the file exists
	fileURL := shareURL.NewRootDirectoryURL().NewFileURL(fileName)
	_, err := fileURL.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)

	// construct the cooked input to simulate user input
	rawShareSAS := scenarioHelper{}.getRawShareURLWithSAS(c, shareName)
	cca := &cookedSyncCmdArgs{
		destination:       newRemoteRes(rawShareSAS.String()),
		credentialInfo:    common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()},
		deleteDestination: common.EDeleteDestination.True(),
		fromTo:            common.EFromTo.FileFile(),
	}

	// set up the file deleter
	deleter, err := newSyncDeleteProcessor(cca)
	c.Assert(err, chk.IsNil)

	// exercise the deleter
	err = deleter.removeImmediately(StoredObject{relativePath: fileName})
	c.Assert(err, chk.IsNil)

	// validate that the file was deleted
	_, err = fileURL.GetProperties(context.Background())
	c.Assert(err, chk.NotNil)
}
