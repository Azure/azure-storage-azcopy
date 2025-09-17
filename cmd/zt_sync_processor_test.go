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
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"

	"github.com/Azure/azure-storage-azcopy/v10/common"
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
		s: &azcopy.CookedSyncOptions{
			Destination:       newLocalRes(dstDirName),
			DeleteDestination: common.EDeleteDestination.True(),
		},
	}

	// set up local deleter
	deleter := azcopy.newSyncLocalDeleteProcessor(cca, common.EFolderPropertiesOption.NoFolders())

	// validate that the file still exists
	_, err := os.Stat(filepath.Join(dstDirName, dstFileName))
	a.Nil(err)

	// exercise the deleter
	err = deleter.removeImmediately(traverser.StoredObject{RelativePath: dstFileName})
	a.Nil(err)

	// validate that the file no longer exists
	_, err = os.Stat(filepath.Join(dstDirName, dstFileName))
	a.NotNil(err)
}

func TestBlobDeleter(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	blobName := "extraBlob.pdf"

	// set up the blob to delete
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	scenarioHelper{}.generateBlobsFromList(a, cc, []string{blobName}, blockBlobDefaultData)

	// validate that the blob exists
	bc := cc.NewBlobClient(blobName)
	_, err := bc.GetProperties(context.Background(), nil)
	a.Nil(err)

	// construct the cooked input to simulate user input
	rawContainerURL := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	cca := &cookedSyncCmdArgs{
		s: &azcopy.CookedSyncOptions{
			Destination:       newRemoteRes(rawContainerURL.String()),
			DeleteDestination: common.EDeleteDestination.True(),
			FromTo:            common.EFromTo.LocalBlob(),
		},
	}
	sc := common.NewServiceClient(bsc, nil, nil)

	// set up the blob deleter
	deleter, err := azcopy.newSyncDeleteProcessor(cca, common.EFolderPropertiesOption.NoFolders(), sc)
	a.Nil(err)

	// exercise the deleter
	err = deleter.removeImmediately(traverser.StoredObject{RelativePath: blobName})
	a.Nil(err)

	// validate that the blob was deleted
	_, err = bc.GetProperties(context.Background(), nil)
	a.NotNil(err)
}

func TestFileDeleter(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	fileName := "extraFile.pdf"

	// set up the file to delete
	shareClient, shareName := createNewShare(a, fsc)
	defer deleteShare(a, shareClient)
	scenarioHelper{}.generateShareFilesFromList(a, shareClient, fsc, []string{fileName})

	// validate that the file exists
	fileClient := shareClient.NewRootDirectoryClient().NewFileClient(fileName)
	_, err := fileClient.GetProperties(context.Background(), nil)
	a.Nil(err)

	// construct the cooked input to simulate user input
	rawShareSAS := scenarioHelper{}.getRawShareURLWithSAS(a, shareName)
	cca := &cookedSyncCmdArgs{
		s: &azcopy.CookedSyncOptions{
			Destination:       newRemoteRes(rawShareSAS.String()),
			DeleteDestination: common.EDeleteDestination.True(),
			FromTo:            common.EFromTo.FileFile(),
		},
	}

	sc := common.NewServiceClient(nil, fsc, nil)
	// set up the file deleter
	deleter, err := azcopy.newSyncDeleteProcessor(cca, common.EFolderPropertiesOption.NoFolders(), sc)
	a.Nil(err)

	// exercise the deleter
	err = deleter.removeImmediately(traverser.StoredObject{RelativePath: fileName})
	a.Nil(err)

	// validate that the file was deleted
	_, err = fileClient.GetProperties(context.Background(), nil)
	a.NotNil(err)
}
