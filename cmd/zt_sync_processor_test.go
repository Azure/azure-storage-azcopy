package cmd

import (
	"context"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
	"os"
	"path/filepath"
)

type syncProcessorSuite struct{}

var _ = chk.Suite(&syncProcessorSuite{})

func (s *syncProcessorSuite) TestLocalDeleter(c *chk.C) {
	// set up the local file
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	dstFileName := "extraFile.txt"
	scenarioHelper{}.generateFilesFromList(c, dstDirName, []string{dstFileName})

	// construct the cooked input to simulate user input
	cca := &cookedSyncCmdArgs{
		destination: dstDirName,
		force:       true,
	}

	// set up local deleter
	deleter := newSyncLocalDeleteProcessor(cca)

	// validate that the file still exists
	_, err := os.Stat(filepath.Join(dstDirName, dstFileName))
	c.Assert(err, chk.IsNil)

	// exercise the deleter
	err = deleter.removeImmediately(storedObject{relativePath: dstFileName})
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
	scenarioHelper{}.generateBlobs(c, containerURL, []string{blobName})

	// validate that the blob exists
	blobURL := containerURL.NewBlobURL(blobName)
	_, err := blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{})
	c.Assert(err, chk.IsNil)

	// construct the cooked input to simulate user input
	rawContainerURL := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	parts := azblob.NewBlobURLParts(rawContainerURL)
	cca := &cookedSyncCmdArgs{
		destination:    containerURL.String(),
		destinationSAS: parts.SAS.Encode(),
		credentialInfo: common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()},
		force:          true,
	}

	// set up the blob deleter
	deleter, err := newSyncBlobDeleteProcessor(cca)
	c.Assert(err, chk.IsNil)

	// exercise the deleter
	err = deleter.removeImmediately(storedObject{relativePath: blobName})
	c.Assert(err, chk.IsNil)

	// validate that the blob was deleted
	_, err = blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{})
	c.Assert(err, chk.NotNil)
}
