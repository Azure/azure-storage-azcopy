package cmd

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
	"io/ioutil"
	"strings"
)

// create a test file
func generateFile(fileName string, fileSize int) ([]byte, error) {
	// generate random data
	_, bigBuff := getRandomDataAndReader(fileSize)

	// write to file and return the data
	err := ioutil.WriteFile(fileName, bigBuff, 0666)
	return bigBuff, err
}

// create the necessary blobs with and without virtual directories
func generateCommonScenarioForDownloadSync(c *chk.C) (containerName string, containerUrl azblob.ContainerURL, blobList []string) {
	bsu := getBSU()
	containerUrl, containerName = createNewContainer(c, bsu)

	blobList = make([]string, 30)
	for i := 0; i < 10; i++ {
		_, blobName1 := createNewBlockBlob(c, containerUrl, "top")
		_, blobName2 := createNewBlockBlob(c, containerUrl, "sub1/")
		_, blobName3 := createNewBlockBlob(c, containerUrl, "sub2/")

		blobList[3*i] = blobName1
		blobList[3*i+1] = blobName2
		blobList[3*i+2] = blobName3
	}

	return
}

// Golang does not have sets, so we have to use a map to fulfill the same functionality
func convertListToMap(list []string) map[string]int {
	lookupMap := make(map[string]int)
	for _, entryName := range list {
		lookupMap[entryName] = 0
	}

	return lookupMap
}

func (s *cmdIntegrationSuite) TestSyncDownloadWithEmptyDestination(c *chk.C) {
	// set up the container with numerous blobs
	containerName, containerURL, blobList := generateCommonScenarioForDownloadSync(c)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination with an empty folder
	dstDirName, err := ioutil.TempDir("", "AzCopySyncDownload")
	c.Assert(err, chk.IsNil)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	credential, err := getGenericCredential("")
	c.Assert(err, chk.IsNil)
	containerUrlWithSAS := getContainerURLWithSAS(c, *credential, containerName)
	rawContainerURLWithSAS := containerUrlWithSAS.URL()
	raw := rawSyncCmdArgs{
		src:          rawContainerURLWithSAS.String(),
		dst:          dstDirName,
		recursive:    true,
		logVerbosity: "WARNING",
		output:       "text",
		force:        false,
	}

	// the simulated user input should parse properly
	cooked, err := raw.cook()
	c.Assert(err, chk.IsNil)

	// the enumeration ends when process() returns
	err = cooked.process()
	c.Assert(err, chk.IsNil)

	// validate that the right number of transfers were scheduled
	c.Assert(len(mockedRPC.transfers), chk.Equals, 30)

	// validate that the right transfers were sent
	lookupMap := convertListToMap(blobList)
	for _, transfer := range mockedRPC.transfers {
		localRelativeFilePath := strings.Replace(transfer.Destination, dstDirName+common.AZCOPY_PATH_SEPARATOR_STRING, "", 1)

		// look up the source blob, make sure it matches
		_, blobExist := lookupMap[localRelativeFilePath]
		c.Assert(blobExist, chk.Equals, true)
	}
}
