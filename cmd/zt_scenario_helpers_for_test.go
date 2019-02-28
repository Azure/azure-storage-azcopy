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
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const defaultFileSize = 1024

type scenarioHelper struct{}

var specialNames = []string{
	"打麻将.txt",
	"wow such space so much space",
	"saywut.pdf?yo=bla&WUWUWU=foo&sig=yyy",
	"coração",
	"আপনার নাম কি",
	"%4509%4254$85140&",
	"Donaudampfschifffahrtselektrizitätenhauptbetriebswerkbauunterbeamtengesellschaft",
	"お名前は何ですか",
	"Adın ne",
	"як вас звати",
}

func (scenarioHelper) generateLocalDirectory(c *chk.C) (dstDirName string) {
	dstDirName, err := ioutil.TempDir("", "AzCopySyncDownload")
	c.Assert(err, chk.IsNil)
	return
}

// create a test file
func (scenarioHelper) generateFile(filePath string, fileSize int) ([]byte, error) {
	// generate random data
	_, bigBuff := getRandomDataAndReader(fileSize)

	// create all parent directories
	err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		return nil, err
	}

	// write to file and return the data
	err = ioutil.WriteFile(filePath, bigBuff, 0666)
	return bigBuff, err
}

func (s scenarioHelper) generateRandomLocalFiles(c *chk.C, dirPath string, prefix string) (fileList []string) {
	fileList = make([]string, 50)
	for i := 0; i < 10; i++ {
		batch := []string{
			generateName(prefix + "top"),
			generateName(prefix + "sub1/"),
			generateName(prefix + "sub2/"),
			generateName(prefix + "sub1/sub3/sub5/"),
			generateName(prefix + specialNames[i]),
		}

		for j, name := range batch {
			fileList[5*i+j] = name
			_, err := s.generateFile(filepath.Join(dirPath, name), defaultFileSize)
			c.Assert(err, chk.IsNil)
		}
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1500)
	return
}

func (s scenarioHelper) generateFilesFromList(c *chk.C, dirPath string, fileList []string) {
	for _, fileName := range fileList {
		_, err := s.generateFile(filepath.Join(dirPath, fileName), defaultFileSize)
		c.Assert(err, chk.IsNil)
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1500)
}

// make 50 blobs with random names
// 10 of them at the top level
// 10 of them in sub dir "sub1"
// 10 of them in sub dir "sub2"
// 10 of them in deeper sub dir "sub1/sub3/sub5"
// 10 of them with special characters
func (scenarioHelper) generateCommonRemoteScenario(c *chk.C, containerURL azblob.ContainerURL, prefix string) (blobList []string) {
	blobList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, blobName1 := createNewBlockBlob(c, containerURL, prefix+"top")
		_, blobName2 := createNewBlockBlob(c, containerURL, prefix+"sub1/")
		_, blobName3 := createNewBlockBlob(c, containerURL, prefix+"sub2/")
		_, blobName4 := createNewBlockBlob(c, containerURL, prefix+"sub1/sub3/sub5/")
		_, blobName5 := createNewBlockBlob(c, containerURL, prefix+specialNames[i])

		blobList[5*i] = blobName1
		blobList[5*i+1] = blobName2
		blobList[5*i+2] = blobName3
		blobList[5*i+3] = blobName4
		blobList[5*i+4] = blobName5
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1500)
	return
}

// create the demanded blobs
func (scenarioHelper) generateBlobs(c *chk.C, containerURL azblob.ContainerURL, blobList []string) {
	for _, blobName := range blobList {
		blob := containerURL.NewBlockBlobURL(blobName)
		cResp, err := blob.Upload(ctx, strings.NewReader(blockBlobDefaultData), azblob.BlobHTTPHeaders{},
			nil, azblob.BlobAccessConditions{})
		c.Assert(err, chk.IsNil)
		c.Assert(cResp.StatusCode(), chk.Equals, 201)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1500)
}

// Golang does not have sets, so we have to use a map to fulfill the same functionality
func (scenarioHelper) convertListToMap(list []string) map[string]int {
	lookupMap := make(map[string]int)
	for _, entryName := range list {
		lookupMap[entryName] = 0
	}

	return lookupMap
}

func (scenarioHelper) getRawContainerURLWithSAS(c *chk.C, containerName string) url.URL {
	credential, err := getGenericCredential("")
	c.Assert(err, chk.IsNil)
	containerURLWithSAS := getContainerURLWithSAS(c, *credential, containerName)
	return containerURLWithSAS.URL()
}

func (scenarioHelper) getRawBlobURLWithSAS(c *chk.C, containerName string, blobName string) url.URL {
	credential, err := getGenericCredential("")
	c.Assert(err, chk.IsNil)
	containerURLWithSAS := getContainerURLWithSAS(c, *credential, containerName)
	blobURLWithSAS := containerURLWithSAS.NewBlockBlobURL(blobName)
	return blobURLWithSAS.URL()
}

func (scenarioHelper) blobExists(blobURL azblob.BlobURL) bool {
	_, err := blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{})
	if err == nil {
		return true
	}
	return false
}

func runSyncAndVerify(c *chk.C, raw rawSyncCmdArgs, verifier func(err error)) {
	// the simulated user input should parse properly
	cooked, err := raw.cook()
	c.Assert(err, chk.IsNil)

	// the enumeration ends when process() returns
	err = cooked.process()

	// the err is passed to verified, which knows whether it is expected or not
	verifier(err)
}

func validateUploadTransfersAreScheduled(c *chk.C, srcDirName string, dstDirName string, expectedTransfers []string, mockedRPC interceptor) {
	validateTransfersAreScheduled(c, srcDirName, false, dstDirName, true, expectedTransfers, mockedRPC)
}

func validateDownloadTransfersAreScheduled(c *chk.C, srcDirName string, dstDirName string, expectedTransfers []string, mockedRPC interceptor) {
	validateTransfersAreScheduled(c, srcDirName, true, dstDirName, false, expectedTransfers, mockedRPC)
}

func validateTransfersAreScheduled(c *chk.C, srcDirName string, isSrcEncoded bool, dstDirName string, isDstEncoded bool, expectedTransfers []string, mockedRPC interceptor) {
	// validate that the right number of transfers were scheduled
	c.Assert(len(mockedRPC.transfers), chk.Equals, len(expectedTransfers))

	// validate that the right transfers were sent
	lookupMap := scenarioHelper{}.convertListToMap(expectedTransfers)
	for _, transfer := range mockedRPC.transfers {
		srcRelativeFilePath := strings.Replace(transfer.Source, srcDirName+common.AZCOPY_PATH_SEPARATOR_STRING, "", 1)
		dstRelativeFilePath := strings.Replace(transfer.Destination, dstDirName+common.AZCOPY_PATH_SEPARATOR_STRING, "", 1)

		if isSrcEncoded {
			srcRelativeFilePath, _ = url.PathUnescape(srcRelativeFilePath)
		}

		if isDstEncoded {
			dstRelativeFilePath, _ = url.PathUnescape(dstRelativeFilePath)
		}

		// the relative paths should be equal
		c.Assert(srcRelativeFilePath, chk.Equals, dstRelativeFilePath)

		// look up the source from the expected transfers, make sure it exists
		_, srcExist := lookupMap[dstRelativeFilePath]
		c.Assert(srcExist, chk.Equals, true)

		// look up the destination from the expected transfers, make sure it exists
		_, dstExist := lookupMap[dstRelativeFilePath]
		c.Assert(dstExist, chk.Equals, true)
	}
}

func getDefaultRawInput(src, dst string) rawSyncCmdArgs {
	deleteDestination := common.EDeleteDestination.True()

	return rawSyncCmdArgs{
		src:                 src,
		dst:                 dst,
		recursive:           true,
		logVerbosity:        defaultLogVerbosityForSync,
		deleteDestination:   deleteDestination.String(),
		md5ValidationOption: common.DefaultHashValidationOption.String(),
	}
}
