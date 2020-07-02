package azbfs_test

import (
	"bytes"
	"context"
	"time"
	"errors"
	"io"

	//"crypto/md5"
	//"fmt"
	//"io/ioutil"
	//"net/http"
	"net/url"
	//"strings"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	chk "gopkg.in/check.v1" // go get gopkg.in/check.v1
	"io/ioutil"
	"net/http"
)

type FileURLSuite struct{}

var _ = chk.Suite(&FileURLSuite{})

func delFile(c *chk.C, file azbfs.FileURL) {
	resp, err := file.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, 200)
}

func getRandomDataAndReader(n int) (*bytes.Reader, []byte) {
	data := make([]byte, n, n)
	for i := 0; i < n; i++ {
		data[i] = byte(i)
	}
	return bytes.NewReader(data), data
}

func (s *FileURLSuite) TestFileNewFileURLNegative(c *chk.C) {
	c.Assert(func() { azbfs.NewFileURL(url.URL{}, nil) }, chk.Panics, "p can't be nil")
}

func (s *FileURLSuite) TestFileCreateDelete(c *chk.C) {
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// Create and delete file in root directory.
	file, _ := getFileURLFromFileSystem(c, fsURL)

	cResp, err := file.Create(context.Background(), azbfs.BlobFSHTTPHeaders{})
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.Response().StatusCode, chk.Equals, http.StatusCreated)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	delResp, err := file.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(delResp.Response().StatusCode, chk.Equals, http.StatusOK)
	c.Assert(delResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(delResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(delResp.Date(), chk.Not(chk.Equals), "")

	dirURL, _ := createNewDirectoryFromFileSystem(c, fsURL)
	defer deleteDirectory(c, dirURL)

	// Create and delete file in named directory.
	file, _ = getFileURLFromDirectory(c, dirURL)

	cResp, err = file.Create(context.Background(), azbfs.BlobFSHTTPHeaders{})
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.Response().StatusCode, chk.Equals, http.StatusCreated)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	delResp, err = file.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(delResp.Response().StatusCode, chk.Equals, http.StatusOK)
	c.Assert(delResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(delResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(delResp.Date(), chk.Not(chk.Equals), "")
}

func (s *FileURLSuite) TestFileCreateDeleteNonExistingParent(c *chk.C) {
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// Create and delete file in directory that does not exist yet.
	dirNotExist, _ := getDirectoryURLFromFileSystem(c, fsURL)
	file, _ := getFileURLFromDirectory(c, dirNotExist)

	// Verify that the file was created even though its parent directory does not exist yet
	cResp, err := file.Create(context.Background(), azbfs.BlobFSHTTPHeaders{})
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.Response().StatusCode, chk.Equals, http.StatusCreated)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	// Verify that the parent directory was created successfully
	dirResp, err := dirNotExist.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(dirResp.StatusCode(), chk.Equals, http.StatusOK)
}

func (s *FileURLSuite) TestFileGetProperties(c *chk.C) {
	fsu := getBfsServiceURL()
	fileSystemURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fileSystemURL)

	fileURL, _ := createNewFileFromFileSystem(c, fileSystemURL)
	defer delFile(c, fileURL)

	getResp, err := fileURL.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(getResp.Response().StatusCode, chk.Equals, http.StatusOK)
	c.Assert(getResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(getResp.XMsResourceType(), chk.Equals, "file")
	c.Assert(getResp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(getResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(getResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(getResp.Date(), chk.Not(chk.Equals), "")
}

////TODO this is failing on the service side at the moment, the spec is not accurate
//func (s *FileURLSuite) TestCreateFileWithBody(c *chk.C) {
//	fsu := getBfsServiceURL()
//	fileSystemURL, _ := createNewFileSystem(c, fsu)
//	defer delFileSystem(c, fileSystemURL)
//
//	fileURL, _ := createNewFileFromFileSystem(c, fileSystemURL, 2048)
//	defer delFile(c, fileURL)
//
//	contentR, contentD := getRandomDataAndReader(2048)
//
//	pResp, err := fileURL.Create(context.Background(), contentR)
//	c.Assert(err, chk.IsNil)
//	c.Assert(pResp.StatusCode(), chk.Equals, http.StatusCreated)
//	c.Assert(pResp.ETag(), chk.Not(chk.Equals), "")
//	c.Assert(pResp.LastModified(), chk.Not(chk.Equals), "")
//	c.Assert(pResp.XMsRequestID(), chk.Not(chk.Equals), "")
//	c.Assert(pResp.XMsVersion(), chk.Not(chk.Equals), "")
//	c.Assert(pResp.Date(), chk.Not(chk.Equals), "")
//
//	// Get with rangeGetContentMD5 enabled.
//	// Partial data, check status code 206.
//	resp, err := fileURL.Download(context.Background(), 0, 1024)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.StatusCode(), chk.Equals, http.StatusPartialContent)
//	c.Assert(resp.ContentLength(), chk.Equals, "1024")
//	c.Assert(resp.ContentType(), chk.Equals, "application/octet-stream")
//	c.Assert(resp.Status(), chk.Not(chk.Equals), "")
//
//	download, err := ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(download, chk.DeepEquals, contentD[:1024])
//}

func (s *FileURLSuite) TestUnexpectedEOFRecovery(c *chk.C) {
	fsu := getBfsServiceURL()
	fileSystemURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fileSystemURL)

	fileURL, _ := createNewFileFromFileSystem(c, fileSystemURL)
	defer delFile(c, fileURL)

	contentR, contentD := getRandomDataAndReader(2048)

	resp, err := fileURL.AppendData(context.Background(), 0, contentR)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusAccepted)
	c.Assert(resp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(resp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(resp.Date(), chk.Not(chk.Equals), "")

	resp, err = fileURL.FlushData(context.Background(), 2048, nil, azbfs.BlobFSHTTPHeaders{}, false, true)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusOK)
	c.Assert(resp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(resp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(resp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(resp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(resp.Date(), chk.Not(chk.Equals), "")

	dResp, err := fileURL.Download(context.Background(), 0, 2048)
	c.Assert(err, chk.IsNil)

	// Verify that we can inject errors first.
	reader := dResp.Body(azbfs.InjectErrorInRetryReaderOptions(errors.New("unrecoverable error")))

	_, err = ioutil.ReadAll(reader)
	c.Assert(err, chk.NotNil)
	c.Assert(err.Error(), chk.Equals, "unrecoverable error")

	// Then inject the retryable error.
	reader = dResp.Body(azbfs.InjectErrorInRetryReaderOptions(io.ErrUnexpectedEOF))

	buf, err := ioutil.ReadAll(reader)
	c.Assert(err, chk.IsNil)
	c.Assert(buf, chk.DeepEquals, contentD)
}

func (s *FileURLSuite) TestUploadDownloadRoundTrip(c *chk.C) {
	fsu := getBfsServiceURL()
	fileSystemURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fileSystemURL)

	fileURL, _ := createNewFileFromFileSystem(c, fileSystemURL)
	defer delFile(c, fileURL)

	// The file content will be made up of two parts
	contentR1, contentD1 := getRandomDataAndReader(2048)
	contentR2, contentD2 := getRandomDataAndReader(2048)

	// Append first part
	pResp, err := fileURL.AppendData(context.Background(), 0, contentR1)
	c.Assert(err, chk.IsNil)
	c.Assert(pResp.StatusCode(), chk.Equals, http.StatusAccepted)
	c.Assert(pResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(pResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(pResp.Date(), chk.Not(chk.Equals), "")

	// Append second part
	pResp, err = fileURL.AppendData(context.Background(), 2048, contentR2)
	c.Assert(err, chk.IsNil)
	c.Assert(pResp.StatusCode(), chk.Equals, http.StatusAccepted)
	c.Assert(pResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(pResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(pResp.Date(), chk.Not(chk.Equals), "")

	// Flush data
	fResp, err := fileURL.FlushData(context.Background(), 4096, make([]byte, 0), azbfs.BlobFSHTTPHeaders{}, false, true)
	c.Assert(err, chk.IsNil)
	c.Assert(fResp.StatusCode(), chk.Equals, http.StatusOK)
	c.Assert(fResp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(fResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(fResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(fResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(fResp.Date(), chk.Not(chk.Equals), "")

	// Get Partial data, check status code 206.
	resp, err := fileURL.Download(context.Background(), 0, 1024)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusPartialContent)
	c.Assert(resp.ContentLength(), chk.Equals, int64(1024))
	c.Assert(resp.ContentType(), chk.Equals, "application/octet-stream")
	c.Assert(resp.Status(), chk.Not(chk.Equals), "")

	// Verify the partial data
	download, err := ioutil.ReadAll(resp.Response().Body)
	c.Assert(err, chk.IsNil)
	c.Assert(download, chk.DeepEquals, contentD1[:1024])

	// Get entire fileURL, check status code 200.
	resp, err = fileURL.Download(context.Background(), 0, 0)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusOK)
	c.Assert(resp.ContentLength(), chk.Equals, int64(4096))
	c.Assert(resp.Date(), chk.Not(chk.Equals), "")
	c.Assert(resp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(resp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(resp.Version(), chk.Not(chk.Equals), "")

	// Verify the entire content
	download, err = ioutil.ReadAll(resp.Response().Body)
	c.Assert(err, chk.IsNil)
	c.Assert(download[:2048], chk.DeepEquals, contentD1[:])
	c.Assert(download[2048:], chk.DeepEquals, contentD2[:])
}

func (s *FileURLSuite) TestBlobURLPartsSASQueryTimes(c *chk.C) {
	StartTimesInputs := []string{
		"2020-04-20",
		"2020-04-20T07:00Z",
		"2020-04-20T07:15:00Z",
		"2020-04-20T07:30:00.1234567Z",
	}
	StartTimesExpected := []time.Time{
		time.Date(2020, time.April, 20, 0, 0, 0, 0, time.UTC),
		time.Date(2020, time.April, 20, 7, 0, 0, 0, time.UTC),
		time.Date(2020, time.April, 20, 7, 15, 0, 0, time.UTC),
		time.Date(2020, time.April, 20, 7, 30, 0, 123456700, time.UTC),
	}
	ExpiryTimesInputs := []string{
		"2020-04-21",
		"2020-04-20T08:00Z",
		"2020-04-20T08:15:00Z",
		"2020-04-20T08:30:00.2345678Z",
	}
	ExpiryTimesExpected := []time.Time{
		time.Date(2020, time.April, 21, 0, 0, 0, 0, time.UTC),
		time.Date(2020, time.April, 20, 8, 0, 0, 0, time.UTC),
		time.Date(2020, time.April, 20, 8, 15, 0, 0, time.UTC),
		time.Date(2020, time.April, 20, 8, 30, 0, 234567800, time.UTC),
	}

	for i := 0; i < len(StartTimesInputs); i++ {
		urlString :=
			"https://myaccount.dfs.core.windows.net/myfilesystem/mydirectory/myfile.txt?" +
				"se=" + url.QueryEscape(ExpiryTimesInputs[i]) + "&" +
				"sig=NotASignature&" +
				"sp=r&" +
				"spr=https&" +
				"sr=b&" +
				"st=" + url.QueryEscape(StartTimesInputs[i]) + "&" +
				"sv=2019-10-10"
		url, _ := url.Parse(urlString)

		parts := azbfs.NewBfsURLParts(*url)
		c.Assert(parts.Scheme, chk.Equals, "https")
		c.Assert(parts.Host, chk.Equals, "myaccount.dfs.core.windows.net")
		c.Assert(parts.FileSystemName, chk.Equals, "myfilesystem")
		c.Assert(parts.DirectoryOrFilePath, chk.Equals, "mydirectory/myfile.txt")

		sas := parts.SAS
		c.Assert(sas.StartTime(), chk.Equals, StartTimesExpected[i])
		c.Assert(sas.ExpiryTime(), chk.Equals, ExpiryTimesExpected[i])

		uResult := parts.URL()
		c.Log(uResult.String())
		c.Log(urlString)
		c.Assert(uResult.String(), chk.Equals, urlString)
	}
}