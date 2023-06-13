package azbfs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"strings"
	"testing"
	"time"

	//"crypto/md5"
	//"fmt"
	//"io/ioutil"
	//"net/http"
	"net/url"
	//"strings"

	"net/http"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
)

func getRandomDataAndReader(n int) (*bytes.Reader, []byte) {
	data := make([]byte, n, n)
	for i := 0; i < n; i++ {
		data[i] = byte(i)
	}
	return bytes.NewReader(data), data
}

func TestFileNewFileURLNegative(t *testing.T) {
	a := assert.New(t)
	a.Panics(func() { azbfs.NewFileURL(url.URL{}, nil) }, "p can't be nil")
}

func TestFileCreateDelete(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fsURL)

	// Create and delete file in root directory.
	file, _ := getFileURLFromFileSystem(a, fsURL)

	cResp, err := file.Create(context.Background(), azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.Response().StatusCode)
	a.NotEqual("", cResp.ETag())
	a.NotEqual("", cResp.LastModified())
	a.NotEqual("", cResp.XMsRequestID())
	a.NotEqual("", cResp.XMsVersion())
	a.NotEqual("", cResp.Date())

	delResp, err := file.Delete(context.Background())
	a.Nil(err)
	a.Equal(http.StatusOK, delResp.Response().StatusCode)
	a.NotEqual("", delResp.XMsRequestID())
	a.NotEqual("", delResp.XMsVersion())
	a.NotEqual("", delResp.Date())

	dirURL, _ := createNewDirectoryFromFileSystem(a, fsURL)
	defer deleteDirectory(a, dirURL)

	// Create and delete file in named directory.
	file, _ = getFileURLFromDirectory(a, dirURL)

	cResp, err = file.Create(context.Background(), azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.Response().StatusCode)
	a.NotEqual("", cResp.ETag())
	a.NotEqual("", cResp.LastModified())
	a.NotEqual("", cResp.XMsRequestID())
	a.NotEqual("", cResp.XMsVersion())
	a.NotEqual("", cResp.Date())

	delResp, err = file.Delete(context.Background())
	a.Nil(err)
	a.Equal(http.StatusOK, delResp.Response().StatusCode)
	a.NotEqual("", delResp.XMsRequestID())
	a.NotEqual("", delResp.XMsVersion())
	a.NotEqual("", delResp.Date())
}

func TestFileCreateWithPermissions(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fsURL)

	// Create and delete file in root directory.
	file, _ := getFileURLFromFileSystem(a, fsURL)

	_, err := file.Create(context.Background(), azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{Permissions: "0444"})
	defer deleteFile(a, file)

	getResp, err := file.GetAccessControl(context.Background())
	a.Nil(err)
	a.Equal("r--r-----", getResp.Permissions)
}

func TestFileCreateDeleteNonExistingParent(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fsURL)

	// Create and delete file in directory that does not exist yet.
	dirNotExist, _ := getDirectoryURLFromFileSystem(a, fsURL)
	file, _ := getFileURLFromDirectory(a, dirNotExist)

	// Verify that the file was created even though its parent directory does not exist yet
	cResp, err := file.Create(context.Background(), azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.Response().StatusCode)
	a.NotEqual("", cResp.ETag())
	a.NotEqual("", cResp.LastModified())
	a.NotEqual("", cResp.XMsRequestID())
	a.NotEqual("", cResp.XMsVersion())
	a.NotEqual("", cResp.Date())

	// Verify that the parent directory was created successfully
	dirResp, err := dirNotExist.GetProperties(context.Background())
	a.Nil(err)
	a.Equal(http.StatusOK, dirResp.StatusCode())
}

func TestFileCreateWithMetadataDelete(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fsURL)

	file, _ := getFileURLFromFileSystem(a, fsURL)

	metadata := make(map[string]string)
	metadata["foo"] = "bar"

	cResp, err := file.CreateWithOptions(context.Background(), azbfs.CreateFileOptions{Metadata: metadata}, azbfs.BlobFSAccessControl{})
	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.Response().StatusCode)
	a.NotEqual("", cResp.ETag())
	a.NotEqual("", cResp.LastModified())
	a.NotEqual("", cResp.XMsRequestID())
	a.NotEqual("", cResp.XMsVersion())
	a.NotEqual("", cResp.Date())

	getResp, err := file.GetProperties(context.Background())
	a.Nil(err)
	a.Equal(http.StatusOK, getResp.Response().StatusCode)
	a.NotEqual("", getResp.XMsProperties()) // Check metadata returned is not null.

	delResp, err := file.Delete(context.Background())
	a.Nil(err)
	a.Equal(http.StatusOK, delResp.Response().StatusCode)
	a.NotEqual("", delResp.XMsRequestID())
	a.NotEqual("", delResp.XMsVersion())
	a.NotEqual("", delResp.Date())
}

func TestFileGetProperties(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	fileSystemURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fileSystemURL)

	fileURL, _ := createNewFileFromFileSystem(a, fileSystemURL)
	defer deleteFile(a, fileURL)

	getResp, err := fileURL.GetProperties(context.Background())
	a.Nil(err)
	a.Equal(http.StatusOK, getResp.Response().StatusCode)
	a.NotEqual("", getResp.LastModified())
	a.Equal("file", getResp.XMsResourceType())
	a.NotEqual("", getResp.ETag())
	a.NotEqual("", getResp.XMsRequestID())
	a.NotEqual("", getResp.XMsVersion())
	a.NotEqual("", getResp.Date())
}

////TODO this is failing on the service side at the moment, the spec is not accurate
//func TestCreateFileWithBody(t *testing.T) {
//  a := assert.New(t)
//	fsu := getBfsServiceURL()
//	fileSystemURL, _ := createNewFileSystem(a, fsu)
//	defer deleteFileSystem(a, fileSystemURL)
//
//	fileURL, _ := createNewFileFromFileSystem(a, fileSystemURL, 2048)
//	defer deleteFile(a, fileURL)
//
//	contentR, contentD := getRandomDataAndReader(2048)
//
//	pResp, err := fileURL.Create(context.Background(), contentR)
//	a.Nil(err)
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
//	a.Nil(err)
//	c.Assert(resp.StatusCode(), chk.Equals, http.StatusPartialContent)
//	c.Assert(resp.ContentLength(), chk.Equals, "1024")
//	c.Assert(resp.ContentType(), chk.Equals, "application/octet-stream")
//	c.Assert(resp.Status(), chk.Not(chk.Equals), "")
//
//	download, err := io.ReadAll(resp.Response().Body)
//	a.Nil(err)
//	c.Assert(download, chk.DeepEquals, contentD[:1024])
//}

func TestUnexpectedEOFRecovery(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	fileSystemURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fileSystemURL)

	fileURL, _ := createNewFileFromFileSystem(a, fileSystemURL)
	defer deleteFile(a, fileURL)

	contentR, contentD := getRandomDataAndReader(2048)

	resp, err := fileURL.AppendData(context.Background(), 0, contentR)
	a.Nil(err)
	a.Equal(http.StatusAccepted, resp.StatusCode())
	a.NotEqual("", resp.XMsRequestID())
	a.NotEqual("", resp.XMsVersion())
	a.NotEqual("", resp.Date())

	resp, err = fileURL.FlushData(context.Background(), 2048, nil, azbfs.BlobFSHTTPHeaders{}, false, true)
	a.Nil(err)
	a.Equal(http.StatusOK, resp.StatusCode())
	a.NotEqual("", resp.ETag())
	a.NotEqual("", resp.LastModified())
	a.NotEqual("", resp.XMsRequestID())
	a.NotEqual("", resp.XMsVersion())
	a.NotEqual("", resp.Date())

	dResp, err := fileURL.Download(context.Background(), 0, 2048)
	a.Nil(err)

	// Verify that we can inject errors first.
	reader := dResp.Body(azbfs.InjectErrorInRetryReaderOptions(errors.New("unrecoverable error")))

	_, err = io.ReadAll(reader)
	a.NotNil(err)
	a.Equal("unrecoverable error", err.Error())

	// Then inject the retryable error.
	reader = dResp.Body(azbfs.InjectErrorInRetryReaderOptions(io.ErrUnexpectedEOF))

	buf, err := io.ReadAll(reader)
	a.Nil(err)
	a.Equal(contentD, buf)
}

func TestUploadDownloadRoundTrip(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	fileSystemURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fileSystemURL)

	fileURL, _ := createNewFileFromFileSystem(a, fileSystemURL)
	defer deleteFile(a, fileURL)

	// The file content will be made up of two parts
	contentR1, contentD1 := getRandomDataAndReader(2048)
	contentR2, contentD2 := getRandomDataAndReader(2048)

	// Append first part
	pResp, err := fileURL.AppendData(context.Background(), 0, contentR1)
	a.Nil(err)
	a.Equal(http.StatusAccepted, pResp.StatusCode())
	a.NotEqual("", pResp.XMsRequestID())
	a.NotEqual("", pResp.XMsVersion())
	a.NotEqual("", pResp.Date())

	// Append second part
	pResp, err = fileURL.AppendData(context.Background(), 2048, contentR2)
	a.Nil(err)
	a.Equal(http.StatusAccepted, pResp.StatusCode())
	a.NotEqual("", pResp.XMsRequestID())
	a.NotEqual("", pResp.XMsVersion())
	a.NotEqual("", pResp.Date())

	// Flush data
	fResp, err := fileURL.FlushData(context.Background(), 4096, make([]byte, 0), azbfs.BlobFSHTTPHeaders{}, false, true)
	a.Nil(err)
	a.Equal(http.StatusOK, fResp.StatusCode())
	a.NotEqual("", fResp.ETag())
	a.NotEqual("", fResp.LastModified())
	a.NotEqual("", fResp.XMsRequestID())
	a.NotEqual("", fResp.XMsVersion())
	a.NotEqual("", fResp.Date())

	// Get Partial data, check status code 206.
	resp, err := fileURL.Download(context.Background(), 0, 1024)
	a.Nil(err)
	a.Equal(http.StatusPartialContent, resp.StatusCode())
	a.EqualValues(1024, resp.ContentLength())
	a.Equal("application/octet-stream", resp.ContentType())
	a.NotEqual("", resp.Status())

	// Verify the partial data
	download, err := io.ReadAll(resp.Response().Body)
	a.Nil(err)
	a.Equal(contentD1[:1024], download)

	// Get entire fileURL, check status code 200.
	resp, err = fileURL.Download(context.Background(), 0, 0)
	a.Nil(err)
	a.Equal(http.StatusOK, resp.StatusCode())
	a.EqualValues(4096, resp.ContentLength())
	a.NotEqual("", resp.Date())
	a.NotEqual("", resp.ETag())
	a.NotEqual("", resp.LastModified())
	a.NotEqual("", resp.RequestID())
	a.NotEqual("", resp.Version())

	// Verify the entire content
	download, err = io.ReadAll(resp.Response().Body)
	a.Nil(err)
	a.Equal(contentD1[:], download[:2048])
	a.Equal(contentD2[:], download[2048:])
}

func TestBlobURLPartsSASQueryTimes(t *testing.T) {
	a := assert.New(t)
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
		a.Equal("https", parts.Scheme)
		a.Equal("myaccount.dfs.core.windows.net", parts.Host)
		a.Equal("myfilesystem", parts.FileSystemName)
		a.Equal("mydirectory/myfile.txt", parts.DirectoryOrFilePath)

		sas := parts.SAS
		a.Equal(StartTimesExpected[i], sas.StartTime())
		a.Equal(ExpiryTimesExpected[i], sas.ExpiryTime())

		uResult := parts.URL()
		a.Equal(urlString, uResult.String())
	}
}

func TestRenameFile(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	fileSystemURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fileSystemURL)

	fileURL, fileName := createNewFileFromFileSystem(a, fileSystemURL)
	fileRename := fileName + "rename"

	renamedFileURL, err := fileURL.Rename(context.Background(), azbfs.RenameFileOptions{DestinationPath: fileRename})
	a.NotNil(renamedFileURL)
	a.Nil(err)

	// Check that the old file does not exist
	getPropertiesResp, err := fileURL.GetProperties(context.Background())
	a.NotNil(err) // TODO: I want to check the status code is 404 but not sure how since the resp is nil
	a.Nil(getPropertiesResp)

	// Check that the renamed file does exist
	getPropertiesResp, err = renamedFileURL.GetProperties(context.Background())
	a.Equal(http.StatusOK, getPropertiesResp.StatusCode())
	a.Nil(err)
}

func TestRenameFileWithSas(t *testing.T) {
	a := assert.New(t)
	name, key := getAccountAndKey()
	credential := azbfs.NewSharedKeyCredential(name, key)
	sasQueryParams, err := azbfs.AccountSASSignatureValues{
		Protocol:      azbfs.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		Permissions:   azbfs.AccountSASPermissions{Read: true, List: true, Write: true, Delete: true, Add: true, Create: true, Update: true, Process: true}.String(),
		Services:      azbfs.AccountSASServices{File: true, Blob: true, Queue: true}.String(),
		ResourceTypes: azbfs.AccountSASResourceTypes{Service: true, Container: true, Object: true}.String(),
	}.NewSASQueryParameters(credential)
	a.Nil(err)

	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.dfs.core.windows.net/?%s",
		credential.AccountName(), qp)
	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	fsu := azbfs.NewServiceURL(*fullURL, azbfs.NewPipeline(azbfs.NewAnonymousCredential(), azbfs.PipelineOptions{}))

	fileSystemURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fileSystemURL)

	fileURL, fileName := createNewFileFromFileSystem(a, fileSystemURL)
	fileRename := fileName + "rename"

	renamedFileURL, err := fileURL.Rename(context.Background(), azbfs.RenameFileOptions{DestinationPath: fileRename})
	a.NotNil(renamedFileURL)
	a.Nil(err)

	// Check that the old file does not exist
	getPropertiesResp, err := fileURL.GetProperties(context.Background())
	a.NotNil(err) // TODO: I want to check the status code is 404 but not sure how since the resp is nil
	a.Nil(getPropertiesResp)

	// Check that the renamed file does exist
	getPropertiesResp, err = renamedFileURL.GetProperties(context.Background())
	a.Equal(http.StatusOK, getPropertiesResp.StatusCode())
	a.Nil(err)
}

func TestRenameFileWithDestinationSas(t *testing.T) {
	a := assert.New(t)
	name, key := getAccountAndKey()
	credential := azbfs.NewSharedKeyCredential(name, key)
	sourceSasQueryParams, err := azbfs.AccountSASSignatureValues{
		Protocol:      azbfs.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		Permissions:   azbfs.AccountSASPermissions{Read: true, List: true, Write: true, Delete: true, Add: true, Create: true, Update: true, Process: true}.String(),
		Services:      azbfs.AccountSASServices{File: true, Blob: true, Queue: true}.String(),
		ResourceTypes: azbfs.AccountSASResourceTypes{Service: true, Container: true, Object: true}.String(),
	}.NewSASQueryParameters(credential)
	a.Nil(err)

	// new SAS
	destinationSasQueryParams, err := azbfs.AccountSASSignatureValues{
		Protocol:      azbfs.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(24 * time.Hour),
		Permissions:   azbfs.AccountSASPermissions{Read: true, Write: true, Delete: true, Add: true, Create: true, Update: true, Process: true}.String(),
		Services:      azbfs.AccountSASServices{File: true, Blob: true}.String(),
		ResourceTypes: azbfs.AccountSASResourceTypes{Service: true, Container: true, Object: true}.String(),
	}.NewSASQueryParameters(credential)
	a.Nil(err)

	sourceQp := sourceSasQueryParams.Encode()
	destQp := destinationSasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.dfs.core.windows.net/?%s",
		credential.AccountName(), sourceQp)
	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	fsu := azbfs.NewServiceURL(*fullURL, azbfs.NewPipeline(azbfs.NewAnonymousCredential(), azbfs.PipelineOptions{}))

	fileSystemURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fileSystemURL)

	fileURL, fileName := createNewFileFromFileSystem(a, fileSystemURL)
	fileRename := fileName + "rename"

	renamedFileURL, err := fileURL.Rename(
		context.Background(), azbfs.RenameFileOptions{DestinationPath: fileRename, DestinationSas: &destQp})
	a.NotNil(renamedFileURL)
	a.Nil(err)
	found := strings.Contains(renamedFileURL.String(), destQp)
	// make sure the correct SAS is used
	a.True(found)

	// Check that the old file does not exist
	getPropertiesResp, err := fileURL.GetProperties(context.Background())
	a.NotNil(err) // TODO: I want to check the status code is 404 but not sure how since the resp is nil
	a.Nil(getPropertiesResp)

	// Check that the renamed file does exist
	getPropertiesResp, err = renamedFileURL.GetProperties(context.Background())
	a.Equal(http.StatusOK, getPropertiesResp.StatusCode())
	a.Nil(err)
}