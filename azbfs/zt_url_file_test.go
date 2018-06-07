package azbfs_test

import (
	"bytes"
	"context"
	//"crypto/md5"
	//"fmt"
	//"io/ioutil"
	//"net/http"
	"net/url"
	//"strings"
	//"time"

	chk "gopkg.in/check.v1" // go get gopkg.in/check.v1
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

type FileURLSuite struct{}

var _ = chk.Suite(&FileURLSuite{})

const (
	testFileRangeSize = 512 // Use this number considering clear range's function
)

func delFile(c *chk.C, file azbfs.FileURL) {
	resp, err := file.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, 200)
}

func getReaderToRandomBytes(n int) *bytes.Reader {
	r, _ := getRandomDataAndReader(n)
	return r
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


func (s *FileURLSuite) TestFileCreateDeleteDefault(c *chk.C) {
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// Create and delete file in root directory.
	file := fsURL.NewRootDirectoryURL().NewFileURL(generateFileName())

	cResp, err := file.Create(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.Response().StatusCode, chk.Equals, 201)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	delResp, err := file.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(delResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(delResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(delResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(delResp.Date(), chk.Not(chk.Equals), "")

	//dir, _ := createNewDirectoryFromShare(c, fsURL)
	//defer delDirectory(c, dir)
	//
	//// Create and delete file in named directory.
	//file = dir.NewFileURL(generateFileName())
	//
	//cResp, err = file.Create(context.Background(), 0, azfile.FileHTTPHeaders{}, nil)
	//c.Assert(err, chk.IsNil)
	//c.Assert(cResp.Response().StatusCode, chk.Equals, 201)
	//c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	//c.Assert(cResp.LastModified().IsZero(), chk.Equals, false)
	//c.Assert(cResp.RequestID(), chk.Not(chk.Equals), "")
	//c.Assert(cResp.Version(), chk.Not(chk.Equals), "")
	//c.Assert(cResp.Date().IsZero(), chk.Equals, false)
	//c.Assert(cResp.IsServerEncrypted(), chk.NotNil)
	//
	//delResp, err = file.Delete(context.Background())
	//c.Assert(err, chk.IsNil)
	//c.Assert(delResp.Response().StatusCode, chk.Equals, 202)
	//c.Assert(delResp.RequestID(), chk.Not(chk.Equals), "")
	//c.Assert(delResp.Version(), chk.Not(chk.Equals), "")
	//c.Assert(delResp.Date().IsZero(), chk.Equals, false)
}
//
//func (s *FileURLSuite) TestFileCreateNonDefaultMetadataNonEmpty(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := fileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, basicMetadata)
//
//	resp, err := fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.NewMetadata(), chk.DeepEquals, basicMetadata)
//}
//
//func (s *FileURLSuite) TestFileCreateNonDefaultHTTPHeaders(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := fileURL.Create(ctx, 0, basicHeaders, nil)
//	c.Assert(err, chk.IsNil)
//
//	resp, err := fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	h := resp.NewHTTPHeaders()
//	c.Assert(h, chk.DeepEquals, basicHeaders)
//}
//
//func (s *FileURLSuite) TestFileCreateNegativeMetadataInvalid(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := fileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, azfile.Metadata{"In valid1": "bar"})
//	c.Assert(strings.Contains(err.Error(), validationErrorSubstring), chk.Equals, true)
//
//}
//
//func (s *FileURLSuite) TestFileGetSetPropertiesNonDefault(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//	defer delFile(c, fileURL)
//
//	md5Str := "MDAwMDAwMDA="
//	var testMd5 [md5.Size]byte
//	copy(testMd5[:], md5Str)
//
//	properties := azfile.FileHTTPHeaders{
//		ContentType:        "text/html",
//		ContentEncoding:    "gzip",
//		ContentLanguage:    "tr,en",
//		ContentMD5:         testMd5,
//		CacheControl:       "no-transform",
//		ContentDisposition: "attachment",
//	}
//	setResp, err := fileURL.SetHTTPHeaders(context.Background(), properties)
//	c.Assert(err, chk.IsNil)
//	c.Assert(setResp.Response().StatusCode, chk.Equals, 200)
//	c.Assert(setResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(setResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(setResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(setResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(setResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(setResp.IsServerEncrypted(), chk.NotNil)
//
//	getResp, err := fileURL.GetProperties(context.Background())
//	c.Assert(err, chk.IsNil)
//	c.Assert(getResp.Response().StatusCode, chk.Equals, 200)
//	c.Assert(setResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(getResp.FileType(), chk.Equals, "File")
//
//	c.Assert(getResp.ContentType(), chk.Equals, properties.ContentType)
//	c.Assert(getResp.ContentEncoding(), chk.Equals, properties.ContentEncoding)
//	c.Assert(getResp.ContentLanguage(), chk.Equals, properties.ContentLanguage)
//	c.Assert(getResp.ContentMD5(), chk.DeepEquals, properties.ContentMD5)
//	c.Assert(getResp.CacheControl(), chk.Equals, properties.CacheControl)
//	c.Assert(getResp.ContentDisposition(), chk.Equals, properties.ContentDisposition)
//	c.Assert(getResp.ContentLength(), chk.Equals, int64(0))
//
//	c.Assert(getResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(getResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(getResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(getResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(getResp.IsServerEncrypted(), chk.NotNil)
//}
//
//func (s *FileURLSuite) TestFileGetSetPropertiesSnapshot(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionInclude)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	md5Str := "MDAwMDAwMDA="
//	var testMd5 [md5.Size]byte
//	copy(testMd5[:], md5Str)
//
//	properties := azfile.FileHTTPHeaders{
//		ContentType:        "text/html",
//		ContentEncoding:    "gzip",
//		ContentLanguage:    "tr,en",
//		ContentMD5:         testMd5,
//		CacheControl:       "no-transform",
//		ContentDisposition: "attachment",
//	}
//	setResp, err := fileURL.SetHTTPHeaders(context.Background(), properties)
//	c.Assert(err, chk.IsNil)
//	c.Assert(setResp.Response().StatusCode, chk.Equals, 200)
//	c.Assert(setResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(setResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(setResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(setResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(setResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(setResp.IsServerEncrypted(), chk.NotNil)
//
//	metadata := azfile.Metadata{
//		"foo": "foovalue",
//		"bar": "barvalue",
//	}
//	setResp2, err := fileURL.SetMetadata(context.Background(), metadata)
//	c.Assert(err, chk.IsNil)
//	c.Assert(setResp2.Response().StatusCode, chk.Equals, 200)
//
//	resp, _ := shareURL.CreateSnapshot(ctx, azfile.Metadata{})
//	snapshotURL := fileURL.WithSnapshot(resp.Snapshot())
//
//	getResp, err := snapshotURL.GetProperties(context.Background())
//	c.Assert(err, chk.IsNil)
//	c.Assert(getResp.Response().StatusCode, chk.Equals, 200)
//	c.Assert(setResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(getResp.FileType(), chk.Equals, "File")
//
//	c.Assert(getResp.ContentType(), chk.Equals, properties.ContentType)
//	c.Assert(getResp.ContentEncoding(), chk.Equals, properties.ContentEncoding)
//	c.Assert(getResp.ContentLanguage(), chk.Equals, properties.ContentLanguage)
//	c.Assert(getResp.ContentMD5(), chk.DeepEquals, properties.ContentMD5)
//	c.Assert(getResp.CacheControl(), chk.Equals, properties.CacheControl)
//	c.Assert(getResp.ContentDisposition(), chk.Equals, properties.ContentDisposition)
//	c.Assert(getResp.ContentLength(), chk.Equals, int64(0))
//
//	c.Assert(getResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(getResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(getResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(getResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(getResp.IsServerEncrypted(), chk.NotNil)
//	c.Assert(getResp.NewMetadata(), chk.DeepEquals, metadata)
//}
//
//func (s *FileURLSuite) TestGetSetMetadataNonDefault(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	metadata := azfile.Metadata{
//		"foo": "foovalue",
//		"bar": "barvalue",
//	}
//	setResp, err := fileURL.SetMetadata(context.Background(), metadata)
//	c.Assert(err, chk.IsNil)
//	c.Assert(setResp.Response().StatusCode, chk.Equals, 200)
//	c.Assert(setResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(setResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(setResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(setResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(setResp.IsServerEncrypted(), chk.NotNil)
//
//	getResp, err := fileURL.GetProperties(context.Background())
//	c.Assert(err, chk.IsNil)
//	c.Assert(getResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(getResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(getResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(getResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(getResp.Date().IsZero(), chk.Equals, false)
//	md := getResp.NewMetadata()
//	c.Assert(md, chk.DeepEquals, metadata)
//}
//
//func (s *FileURLSuite) TestFileSetMetadataNil(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	_, err := fileURL.SetMetadata(ctx, azfile.Metadata{"not": "nil"})
//	c.Assert(err, chk.IsNil)
//
//	_, err = fileURL.SetMetadata(ctx, nil)
//	c.Assert(err, chk.IsNil)
//
//	resp, err := fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.NewMetadata(), chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestFileSetMetadataDefaultEmpty(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	_, err := fileURL.SetMetadata(ctx, azfile.Metadata{"not": "nil"})
//	c.Assert(err, chk.IsNil)
//
//	_, err = fileURL.SetMetadata(ctx, azfile.Metadata{})
//	c.Assert(err, chk.IsNil)
//
//	resp, err := fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.NewMetadata(), chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestFileSetMetadataInvalidField(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	_, err := fileURL.SetMetadata(ctx, azfile.Metadata{"Invalid field!": "value"})
//	c.Assert(err, chk.NotNil)
//	c.Assert(strings.Contains(err.Error(), validationErrorSubstring), chk.Equals, true)
//}
//
//func (s *FileURLSuite) TestStartCopyDefault(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	srcFile, _ := createNewFileFromShare(c, shareURL, 2048)
//	defer delFile(c, srcFile)
//
//	destFile, _ := getFileURLFromShare(c, shareURL)
//	defer delFile(c, destFile)
//
//	_, err := srcFile.UploadRange(context.Background(), 0, getReaderToRandomBytes(2048))
//	c.Assert(err, chk.IsNil)
//
//	copyResp, err := destFile.StartCopy(context.Background(), srcFile.URL(), nil)
//	c.Assert(err, chk.IsNil)
//	c.Assert(copyResp.Response().StatusCode, chk.Equals, 202)
//	c.Assert(copyResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(copyResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(copyResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(copyResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(copyResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(copyResp.CopyID(), chk.Not(chk.Equals), "")
//	c.Assert(copyResp.CopyStatus(), chk.Not(chk.Equals), "")
//
//	var copyStatus azfile.CopyStatusType
//	timeout := time.Duration(2) * time.Minute
//	start := time.Now()
//
//	var getResp *azfile.FileGetPropertiesResponse
//
//	for copyStatus != azfile.CopyStatusSuccess && time.Now().Sub(start) < timeout {
//		getResp, err = destFile.GetProperties(context.Background())
//		c.Assert(err, chk.IsNil)
//		c.Assert(getResp.CopyID(), chk.Equals, copyResp.CopyID())
//		c.Assert(getResp.CopyStatus(), chk.Not(chk.Equals), azfile.CopyStatusNone)
//		c.Assert(getResp.CopySource(), chk.Equals, srcFile.String())
//		copyStatus = getResp.CopyStatus()
//
//		time.Sleep(time.Duration(5) * time.Second)
//	}
//
//	if getResp != nil && getResp.CopyStatus() == azfile.CopyStatusSuccess {
//		// Abort will fail after copy finished
//		abortResp, err := destFile.AbortCopy(context.Background(), copyResp.CopyID())
//		c.Assert(err, chk.NotNil)
//		c.Assert(abortResp, chk.IsNil)
//		se, ok := err.(azfile.StorageError)
//		c.Assert(ok, chk.Equals, true)
//		c.Assert(se.Response().StatusCode, chk.Equals, http.StatusConflict)
//	}
//}
//
//func waitForCopy(c *chk.C, copyFileURL azfile.FileURL, fileCopyResponse *azfile.FileStartCopyResponse) {
//	status := fileCopyResponse.CopyStatus()
//	// Wait for the copy to finish. If the copy takes longer than a minute, we will fail
//	start := time.Now()
//	for status != azfile.CopyStatusSuccess {
//		GetPropertiesResult, _ := copyFileURL.GetProperties(ctx)
//		status = GetPropertiesResult.CopyStatus()
//		currentTime := time.Now()
//		if currentTime.Sub(start) >= time.Minute {
//			c.Fail()
//		}
//	}
//}
//
//func (s *FileURLSuite) TestFileStartCopyDestEmpty(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShareWithDefaultData(c, shareURL)
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//
//	fileCopyResponse, err := copyFileURL.StartCopy(ctx, fileURL.URL(), nil)
//	c.Assert(err, chk.IsNil)
//	waitForCopy(c, copyFileURL, fileCopyResponse)
//
//	resp, err := copyFileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//
//	// Read the file data to verify the copy
//	data, _ := ioutil.ReadAll(resp.Response().Body)
//	c.Assert(resp.ContentLength(), chk.Equals, int64(len(fileDefaultData)))
//	c.Assert(string(data), chk.Equals, fileDefaultData)
//	resp.Response().Body.Close()
//}
//
//func (s *FileURLSuite) TestFileStartCopyMetadata(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//
//	resp, err := copyFileURL.StartCopy(ctx, fileURL.URL(), basicMetadata)
//	c.Assert(err, chk.IsNil)
//	waitForCopy(c, copyFileURL, resp)
//
//	resp2, err := copyFileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp2.NewMetadata(), chk.DeepEquals, basicMetadata)
//}
//
//func (s *FileURLSuite) TestFileStartCopyMetadataNil(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//
//	// Have the destination start with metadata so we ensure the nil metadata passed later takes effect
//	_, err := copyFileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, basicMetadata)
//	c.Assert(err, chk.IsNil)
//
//	resp, err := copyFileURL.StartCopy(ctx, fileURL.URL(), nil)
//	c.Assert(err, chk.IsNil)
//
//	waitForCopy(c, copyFileURL, resp)
//
//	resp2, err := copyFileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp2.NewMetadata(), chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestFileStartCopyMetadataEmpty(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//
//	// Have the destination start with metadata so we ensure the empty metadata passed later takes effect
//	_, err := copyFileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, basicMetadata)
//	c.Assert(err, chk.IsNil)
//
//	resp, err := copyFileURL.StartCopy(ctx, fileURL.URL(), azfile.Metadata{})
//	c.Assert(err, chk.IsNil)
//
//	waitForCopy(c, copyFileURL, resp)
//
//	resp2, err := copyFileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp2.NewMetadata(), chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestFileStartCopyNegativeMetadataInvalidField(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := copyFileURL.StartCopy(ctx, fileURL.URL(), azfile.Metadata{"I nvalid.": "bar"})
//	c.Assert(err, chk.NotNil)
//	c.Assert(strings.Contains(err.Error(), validationErrorSubstring), chk.Equals, true)
//}
//
//func (s *FileURLSuite) TestFileStartCopySourceNonExistant(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := copyFileURL.StartCopy(ctx, fileURL.URL(), nil)
//	validateStorageError(c, err, azfile.ServiceCodeResourceNotFound)
//}
//
//func (s *FileURLSuite) TestFileStartCopyUsingSASSrc(c *chk.C) {
//	fsu := getFSU()
//	shareURL, shareName := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, fileName := createNewFileFromShareWithDefaultData(c, shareURL)
//
//	// Create sas values for the source file
//	credential, _ := getCredential()
//	serviceSASValues := azfile.FileSASSignatureValues{Version: "2015-04-05", StartTime: time.Now().Add(-1 * time.Hour).UTC(),
//		ExpiryTime: time.Now().Add(time.Hour).UTC(), Permissions: azfile.FileSASPermissions{Read: true, Write: true, Create: true, Delete: true}.String(),
//		ShareName: shareName, FilePath: fileName}
//	queryParams := serviceSASValues.NewSASQueryParameters(credential)
//
//	// Create URLs to the destination file with sas parameters
//	sasURL := fileURL.URL()
//	sasURL.RawQuery = queryParams.Encode()
//
//	// Create a new container for the destination
//	copyShareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, copyShareURL, azfile.DeleteSnapshotsOptionNone)
//	copyFileURL, _ := getFileURLFromShare(c, copyShareURL)
//
//	resp, err := copyFileURL.StartCopy(ctx, sasURL, nil)
//	c.Assert(err, chk.IsNil)
//
//	waitForCopy(c, copyFileURL, resp)
//
//	resp2, err := copyFileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//
//	data, err := ioutil.ReadAll(resp2.Response().Body)
//	c.Assert(resp2.ContentLength(), chk.Equals, int64(len(fileDefaultData)))
//	c.Assert(string(data), chk.Equals, fileDefaultData)
//	resp2.Response().Body.Close()
//}
//
//func (s *FileURLSuite) TestFileStartCopyUsingSASDest(c *chk.C) {
//	fsu := getFSU()
//	shareURL, shareName := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, fileName := createNewFileFromShareWithDefaultData(c, shareURL)
//	_ = fileURL
//
//	// Generate SAS on the source
//	serviceSASValues := azfile.FileSASSignatureValues{ExpiryTime: time.Now().Add(time.Hour).UTC(),
//		Permissions: azfile.FileSASPermissions{Read: true, Write: true, Create: true}.String(), ShareName: shareName, FilePath: fileName}
//	credentials, _ := getCredential()
//	queryParams := serviceSASValues.NewSASQueryParameters(credentials)
//
//	copyShareURL, copyShareName := createNewFileSystem(c, fsu)
//	defer delShare(c, copyShareURL, azfile.DeleteSnapshotsOptionNone)
//	copyFileURL, copyFileName := getFileURLFromShare(c, copyShareURL)
//
//	// Generate Sas for the destination
//	copyServiceSASvalues := azfile.FileSASSignatureValues{StartTime: time.Now().Add(-1 * time.Hour).UTC(),
//		ExpiryTime: time.Now().Add(time.Hour).UTC(), Permissions: azfile.FileSASPermissions{Read: true, Write: true}.String(),
//		ShareName: copyShareName, FilePath: copyFileName}
//	copyQueryParams := copyServiceSASvalues.NewSASQueryParameters(credentials)
//
//	// Generate anonymous URL to destination with SAS
//	anonURL := fsu.URL()
//	anonURL.RawQuery = copyQueryParams.Encode()
//	anonPipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
//	anonFSU := azfile.NewServiceURL(anonURL, anonPipeline)
//	anonFileURL := anonFSU.NewShareURL(copyShareName)
//	anonfileURL := anonFileURL.NewRootDirectoryURL().NewFileURL(copyFileName)
//
//	// Apply sas to source
//	srcFileWithSasURL := fileURL.URL()
//	srcFileWithSasURL.RawQuery = queryParams.Encode()
//
//	resp, err := anonfileURL.StartCopy(ctx, srcFileWithSasURL, nil)
//	c.Assert(err, chk.IsNil)
//
//	// Allow copy to happen
//	waitForCopy(c, anonfileURL, resp)
//
//	resp2, err := copyFileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//
//	data, err := ioutil.ReadAll(resp2.Response().Body)
//	_, err = resp2.Body(azfile.RetryReaderOptions{}).Read(data)
//	c.Assert(resp2.ContentLength(), chk.Equals, int64(len(fileDefaultData)))
//	c.Assert(string(data), chk.Equals, fileDefaultData)
//	resp2.Body(azfile.RetryReaderOptions{}).Close()
//}
//
//func (s *FileURLSuite) TestFileAbortCopyInProgress(c *chk.C) {
//	fsu := getFSU()
//	shareURL, shareName := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, fileName := getFileURLFromShare(c, shareURL)
//
//	// Create a large file that takes time to copy
//	fileSize := 12 * 1024 * 1024
//	fileData := make([]byte, fileSize, fileSize)
//	for i := range fileData {
//		fileData[i] = byte('a' + i%26)
//	}
//	_, err := fileURL.Create(ctx, int64(fileSize), azfile.FileHTTPHeaders{}, nil)
//	c.Assert(err, chk.IsNil)
//
//	_, err = fileURL.UploadRange(ctx, 0, bytes.NewReader(fileData[0:4*1024*1024]))
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.UploadRange(ctx, 4*1024*1024, bytes.NewReader(fileData[4*1024*1024:8*1024*1024]))
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.UploadRange(ctx, 8*1024*1024, bytes.NewReader(fileData[8*1024*1024:]))
//	c.Assert(err, chk.IsNil)
//	serviceSASValues := azfile.FileSASSignatureValues{ExpiryTime: time.Now().Add(time.Hour).UTC(),
//		Permissions: azfile.FileSASPermissions{Read: true, Write: true, Create: true}.String(), ShareName: shareName, FilePath: fileName}
//	credentials, _ := getCredential()
//	queryParams := serviceSASValues.NewSASQueryParameters(credentials)
//	srcFileWithSasURL := fileURL.URL()
//	srcFileWithSasURL.RawQuery = queryParams.Encode()
//
//	fsu2, err := getAlternateFSU()
//	c.Assert(err, chk.IsNil)
//	copyShareURL, _ := createNewFileSystem(c, fsu2)
//	copyFileURL, _ := getFileURLFromShare(c, copyShareURL)
//
//	defer delShare(c, copyShareURL, azfile.DeleteSnapshotsOptionNone)
//
//	resp, err := copyFileURL.StartCopy(ctx, srcFileWithSasURL, nil)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.CopyStatus(), chk.Equals, azfile.CopyStatusPending)
//
//	_, err = copyFileURL.AbortCopy(ctx, resp.CopyID())
//	if err != nil {
//		// If the error is nil, the test continues as normal.
//		// If the error is not nil, we want to check if it's because the copy is finished and send a message indicating this.
//		c.Assert((err.(azfile.StorageError)).Response().StatusCode, chk.Equals, 409)
//		c.Error("The test failed because the copy completed because it was aborted")
//	}
//
//	resp2, _ := copyFileURL.GetProperties(ctx)
//	c.Assert(resp2.CopyStatus(), chk.Equals, azfile.CopyStatusAborted)
//}
//
//func (s *FileURLSuite) TestFileAbortCopyNoCopyStarted(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//	_, err := copyFileURL.AbortCopy(ctx, "copynotstarted")
//	validateStorageError(c, err, azfile.ServiceCodeInvalidQueryParameterValue)
//}
//
//func (s *FileURLSuite) TestResizeFile(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 1234)
//
//	gResp, err := fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(gResp.ContentLength(), chk.Equals, int64(1234))
//
//	rResp, err := fileURL.Resize(context.Background(), 4096)
//	c.Assert(err, chk.IsNil)
//	c.Assert(rResp.Response().StatusCode, chk.Equals, 200)
//
//	gResp, err = fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(gResp.ContentLength(), chk.Equals, int64(4096))
//}
//
//func (s *FileURLSuite) TestFileResizeZero(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 10)
//
//	// The default file is created with size > 0, so this should actually update
//	_, err := fileURL.Resize(ctx, 0)
//	c.Assert(err, chk.IsNil)
//
//	resp, err := fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.ContentLength(), chk.Equals, int64(0))
//}
//
//func (s *FileURLSuite) TestFileResizeInvalidSizeNegative(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	_, err := fileURL.Resize(ctx, -4)
//	c.Assert(err, chk.NotNil)
//	sErr := (err.(azfile.StorageError))
//	c.Assert(sErr.Response().StatusCode, chk.Equals, http.StatusBadRequest)
//}
//
//func (f *FileURLSuite) TestServiceSASShareSAS(c *chk.C) {
//	fsu := getFSU()
//	shareURL, shareName := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	credential, accountName := getCredential()
//
//	sasQueryParams := azfile.FileSASSignatureValues{
//		Protocol:    azfile.SASProtocolHTTPS,
//		ExpiryTime:  time.Now().UTC().Add(48 * time.Hour),
//		ShareName:   shareName,
//		Permissions: azfile.ShareSASPermissions{Create: true, Read: true, Write: true, Delete: true, List: true}.String(),
//	}.NewSASQueryParameters(credential)
//
//	qp := sasQueryParams.Encode()
//
//	fileName := "testFile"
//	dirName := "testDir"
//	fileUrlStr := fmt.Sprintf("https://%s.file.core.windows.net/%s/%s?%s",
//		accountName, shareName, fileName, qp)
//	fu, _ := url.Parse(fileUrlStr)
//
//	dirUrlStr := fmt.Sprintf("https://%s.file.core.windows.net/%s/%s?%s",
//		accountName, shareName, dirName, qp)
//	du, _ := url.Parse(dirUrlStr)
//
//	fileURL := azfile.NewFileURL(*fu, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
//	dirURL := azfile.NewDirectoryURL(*du, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
//
//	s := "Hello"
//	_, err := fileURL.Create(ctx, int64(len(s)), azfile.FileHTTPHeaders{}, azfile.Metadata{})
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.UploadRange(ctx, 0, bytes.NewReader([]byte(s)))
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.Delete(ctx)
//	c.Assert(err, chk.IsNil)
//
//	_, err = dirURL.Create(ctx, azfile.Metadata{})
//	c.Assert(err, chk.IsNil)
//
//	_, err = dirURL.ListFilesAndDirectoriesSegment(ctx, azfile.Marker{}, azfile.ListFilesAndDirectoriesOptions{})
//	c.Assert(err, chk.IsNil)
//}
//
//func (f *FileURLSuite) TestServiceSASFileSAS(c *chk.C) {
//	fsu := getFSU()
//	shareURL, shareName := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	credential, accountName := getCredential()
//
//	sasQueryParams := azfile.FileSASSignatureValues{
//		Protocol:    azfile.SASProtocolHTTPS,
//		ExpiryTime:  time.Now().UTC().Add(48 * time.Hour),
//		ShareName:   shareName,
//		Permissions: azfile.FileSASPermissions{Create: true, Read: true, Write: true, Delete: true}.String(),
//	}.NewSASQueryParameters(credential)
//
//	qp := sasQueryParams.Encode()
//
//	fileName := "testFile"
//	urlWithSAS := fmt.Sprintf("https://%s.file.core.windows.net/%s/%s?%s",
//		accountName, shareName, fileName, qp)
//	u, _ := url.Parse(urlWithSAS)
//
//	fileURL := azfile.NewFileURL(*u, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
//
//	s := "Hello"
//	_, err := fileURL.Create(ctx, int64(len(s)), azfile.FileHTTPHeaders{}, azfile.Metadata{})
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.UploadRange(ctx, 0, bytes.NewReader([]byte(s)))
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.Delete(ctx)
//	c.Assert(err, chk.IsNil)
//}
//
//func (s *FileURLSuite) TestDownloadEmptyZeroSizeFile(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//	defer delFile(c, fileURL)
//
//	// Download entire fileURL, check status code 200.
//	resp, err := fileURL.Download(context.Background(), 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.StatusCode(), chk.Equals, http.StatusOK)
//	c.Assert(resp.ContentLength(), chk.Equals, int64(0))
//	c.Assert(resp.FileContentMD5(), chk.Equals, [md5.Size]byte{}) // Note: FileContentMD5 is returned, only when range is specified explicitly.
//
//	download, err := ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(download, chk.HasLen, 0)
//	c.Assert(resp.AcceptRanges(), chk.Equals, "bytes")
//	c.Assert(resp.CacheControl(), chk.Equals, "")
//	c.Assert(resp.ContentDisposition(), chk.Equals, "")
//	c.Assert(resp.ContentEncoding(), chk.Equals, "")
//	c.Assert(resp.ContentRange(), chk.Equals, "") // Note: ContentRange is returned, only when range is specified explicitly.
//	c.Assert(resp.ContentType(), chk.Equals, "application/octet-stream")
//	c.Assert(resp.CopyCompletionTime().IsZero(), chk.Equals, true)
//	c.Assert(resp.CopyID(), chk.Equals, "")
//	c.Assert(resp.CopyProgress(), chk.Equals, "")
//	c.Assert(resp.CopySource(), chk.Equals, "")
//	c.Assert(resp.CopyStatus(), chk.Equals, azfile.CopyStatusNone)
//	c.Assert(resp.CopyStatusDescription(), chk.Equals, "")
//	c.Assert(resp.Date().IsZero(), chk.Equals, false)
//	c.Assert(resp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(resp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(resp.NewMetadata(), chk.DeepEquals, azfile.Metadata{})
//	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(resp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(resp.IsServerEncrypted(), chk.NotNil)
//}
//
//func (s *FileURLSuite) TestUploadDownloadDefaultNonDefaultMD5(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 2048)
//	defer delFile(c, fileURL)
//
//	contentR, contentD := getRandomDataAndReader(2048)
//
//	pResp, err := fileURL.UploadRange(context.Background(), 0, contentR)
//	c.Assert(err, chk.IsNil)
//	c.Assert(pResp.ContentMD5(), chk.Not(chk.Equals), [md5.Size]byte{})
//	c.Assert(pResp.StatusCode(), chk.Equals, http.StatusCreated)
//	c.Assert(pResp.IsServerEncrypted(), chk.NotNil)
//	c.Assert(pResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(pResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(pResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(pResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(pResp.Date().IsZero(), chk.Equals, false)
//
//	// Get with rangeGetContentMD5 enabled.
//	// Partial data, check status code 206.
//	resp, err := fileURL.Download(context.Background(), 0, 1024, true)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.StatusCode(), chk.Equals, http.StatusPartialContent)
//	c.Assert(resp.ContentLength(), chk.Equals, int64(1024))
//	c.Assert(resp.ContentMD5(), chk.Not(chk.Equals), [md5.Size]byte{})
//	c.Assert(resp.ContentType(), chk.Equals, "application/octet-stream")
//	c.Assert(resp.Status(), chk.Not(chk.Equals), "")
//
//	download, err := ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(download, chk.DeepEquals, contentD[:1024])
//
//	// Set ContentMD5 for the entire file.
//	_, err = fileURL.SetHTTPHeaders(context.Background(), azfile.FileHTTPHeaders{ContentMD5: pResp.ContentMD5(), ContentLanguage: "test"})
//	c.Assert(err, chk.IsNil)
//
//	// Test get with another type of range index, and validate if FileContentMD5 can be get correclty.
//	resp, err = fileURL.Download(context.Background(), 1024, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.StatusCode(), chk.Equals, http.StatusPartialContent)
//	c.Assert(resp.ContentLength(), chk.Equals, int64(1024))
//	c.Assert(resp.ContentMD5(), chk.Equals, [md5.Size]byte{})
//	c.Assert(resp.FileContentMD5(), chk.DeepEquals, pResp.ContentMD5())
//	c.Assert(resp.ContentLanguage(), chk.Equals, "test")
//	// Note: when it's downloading range, range's MD5 is returned, when set rangeGetContentMD5=true, currently set it to false, so should be empty
//	c.Assert(resp.NewHTTPHeaders(), chk.DeepEquals, azfile.FileHTTPHeaders{ContentMD5: [md5.Size]byte{}, ContentLanguage: "test"})
//
//	download, err = ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(download, chk.DeepEquals, contentD[1024:])
//
//	c.Assert(resp.AcceptRanges(), chk.Equals, "bytes")
//	c.Assert(resp.CacheControl(), chk.Equals, "")
//	c.Assert(resp.ContentDisposition(), chk.Equals, "")
//	c.Assert(resp.ContentEncoding(), chk.Equals, "")
//	c.Assert(resp.ContentRange(), chk.Equals, "bytes 1024-2047/2048")
//	c.Assert(resp.ContentType(), chk.Equals, "") // Note ContentType is set to empty during SetHTTPHeaders
//	c.Assert(resp.CopyCompletionTime().IsZero(), chk.Equals, true)
//	c.Assert(resp.CopyID(), chk.Equals, "")
//	c.Assert(resp.CopyProgress(), chk.Equals, "")
//	c.Assert(resp.CopySource(), chk.Equals, "")
//	c.Assert(resp.CopyStatus(), chk.Equals, azfile.CopyStatusNone)
//	c.Assert(resp.CopyStatusDescription(), chk.Equals, "")
//	c.Assert(resp.Date().IsZero(), chk.Equals, false)
//	c.Assert(resp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(resp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(resp.NewMetadata(), chk.DeepEquals, azfile.Metadata{})
//	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(resp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(resp.IsServerEncrypted(), chk.NotNil)
//
//	// Get entire fileURL, check status code 200.
//	resp, err = fileURL.Download(context.Background(), 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.StatusCode(), chk.Equals, http.StatusOK)
//	c.Assert(resp.ContentLength(), chk.Equals, int64(2048))
//	c.Assert(resp.ContentMD5(), chk.Equals, pResp.ContentMD5())   // Note: This case is inted to get entire fileURL, entire file's MD5 will be returned.
//	c.Assert(resp.FileContentMD5(), chk.Equals, [md5.Size]byte{}) // Note: FileContentMD5 is returned, only when range is specified explicitly.
//
//	download, err = ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(download, chk.DeepEquals, contentD[:])
//
//	c.Assert(resp.AcceptRanges(), chk.Equals, "bytes")
//	c.Assert(resp.CacheControl(), chk.Equals, "")
//	c.Assert(resp.ContentDisposition(), chk.Equals, "")
//	c.Assert(resp.ContentEncoding(), chk.Equals, "")
//	c.Assert(resp.ContentRange(), chk.Equals, "") // Note: ContentRange is returned, only when range is specified explicitly.
//	c.Assert(resp.ContentType(), chk.Equals, "")
//	c.Assert(resp.CopyCompletionTime().IsZero(), chk.Equals, true)
//	c.Assert(resp.CopyID(), chk.Equals, "")
//	c.Assert(resp.CopyProgress(), chk.Equals, "")
//	c.Assert(resp.CopySource(), chk.Equals, "")
//	c.Assert(resp.CopyStatus(), chk.Equals, azfile.CopyStatusNone)
//	c.Assert(resp.CopyStatusDescription(), chk.Equals, "")
//	c.Assert(resp.Date().IsZero(), chk.Equals, false)
//	c.Assert(resp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(resp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(resp.NewMetadata(), chk.DeepEquals, azfile.Metadata{})
//	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(resp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(resp.IsServerEncrypted(), chk.NotNil)
//}
//
//func (s *FileURLSuite) TestFileDownloadDataNonExistantFile(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	validateStorageError(c, err, azfile.ServiceCodeResourceNotFound)
//}
//
//func (s *FileURLSuite) TestFileDownloadDataNegativeOffset(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	defer func() { // The library should fail if it seems numeric parameters that are guaranteed invalid
//		recover()
//	}()
//
//	fileURL.Download(ctx, -1, azfile.CountToEnd, false)
//
//	c.Fail()
//}
//
//func (s *FileURLSuite) TestFileDownloadDataOffsetOutOfRange(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	_, err := fileURL.Download(ctx, int64(len(fileDefaultData)), azfile.CountToEnd, false)
//	validateStorageError(c, err, azfile.ServiceCodeInvalidRange)
//}
//
//func (s *FileURLSuite) TestFileDownloadDataInvalidCount(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	defer func() { // The library should panic if it sees numeric parameters that are guaranteed invalid
//		recover()
//	}()
//
//	fileURL.Download(ctx, 0, -100, false)
//
//	c.Fail()
//}
//
//func (s *FileURLSuite) TestFileDownloadDataEntireFile(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShareWithDefaultData(c, shareURL)
//
//	resp, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//
//	// Specifying a count of 0 results in the value being ignored
//	data, err := ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(string(data), chk.Equals, fileDefaultData)
//}
//
//func (s *FileURLSuite) TestFileDownloadDataCountExact(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShareWithDefaultData(c, shareURL)
//
//	resp, err := fileURL.Download(ctx, 0, int64(len(fileDefaultData)), false)
//	c.Assert(err, chk.IsNil)
//
//	data, err := ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(string(data), chk.Equals, fileDefaultData)
//}
//
//func (s *FileURLSuite) TestFileDownloadDataCountOutOfRange(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShareWithDefaultData(c, shareURL)
//
//	resp, err := fileURL.Download(ctx, 0, int64(len(fileDefaultData))*2, false)
//	c.Assert(err, chk.IsNil)
//
//	data, err := ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(string(data), chk.Equals, fileDefaultData)
//}
//
//func (s *FileURLSuite) TestFileUploadRangeNegativeInvalidOffset(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	c.Assert(func() { fileURL.UploadRange(ctx, -2, strings.NewReader(fileDefaultData)) }, chk.Panics, "offset must be >= 0")
//}
//
//func (s *FileURLSuite) TestFileUploadRangeNilBody(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	// A page range that starts and ends at 0 should panic
//	defer func() {
//		recover()
//	}()
//
//	fileURL.UploadRange(ctx, 0, nil)
//	c.Fail()
//}
//
//func (s *FileURLSuite) TestFileUploadRangeEmptyBody(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	c.Assert(func() { fileURL.UploadRange(ctx, 0, bytes.NewReader([]byte{})) }, chk.Panics, "body must contain readable data whose size is > 0")
//}
//
//func (s *FileURLSuite) TestFileUploadRangeNonExistantFile(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := fileURL.UploadRange(ctx, 0, getReaderToRandomBytes(12))
//	validateStorageError(c, err, azfile.ServiceCodeResourceNotFound)
//}
//
//// Testings for GetRangeList and ClearRange
//func (s *FileURLSuite) TestGetRangeListNonDefaultExact(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	fileSize := int64(512 * 10)
//
//	fileURL.Create(context.Background(), fileSize, azfile.FileHTTPHeaders{}, nil)
//
//	defer delFile(c, fileURL)
//
//	putResp, err := fileURL.UploadRange(context.Background(), 0, getReaderToRandomBytes(1024))
//	c.Assert(err, chk.IsNil)
//	c.Assert(putResp.Response().StatusCode, chk.Equals, 201)
//	c.Assert(putResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(putResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(putResp.ContentMD5(), chk.Not(chk.Equals), "")
//	c.Assert(putResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(putResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(putResp.Date().IsZero(), chk.Equals, false)
//
//	rangeList, err := fileURL.GetRangeList(context.Background(), 0, 1023)
//	c.Assert(err, chk.IsNil)
//	c.Assert(rangeList.Response().StatusCode, chk.Equals, 200)
//	c.Assert(rangeList.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(rangeList.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(rangeList.FileContentLength(), chk.Equals, fileSize)
//	c.Assert(rangeList.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(rangeList.Version(), chk.Not(chk.Equals), "")
//	c.Assert(rangeList.Date().IsZero(), chk.Equals, false)
//	c.Assert(rangeList.Value, chk.HasLen, 1)
//	c.Assert(rangeList.Value[0], chk.DeepEquals, azfile.Range{Start: 0, End: 1022})
//}
//
//// Default means clear the entire file's range
//func (s *FileURLSuite) TestClearRangeDefault(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 2048)
//	defer delFile(c, fileURL)
//
//	_, err := fileURL.UploadRange(context.Background(), 0, getReaderToRandomBytes(2048))
//	c.Assert(err, chk.IsNil)
//
//	clearResp, err := fileURL.ClearRange(context.Background(), 0, 2048)
//	c.Assert(err, chk.IsNil)
//	c.Assert(clearResp.Response().StatusCode, chk.Equals, 201)
//
//	rangeList, err := fileURL.GetRangeList(context.Background(), 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	c.Assert(rangeList.Value, chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestClearRangeNonDefault(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 4096)
//	defer delFile(c, fileURL)
//
//	_, err := fileURL.UploadRange(context.Background(), 2048, getReaderToRandomBytes(2048))
//	c.Assert(err, chk.IsNil)
//
//	clearResp, err := fileURL.ClearRange(context.Background(), 2048, 2048)
//	c.Assert(err, chk.IsNil)
//	c.Assert(clearResp.Response().StatusCode, chk.Equals, 201)
//
//	rangeList, err := fileURL.GetRangeList(context.Background(), 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	c.Assert(rangeList.Value, chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestClearRangeMultipleRanges(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 2048)
//	defer delFile(c, fileURL)
//
//	_, err := fileURL.UploadRange(context.Background(), 0, getReaderToRandomBytes(2048))
//	c.Assert(err, chk.IsNil)
//
//	clearResp, err := fileURL.ClearRange(context.Background(), 1024, 1024)
//	c.Assert(err, chk.IsNil)
//	c.Assert(clearResp.Response().StatusCode, chk.Equals, 201)
//
//	rangeList, err := fileURL.GetRangeList(context.Background(), 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	c.Assert(rangeList.Value, chk.HasLen, 1)
//	c.Assert(rangeList.Value[0], chk.DeepEquals, azfile.Range{Start: 0, End: 1023})
//}
//
//// When not 512 aligned, clear range will set 0 the non-512 aligned range, and will not eliminate the range.
//func (s *FileURLSuite) TestClearRangeNonDefault1Count(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 1)
//	defer delFile(c, fileURL)
//
//	d := []byte{1}
//	_, err := fileURL.UploadRange(context.Background(), 0, bytes.NewReader(d))
//	c.Assert(err, chk.IsNil)
//
//	clearResp, err := fileURL.ClearRange(context.Background(), 0, 1)
//	c.Assert(err, chk.IsNil)
//	c.Assert(clearResp.Response().StatusCode, chk.Equals, 201)
//
//	rangeList, err := fileURL.GetRangeList(context.Background(), 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	c.Assert(rangeList.Value, chk.HasLen, 1)
//	c.Assert(rangeList.Value[0], chk.DeepEquals, azfile.Range{Start: 0, End: 0})
//
//	dResp, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//	bytes, err := ioutil.ReadAll(dResp.Body(azfile.RetryReaderOptions{}))
//	c.Assert(err, chk.IsNil)
//	c.Assert(bytes, chk.DeepEquals, []byte{0})
//}
//
//func (s *FileURLSuite) TestFileClearRangeNegativeInvalidOffset(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := getShareURL(c, fsu)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	c.Assert(func() { fileURL.ClearRange(ctx, -1, 1) }, chk.Panics, "offset must be >= 0")
//}
//
//func (s *FileURLSuite) TestFileClearRangeNegativeInvalidCount(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := getShareURL(c, fsu)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	c.Assert(func() { fileURL.ClearRange(ctx, 0, 0) }, chk.Panics, "count cannot be CountToEnd, and must be > 0")
//}
//
//func setupGetRangeListTest(c *chk.C) (shareURL azfile.ShareURL, fileURL azfile.FileURL) {
//	fsu := getFSU()
//	shareURL, _ = createNewFileSystem(c, fsu)
//	fileURL, _ = createNewFileFromShare(c, shareURL, int64(testFileRangeSize))
//
//	_, err := fileURL.UploadRange(ctx, 0, getReaderToRandomBytes(testFileRangeSize))
//	c.Assert(err, chk.IsNil)
//
//	return
//}
//
//func validateBasicGetRangeList(c *chk.C, resp *azfile.Ranges, err error) {
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.Value, chk.HasLen, 1)
//	c.Assert(resp.Value[0], chk.Equals, azfile.Range{Start: 0, End: testFileRangeSize - 1})
//}
//
//func (s *FileURLSuite) TestFileGetRangeListDefaultEmptyFile(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	resp, err := fileURL.GetRangeList(ctx, 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.Value, chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestFileGetRangeListDefault1Range(c *chk.C) {
//	shareURL, fileURL := setupGetRangeListTest(c)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	resp, err := fileURL.GetRangeList(ctx, 0, azfile.CountToEnd)
//	validateBasicGetRangeList(c, resp, err)
//}
//
//func (s *FileURLSuite) TestFileGetRangeListNonContiguousRanges(c *chk.C) {
//	shareURL, fileURL := setupGetRangeListTest(c)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	_, err := fileURL.Resize(ctx, int64(testFileRangeSize*3))
//	c.Assert(err, chk.IsNil)
//
//	_, err = fileURL.UploadRange(ctx, testFileRangeSize*2, getReaderToRandomBytes(testFileRangeSize))
//	c.Assert(err, chk.IsNil)
//	resp, err := fileURL.GetRangeList(ctx, 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.Value, chk.HasLen, 2)
//	c.Assert(resp.Value[0], chk.Equals, azfile.Range{Start: 0, End: testFileRangeSize - 1})
//	c.Assert(resp.Value[1], chk.Equals, azfile.Range{Start: testFileRangeSize * 2, End: (testFileRangeSize * 3) - 1})
//}
//
//func (s *FileURLSuite) TestFileGetRangeListNonContiguousRangesCountLess(c *chk.C) {
//	shareURL, fileURL := setupGetRangeListTest(c)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	resp, err := fileURL.GetRangeList(ctx, 0, testFileRangeSize-1)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.Value, chk.HasLen, 1)
//	c.Assert(resp.Value[0], chk.Equals, azfile.Range{Start: 0, End: testFileRangeSize - 2})
//}
//
//func (s *FileURLSuite) TestFileGetRangeListNonContiguousRangesCountExceed(c *chk.C) {
//	shareURL, fileURL := setupGetRangeListTest(c)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	resp, err := fileURL.GetRangeList(ctx, 0, testFileRangeSize+1)
//	c.Assert(err, chk.IsNil)
//	validateBasicGetRangeList(c, resp, err)
//}
//
//func (s *FileURLSuite) TestFileGetRangeListSnapshot(c *chk.C) {//
//func (s *FileURLSuite) TestFileCreateDeleteDefault(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	// Create and delete file in root directory.
//	file := shareURL.NewRootDirectoryURL().NewFileURL(generateFileName())
//
//	cResp, err := file.Create(context.Background(), 0, azfile.FileHTTPHeaders{}, nil)
//	c.Assert(err, chk.IsNil)
//	c.Assert(cResp.Response().StatusCode, chk.Equals, 201)
//	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(cResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(cResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(cResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(cResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(cResp.IsServerEncrypted(), chk.NotNil)
//
//	delResp, err := file.Delete(context.Background())
//	c.Assert(err, chk.IsNil)
//	c.Assert(delResp.Response().StatusCode, chk.Equals, 202)
//	c.Assert(delResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(delResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(delResp.Date().IsZero(), chk.Equals, false)
//
//	dir, _ := createNewDirectoryFromShare(c, shareURL)
//	defer delDirectory(c, dir)
//
//	// Create and delete file in named directory.
//	file = dir.NewFileURL(generateFileName())
//
//	cResp, err = file.Create(context.Background(), 0, azfile.FileHTTPHeaders{}, nil)
//	c.Assert(err, chk.IsNil)
//	c.Assert(cResp.Response().StatusCode, chk.Equals, 201)
//	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(cResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(cResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(cResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(cResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(cResp.IsServerEncrypted(), chk.NotNil)
//
//	delResp, err = file.Delete(context.Background())
//	c.Assert(err, chk.IsNil)
//	c.Assert(delResp.Response().StatusCode, chk.Equals, 202)
//	c.Assert(delResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(delResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(delResp.Date().IsZero(), chk.Equals, false)
//}
//
//func (s *FileURLSuite) TestFileCreateNonDefaultMetadataNonEmpty(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := fileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, basicMetadata)
//
//	resp, err := fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.NewMetadata(), chk.DeepEquals, basicMetadata)
//}
//
//func (s *FileURLSuite) TestFileCreateNonDefaultHTTPHeaders(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := fileURL.Create(ctx, 0, basicHeaders, nil)
//	c.Assert(err, chk.IsNil)
//
//	resp, err := fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	h := resp.NewHTTPHeaders()
//	c.Assert(h, chk.DeepEquals, basicHeaders)
//}
//
//func (s *FileURLSuite) TestFileCreateNegativeMetadataInvalid(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := fileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, azfile.Metadata{"In valid1": "bar"})
//	c.Assert(strings.Contains(err.Error(), validationErrorSubstring), chk.Equals, true)
//
//}
//
//func (s *FileURLSuite) TestFileGetSetPropertiesNonDefault(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//	defer delFile(c, fileURL)
//
//	md5Str := "MDAwMDAwMDA="
//	var testMd5 [md5.Size]byte
//	copy(testMd5[:], md5Str)
//
//	properties := azfile.FileHTTPHeaders{
//		ContentType:        "text/html",
//		ContentEncoding:    "gzip",
//		ContentLanguage:    "tr,en",
//		ContentMD5:         testMd5,
//		CacheControl:       "no-transform",
//		ContentDisposition: "attachment",
//	}
//	setResp, err := fileURL.SetHTTPHeaders(context.Background(), properties)
//	c.Assert(err, chk.IsNil)
//	c.Assert(setResp.Response().StatusCode, chk.Equals, 200)
//	c.Assert(setResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(setResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(setResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(setResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(setResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(setResp.IsServerEncrypted(), chk.NotNil)
//
//	getResp, err := fileURL.GetProperties(context.Background())
//	c.Assert(err, chk.IsNil)
//	c.Assert(getResp.Response().StatusCode, chk.Equals, 200)
//	c.Assert(setResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(getResp.FileType(), chk.Equals, "File")
//
//	c.Assert(getResp.ContentType(), chk.Equals, properties.ContentType)
//	c.Assert(getResp.ContentEncoding(), chk.Equals, properties.ContentEncoding)
//	c.Assert(getResp.ContentLanguage(), chk.Equals, properties.ContentLanguage)
//	c.Assert(getResp.ContentMD5(), chk.DeepEquals, properties.ContentMD5)
//	c.Assert(getResp.CacheControl(), chk.Equals, properties.CacheControl)
//	c.Assert(getResp.ContentDisposition(), chk.Equals, properties.ContentDisposition)
//	c.Assert(getResp.ContentLength(), chk.Equals, int64(0))
//
//	c.Assert(getResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(getResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(getResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(getResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(getResp.IsServerEncrypted(), chk.NotNil)
//}
//
//func (s *FileURLSuite) TestFileGetSetPropertiesSnapshot(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionInclude)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	md5Str := "MDAwMDAwMDA="
//	var testMd5 [md5.Size]byte
//	copy(testMd5[:], md5Str)
//
//	properties := azfile.FileHTTPHeaders{
//		ContentType:        "text/html",
//		ContentEncoding:    "gzip",
//		ContentLanguage:    "tr,en",
//		ContentMD5:         testMd5,
//		CacheControl:       "no-transform",
//		ContentDisposition: "attachment",
//	}
//	setResp, err := fileURL.SetHTTPHeaders(context.Background(), properties)
//	c.Assert(err, chk.IsNil)
//	c.Assert(setResp.Response().StatusCode, chk.Equals, 200)
//	c.Assert(setResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(setResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(setResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(setResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(setResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(setResp.IsServerEncrypted(), chk.NotNil)
//
//	metadata := azfile.Metadata{
//		"foo": "foovalue",
//		"bar": "barvalue",
//	}
//	setResp2, err := fileURL.SetMetadata(context.Background(), metadata)
//	c.Assert(err, chk.IsNil)
//	c.Assert(setResp2.Response().StatusCode, chk.Equals, 200)
//
//	resp, _ := shareURL.CreateSnapshot(ctx, azfile.Metadata{})
//	snapshotURL := fileURL.WithSnapshot(resp.Snapshot())
//
//	getResp, err := snapshotURL.GetProperties(context.Background())
//	c.Assert(err, chk.IsNil)
//	c.Assert(getResp.Response().StatusCode, chk.Equals, 200)
//	c.Assert(setResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(getResp.FileType(), chk.Equals, "File")
//
//	c.Assert(getResp.ContentType(), chk.Equals, properties.ContentType)
//	c.Assert(getResp.ContentEncoding(), chk.Equals, properties.ContentEncoding)
//	c.Assert(getResp.ContentLanguage(), chk.Equals, properties.ContentLanguage)
//	c.Assert(getResp.ContentMD5(), chk.DeepEquals, properties.ContentMD5)
//	c.Assert(getResp.CacheControl(), chk.Equals, properties.CacheControl)
//	c.Assert(getResp.ContentDisposition(), chk.Equals, properties.ContentDisposition)
//	c.Assert(getResp.ContentLength(), chk.Equals, int64(0))
//
//	c.Assert(getResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(getResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(getResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(getResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(getResp.IsServerEncrypted(), chk.NotNil)
//	c.Assert(getResp.NewMetadata(), chk.DeepEquals, metadata)
//}
//
//func (s *FileURLSuite) TestGetSetMetadataNonDefault(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	metadata := azfile.Metadata{
//		"foo": "foovalue",
//		"bar": "barvalue",
//	}
//	setResp, err := fileURL.SetMetadata(context.Background(), metadata)
//	c.Assert(err, chk.IsNil)
//	c.Assert(setResp.Response().StatusCode, chk.Equals, 200)
//	c.Assert(setResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(setResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(setResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(setResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(setResp.IsServerEncrypted(), chk.NotNil)
//
//	getResp, err := fileURL.GetProperties(context.Background())
//	c.Assert(err, chk.IsNil)
//	c.Assert(getResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(getResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(getResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(getResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(getResp.Date().IsZero(), chk.Equals, false)
//	md := getResp.NewMetadata()
//	c.Assert(md, chk.DeepEquals, metadata)
//}
//
//func (s *FileURLSuite) TestFileSetMetadataNil(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	_, err := fileURL.SetMetadata(ctx, azfile.Metadata{"not": "nil"})
//	c.Assert(err, chk.IsNil)
//
//	_, err = fileURL.SetMetadata(ctx, nil)
//	c.Assert(err, chk.IsNil)
//
//	resp, err := fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.NewMetadata(), chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestFileSetMetadataDefaultEmpty(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	_, err := fileURL.SetMetadata(ctx, azfile.Metadata{"not": "nil"})
//	c.Assert(err, chk.IsNil)
//
//	_, err = fileURL.SetMetadata(ctx, azfile.Metadata{})
//	c.Assert(err, chk.IsNil)
//
//	resp, err := fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.NewMetadata(), chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestFileSetMetadataInvalidField(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	_, err := fileURL.SetMetadata(ctx, azfile.Metadata{"Invalid field!": "value"})
//	c.Assert(err, chk.NotNil)
//	c.Assert(strings.Contains(err.Error(), validationErrorSubstring), chk.Equals, true)
//}
//
//func (s *FileURLSuite) TestStartCopyDefault(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	srcFile, _ := createNewFileFromShare(c, shareURL, 2048)
//	defer delFile(c, srcFile)
//
//	destFile, _ := getFileURLFromShare(c, shareURL)
//	defer delFile(c, destFile)
//
//	_, err := srcFile.UploadRange(context.Background(), 0, getReaderToRandomBytes(2048))
//	c.Assert(err, chk.IsNil)
//
//	copyResp, err := destFile.StartCopy(context.Background(), srcFile.URL(), nil)
//	c.Assert(err, chk.IsNil)
//	c.Assert(copyResp.Response().StatusCode, chk.Equals, 202)
//	c.Assert(copyResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(copyResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(copyResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(copyResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(copyResp.Date().IsZero(), chk.Equals, false)
//	c.Assert(copyResp.CopyID(), chk.Not(chk.Equals), "")
//	c.Assert(copyResp.CopyStatus(), chk.Not(chk.Equals), "")
//
//	var copyStatus azfile.CopyStatusType
//	timeout := time.Duration(2) * time.Minute
//	start := time.Now()
//
//	var getResp *azfile.FileGetPropertiesResponse
//
//	for copyStatus != azfile.CopyStatusSuccess && time.Now().Sub(start) < timeout {
//		getResp, err = destFile.GetProperties(context.Background())
//		c.Assert(err, chk.IsNil)
//		c.Assert(getResp.CopyID(), chk.Equals, copyResp.CopyID())
//		c.Assert(getResp.CopyStatus(), chk.Not(chk.Equals), azfile.CopyStatusNone)
//		c.Assert(getResp.CopySource(), chk.Equals, srcFile.String())
//		copyStatus = getResp.CopyStatus()
//
//		time.Sleep(time.Duration(5) * time.Second)
//	}
//
//	if getResp != nil && getResp.CopyStatus() == azfile.CopyStatusSuccess {
//		// Abort will fail after copy finished
//		abortResp, err := destFile.AbortCopy(context.Background(), copyResp.CopyID())
//		c.Assert(err, chk.NotNil)
//		c.Assert(abortResp, chk.IsNil)
//		se, ok := err.(azfile.StorageError)
//		c.Assert(ok, chk.Equals, true)
//		c.Assert(se.Response().StatusCode, chk.Equals, http.StatusConflict)
//	}
//}
//
//func waitForCopy(c *chk.C, copyFileURL azfile.FileURL, fileCopyResponse *azfile.FileStartCopyResponse) {
//	status := fileCopyResponse.CopyStatus()
//	// Wait for the copy to finish. If the copy takes longer than a minute, we will fail
//	start := time.Now()
//	for status != azfile.CopyStatusSuccess {
//		GetPropertiesResult, _ := copyFileURL.GetProperties(ctx)
//		status = GetPropertiesResult.CopyStatus()
//		currentTime := time.Now()
//		if currentTime.Sub(start) >= time.Minute {
//			c.Fail()
//		}
//	}
//}
//
//func (s *FileURLSuite) TestFileStartCopyDestEmpty(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShareWithDefaultData(c, shareURL)
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//
//	fileCopyResponse, err := copyFileURL.StartCopy(ctx, fileURL.URL(), nil)
//	c.Assert(err, chk.IsNil)
//	waitForCopy(c, copyFileURL, fileCopyResponse)
//
//	resp, err := copyFileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//
//	// Read the file data to verify the copy
//	data, _ := ioutil.ReadAll(resp.Response().Body)
//	c.Assert(resp.ContentLength(), chk.Equals, int64(len(fileDefaultData)))
//	c.Assert(string(data), chk.Equals, fileDefaultData)
//	resp.Response().Body.Close()
//}
//
//func (s *FileURLSuite) TestFileStartCopyMetadata(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//
//	resp, err := copyFileURL.StartCopy(ctx, fileURL.URL(), basicMetadata)
//	c.Assert(err, chk.IsNil)
//	waitForCopy(c, copyFileURL, resp)
//
//	resp2, err := copyFileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp2.NewMetadata(), chk.DeepEquals, basicMetadata)
//}
//
//func (s *FileURLSuite) TestFileStartCopyMetadataNil(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//
//	// Have the destination start with metadata so we ensure the nil metadata passed later takes effect
//	_, err := copyFileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, basicMetadata)
//	c.Assert(err, chk.IsNil)
//
//	resp, err := copyFileURL.StartCopy(ctx, fileURL.URL(), nil)
//	c.Assert(err, chk.IsNil)
//
//	waitForCopy(c, copyFileURL, resp)
//
//	resp2, err := copyFileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp2.NewMetadata(), chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestFileStartCopyMetadataEmpty(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//
//	// Have the destination start with metadata so we ensure the empty metadata passed later takes effect
//	_, err := copyFileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, basicMetadata)
//	c.Assert(err, chk.IsNil)
//
//	resp, err := copyFileURL.StartCopy(ctx, fileURL.URL(), azfile.Metadata{})
//	c.Assert(err, chk.IsNil)
//
//	waitForCopy(c, copyFileURL, resp)
//
//	resp2, err := copyFileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp2.NewMetadata(), chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestFileStartCopyNegativeMetadataInvalidField(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := copyFileURL.StartCopy(ctx, fileURL.URL(), azfile.Metadata{"I nvalid.": "bar"})
//	c.Assert(err, chk.NotNil)
//	c.Assert(strings.Contains(err.Error(), validationErrorSubstring), chk.Equals, true)
//}
//
//func (s *FileURLSuite) TestFileStartCopySourceNonExistant(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := copyFileURL.StartCopy(ctx, fileURL.URL(), nil)
//	validateStorageError(c, err, azfile.ServiceCodeResourceNotFound)
//}
//
//func (s *FileURLSuite) TestFileStartCopyUsingSASSrc(c *chk.C) {
//	fsu := getFSU()
//	shareURL, shareName := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, fileName := createNewFileFromShareWithDefaultData(c, shareURL)
//
//	// Create sas values for the source file
//	credential, _ := getCredential()
//	serviceSASValues := azfile.FileSASSignatureValues{Version: "2015-04-05", StartTime: time.Now().Add(-1 * time.Hour).UTC(),
//		ExpiryTime: time.Now().Add(time.Hour).UTC(), Permissions: azfile.FileSASPermissions{Read: true, Write: true, Create: true, Delete: true}.String(),
//		ShareName: shareName, FilePath: fileName}
//	queryParams := serviceSASValues.NewSASQueryParameters(credential)
//
//	// Create URLs to the destination file with sas parameters
//	sasURL := fileURL.URL()
//	sasURL.RawQuery = queryParams.Encode()
//
//	// Create a new container for the destination
//	copyShareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, copyShareURL, azfile.DeleteSnapshotsOptionNone)
//	copyFileURL, _ := getFileURLFromShare(c, copyShareURL)
//
//	resp, err := copyFileURL.StartCopy(ctx, sasURL, nil)
//	c.Assert(err, chk.IsNil)
//
//	waitForCopy(c, copyFileURL, resp)
//
//	resp2, err := copyFileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//
//	data, err := ioutil.ReadAll(resp2.Response().Body)
//	c.Assert(resp2.ContentLength(), chk.Equals, int64(len(fileDefaultData)))
//	c.Assert(string(data), chk.Equals, fileDefaultData)
//	resp2.Response().Body.Close()
//}
//
//func (s *FileURLSuite) TestFileStartCopyUsingSASDest(c *chk.C) {
//	fsu := getFSU()
//	shareURL, shareName := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, fileName := createNewFileFromShareWithDefaultData(c, shareURL)
//	_ = fileURL
//
//	// Generate SAS on the source
//	serviceSASValues := azfile.FileSASSignatureValues{ExpiryTime: time.Now().Add(time.Hour).UTC(),
//		Permissions: azfile.FileSASPermissions{Read: true, Write: true, Create: true}.String(), ShareName: shareName, FilePath: fileName}
//	credentials, _ := getCredential()
//	queryParams := serviceSASValues.NewSASQueryParameters(credentials)
//
//	copyShareURL, copyShareName := createNewFileSystem(c, fsu)
//	defer delShare(c, copyShareURL, azfile.DeleteSnapshotsOptionNone)
//	copyFileURL, copyFileName := getFileURLFromShare(c, copyShareURL)
//
//	// Generate Sas for the destination
//	copyServiceSASvalues := azfile.FileSASSignatureValues{StartTime: time.Now().Add(-1 * time.Hour).UTC(),
//		ExpiryTime: time.Now().Add(time.Hour).UTC(), Permissions: azfile.FileSASPermissions{Read: true, Write: true}.String(),
//		ShareName: copyShareName, FilePath: copyFileName}
//	copyQueryParams := copyServiceSASvalues.NewSASQueryParameters(credentials)
//
//	// Generate anonymous URL to destination with SAS
//	anonURL := fsu.URL()
//	anonURL.RawQuery = copyQueryParams.Encode()
//	anonPipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
//	anonFSU := azfile.NewServiceURL(anonURL, anonPipeline)
//	anonFileURL := anonFSU.NewShareURL(copyShareName)
//	anonfileURL := anonFileURL.NewRootDirectoryURL().NewFileURL(copyFileName)
//
//	// Apply sas to source
//	srcFileWithSasURL := fileURL.URL()
//	srcFileWithSasURL.RawQuery = queryParams.Encode()
//
//	resp, err := anonfileURL.StartCopy(ctx, srcFileWithSasURL, nil)
//	c.Assert(err, chk.IsNil)
//
//	// Allow copy to happen
//	waitForCopy(c, anonfileURL, resp)
//
//	resp2, err := copyFileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//
//	data, err := ioutil.ReadAll(resp2.Response().Body)
//	_, err = resp2.Body(azfile.RetryReaderOptions{}).Read(data)
//	c.Assert(resp2.ContentLength(), chk.Equals, int64(len(fileDefaultData)))
//	c.Assert(string(data), chk.Equals, fileDefaultData)
//	resp2.Body(azfile.RetryReaderOptions{}).Close()
//}
//
//func (s *FileURLSuite) TestFileAbortCopyInProgress(c *chk.C) {
//	fsu := getFSU()
//	shareURL, shareName := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, fileName := getFileURLFromShare(c, shareURL)
//
//	// Create a large file that takes time to copy
//	fileSize := 12 * 1024 * 1024
//	fileData := make([]byte, fileSize, fileSize)
//	for i := range fileData {
//		fileData[i] = byte('a' + i%26)
//	}
//	_, err := fileURL.Create(ctx, int64(fileSize), azfile.FileHTTPHeaders{}, nil)
//	c.Assert(err, chk.IsNil)
//
//	_, err = fileURL.UploadRange(ctx, 0, bytes.NewReader(fileData[0:4*1024*1024]))
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.UploadRange(ctx, 4*1024*1024, bytes.NewReader(fileData[4*1024*1024:8*1024*1024]))
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.UploadRange(ctx, 8*1024*1024, bytes.NewReader(fileData[8*1024*1024:]))
//	c.Assert(err, chk.IsNil)
//	serviceSASValues := azfile.FileSASSignatureValues{ExpiryTime: time.Now().Add(time.Hour).UTC(),
//		Permissions: azfile.FileSASPermissions{Read: true, Write: true, Create: true}.String(), ShareName: shareName, FilePath: fileName}
//	credentials, _ := getCredential()
//	queryParams := serviceSASValues.NewSASQueryParameters(credentials)
//	srcFileWithSasURL := fileURL.URL()
//	srcFileWithSasURL.RawQuery = queryParams.Encode()
//
//	fsu2, err := getAlternateFSU()
//	c.Assert(err, chk.IsNil)
//	copyShareURL, _ := createNewFileSystem(c, fsu2)
//	copyFileURL, _ := getFileURLFromShare(c, copyShareURL)
//
//	defer delShare(c, copyShareURL, azfile.DeleteSnapshotsOptionNone)
//
//	resp, err := copyFileURL.StartCopy(ctx, srcFileWithSasURL, nil)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.CopyStatus(), chk.Equals, azfile.CopyStatusPending)
//
//	_, err = copyFileURL.AbortCopy(ctx, resp.CopyID())
//	if err != nil {
//		// If the error is nil, the test continues as normal.
//		// If the error is not nil, we want to check if it's because the copy is finished and send a message indicating this.
//		c.Assert((err.(azfile.StorageError)).Response().StatusCode, chk.Equals, 409)
//		c.Error("The test failed because the copy completed because it was aborted")
//	}
//
//	resp2, _ := copyFileURL.GetProperties(ctx)
//	c.Assert(resp2.CopyStatus(), chk.Equals, azfile.CopyStatusAborted)
//}
//
//func (s *FileURLSuite) TestFileAbortCopyNoCopyStarted(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	copyFileURL, _ := getFileURLFromShare(c, shareURL)
//	_, err := copyFileURL.AbortCopy(ctx, "copynotstarted")
//	validateStorageError(c, err, azfile.ServiceCodeInvalidQueryParameterValue)
//}
//
//func (s *FileURLSuite) TestResizeFile(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 1234)
//
//	gResp, err := fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(gResp.ContentLength(), chk.Equals, int64(1234))
//
//	rResp, err := fileURL.Resize(context.Background(), 4096)
//	c.Assert(err, chk.IsNil)
//	c.Assert(rResp.Response().StatusCode, chk.Equals, 200)
//
//	gResp, err = fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(gResp.ContentLength(), chk.Equals, int64(4096))
//}
//
//func (s *FileURLSuite) TestFileResizeZero(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 10)
//
//	// The default file is created with size > 0, so this should actually update
//	_, err := fileURL.Resize(ctx, 0)
//	c.Assert(err, chk.IsNil)
//
//	resp, err := fileURL.GetProperties(ctx)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.ContentLength(), chk.Equals, int64(0))
//}
//
//func (s *FileURLSuite) TestFileResizeInvalidSizeNegative(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	_, err := fileURL.Resize(ctx, -4)
//	c.Assert(err, chk.NotNil)
//	sErr := (err.(azfile.StorageError))
//	c.Assert(sErr.Response().StatusCode, chk.Equals, http.StatusBadRequest)
//}
//
//func (f *FileURLSuite) TestServiceSASShareSAS(c *chk.C) {
//	fsu := getFSU()
//	shareURL, shareName := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	credential, accountName := getCredential()
//
//	sasQueryParams := azfile.FileSASSignatureValues{
//		Protocol:    azfile.SASProtocolHTTPS,
//		ExpiryTime:  time.Now().UTC().Add(48 * time.Hour),
//		ShareName:   shareName,
//		Permissions: azfile.ShareSASPermissions{Create: true, Read: true, Write: true, Delete: true, List: true}.String(),
//	}.NewSASQueryParameters(credential)
//
//	qp := sasQueryParams.Encode()
//
//	fileName := "testFile"
//	dirName := "testDir"
//	fileUrlStr := fmt.Sprintf("https://%s.file.core.windows.net/%s/%s?%s",
//		accountName, shareName, fileName, qp)
//	fu, _ := url.Parse(fileUrlStr)
//
//	dirUrlStr := fmt.Sprintf("https://%s.file.core.windows.net/%s/%s?%s",
//		accountName, shareName, dirName, qp)
//	du, _ := url.Parse(dirUrlStr)
//
//	fileURL := azfile.NewFileURL(*fu, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
//	dirURL := azfile.NewDirectoryURL(*du, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
//
//	s := "Hello"
//	_, err := fileURL.Create(ctx, int64(len(s)), azfile.FileHTTPHeaders{}, azfile.Metadata{})
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.UploadRange(ctx, 0, bytes.NewReader([]byte(s)))
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.Delete(ctx)
//	c.Assert(err, chk.IsNil)
//
//	_, err = dirURL.Create(ctx, azfile.Metadata{})
//	c.Assert(err, chk.IsNil)
//
//	_, err = dirURL.ListFilesAndDirectoriesSegment(ctx, azfile.Marker{}, azfile.ListFilesAndDirectoriesOptions{})
//	c.Assert(err, chk.IsNil)
//}
//
//func (f *FileURLSuite) TestServiceSASFileSAS(c *chk.C) {
//	fsu := getFSU()
//	shareURL, shareName := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	credential, accountName := getCredential()
//
//	sasQueryParams := azfile.FileSASSignatureValues{
//		Protocol:    azfile.SASProtocolHTTPS,
//		ExpiryTime:  time.Now().UTC().Add(48 * time.Hour),
//		ShareName:   shareName,
//		Permissions: azfile.FileSASPermissions{Create: true, Read: true, Write: true, Delete: true}.String(),
//	}.NewSASQueryParameters(credential)
//
//	qp := sasQueryParams.Encode()
//
//	fileName := "testFile"
//	urlWithSAS := fmt.Sprintf("https://%s.file.core.windows.net/%s/%s?%s",
//		accountName, shareName, fileName, qp)
//	u, _ := url.Parse(urlWithSAS)
//
//	fileURL := azfile.NewFileURL(*u, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
//
//	s := "Hello"
//	_, err := fileURL.Create(ctx, int64(len(s)), azfile.FileHTTPHeaders{}, azfile.Metadata{})
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.UploadRange(ctx, 0, bytes.NewReader([]byte(s)))
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//	_, err = fileURL.Delete(ctx)
//	c.Assert(err, chk.IsNil)
//}
//
//func (s *FileURLSuite) TestDownloadEmptyZeroSizeFile(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//	defer delFile(c, fileURL)
//
//	// Download entire fileURL, check status code 200.
//	resp, err := fileURL.Download(context.Background(), 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.StatusCode(), chk.Equals, http.StatusOK)
//	c.Assert(resp.ContentLength(), chk.Equals, int64(0))
//	c.Assert(resp.FileContentMD5(), chk.Equals, [md5.Size]byte{}) // Note: FileContentMD5 is returned, only when range is specified explicitly.
//
//	download, err := ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(download, chk.HasLen, 0)
//	c.Assert(resp.AcceptRanges(), chk.Equals, "bytes")
//	c.Assert(resp.CacheControl(), chk.Equals, "")
//	c.Assert(resp.ContentDisposition(), chk.Equals, "")
//	c.Assert(resp.ContentEncoding(), chk.Equals, "")
//	c.Assert(resp.ContentRange(), chk.Equals, "") // Note: ContentRange is returned, only when range is specified explicitly.
//	c.Assert(resp.ContentType(), chk.Equals, "application/octet-stream")
//	c.Assert(resp.CopyCompletionTime().IsZero(), chk.Equals, true)
//	c.Assert(resp.CopyID(), chk.Equals, "")
//	c.Assert(resp.CopyProgress(), chk.Equals, "")
//	c.Assert(resp.CopySource(), chk.Equals, "")
//	c.Assert(resp.CopyStatus(), chk.Equals, azfile.CopyStatusNone)
//	c.Assert(resp.CopyStatusDescription(), chk.Equals, "")
//	c.Assert(resp.Date().IsZero(), chk.Equals, false)
//	c.Assert(resp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(resp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(resp.NewMetadata(), chk.DeepEquals, azfile.Metadata{})
//	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(resp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(resp.IsServerEncrypted(), chk.NotNil)
//}
//
//func (s *FileURLSuite) TestUploadDownloadDefaultNonDefaultMD5(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 2048)
//	defer delFile(c, fileURL)
//
//	contentR, contentD := getRandomDataAndReader(2048)
//
//	pResp, err := fileURL.UploadRange(context.Background(), 0, contentR)
//	c.Assert(err, chk.IsNil)
//	c.Assert(pResp.ContentMD5(), chk.Not(chk.Equals), [md5.Size]byte{})
//	c.Assert(pResp.StatusCode(), chk.Equals, http.StatusCreated)
//	c.Assert(pResp.IsServerEncrypted(), chk.NotNil)
//	c.Assert(pResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(pResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(pResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(pResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(pResp.Date().IsZero(), chk.Equals, false)
//
//	// Get with rangeGetContentMD5 enabled.
//	// Partial data, check status code 206.
//	resp, err := fileURL.Download(context.Background(), 0, 1024, true)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.StatusCode(), chk.Equals, http.StatusPartialContent)
//	c.Assert(resp.ContentLength(), chk.Equals, int64(1024))
//	c.Assert(resp.ContentMD5(), chk.Not(chk.Equals), [md5.Size]byte{})
//	c.Assert(resp.ContentType(), chk.Equals, "application/octet-stream")
//	c.Assert(resp.Status(), chk.Not(chk.Equals), "")
//
//	download, err := ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(download, chk.DeepEquals, contentD[:1024])
//
//	// Set ContentMD5 for the entire file.
//	_, err = fileURL.SetHTTPHeaders(context.Background(), azfile.FileHTTPHeaders{ContentMD5: pResp.ContentMD5(), ContentLanguage: "test"})
//	c.Assert(err, chk.IsNil)
//
//	// Test get with another type of range index, and validate if FileContentMD5 can be get correclty.
//	resp, err = fileURL.Download(context.Background(), 1024, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.StatusCode(), chk.Equals, http.StatusPartialContent)
//	c.Assert(resp.ContentLength(), chk.Equals, int64(1024))
//	c.Assert(resp.ContentMD5(), chk.Equals, [md5.Size]byte{})
//	c.Assert(resp.FileContentMD5(), chk.DeepEquals, pResp.ContentMD5())
//	c.Assert(resp.ContentLanguage(), chk.Equals, "test")
//	// Note: when it's downloading range, range's MD5 is returned, when set rangeGetContentMD5=true, currently set it to false, so should be empty
//	c.Assert(resp.NewHTTPHeaders(), chk.DeepEquals, azfile.FileHTTPHeaders{ContentMD5: [md5.Size]byte{}, ContentLanguage: "test"})
//
//	download, err = ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(download, chk.DeepEquals, contentD[1024:])
//
//	c.Assert(resp.AcceptRanges(), chk.Equals, "bytes")
//	c.Assert(resp.CacheControl(), chk.Equals, "")
//	c.Assert(resp.ContentDisposition(), chk.Equals, "")
//	c.Assert(resp.ContentEncoding(), chk.Equals, "")
//	c.Assert(resp.ContentRange(), chk.Equals, "bytes 1024-2047/2048")
//	c.Assert(resp.ContentType(), chk.Equals, "") // Note ContentType is set to empty during SetHTTPHeaders
//	c.Assert(resp.CopyCompletionTime().IsZero(), chk.Equals, true)
//	c.Assert(resp.CopyID(), chk.Equals, "")
//	c.Assert(resp.CopyProgress(), chk.Equals, "")
//	c.Assert(resp.CopySource(), chk.Equals, "")
//	c.Assert(resp.CopyStatus(), chk.Equals, azfile.CopyStatusNone)
//	c.Assert(resp.CopyStatusDescription(), chk.Equals, "")
//	c.Assert(resp.Date().IsZero(), chk.Equals, false)
//	c.Assert(resp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(resp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(resp.NewMetadata(), chk.DeepEquals, azfile.Metadata{})
//	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(resp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(resp.IsServerEncrypted(), chk.NotNil)
//
//	// Get entire fileURL, check status code 200.
//	resp, err = fileURL.Download(context.Background(), 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.StatusCode(), chk.Equals, http.StatusOK)
//	c.Assert(resp.ContentLength(), chk.Equals, int64(2048))
//	c.Assert(resp.ContentMD5(), chk.Equals, pResp.ContentMD5())   // Note: This case is inted to get entire fileURL, entire file's MD5 will be returned.
//	c.Assert(resp.FileContentMD5(), chk.Equals, [md5.Size]byte{}) // Note: FileContentMD5 is returned, only when range is specified explicitly.
//
//	download, err = ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(download, chk.DeepEquals, contentD[:])
//
//	c.Assert(resp.AcceptRanges(), chk.Equals, "bytes")
//	c.Assert(resp.CacheControl(), chk.Equals, "")
//	c.Assert(resp.ContentDisposition(), chk.Equals, "")
//	c.Assert(resp.ContentEncoding(), chk.Equals, "")
//	c.Assert(resp.ContentRange(), chk.Equals, "") // Note: ContentRange is returned, only when range is specified explicitly.
//	c.Assert(resp.ContentType(), chk.Equals, "")
//	c.Assert(resp.CopyCompletionTime().IsZero(), chk.Equals, true)
//	c.Assert(resp.CopyID(), chk.Equals, "")
//	c.Assert(resp.CopyProgress(), chk.Equals, "")
//	c.Assert(resp.CopySource(), chk.Equals, "")
//	c.Assert(resp.CopyStatus(), chk.Equals, azfile.CopyStatusNone)
//	c.Assert(resp.CopyStatusDescription(), chk.Equals, "")
//	c.Assert(resp.Date().IsZero(), chk.Equals, false)
//	c.Assert(resp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(resp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(resp.NewMetadata(), chk.DeepEquals, azfile.Metadata{})
//	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(resp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(resp.IsServerEncrypted(), chk.NotNil)
//}
//
//func (s *FileURLSuite) TestFileDownloadDataNonExistantFile(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	validateStorageError(c, err, azfile.ServiceCodeResourceNotFound)
//}
//
//func (s *FileURLSuite) TestFileDownloadDataNegativeOffset(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	defer func() { // The library should fail if it seems numeric parameters that are guaranteed invalid
//		recover()
//	}()
//
//	fileURL.Download(ctx, -1, azfile.CountToEnd, false)
//
//	c.Fail()
//}
//
//func (s *FileURLSuite) TestFileDownloadDataOffsetOutOfRange(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	_, err := fileURL.Download(ctx, int64(len(fileDefaultData)), azfile.CountToEnd, false)
//	validateStorageError(c, err, azfile.ServiceCodeInvalidRange)
//}
//
//func (s *FileURLSuite) TestFileDownloadDataInvalidCount(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	defer func() { // The library should panic if it sees numeric parameters that are guaranteed invalid
//		recover()
//	}()
//
//	fileURL.Download(ctx, 0, -100, false)
//
//	c.Fail()
//}
//
//func (s *FileURLSuite) TestFileDownloadDataEntireFile(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShareWithDefaultData(c, shareURL)
//
//	resp, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//
//	// Specifying a count of 0 results in the value being ignored
//	data, err := ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(string(data), chk.Equals, fileDefaultData)
//}
//
//func (s *FileURLSuite) TestFileDownloadDataCountExact(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShareWithDefaultData(c, shareURL)
//
//	resp, err := fileURL.Download(ctx, 0, int64(len(fileDefaultData)), false)
//	c.Assert(err, chk.IsNil)
//
//	data, err := ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(string(data), chk.Equals, fileDefaultData)
//}
//
//func (s *FileURLSuite) TestFileDownloadDataCountOutOfRange(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShareWithDefaultData(c, shareURL)
//
//	resp, err := fileURL.Download(ctx, 0, int64(len(fileDefaultData))*2, false)
//	c.Assert(err, chk.IsNil)
//
//	data, err := ioutil.ReadAll(resp.Response().Body)
//	c.Assert(err, chk.IsNil)
//	c.Assert(string(data), chk.Equals, fileDefaultData)
//}
//
//func (s *FileURLSuite) TestFileUploadRangeNegativeInvalidOffset(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	c.Assert(func() { fileURL.UploadRange(ctx, -2, strings.NewReader(fileDefaultData)) }, chk.Panics, "offset must be >= 0")
//}
//
//func (s *FileURLSuite) TestFileUploadRangeNilBody(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	// A page range that starts and ends at 0 should panic
//	defer func() {
//		recover()
//	}()
//
//	fileURL.UploadRange(ctx, 0, nil)
//	c.Fail()
//}
//
//func (s *FileURLSuite) TestFileUploadRangeEmptyBody(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	c.Assert(func() { fileURL.UploadRange(ctx, 0, bytes.NewReader([]byte{})) }, chk.Panics, "body must contain readable data whose size is > 0")
//}
//
//func (s *FileURLSuite) TestFileUploadRangeNonExistantFile(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	_, err := fileURL.UploadRange(ctx, 0, getReaderToRandomBytes(12))
//	validateStorageError(c, err, azfile.ServiceCodeResourceNotFound)
//}
//
//// Testings for GetRangeList and ClearRange
//func (s *FileURLSuite) TestGetRangeListNonDefaultExact(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	fileSize := int64(512 * 10)
//
//	fileURL.Create(context.Background(), fileSize, azfile.FileHTTPHeaders{}, nil)
//
//	defer delFile(c, fileURL)
//
//	putResp, err := fileURL.UploadRange(context.Background(), 0, getReaderToRandomBytes(1024))
//	c.Assert(err, chk.IsNil)
//	c.Assert(putResp.Response().StatusCode, chk.Equals, 201)
//	c.Assert(putResp.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(putResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(putResp.ContentMD5(), chk.Not(chk.Equals), "")
//	c.Assert(putResp.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(putResp.Version(), chk.Not(chk.Equals), "")
//	c.Assert(putResp.Date().IsZero(), chk.Equals, false)
//
//	rangeList, err := fileURL.GetRangeList(context.Background(), 0, 1023)
//	c.Assert(err, chk.IsNil)
//	c.Assert(rangeList.Response().StatusCode, chk.Equals, 200)
//	c.Assert(rangeList.LastModified().IsZero(), chk.Equals, false)
//	c.Assert(rangeList.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
//	c.Assert(rangeList.FileContentLength(), chk.Equals, fileSize)
//	c.Assert(rangeList.RequestID(), chk.Not(chk.Equals), "")
//	c.Assert(rangeList.Version(), chk.Not(chk.Equals), "")
//	c.Assert(rangeList.Date().IsZero(), chk.Equals, false)
//	c.Assert(rangeList.Value, chk.HasLen, 1)
//	c.Assert(rangeList.Value[0], chk.DeepEquals, azfile.Range{Start: 0, End: 1022})
//}
//
//// Default means clear the entire file's range
//func (s *FileURLSuite) TestClearRangeDefault(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 2048)
//	defer delFile(c, fileURL)
//
//	_, err := fileURL.UploadRange(context.Background(), 0, getReaderToRandomBytes(2048))
//	c.Assert(err, chk.IsNil)
//
//	clearResp, err := fileURL.ClearRange(context.Background(), 0, 2048)
//	c.Assert(err, chk.IsNil)
//	c.Assert(clearResp.Response().StatusCode, chk.Equals, 201)
//
//	rangeList, err := fileURL.GetRangeList(context.Background(), 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	c.Assert(rangeList.Value, chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestClearRangeNonDefault(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 4096)
//	defer delFile(c, fileURL)
//
//	_, err := fileURL.UploadRange(context.Background(), 2048, getReaderToRandomBytes(2048))
//	c.Assert(err, chk.IsNil)
//
//	clearResp, err := fileURL.ClearRange(context.Background(), 2048, 2048)
//	c.Assert(err, chk.IsNil)
//	c.Assert(clearResp.Response().StatusCode, chk.Equals, 201)
//
//	rangeList, err := fileURL.GetRangeList(context.Background(), 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	c.Assert(rangeList.Value, chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestClearRangeMultipleRanges(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 2048)
//	defer delFile(c, fileURL)
//
//	_, err := fileURL.UploadRange(context.Background(), 0, getReaderToRandomBytes(2048))
//	c.Assert(err, chk.IsNil)
//
//	clearResp, err := fileURL.ClearRange(context.Background(), 1024, 1024)
//	c.Assert(err, chk.IsNil)
//	c.Assert(clearResp.Response().StatusCode, chk.Equals, 201)
//
//	rangeList, err := fileURL.GetRangeList(context.Background(), 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	c.Assert(rangeList.Value, chk.HasLen, 1)
//	c.Assert(rangeList.Value[0], chk.DeepEquals, azfile.Range{Start: 0, End: 1023})
//}
//
//// When not 512 aligned, clear range will set 0 the non-512 aligned range, and will not eliminate the range.
//func (s *FileURLSuite) TestClearRangeNonDefault1Count(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	fileURL, _ := createNewFileFromShare(c, shareURL, 1)
//	defer delFile(c, fileURL)
//
//	d := []byte{1}
//	_, err := fileURL.UploadRange(context.Background(), 0, bytes.NewReader(d))
//	c.Assert(err, chk.IsNil)
//
//	clearResp, err := fileURL.ClearRange(context.Background(), 0, 1)
//	c.Assert(err, chk.IsNil)
//	c.Assert(clearResp.Response().StatusCode, chk.Equals, 201)
//
//	rangeList, err := fileURL.GetRangeList(context.Background(), 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	c.Assert(rangeList.Value, chk.HasLen, 1)
//	c.Assert(rangeList.Value[0], chk.DeepEquals, azfile.Range{Start: 0, End: 0})
//
//	dResp, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
//	c.Assert(err, chk.IsNil)
//	bytes, err := ioutil.ReadAll(dResp.Body(azfile.RetryReaderOptions{}))
//	c.Assert(err, chk.IsNil)
//	c.Assert(bytes, chk.DeepEquals, []byte{0})
//}
//
//func (s *FileURLSuite) TestFileClearRangeNegativeInvalidOffset(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := getShareURL(c, fsu)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	c.Assert(func() { fileURL.ClearRange(ctx, -1, 1) }, chk.Panics, "offset must be >= 0")
//}
//
//func (s *FileURLSuite) TestFileClearRangeNegativeInvalidCount(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := getShareURL(c, fsu)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	c.Assert(func() { fileURL.ClearRange(ctx, 0, 0) }, chk.Panics, "count cannot be CountToEnd, and must be > 0")
//}
//
//func setupGetRangeListTest(c *chk.C) (shareURL azfile.ShareURL, fileURL azfile.FileURL) {
//	fsu := getFSU()
//	shareURL, _ = createNewFileSystem(c, fsu)
//	fileURL, _ = createNewFileFromShare(c, shareURL, int64(testFileRangeSize))
//
//	_, err := fileURL.UploadRange(ctx, 0, getReaderToRandomBytes(testFileRangeSize))
//	c.Assert(err, chk.IsNil)
//
//	return
//}
//
//func validateBasicGetRangeList(c *chk.C, resp *azfile.Ranges, err error) {
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.Value, chk.HasLen, 1)
//	c.Assert(resp.Value[0], chk.Equals, azfile.Range{Start: 0, End: testFileRangeSize - 1})
//}
//
//func (s *FileURLSuite) TestFileGetRangeListDefaultEmptyFile(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := createNewFileSystem(c, fsu)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
//
//	resp, err := fileURL.GetRangeList(ctx, 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.Value, chk.HasLen, 0)
//}
//
//func (s *FileURLSuite) TestFileGetRangeListDefault1Range(c *chk.C) {
//	shareURL, fileURL := setupGetRangeListTest(c)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	resp, err := fileURL.GetRangeList(ctx, 0, azfile.CountToEnd)
//	validateBasicGetRangeList(c, resp, err)
//}
//
//func (s *FileURLSuite) TestFileGetRangeListNonContiguousRanges(c *chk.C) {
//	shareURL, fileURL := setupGetRangeListTest(c)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	_, err := fileURL.Resize(ctx, int64(testFileRangeSize*3))
//	c.Assert(err, chk.IsNil)
//
//	_, err = fileURL.UploadRange(ctx, testFileRangeSize*2, getReaderToRandomBytes(testFileRangeSize))
//	c.Assert(err, chk.IsNil)
//	resp, err := fileURL.GetRangeList(ctx, 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.Value, chk.HasLen, 2)
//	c.Assert(resp.Value[0], chk.Equals, azfile.Range{Start: 0, End: testFileRangeSize - 1})
//	c.Assert(resp.Value[1], chk.Equals, azfile.Range{Start: testFileRangeSize * 2, End: (testFileRangeSize * 3) - 1})
//}
//
//func (s *FileURLSuite) TestFileGetRangeListNonContiguousRangesCountLess(c *chk.C) {
//	shareURL, fileURL := setupGetRangeListTest(c)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	resp, err := fileURL.GetRangeList(ctx, 0, testFileRangeSize-1)
//	c.Assert(err, chk.IsNil)
//	c.Assert(resp.Value, chk.HasLen, 1)
//	c.Assert(resp.Value[0], chk.Equals, azfile.Range{Start: 0, End: testFileRangeSize - 2})
//}
//
//func (s *FileURLSuite) TestFileGetRangeListNonContiguousRangesCountExceed(c *chk.C) {
//	shareURL, fileURL := setupGetRangeListTest(c)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
//
//	resp, err := fileURL.GetRangeList(ctx, 0, testFileRangeSize+1)
//	c.Assert(err, chk.IsNil)
//	validateBasicGetRangeList(c, resp, err)
//}
//
//func (s *FileURLSuite) TestFileGetRangeListSnapshot(c *chk.C) {
//	shareURL, fileURL := setupGetRangeListTest(c)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionInclude)
//
//	resp, _ := shareURL.CreateSnapshot(ctx, azfile.Metadata{})
//	snapshotURL := fileURL.WithSnapshot(resp.Snapshot())
//	resp2, err := snapshotURL.GetRangeList(ctx, 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	validateBasicGetRangeList(c, resp2, err)
//}
//
//func (s *FileURLSuite) TestFileGetRangeListNegativeInvalidOffset(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := getShareURL(c, fsu)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	c.Assert(func() { fileURL.GetRangeList(ctx, -2, 500) }, chk.Panics, "The range offset must be >= 0")
//}
//
//func (s *FileURLSuite) TestFileGetRangeListNegativeInvalidCount(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := getShareURL(c, fsu)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	c.Assert(func() { fileURL.GetRangeList(ctx, 0, -3) }, chk.Panics, "The range count must be either equal to CountToEnd (0) or > 0")
//}

//	shareURL, fileURL := setupGetRangeListTest(c)
//	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionInclude)
//
//	resp, _ := shareURL.CreateSnapshot(ctx, azfile.Metadata{})
//	snapshotURL := fileURL.WithSnapshot(resp.Snapshot())
//	resp2, err := snapshotURL.GetRangeList(ctx, 0, azfile.CountToEnd)
//	c.Assert(err, chk.IsNil)
//	validateBasicGetRangeList(c, resp2, err)
//}
//
//func (s *FileURLSuite) TestFileGetRangeListNegativeInvalidOffset(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := getShareURL(c, fsu)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	c.Assert(func() { fileURL.GetRangeList(ctx, -2, 500) }, chk.Panics, "The range offset must be >= 0")
//}
//
//func (s *FileURLSuite) TestFileGetRangeListNegativeInvalidCount(c *chk.C) {
//	fsu := getFSU()
//	shareURL, _ := getShareURL(c, fsu)
//	fileURL, _ := getFileURLFromShare(c, shareURL)
//
//	c.Assert(func() { fileURL.GetRangeList(ctx, 0, -3) }, chk.Panics, "The range count must be either equal to CountToEnd (0) or > 0")
//}
