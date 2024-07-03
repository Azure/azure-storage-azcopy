package ste

import (
	"encoding/xml"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/mock_server"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func SetUpVariables() (AccountName, RawURL, ContainerName, BlobName string) {
	accountName := "myfakeaccount"
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	cName := generateContainerName()
	bName := generateBlobName()
	return accountName, rawURL, cName, bName
}

// This test calls GetFreshFileLastModifiedTime on the BlobSourceInfoProvider and successfully returns the last modified properties time
func TestGetFreshFile(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	_, rawURL, cName, bName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  bName,
		}),
		fromTo:    common.EFromTo.BlobBlob(),
		transport: srv,
	}
	blobSIP, err := newBlobSourceInfoProvider(&jptm)
	a.Nil(err)

	// mock last modified time response and successful status code
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("Last-Modified", "Tue, 25 Jun 2024 21:28:05 GMT"))

	LastModified, err := blobSIP.GetFreshFileLastModifiedTime()
	a.Nil(err)
	// check to make sure the last modified time was properly set and is not the default time.Time value
	a.NotEqual(LastModified, time.Time{})
}

func MockErrorBody(message string) string {
	return "<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>" + message + "</Code><Message> </Message></Error>"
}

// This test calls GetFreshFileLastModifiedTime on the BlobSourceInfoProvider and returns an error, and a default last modified time value
func TestGetFreshFileError(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	_, rawURL, cName, bName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  bName,
		}),
		fromTo:    common.EFromTo.BlobBlob(),
		transport: srv,
	}
	blobSIP, err := newBlobSourceInfoProvider(&jptm)
	a.Nil(err)

	// mock error response and body
	srv.AppendResponse(mock_server.WithStatusCode(404), mock_server.WithHeader("x-ms-error-code", "BlobNotFound"), mock_server.WithBody([]byte(MockErrorBody("BlobNotFound"))))

	LastModified, err := blobSIP.GetFreshFileLastModifiedTime()
	a.NotNil(err)
	//check that last modified time is set to default time.Time due to error
	a.Equal(LastModified, time.Time{})
}

func SetBodyWithBlob(accounturl string, containername string) string {
	testmetadata := map[string]*string{
		"key1": to.Ptr("value1"),
	}
	blobProp := container.BlobProperties{ContentLength: to.Ptr(int64(2))}
	blobitem := []*container.BlobItem{to.Ptr(container.BlobItem{Name: to.Ptr("blob-name"), Properties: to.Ptr(blobProp), Metadata: testmetadata})}
	segment := container.BlobFlatListSegment{BlobItems: blobitem}
	resp := container.ListBlobsFlatSegmentResponse{ContainerName: to.Ptr(containername), Segment: to.Ptr(segment), ServiceEndpoint: to.Ptr(accounturl), MaxResults: to.Ptr(int32(1))}
	out, err := xml.Marshal(resp)
	if err != nil {
		fmt.Printf(err.Error())
	}
	body := string(out)
	newbody := strings.Replace(body, "ListBlobsFlatSegmentResponse", "EnumerationResults", -1)
	return newbody
}

// This test calls ReadLink which successfully mocks a download call and returns the full blob content
func TestReadLink(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, bName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  bName,
		}),
		fromTo:    common.EFromTo.BlobBlob(),
		transport: srv,
	}

	// mock status code for full blob read (200) and the get blob response body to contain the content of the blob
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetBodyWithBlob(rawURL, cName))))

	blobSIP, err := newBlobSourceInfoProvider(&jptm)
	symsip := blobSIP.(ISymlinkBearingSourceInfoProvider)
	a.Nil(err)
	symlinkInfo, err := symsip.ReadLink()
	a.Nil(err)
	// check that the ReadLink response matches the blob content
	a.Equal(symlinkInfo, SetBodyWithBlob(rawURL, cName))
}

// This test calls ReadLink which successfully mocks a download call to read a specified range (Partial Content)
func TestReadLinkPartialContent(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, bName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  bName,
		}),
		fromTo:    common.EFromTo.BlobBlob(),
		transport: srv,
	}

	// mock status code for partial blob read (206) and the partial blob response body to contain the content of the blob in the specified range
	srv.AppendResponse(mock_server.WithStatusCode(206), mock_server.WithHeader("Content-Range", "0-20"), mock_server.WithBody([]byte(SetBodyWithBlob(rawURL, cName))[0:20]))

	blobSIP, err := newBlobSourceInfoProvider(&jptm)
	symsip := blobSIP.(ISymlinkBearingSourceInfoProvider)
	a.Nil(err)
	symlinkInfo, err := symsip.ReadLink()
	a.Nil(err)
	// check that the ReadLink response only returns the portion of the blob specified by the bytes in content-range
	a.Equal(symlinkInfo, "<EnumerationResults ")
}

// This test calls ReadLink and mocks an error where no blob info is returned
func TestReadLinkError(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, bName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  bName,
		}),
		fromTo:    common.EFromTo.BlobBlob(),
		transport: srv,
	}

	// mock error response and body
	srv.AppendResponse(mock_server.WithStatusCode(404), mock_server.WithHeader("x-ms-error-code", "BlobNotFound"), mock_server.WithBody([]byte(MockErrorBody("BlobNotFound"))))

	blobSIP, err := newBlobSourceInfoProvider(&jptm)
	symsip := blobSIP.(ISymlinkBearingSourceInfoProvider)
	a.Nil(err)
	symlinkInfo, err := symsip.ReadLink()
	a.NotNil(err)
	// check that returned blob content is empty
	a.Equal(symlinkInfo, "")
}

// This test calls Access Control which successfully returns the access control entry in format [scope]:[type]:[id]:[permissions]
func TestAccessControl(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, bName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  bName,
		}),
		fromTo:    common.EFromTo.BlobBlob(),
		transport: srv,
	}
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-acl", "default:user:id:rrr"))

	blobSIP, err := newBlobSourceInfoProvider(&jptm)
	respACL, err := blobSIP.(*blobSourceInfoProvider).AccessControl()
	a.Nil(err)
	// check that the correct access control information is returned through the access control get properties call
	a.Equal(*respACL, "default:user:id:rrr")
}

// This test calls Access Control and returns an error from the get properties call and no access control information
func TestAccessControlError(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	_, rawURL, cName, bName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  bName,
		}),
		fromTo:    common.EFromTo.BlobBlob(),
		transport: srv,
	}
	srv.AppendResponse(mock_server.WithStatusCode(404), mock_server.WithHeader("x-ms-error-code", "BlobNotFound"), mock_server.WithBody([]byte(MockErrorBody("BlobNotFound"))))

	blobSIP, err := newBlobSourceInfoProvider(&jptm)
	respACL, err := blobSIP.(*blobSourceInfoProvider).AccessControl()
	a.NotNil(err)
	a.Nil(respACL)
}
