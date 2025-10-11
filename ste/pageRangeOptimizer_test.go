// Copyright © Microsoft <wastore@microsoft.com>
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

package ste

import (
	"context"
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/mock_server"
	"github.com/stretchr/testify/assert"
)

func TestRangeWorthTransferring(t *testing.T) {
	a := assert.New(t)
	// Arrange
	copier := pageRangeOptimizer{}
	copier.srcPageList = &pageblob.PageList{
		PageRange: []*pageblob.PageRange{
			{Start: to.Ptr(int64(512)), End: to.Ptr(int64(1023))},
			{Start: to.Ptr(int64(2560)), End: to.Ptr(int64(4095))},
			{Start: to.Ptr(int64(7168)), End: to.Ptr(int64(8191))},
		},
	}

	testCases := map[pageblob.PageRange]bool{
		{Start: to.Ptr(int64(512)), End: to.Ptr(int64(1023))}:    true,  // fully included
		{Start: to.Ptr(int64(2048)), End: to.Ptr(int64(3071))}:   true,  // overlapping
		{Start: to.Ptr(int64(3071)), End: to.Ptr(int64(4606))}:   true,  // overlapping
		{Start: to.Ptr(int64(0)), End: to.Ptr(int64(511))}:       false, // before all ranges
		{Start: to.Ptr(int64(1536)), End: to.Ptr(int64(2559))}:   false, // in between ranges
		{Start: to.Ptr(int64(15360)), End: to.Ptr(int64(15871))}: false, // all the way out
	}

	// Action & Assert
	for testRange, expectedResult := range testCases {
		doesContainData := copier.doesRangeContainData(testRange)
		a.Equal(expectedResult, doesContainData)
	}
}

func TestRangeWorthTransferringNil(t *testing.T) {
	a := assert.New(t)
	// Arrange
	copier := pageRangeOptimizer{}
	copier.srcPageList = nil

	testCases := map[pageblob.PageRange]bool{
		{Start: to.Ptr(int64(512)), End: to.Ptr(int64(1023))}:    true, // fully included
		{Start: to.Ptr(int64(2048)), End: to.Ptr(int64(3071))}:   true, // overlapping
		{Start: to.Ptr(int64(3071)), End: to.Ptr(int64(4606))}:   true, // overlapping
		{Start: to.Ptr(int64(0)), End: to.Ptr(int64(511))}:       true, // before all ranges
		{Start: to.Ptr(int64(1536)), End: to.Ptr(int64(2559))}:   true, // in between ranges
		{Start: to.Ptr(int64(15360)), End: to.Ptr(int64(15871))}: true, // all the way out
	}

	// Action & Assert
	for testRange, expectedResult := range testCases {
		doesContainData := copier.doesRangeContainData(testRange)
		a.Equal(expectedResult, doesContainData)
	}
}

func getPageRangesBody(pageList *pageblob.PageList) string {
	body := "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
	body += "<PageList>"
	for _, page := range pageList.PageRange {
		body += fmt.Sprintf("<PageRange><Start>%d</Start><End>%d</End></PageRange>", *page.Start, *page.End)
	}

	for _, page := range pageList.ClearRange {
		body += fmt.Sprintf("<ClearRange><Start>%d</Start><End>%d</End></ClearRange>", *page.Start, *page.End)
	}

	if pageList.NextMarker != nil {
		body += fmt.Sprintf("<NextMarker>%s</NextMarker>", *pageList.NextMarker)
	}

	body += "</PageList>"
	return body
}

func TestPageRangeOptimizerSinglePage(t *testing.T) {
	a := assert.New(t)

	// Setup
	// Mock the server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	pageList := &pageblob.PageList{
		PageRange:  []*pageblob.PageRange{{Start: to.Ptr(int64(0)), End: to.Ptr(int64(511))}, {Start: to.Ptr(int64(1024)), End: to.Ptr(int64(1535))}},
		ClearRange: []*pageblob.ClearRange{{Start: to.Ptr(int64(512)), End: to.Ptr(int64(1023))}},
	}
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(getPageRangesBody(pageList))))

	// Create a client
	// Note: the key below is not a secret, this is the publicly documented Azurite key
	accountName := "myfakeaccount"
	accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv,
			}})
	a.Nil(err)

	cName := generateContainerName()
	cc := client.NewContainerClient(cName)

	bName := generateBlobName()
	bc := cc.NewPageBlobClient(bName)

	pro := newPageRangeOptimizer(bc, context.Background())

	pro.fetchPages()

	a.NotNil(pro.srcPageList)
	a.Equal(2, len(pro.srcPageList.PageRange))
	a.Equal(1, len(pro.srcPageList.ClearRange))
	a.Equal(int64(0), *pro.srcPageList.PageRange[0].Start)
	a.Equal(int64(511), *pro.srcPageList.PageRange[0].End)
	a.Equal(int64(1024), *pro.srcPageList.PageRange[1].Start)
	a.Equal(int64(1535), *pro.srcPageList.PageRange[1].End)
	a.Equal(int64(512), *pro.srcPageList.ClearRange[0].Start)
	a.Equal(int64(1023), *pro.srcPageList.ClearRange[0].End)
	a.Equal(1, srv.Requests())
}

func TestPageRangeOptimizerSinglePageFail(t *testing.T) {
	a := assert.New(t)

	// Setup
	// Mock the server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	srv.AppendResponse(mock_server.WithStatusCode(500))

	// Create a client
	// Note: the key below is not a secret, this is the publicly documented Azurite key
	accountName := "myfakeaccount"
	accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv,
			}})
	a.Nil(err)

	cName := generateContainerName()
	cc := client.NewContainerClient(cName)

	bName := generateBlobName()
	bc := cc.NewPageBlobClient(bName)

	pro := newPageRangeOptimizer(bc, context.Background())

	pro.fetchPages()

	a.Nil(pro.srcPageList)
	a.Equal(1, srv.Requests()) // On failure, no retries should be made
}

func TestPageRangeOptimizerMultiplePages(t *testing.T) {
	a := assert.New(t)

	// Setup
	// Mock the server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	pageList1 := &pageblob.PageList{
		PageRange:  []*pageblob.PageRange{{Start: to.Ptr(int64(0)), End: to.Ptr(int64(511))}, {Start: to.Ptr(int64(1024)), End: to.Ptr(int64(1535))}},
		ClearRange: []*pageblob.ClearRange{{Start: to.Ptr(int64(512)), End: to.Ptr(int64(1023))}},
		NextMarker: to.Ptr("marker1"),
	}
	pageList2 := &pageblob.PageList{
		PageRange:  []*pageblob.PageRange{{Start: to.Ptr(int64(2048)), End: to.Ptr(int64(2559))}},
		ClearRange: []*pageblob.ClearRange{{Start: to.Ptr(int64(1536)), End: to.Ptr(int64(2047))}},
	}
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(getPageRangesBody(pageList1))))
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(getPageRangesBody(pageList2))))

	// Create a client
	// Note: the key below is not a secret, this is the publicly documented Azurite key
	accountName := "myfakeaccount"
	accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv,
			}})
	a.Nil(err)

	cName := generateContainerName()
	cc := client.NewContainerClient(cName)

	bName := generateBlobName()
	bc := cc.NewPageBlobClient(bName)

	pro := newPageRangeOptimizer(bc, context.Background())

	pro.fetchPages()

	a.NotNil(pro.srcPageList)
	a.Equal(3, len(pro.srcPageList.PageRange))
	a.Equal(2, len(pro.srcPageList.ClearRange))
	a.Equal(int64(0), *pro.srcPageList.PageRange[0].Start)
	a.Equal(int64(511), *pro.srcPageList.PageRange[0].End)
	a.Equal(int64(1024), *pro.srcPageList.PageRange[1].Start)
	a.Equal(int64(1535), *pro.srcPageList.PageRange[1].End)
	a.Equal(int64(512), *pro.srcPageList.ClearRange[0].Start)
	a.Equal(int64(1023), *pro.srcPageList.ClearRange[0].End)
	a.Equal(int64(1536), *pro.srcPageList.ClearRange[1].Start)
	a.Equal(int64(2047), *pro.srcPageList.ClearRange[1].End)
	a.Equal(int64(2048), *pro.srcPageList.PageRange[2].Start)
	a.Equal(int64(2559), *pro.srcPageList.PageRange[2].End)
	a.Equal(2, srv.Requests())
}

func TestPageRangeOptimizerMultiplePagesFail(t *testing.T) {
	a := assert.New(t)

	// Setup
	// Mock the server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	pageList1 := &pageblob.PageList{
		PageRange:  []*pageblob.PageRange{{Start: to.Ptr(int64(0)), End: to.Ptr(int64(511))}, {Start: to.Ptr(int64(1024)), End: to.Ptr(int64(1535))}},
		ClearRange: []*pageblob.ClearRange{{Start: to.Ptr(int64(512)), End: to.Ptr(int64(1023))}},
		NextMarker: to.Ptr("marker1"),
	}
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(getPageRangesBody(pageList1))))
	srv.AppendResponse(mock_server.WithStatusCode(500))

	// Create a client
	// Note: the key below is not a secret, this is the publicly documented Azurite key
	accountName := "myfakeaccount"
	accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv,
			}})
	a.Nil(err)

	cName := generateContainerName()
	cc := client.NewContainerClient(cName)

	bName := generateBlobName()
	bc := cc.NewPageBlobClient(bName)

	pro := newPageRangeOptimizer(bc, context.Background())

	pro.fetchPages()

	a.Nil(pro.srcPageList)
	a.Equal(2, srv.Requests()) // On failure, no retries should be made
}
