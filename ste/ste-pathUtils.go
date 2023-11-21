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

package ste

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
)

var ctx = context.Background()

const (
	blockBlobDefaultData = "AzCopy Random Test Data"
)

// get blob account service client
func getBlobServiceClient() *blobservice.Client {
	accountName, accountKey := getAccountAndKey()
	u := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}
	client, err := blobservice.NewClientWithSharedKeyCredential(u, credential, nil)
	if err != nil {
		panic(err)
	}
	return client
}

func getAccountAndKey() (string, string) {
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	if name == "" || key == "" {
		panic("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before running tests")
	}

	return name, key
}

func createNewContainer(t *testing.T, a *assert.Assertions, bsc *blobservice.Client) (cc *container.Client, name string) {
	cc, name = getContainerClient(t, bsc)

	_, err := cc.Create(ctx, nil)
	a.Nil(err)
	return cc, name
}

func getContainerClient(t *testing.T, bsc *blobservice.Client) (container *container.Client, name string) {
	name = strings.ToLower(t.Name())
	container = bsc.NewContainerClient(name)
	return
}

func deleteContainer(a *assert.Assertions, cc *container.Client) {
	_, err := cc.Delete(ctx, nil)
	a.Nil(err)
}

func generateBlockIDsList(count int) []string {
	blockIDs := make([]string, count)
	for i := 0; i < count; i++ {
		blockIDs[i] = BlockIDIntToBase64(i)
	}
	return blockIDs
}

// BlockIDIntToBase64 functions convert an int block ID to a base-64 string and vice versa
func BlockIDIntToBase64(blockID int) string {
	binaryBlockID := (&[4]byte{})[:]
	binary.LittleEndian.PutUint32(binaryBlockID, uint32(blockID))
	return base64.StdEncoding.EncodeToString(binaryBlockID)
}
