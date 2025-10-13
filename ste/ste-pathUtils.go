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
	"os"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/stretchr/testify/assert"
)

var ctxSender = context.Background()

const (
	BlockBlobDefaultData = "AzCopy Random Test Data"
)

// get blob account service client
func GetBlobServiceClient() *blobservice.Client {
	accountName, accountKey := GetAccountAndKey()
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

func GetAccountAndKey() (string, string) {
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	if name == "" || key == "" {
		panic("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before running tests")
	}

	return name, key
}

func CreateNewContainer(t *testing.T, a *assert.Assertions, bsc *blobservice.Client) (cc *container.Client, name string) {
	cc, name = GetContainerClient(t, bsc)

	_, err := cc.Create(ctxSender, nil)
	a.NoError(err)
	return cc, name
}

func GetContainerClient(t *testing.T, bsc *blobservice.Client) (container *container.Client, name string) {
	name = strings.ToLower(t.Name())
	container = bsc.NewContainerClient(name)
	return
}

func DeleteContainer(a *assert.Assertions, cc *container.Client) {
	_, err := cc.Delete(ctxSender, nil)
	a.NoError(err)
}

func GenerateBlockIDsList(count int) []string {
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
