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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/stretchr/testify/assert"
	"strings"
	"sync"
	"testing"
)

func TestGetVerifiedChunkParams(t *testing.T) {
	a := assert.New(t)
	// Mock required params
	transferInfo := TransferInfo{
		BlockSize:  4195352576, // 4001MiB
		Source:     "tmpSrc",
		SourceSize: 8389656576, // 8001MiB
	}

	//Verify memory limit
	memLimit := int64(2097152000) // 2000Mib
	expectedErr := "Cannot use a block size of 3.91GiB. AzCopy is limited to use only 1.95GiB of memory"
	_, _, err := getVerifiedChunkParams(transferInfo, memLimit, memLimit)
	a.Equal(expectedErr, err.Error())

	// Verify large block Size
	memLimit = int64(8388608000) // 8000MiB
	expectedErr = "block size of 3.91GiB for file tmpSrc of size 7.81GiB exceeds maximum allowed block size for a BlockBlob"
	_, _, err = getVerifiedChunkParams(transferInfo, memLimit, memLimit)
	a.Equal(expectedErr, err.Error())

	// High block count
	transferInfo.SourceSize = 2147483648 //16GiB
	transferInfo.BlockSize = 2048        // 2KiB
	expectedErr = "Block size 2048 for source of size 2147483648 is not correct. Number of blocks will exceed the limit"
	_, _, err = getVerifiedChunkParams(transferInfo, memLimit, memLimit)
	a.Equal(expectedErr, err.Error())
}

func TestDeleteDstBlob(t *testing.T) {
	a := assert.New(t)
	bsc := GetBlobServiceClient()
	dstContainerClient, _ := CreateNewContainer(t, a, bsc)
	defer DeleteContainer(a, dstContainerClient)

	// set up the destination container with a single blob with uncommitted block
	dstBlobClient := dstContainerClient.NewBlockBlobClient("foo")
	blockIDs := GenerateBlockIDsList(1)
	_, err := dstBlobClient.StageBlock(ctxSender, blockIDs[0], streaming.NopCloser(strings.NewReader(BlockBlobDefaultData)), nil)
	a.NoError(err)
	_, err = dstBlobClient.CommitBlockList(ctxSender, blockIDs, nil)
	a.NoError(err)
	_, err = dstBlobClient.StageBlock(ctxSender, "0001", streaming.NopCloser(strings.NewReader(BlockBlobDefaultData)), nil)
	a.NoError(err)

	// check if dst blob was set up with one uncommitted block
	resp, err := dstBlobClient.GetBlockList(ctxSender, blockblob.BlockListTypeUncommitted, nil)
	a.NoError(err)
	a.Equal(len(resp.UncommittedBlocks), 1)

	// set up job part manager
	jptm := jobPartTransferMgr{
		jobPartMgr: &jobPartMgr{},
		ctx:        ctxSender,
	}

	bbSender := &blockBlobSenderBase{
		jptm:                               &jptm,
		destBlockBlobClient:                dstBlobClient,
		deleteDstBlobWithUncommittedBlocks: &sync.Once{},
	}

	bbSender.DeleteDstBlob()

	// check if dst blob was deleted
	_, err = dstBlobClient.GetProperties(ctxSender, nil)
	a.Error(err)
	a.True(bloberror.HasCode(err, bloberror.BlobNotFound))
}
