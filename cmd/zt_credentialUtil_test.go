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

package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

/*
 * This function tests that common.isPublic routine is works fine.
 * Two cases are considered, a blob is public or a container is public.
 */
func TestIsPublic(t *testing.T) {
	// TODO: Migrate this test to mocked UT.
	t.Skip("Public access is sometimes turned off due to organization policy. This test should ideally be migrated to a mocked UT.")

	a := assert.New(t)
	ctx, _ := context.WithTimeout(context.TODO(), 5*time.Minute)
	bsc := getBlobServiceClient()
	ctr, _ := getContainerClient(a, bsc)
	defer ctr.Delete(ctx, nil)

	publicAccess := container.PublicAccessTypeContainer

	// Create a public container
	_, err := ctr.Create(ctx, &container.CreateOptions{Access: &publicAccess})
	a.Nil(err)

	// verify that container is public
	a.True(isPublic(ctx, ctr.URL(), common.CpkOptions{}))

	publicAccess = container.PublicAccessTypeBlob
	_, err = ctr.SetAccessPolicy(ctx, &container.SetAccessPolicyOptions{Access: &publicAccess})
	a.Nil(err)

	// Verify that blob is public.
	bb, _ := getBlockBlobClient(a, ctr, "")
	_, err = bb.UploadBuffer(ctx, []byte("I'm a block blob."), nil)
	a.Nil(err)

	a.True(isPublic(ctx, bb.URL(), common.CpkOptions{}))

}
