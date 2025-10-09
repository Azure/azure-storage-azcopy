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
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"
)

// ============================================= BLOB TRAVERSER TESTS =======================================
func TestValidateSourceDirThatExists(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	a.NotNil(cc)

	dirName := "source_dir"
	createNewDirectoryStub(a, cc, dirName)
	// set up to create blob traverser
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, containerName, dirName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawBlobURLWithSAS)
	blobTraverser := traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	})

	// dir but recursive flag not set - fail
	cca := CookedCopyCmdArgs{StripTopDir: false, Recursive: false}
	err := cca.validateSourceDir(blobTraverser)
	a.Equal("cannot use directory as source without --recursive or a trailing wildcard (/*)", err.Error())

	// dir but recursive flag set - pass
	cca.Recursive = true
	err = cca.validateSourceDir(blobTraverser)
	a.Nil(err)
	a.True(cca.IsSourceDir)
}

func TestValidateSourceDirDoesNotExist(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	a.NotNil(cc)

	dirName := "source_dir/"
	// set up to create blob traverser
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, containerName, dirName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawBlobURLWithSAS)
	blobTraverser := traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	})

	// dir but recursive flag not set - fail
	cca := CookedCopyCmdArgs{StripTopDir: false, Recursive: false}
	err := cca.validateSourceDir(blobTraverser)
	a.Equal("cannot use directory as source without --recursive or a trailing wildcard (/*)", err.Error())

	// dir but recursive flag set - pass
	cca.Recursive = true
	err = cca.validateSourceDir(blobTraverser)
	a.Nil(err)
	a.True(cca.IsSourceDir)
}

func TestValidateSourceFileExists(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	a.NotNil(cc)

	fileName := "source_file"
	_, fileName = createNewBlockBlob(a, cc, fileName)

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, containerName, fileName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawBlobURLWithSAS)
	blobTraverser := traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{

		Recursive:             true,
		IncludeDirectoryStubs: true,
	})

	cca := CookedCopyCmdArgs{StripTopDir: false, Recursive: false}
	err := cca.validateSourceDir(blobTraverser)
	a.Nil(err)
	a.False(cca.IsSourceDir)
}

func TestValidateSourceFileDoesNotExist(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	a.NotNil(cc)

	fileName := "source_file"

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, containerName, fileName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawBlobURLWithSAS)
	blobTraverser := traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	})

	cca := CookedCopyCmdArgs{StripTopDir: false, Recursive: false}
	err := cca.validateSourceDir(blobTraverser)
	a.Equal(common.FILE_NOT_FOUND, err.Error())
	a.False(cca.IsSourceDir)
}

func TestValidateSourceWithWildCard(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	a.NotNil(cc)

	dirName := "source_dir_does_not_exist"
	// set up to create blob traverser
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, containerName, dirName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawBlobURLWithSAS)
	blobTraverser := traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	})

	// dir but recursive flag not set - fail
	cca := CookedCopyCmdArgs{StripTopDir: true, Recursive: false}
	err := cca.validateSourceDir(blobTraverser)
	a.Nil(err)
	a.False(cca.IsSourceDir)
}
