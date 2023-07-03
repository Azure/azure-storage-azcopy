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
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIsSourceDirWithStub(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// Generate source container and blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)

	dirName := "source_dir"
	createNewDirectoryStub(a, containerURL, dirName)
	// set up to create blob traverser
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, dirName)
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	isDir, err := blobTraverser.IsDirectory(true)
	a.True(isDir)
	a.Nil(err)
}

func TestIsSourceDirWithNoStub(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// Generate source container and blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)

	dirName := "source_dir/"
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, dirName)
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	isDir, err := blobTraverser.IsDirectory(true)
	a.True(isDir)
	a.Nil(err)
}

func TestIsDestDirWithBlobEP(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// Generate source container and blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)

	dirName := "dest_dir/"
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, dirName)
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	isDir, err := blobTraverser.IsDirectory(false)
	a.True(isDir)
	a.Nil(err)

	//===========================================================
	dirName = "dest_file"
	// List
	rawBlobURLWithSAS = scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, dirName)
	blobTraverser = newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	isDir, err = blobTraverser.IsDirectory(false)
	a.False(isDir)
	a.Nil(err)
}

func TestIsDestDirWithDFSEP(t *testing.T) {
	a := assert.New(t)
	bfsu := GetBFSSU()

	// Generate source container and blobs
	fileSystemURL, fileSystemName := createNewFilesystem(a, bfsu)
	defer deleteFilesystem(a, fileSystemURL)
	a.NotNil(fileSystemURL)

	parentDirName := "dest_dir"
	parentDirURL := fileSystemURL.NewDirectoryURL(parentDirName)
	_, err := parentDirURL.Create(ctx, true)
	a.Nil(err)

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, fileSystemName, parentDirName)
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), true)

	// a directory with name parentDirName exists on target. So irrespective of 
	// isSource, IsDirectory()  should return true.
	isDir, err := blobTraverser.IsDirectory(true)
	a.True(isDir)
	a.Nil(err)

	isDir, err = blobTraverser.IsDirectory(false)
	a.True(isDir)
	a.Nil(err)

	//===================================================================//

	// With a directory that does not exist, without path separator.
	parentDirName = "dirDoesNotExist"
	rawBlobURLWithSAS = scenarioHelper{}.getRawBlobURLWithSAS(a, fileSystemName, parentDirName)
	blobTraverser = newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), true)

	// The directory does not exist, so IsDirectory()
	// should return false, in all cases
	isDir, err = blobTraverser.IsDirectory(true)
	a.False(isDir)
	a.Nil(err)

	isDir, err = blobTraverser.IsDirectory(false)
	a.False(isDir)
	a.Nil(err)

	//===================================================================//

	// With a directory that does not exist, with path separator
	parentDirNameWithSeparator := "dirDoesNotExist\\"
	rawBlobURLWithSAS = scenarioHelper{}.getRawBlobURLWithSAS(a, fileSystemName, parentDirNameWithSeparator)
	blobTraverser = newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), true)

	// The directory does not exist, but with a path separator
	// we should identify it as a directory.
	isDir, err = blobTraverser.IsDirectory(true)
	a.True(isDir)
	a.Nil(err)

	isDir, err = blobTraverser.IsDirectory(false)
	a.True(isDir)
	a.Nil(err)

}

func TestIsSourceFileExists(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// Generate source container and blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)

	fileName := "source_file"
	_, fileName = createNewBlockBlob(a, containerURL, fileName)

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, fileName)
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	isDir, err := blobTraverser.IsDirectory(true)
	a.False(isDir)
	a.Nil(err)
}

func TestIsSourceFileDoesNotExist(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// Generate source container and blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)

	fileName := "file_does_not_exist"
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, fileName)
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	isDir, err := blobTraverser.IsDirectory(true)
	a.False(isDir)
	a.Equal(common.FILE_NOT_FOUND, err.Error())
}