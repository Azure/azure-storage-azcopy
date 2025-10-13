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
	"testing"

	"github.com/stretchr/testify/assert"
)

func runMakeAndVerify(raw rawMakeCmdArgs, verifier func(err error)) {
	// the simulated user input should parse properly
	cooked, err := raw.cook()
	if err != nil {
		verifier(err)
		return
	}

	// the enumeration ends when process() returns
	err = cooked.process()

	// the err is passed to verified, which knows whether it is expected or not
	verifier(err)
}

func TestMakeBlobContainer(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, name := getContainerClient(a, bsc)
	defer deleteContainer(a, cc)

	bscSAS := scenarioHelper{}.getBlobServiceClientWithSAS(a)
	ccSAS := bscSAS.NewContainerClient(name)

	args := rawMakeCmdArgs{
		resourceToCreate: ccSAS.URL(),
	}

	runMakeAndVerify(args, func(err error) {
		a.NoError(err)
		_, err = cc.GetProperties(ctx, nil)
		a.NoError(err)
	})
}

func TestMakeBlobContainerExists(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, name := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	bscSAS := scenarioHelper{}.getBlobServiceClientWithSAS(a)
	ccSAS := bscSAS.NewContainerClient(name)

	args := rawMakeCmdArgs{
		resourceToCreate: ccSAS.URL(),
	}

	runMakeAndVerify(args, func(err error) {
		a.NotNil(err)
		a.Equal("the container already exists", err.Error())
		_, err = cc.GetProperties(ctx, nil)
		a.NoError(err)
	})
}

func TestMakeBlobFSFilesystem(t *testing.T) {
	a := assert.New(t)
	dsc := getDatalakeServiceClient()
	fsc, name := getFilesystemClient(a, dsc)
	defer deleteFilesystem(a, fsc)

	bscSAS := scenarioHelper{}.getDatalakeServiceClientWithSAS(a)
	ccSAS := bscSAS.NewFileSystemClient(name)

	args := rawMakeCmdArgs{
		resourceToCreate: ccSAS.DFSURL(),
	}

	runMakeAndVerify(args, func(err error) {
		a.NoError(err)
		_, err = fsc.GetProperties(ctx, nil)
		a.NoError(err)
	})
}

func TestMakeBlobFSFilesystemExists(t *testing.T) {
	a := assert.New(t)
	bsc := getDatalakeServiceClient()
	fsc, name := getFilesystemClient(a, bsc)
	_, err := fsc.Create(ctx, nil)
	a.NoError(err)
	defer deleteFilesystem(a, fsc)

	bscSAS := scenarioHelper{}.getDatalakeServiceClientWithSAS(a)
	ccSAS := bscSAS.NewFileSystemClient(name)

	args := rawMakeCmdArgs{
		resourceToCreate: ccSAS.DFSURL(),
	}

	runMakeAndVerify(args, func(err error) {
		a.NotNil(err)
		a.Equal("the filesystem already exists", err.Error())
		_, err = fsc.GetProperties(ctx, nil)
		a.NoError(err)
	})
}

func TestMakeFileShare(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	sc, name := getShareClient(a, fsc)
	defer deleteShare(a, sc)

	fscSAS := scenarioHelper{}.getRawFileServiceURLWithSAS(a)
	scSAS := fscSAS
	scSAS.Path = name

	args := rawMakeCmdArgs{
		resourceToCreate: scSAS.String(),
	}

	runMakeAndVerify(args, func(err error) {
		a.NoError(err)
		props, err := sc.GetProperties(ctx, nil)
		a.NoError(err)
		a.EqualValues(102400, *props.Quota)
	})
}

func TestMakeFileShareQuota(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	sc, name := getShareClient(a, fsc)
	defer deleteShare(a, sc)

	fscSAS := scenarioHelper{}.getRawFileServiceURLWithSAS(a)
	scSAS := fscSAS
	scSAS.Path = name

	args := rawMakeCmdArgs{
		resourceToCreate: scSAS.String(),
		quota:            5,
	}

	runMakeAndVerify(args, func(err error) {
		a.NoError(err)
		props, err := sc.GetProperties(ctx, nil)
		a.NoError(err)
		a.EqualValues(args.quota, *props.Quota)
	})
}

func TestMakeFileShareExists(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	sc, name := getShareClient(a, fsc)
	_, err := sc.Create(ctx, nil)
	a.NoError(err)
	defer deleteShare(a, sc)

	fscSAS := scenarioHelper{}.getRawFileServiceURLWithSAS(a)
	scSAS := fscSAS
	scSAS.Path = name

	args := rawMakeCmdArgs{
		resourceToCreate: scSAS.String(),
	}

	runMakeAndVerify(args, func(err error) {
		a.NotNil(err)
		a.Equal("the file share already exists", err.Error())
		_, err = sc.GetProperties(ctx, nil)
		a.NoError(err)
	})
}
