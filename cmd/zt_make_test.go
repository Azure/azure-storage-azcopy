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
	"github.com/stretchr/testify/assert"
	"testing"
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
		a.Nil(err)
		_, err = cc.GetProperties(ctx, nil)
		a.Nil(err)
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
		a.Nil(err)
	})
}