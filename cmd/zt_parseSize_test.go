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
	chk "gopkg.in/check.v1"
	"testing"
)

type parseSizeSuite struct{}

var _ = chk.Suite(&parseSizeSuite{})

func TestParseSize(t *testing.T) {
	a := assert.New(t)
	b, _ := ParseSizeString("123K", "x")
	a.Equal(int64(123*1024), b)

	b, _ = ParseSizeString("456m", "x")
	a.Equal(int64(456*1024*1024), b)

	b, _ = ParseSizeString("789G", "x")
	a.Equal(int64(789*1024*1024*1024), b)

	expectedError := "foo-bar must be a number immediately followed by K, M or G. E.g. 12k or 200G"

	_, err := ParseSizeString("123", "foo-bar")
	a.Equal(expectedError, err.Error())

	_, err = ParseSizeString("123 K", "foo-bar")
	a.Equal(expectedError, err.Error())

	_, err = ParseSizeString("123KB", "foo-bar")
	a.Equal(expectedError, err.Error())

	_, err = ParseSizeString("123T", "foo-bar") // we don't support terabytes
	a.Equal(expectedError, err.Error())

	_, err = ParseSizeString("abcK", "foo-bar") //codespell:ignore
	a.Equal(expectedError, err.Error())

}
