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

func TestConversions(t *testing.T) {
	a := assert.New(t)
	testData := []struct {
		floatMiB         float64
		expectedBytes    int64
		expectedErrorMsg string
	}{
		{100, 100 * 1024 * 1024, ""},
		{1, 1024 * 1024, ""},
		{0.25, 256 * 1024, ""},
		{0.000030517578125, 32, ""}, // 32 bytes, extremely small case
		{-1, 0, "negative block size not allowed"},
		{0.333, 0, "while fractional numbers of MiB are allowed as the block size, the fraction must result to a whole number of bytes. 0.333000000000 MiB resolves to 349175.808 bytes"},
	}

	for _, d := range testData {
		actualBytes, err := blockSizeInBytes(d.floatMiB)
		if d.expectedErrorMsg != "" {
			a.Equal(d.expectedErrorMsg, err.Error())
		} else {
			a.Equal(d.expectedBytes, actualBytes)
		}
	}
}
