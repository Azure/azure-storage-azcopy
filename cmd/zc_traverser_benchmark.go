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
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
)

type benchmarkTraverser struct {
	fileCount                   uint
	bytesPerFile                int64
	incrementEnumerationCounter func()
}

func newBenchmarkTraverser(source string, incrementEnumerationCounter func()) (*benchmarkTraverser, error) {
	fc, bpf, err := benchmarkSourceHelper{}.FromUrl(source)
	if err != nil {
		return nil, err
	}
	return &benchmarkTraverser{
			fileCount:                   fc,
			bytesPerFile:                bpf,
			incrementEnumerationCounter: incrementEnumerationCounter},
		nil
}

func (t *benchmarkTraverser) isDirectory(bool) bool {
	return true
}

func (t *benchmarkTraverser) traverse(preprocessor objectMorpher, processor objectProcessor, filters []objectFilter) (err error) {
	if len(filters) > 0 {
		panic("filters not expected or supported in benchmark traverser") // but we still call processIfPassedFilters below, for consistency with other traversers
	}

	for i := uint(1); i <= t.fileCount; i++ {

		name := fmt.Sprintf("%d", i)
		relativePath := name

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter()
		}

		err = processIfPassedFilters(filters, newStoredObject(
			preprocessor,
			name,
			relativePath,
			common.BenchmarkLmt,
			t.bytesPerFile,
			nil,
			blobTypeNA,
			""), processor)
		if err != nil {
			return err
		}
	}

	return nil
}
