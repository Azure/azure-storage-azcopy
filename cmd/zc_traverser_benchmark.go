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
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type benchmarkTraverser struct {
	fileCount                   uint
	bytesPerFile                int64
	numOfFolders                uint
	incrementEnumerationCounter enumerationCounterFunc
}

func newBenchmarkTraverser(source string, incrementEnumerationCounter enumerationCounterFunc) (*benchmarkTraverser, error) {
	fc, bpf, nf, err := benchmarkSourceHelper{}.FromUrl(source)
	if err != nil {
		return nil, err
	}
	return &benchmarkTraverser{
			fileCount:                   fc,
			bytesPerFile:                bpf,
			numOfFolders:                nf,
			incrementEnumerationCounter: incrementEnumerationCounter},
		nil
}

func (t *benchmarkTraverser) IsDirectory(bool) bool {
	return true
}

func (_ *benchmarkTraverser) toReversedString(i uint) string {
	s := fmt.Sprintf("%d", i)
	count := len(s)
	b := []byte(s)
	r := make([]byte, count)
	lastIndex := count - 1
	for n, x := range b {
		r[lastIndex-n] = x
	}
	return string(r)
}

func (t *benchmarkTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	if len(filters) > 0 {
		panic("filters not expected or supported in benchmark traverser") // but we still call processIfPassedFilters below, for consistency with other traversers
	}

	for i := uint(1); i <= t.fileCount; i++ {

		name := t.toReversedString(i) // this gives an even distribution through the namespace (compare the starting characters, for 0 to 199, when reversed or not). This is useful for performance when High Throughput Block Blob pathway does not apply
		relativePath := name

		if t.numOfFolders > 0 {
			assignedFolder := t.toReversedString(i % t.numOfFolders)
			relativePath = assignedFolder + common.AZCOPY_PATH_SEPARATOR_STRING + relativePath
		}

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.File())
		}

		err = processIfPassedFilters(filters, newStoredObject(
			preprocessor,
			name,
			relativePath,
			common.EEntityType.File(),
			common.BenchmarkLmt,
			t.bytesPerFile,
			noContentProps,
			noBlobProps,
			noMetdata,
			""), processor)
		_, err = getProcessingError(err)
		if err != nil {
			return err
		}
	}

	return nil
}
