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

package traverser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type BenchmarkSourceHelper struct{}

// our code requires sources to be strings. So we may as well do the benchmark sources as URLs
// so we can identify then as such using a specific domain. ".invalid" is reserved globally for cases where
// you want a URL that can't possibly be a real one, so we'll use that
const BenchmarkSourceHost = "benchmark.invalid"

func (h BenchmarkSourceHelper) ToUrl(fileCount uint, bytesPerFile int64, numOfFolders uint) string {
	return fmt.Sprintf("https://%s?fc=%d&bpf=%d&nf=%d", BenchmarkSourceHost, fileCount, bytesPerFile, numOfFolders)
}

func (h BenchmarkSourceHelper) FromUrl(s string) (fileCount uint, bytesPerFile int64, numOfFolders uint, err error) {
	// TODO: consider replace with regex?

	expectedPrefix := "https://" + BenchmarkSourceHost + "?"
	if !strings.HasPrefix(s, expectedPrefix) {
		return 0, 0, 0, errors.New("invalid benchmark source string")
	}
	s = strings.TrimPrefix(s, expectedPrefix)
	pieces := strings.Split(s, "&")
	if len(pieces) != 3 ||
		!strings.HasPrefix(pieces[0], "fc=") ||
		!strings.HasPrefix(pieces[1], "bpf=") ||
		!strings.HasPrefix(pieces[2], "nf=") {
		return 0, 0, 0, errors.New("invalid benchmark source string")
	}
	pieces[0] = strings.Split(pieces[0], "=")[1]
	pieces[1] = strings.Split(pieces[1], "=")[1]
	pieces[2] = strings.Split(pieces[2], "=")[1]
	fc, err := strconv.ParseUint(pieces[0], 10, 32)
	if err != nil {
		return 0, 0, 0, err
	}
	bpf, err := strconv.ParseInt(pieces[1], 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}
	nf, err := strconv.ParseUint(pieces[2], 10, 32)
	if err != nil {
		return 0, 0, 0, err
	}
	return uint(fc), bpf, uint(nf), nil
}

type benchmarkTraverser struct {
	fileCount                   uint
	bytesPerFile                int64
	numOfFolders                uint
	incrementEnumerationCounter enumerationCounterFunc
}

func newBenchmarkTraverser(source string, incrementEnumerationCounter enumerationCounterFunc) (*benchmarkTraverser, error) {
	fc, bpf, nf, err := BenchmarkSourceHelper{}.FromUrl(source)
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

func (t *benchmarkTraverser) IsDirectory(bool) (bool, error) {
	return true, nil
}

func (*benchmarkTraverser) toReversedString(i uint) string {
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

func (t *benchmarkTraverser) Traverse(preprocessor objectMorpher, processor ObjectProcessor, filters []ObjectFilter) (err error) {
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

		err = ProcessIfPassedFilters(filters, NewStoredObject(
			preprocessor,
			name,
			relativePath,
			common.EEntityType.File(),
			common.BenchmarkLmt,
			t.bytesPerFile,
			NoContentProps,
			noBlobProps,
			noMetadata,
			""), processor)
		_, err = GetProcessingError(err)
		if err != nil {
			return err
		}
	}

	return nil
}
