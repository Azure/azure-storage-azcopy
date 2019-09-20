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

package ste

import (
	"time"

	"github.com/Azure/azure-storage-azcopy/common"
)

type benchmarkSourceInfoProvider struct {
	jptm IJobPartTransferMgr
}

func newBenchmarkSourceInfoProvider(jptm IJobPartTransferMgr) (ISourceInfoProvider, error) {
	return &benchmarkSourceInfoProvider{jptm}, nil
}

func (b benchmarkSourceInfoProvider) Properties() (*SrcProperties, error) {
	return &SrcProperties{
		SrcHTTPHeaders: common.ResourceHTTPHeaders{},
		SrcMetadata:    common.Metadata{},
	}, nil
}

func (b benchmarkSourceInfoProvider) IsLocal() bool {
	return true
}

func (b benchmarkSourceInfoProvider) OpenSourceFile() (common.CloseableReaderAt, error) {
	return common.NewRandomDataGenerator(b.jptm.Info().SourceSize), nil
}

func (b benchmarkSourceInfoProvider) GetLastModifiedTime() (time.Time, error) {
	return common.BenchmarkLmt, nil
}
