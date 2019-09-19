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

package ste

import (
	"os"
	"time"

	"github.com/Azure/azure-storage-azcopy/common"
)

// Source info provider for local files
type localFileSourceInfoProvider struct {
	jptm IJobPartTransferMgr
}

func newLocalSourceInfoProvider(jptm IJobPartTransferMgr) (ISourceInfoProvider, error) {
	return &localFileSourceInfoProvider{jptm}, nil
}

func (f localFileSourceInfoProvider) Properties() (*SrcProperties, error) {
	// create simulated headers, to represent what we want to propagate to the destination based on
	// this file

	// TODO: find a better way to get generic ("Resource" headers/metadata, from jptm)
	headers, metadata := f.jptm.BlobDstData(nil) // we don't have a known MIME type yet, so pass nil for the sniffed content of thefile

	return &SrcProperties{
		SrcHTTPHeaders: common.ResourceHTTPHeaders{
			ContentType:        headers.ContentType,
			ContentEncoding:    headers.ContentEncoding,
			ContentLanguage:    headers.ContentLanguage,
			ContentDisposition: headers.ContentDisposition,
			CacheControl:       headers.CacheControl,
		},
		SrcMetadata: common.FromAzBlobMetadataToCommonMetadata(metadata),
	}, nil
}

func (f localFileSourceInfoProvider) IsLocal() bool {
	return true
}

func (f localFileSourceInfoProvider) OpenSourceFile() (common.CloseableReaderAt, error) {
	return os.Open(f.jptm.Info().Source)
}

func (f localFileSourceInfoProvider) GetLastModifiedTime() (time.Time, error) {
	i, err := os.Stat(f.jptm.Info().Source)
	if err != nil {
		return time.Time{}, err
	}
	return i.ModTime(), nil
}
