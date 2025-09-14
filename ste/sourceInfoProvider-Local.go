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
	"bytes"
	"crypto/md5"
	"errors"
	"io"
	"os"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// Source info provider for local files
type localFileSourceInfoProvider struct {
	jptm         IJobPartTransferMgr
	transferInfo *TransferInfo
}

func (f localFileSourceInfoProvider) ReadLink() (string, error) {
	return os.Readlink(f.jptm.Info().Source)
}

func newLocalSourceInfoProvider(jptm IJobPartTransferMgr) (ISourceInfoProvider, error) {
	return &localFileSourceInfoProvider{jptm, jptm.Info()}, nil
}

func (f localFileSourceInfoProvider) Properties() (*SrcProperties, error) {
	// create simulated headers, to represent what we want to propagate to the destination based on
	// this file

	headers, metadata, blobTags, _ := f.jptm.ResourceDstData(nil) // we don't have a known MIME type yet, so pass nil for the sniffed content of thefile

	return &SrcProperties{
		SrcHTTPHeaders: common.ResourceHTTPHeaders{
			ContentType:        headers.ContentType,
			ContentEncoding:    headers.ContentEncoding,
			ContentLanguage:    headers.ContentLanguage,
			ContentDisposition: headers.ContentDisposition,
			CacheControl:       headers.CacheControl,
		},
		SrcMetadata: metadata,
		SrcBlobTags: blobTags,
	}, nil
}

func (f localFileSourceInfoProvider) IsLocal() bool {
	return true
}

func (f localFileSourceInfoProvider) OpenSourceFile() (common.CloseableReaderAt, error) {
	path := f.jptm.Info().Source

	if custom, ok := interface{}(f).(ICustomLocalOpener); ok {
		return custom.Open(path)
	}
	return os.Open(path)
}

func (f localFileSourceInfoProvider) GetFreshFileLastModifiedTime() (time.Time, error) {
	i, err := common.OSStat(f.jptm.Info().Source)
	if err != nil {
		return time.Time{}, err
	}
	return i.ModTime(), nil
}

func (f localFileSourceInfoProvider) EntityType() common.EntityType {
	return f.transferInfo.EntityType
}

func (f localFileSourceInfoProvider) GetMD5(offset, count int64) ([]byte, error) {
	localFile, err := f.OpenSourceFile()
	if err != nil {
		return nil, err
	}
	defer localFile.Close()
	data := make([]byte, count)
	size, err := localFile.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}
	if int64(size) != count {
		return nil, errors.New("failed to read the full range of the local file")
	}
	h := md5.New()
	if _, err = io.Copy(h, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}
