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
	"net/url"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	minio "github.com/minio/minio-go"
)

type s3SourceInfoProvider struct {
	jptm         IJobPartTransferMgr
	transferInfo TransferInfo

	rawSourceURL *url.URL

	s3Client  *minio.Client
	s3URLPart common.S3URLParts
}

// By default presign expires after 7 days, which is considered enough for large amounts of files transfer.
// This value could be further tuned, or exposed to user for customization, according to user feedback.
const defaultPresignExpires = time.Hour * 7 * 24

var s3ClientFactory = common.NewS3ClientFactory()

func newS3SourceInfoProvider(jptm IJobPartTransferMgr) (sourceInfoProvider, error) {
	var err error
	p := s3SourceInfoProvider{jptm: jptm, transferInfo: jptm.Info()}

	p.rawSourceURL, err = url.Parse(p.transferInfo.Source)
	if err != nil {
		return nil, err
	}

	p.s3URLPart, err = common.NewS3URLParts(*p.rawSourceURL)
	if err != nil {
		return nil, err
	}

	p.s3Client, err = s3ClientFactory.GetS3Client(
		p.jptm.Context(),
		common.CredentialInfo{
			CredentialType: common.ECredentialType.S3AccessKey(),
			S3CredentialInfo: common.S3CredentialInfo{
				Endpoint: p.s3URLPart.Endpoint,
				Region:   p.s3URLPart.Region,
			},
		},
		common.CredentialOpOptions{
			LogInfo:  func(str string) { p.jptm.Log(pipeline.LogInfo, str) },
			LogError: func(str string) { p.jptm.Log(pipeline.LogError, str) },
			Panic:    p.jptm.Panic,
		})
	if err != nil {
		return nil, err
	}

	return &p, nil
}

func (p *s3SourceInfoProvider) PreSignedSourceURL() (*url.URL, error) {
	return p.s3Client.PresignedGetObject(p.s3URLPart.BucketName, p.s3URLPart.ObjectKey, defaultPresignExpires, url.Values{})
}

func (p *s3SourceInfoProvider) Properties() (*SrcProperties, error) {
	srcProperties := SrcProperties{
		SrcHTTPHeaders: p.transferInfo.SrcHTTPHeaders,
		SrcMetadata:    p.transferInfo.SrcMetadata,
	}

	// Get properties in backend.
	if p.transferInfo.S2SGetS3PropertiesInBackend {
		objectInfo, err := p.s3Client.StatObject(p.s3URLPart.BucketName, p.s3URLPart.ObjectKey, minio.StatObjectOptions{})
		if err != nil {
			return nil, err
		}
		oie := common.ObjectInfoExtension{ObjectInfo: objectInfo}
		srcProperties = SrcProperties{
			SrcHTTPHeaders: common.ResourceHTTPHeaders{
				ContentType:        objectInfo.ContentType,
				ContentEncoding:    oie.ContentEncoding(),
				ContentDisposition: oie.ContentDisposition(),
				ContentLanguage:    oie.ContentLanguage(),
				CacheControl:       oie.CacheControl(),
				ContentMD5:         oie.ContentMD5(),
			},
			SrcMetadata: oie.NewCommonMetadata(),
		}
	}

	return &srcProperties, nil
}

func (p *s3SourceInfoProvider) SourceSize() int64 {
	return p.transferInfo.SourceSize
}

func (p *s3SourceInfoProvider) RawSource() string {
	return p.transferInfo.Source
}
