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

package common

import (
	"encoding/base64"
	"strings"

	minio "github.com/minio/minio-go/v7"
)

type ObjectInfoExtension struct {
	ObjectInfo minio.ObjectInfo
}

func (oie *ObjectInfoExtension) ContentType() string {
	return oie.ObjectInfo.ContentType
}

// CacheControl returns the value for header Cache-Control.
func (oie *ObjectInfoExtension) CacheControl() string {
	return oie.ObjectInfo.Metadata.Get("Cache-Control")
}

// ContentDisposition returns the value for header Content-Disposition.
func (oie *ObjectInfoExtension) ContentDisposition() string {
	return oie.ObjectInfo.Metadata.Get("Content-Disposition")
}

// ContentEncoding returns the value for header Content-Encoding.
func (oie *ObjectInfoExtension) ContentEncoding() string {
	return oie.ObjectInfo.Metadata.Get("Content-Encoding")
}

// ContentLanguage returns the value for header Content-Language.
func (oie *ObjectInfoExtension) ContentLanguage() string {
	return oie.ObjectInfo.Metadata.Get("Content-Language")
}

// ContentMD5 returns the value for header Content-MD5.
func (oie *ObjectInfoExtension) ContentMD5() []byte {
	s := oie.ObjectInfo.Metadata.Get("Content-MD5")
	if s == "" {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		b = nil
	}
	return b
}

const s3MetadataPrefix = "x-amz-meta-"

const s3MetadataPrefixLen = len(s3MetadataPrefix)

// NewMetadata returns user-defined key/value pairs.
func (oie *ObjectInfoExtension) NewCommonMetadata() Metadata {
	md := Metadata{}
	for k, v := range oie.ObjectInfo.Metadata {
		if len(k) > s3MetadataPrefixLen {
			if prefix := k[0:s3MetadataPrefixLen]; strings.EqualFold(prefix, s3MetadataPrefix) {
				value := v[0]
				md[k[s3MetadataPrefixLen:]] = &value
			}
		}
	}
	return md
}
