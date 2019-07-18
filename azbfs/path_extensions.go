package azbfs

import (
	"encoding/base64"
	"time"
)

func (p Path) LastModifiedTime() time.Time {
	if p.LastModified == nil {
		return time.Time{}
	}

	t, err := time.Parse(time.RFC1123, *p.LastModified)
	if err != nil {
		return time.Time{}
	}

	return t
}

func (p Path) ContentMD5() []byte {
	if p.ContentMD5Base64 == nil {
		return nil
	}

	md5, err := base64.StdEncoding.DecodeString(*p.ContentMD5Base64)
	if err != nil {
		return nil
	}
	return md5
}