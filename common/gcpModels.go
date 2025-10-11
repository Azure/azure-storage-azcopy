package common

import (
	"strings"

	gcpUtils "cloud.google.com/go/storage"
)

type GCPObjectInfoExtension struct {
	ObjectInfo gcpUtils.ObjectAttrs
}

func (gie *GCPObjectInfoExtension) ContentType() string {
	return gie.ObjectInfo.ContentType
}

func (gie *GCPObjectInfoExtension) CacheControl() string {
	return gie.ObjectInfo.CacheControl
}

func (gie *GCPObjectInfoExtension) ContentDisposition() string {
	return gie.ObjectInfo.ContentDisposition
}

func (gie *GCPObjectInfoExtension) ContentEncoding() string {
	return gie.ObjectInfo.ContentEncoding
}

func (gie *GCPObjectInfoExtension) ContentLanguage() string {
	return gie.ObjectInfo.ContentLanguage
}

func (gie *GCPObjectInfoExtension) ContentMD5() []byte {
	b := gie.ObjectInfo.MD5
	return b
}

const gcpMetadataPrefix = "x-goog-meta-"
const gcpMetadataPrefixLen = len(gcpMetadataPrefix)

// NewCommonMetadata returns a map of user-defined key/value pairs
func (gie *GCPObjectInfoExtension) NewCommonMetadata() Metadata {
	md := Metadata{}
	for k, v := range gie.ObjectInfo.Metadata {
		if len(k) > gcpMetadataPrefixLen {
			if prefix := k[0:gcpMetadataPrefixLen]; strings.EqualFold(prefix, gcpMetadataPrefix) {
				value := v
				md[k[gcpMetadataPrefixLen:]] = &value
			}
		}
	}
	return md
}
