package common

import (
	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	"time"
)

type DropboxObjectInfoExtension struct {
	Metadata files.FileMetadata
}

func (oie *DropboxObjectInfoExtension) ContentType() string {
	return ""
}

func (oie *DropboxObjectInfoExtension) CacheControl() string {
	return ""
}

func (oie *DropboxObjectInfoExtension) ContentDisposition() string {
	return ""
}

func (oie *DropboxObjectInfoExtension) ContentEncoding() string {
	return ""
}

func (oie *DropboxObjectInfoExtension) ContentLanguage() string {
	return ""
}

func (oie *DropboxObjectInfoExtension) ContentMD5() []byte {
	return make([]byte, 0)
}

func (oie *DropboxObjectInfoExtension) LMT() time.Time {
	var lmt time.Time
	if oie.Metadata.ClientModified.After(oie.Metadata.ServerModified) {
		lmt = oie.Metadata.ClientModified
	} else {
		lmt = oie.Metadata.ServerModified
	}
	return lmt
}

func (oie *DropboxObjectInfoExtension) Size() int64 {
	return int64(oie.Metadata.Size)
}

func (oie *DropboxObjectInfoExtension) NewCommonMetadata() Metadata {
	return Metadata{}
}

func (oie *DropboxObjectInfoExtension) ObjectName() string {
	return oie.Metadata.Name
}

func (oie *DropboxObjectInfoExtension) ObjectPath() string {
	return oie.Metadata.PathDisplay[1:]
}
