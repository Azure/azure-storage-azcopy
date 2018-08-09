package ste

import (
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

type ErrorEx struct {
	error
}

func (errex ErrorEx) ErrorCodeAndString() (int, string) {
	switch e := interface{}(errex).(type) {
	case azblob.StorageError:
		return e.Response().StatusCode, e.Response().Status
	case azfile.StorageError:
		return e.Response().StatusCode, e.Response().Status
	case azbfs.StorageError:
		return e.Response().StatusCode, e.Response().Status
	default:
		return 0, errex.Error()
	}
}
