//go:build linux || darwin || freebsd || netbsd || solaris

// additional *NIX based OSes included since it uses a generic xattr implementation.

package common

import (
	"encoding/json"
	"github.com/pkg/xattr"
)

var AzCopySyncMetaXAttr = "azcopy.syncmeta"

func TryGetHashData(fullpath string) (SyncHashData, error) {
	// LGet because we want to target the file we actually specify, not what's on the other end.
	buf, err := xattr.LGet(fullpath, AzCopySyncMetaXAttr)
	if err != nil {
		return SyncHashData{}, err
	}

	var out SyncHashData
	err = json.Unmarshal(buf, &out)

	return out, err
}

func PutHashData(fullpath string, data SyncHashData) error {
	buf, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// LSet because we want to target the file we actually specify, not what's on the other end.
	return xattr.LSet(fullpath, AzCopySyncMetaXAttr, buf)
}
