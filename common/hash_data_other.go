//go:build !windows
// +build !windows

package common

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"
)

func TryGetHashData(fullpath string) (SyncHashData, error) {
	// get meta file name
	dir, fn := filepath.Split(fullpath)
	var metaFile string
	if dir == "." { // Hide the file on UNIX-like systems (e.g. Linux, OSX)
		metaFile = "./." + fn + AzCopyHashDataStream
	} else {
		metaFile = dir + "/." + fn + AzCopyHashDataStream
	}
	// open file for reading
	f, err := os.OpenFile(metaFile, os.O_RDONLY, 0644)
	if err != nil {
		return SyncHashData{}, err
	}
	defer f.Close()

	buf, err := io.ReadAll(f)
	if err != nil {
		return SyncHashData{}, err
	}

	var out SyncHashData
	err = json.Unmarshal(buf, &out)

	return out, err
}

func PutHashData(fullpath string, data SyncHashData) error {
	// get meta file name
	dir, fn := filepath.Split(fullpath)
	var metaFile string
	if dir == "." { // Hide the file on UNIX-like systems (e.g. Linux, OSX)
		metaFile = "./." + fn + AzCopyHashDataStream
	} else {
		metaFile = dir + "/." + fn + AzCopyHashDataStream
	}
	// open file for writing; truncate.
	f, err := os.OpenFile(metaFile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	buf, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = f.Write(buf)
	if err != nil {
		return err
	}

	_ = f.Close() // double closing won't hurt because it's a no-op

	return os.Chtimes(fullpath, time.Now(), data.LMT)
}
