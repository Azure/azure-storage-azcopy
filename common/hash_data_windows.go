package common

import (
	"encoding/json"
	"io"
	"os"
	"time"
)

// TryGetHashData On Windows attempts to use Alternate Data Streams
func TryGetHashData(fullpath string) (SyncHashData, error) {
	// get meta file name
	metaFile := fullpath + ":" + AzCopyHashDataStream
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
	metaFile := fullpath + ":" + AzCopyHashDataStream
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

	err = os.Chtimes(fullpath, time.Now(), data.LMT)
	if err != nil {
		return err
	}

	return nil
}
