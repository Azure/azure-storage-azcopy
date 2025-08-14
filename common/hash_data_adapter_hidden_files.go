package common

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type HiddenFileDataAdapter struct {
	hashBasePath string // "" == dataBasePath
	dataBasePath string
}

func (a *HiddenFileDataAdapter) GetMode() HashStorageMode {
	return EHashStorageMode.HiddenFiles()
}

func (a *HiddenFileDataAdapter) getHashPath(relativePath string) string {
	basePath := a.hashBasePath
	if basePath == "" {
		basePath = a.dataBasePath
	}

	dir, fName := filepath.Split(relativePath)
	fName = fmt.Sprintf(".%s%s", fName, AzCopyHashDataStream)

	// Try to create the directory
	err := os.Mkdir(filepath.Join(basePath, dir), 0775)
	if err != nil && !os.IsExist(err) {
		lcm.OnWarning("Failed to create hash data directory")
	}

	return filepath.Join(basePath, dir, fName)
}

// Commenting out this function as it is not used in the codebase
// func (a *HiddenFileDataAdapter) getDataPath(relativePath string) string {
// 	return filepath.Join(a.dataBasePath, relativePath)
// }

func (a *HiddenFileDataAdapter) GetHashData(relativePath string) (*SyncHashData, error) {
	metaFile := a.getHashPath(relativePath)

	f, err := os.OpenFile(metaFile, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open hash meta file: %w", err)
	}
	defer f.Close()

	buf, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read hash meta file: %w", err)
	}

	var out SyncHashData
	err = json.Unmarshal(buf, &out)

	return &out, err
}

func (a *HiddenFileDataAdapter) SetHashData(relativePath string, data *SyncHashData) error {
	if data == nil {
		return nil // no-op
	}

	metaFile := a.getHashPath(relativePath)

	var f *os.File
	_, err := os.Stat(metaFile)
	// In windows os.OpenFile function uses the Windows API to manage files, and the combination of O_CREATE, O_TRUNC, and O_RDWR
	// flags which results in a system call that might not handle hidden files opening operations as expected with this combination of flags.
	if os.IsNotExist(err) {
		f, err = os.OpenFile(metaFile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("failed to create hash meta file: %w", err)
		}
	} else {
		f, err = os.OpenFile(metaFile, os.O_TRUNC|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("failed to open hash meta file: %w", err)
		}
	}

	buf, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	_, err = f.Write(buf)
	if err != nil {
		return fmt.Errorf("failed to write metadata to file: %w", err)
	}

	// Push types around to check for OS-specific hide file method
	if adapter, canHide := any(a).(interface{ HideFile(string) error }); canHide {
		// if --local-hash-storage-mode HiddenFiles is used, hide the hash file
		metaFile := a.getHashPath(relativePath)
		err := adapter.HideFile(metaFile)
		if err != nil {
			return fmt.Errorf("failed to hide file: %w", err)
		}
	}

	return f.Close()
}
