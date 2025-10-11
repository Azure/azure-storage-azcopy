//go:build windows

package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

// ===== OS-Specific hash adapter changes =====

func (HashStorageMode) AlternateDataStreams() HashStorageMode { return 11 } // It's OK if OS-specific options overlap, but we should leave some room for the agnostic options

func (e *HashStorageMode) osDefault() HashStorageMode { return e.AlternateDataStreams() }

func init() { // Override the default hash data adapter behaviour
	osAgnosticBehavior := NewHashDataAdapter // Copy the function for reuse

	NewHashDataAdapter = func(hashPath, dataPath string, mode HashStorageMode) (adapter HashDataAdapter, err error) {
		switch mode {
		case EHashStorageMode.AlternateDataStreams():
			defer func() { // append to the error if needed
				if err != nil {
					err = fmt.Errorf("%w; consider specifying --hash-storage-mode with an OS-agnostic option", err)
				}
			}()

			// first, validate the filesystem supports named streams
			var dataPathPtr *uint16
			dataPathPtr, err = windows.UTF16PtrFromString(dataPath)
			if err != nil {
				return nil, fmt.Errorf("failed to get UTF-16 ptr of data path: %w", err)
			}

			var volumePath = make([]uint16, 256)
			err = windows.GetVolumePathName(dataPathPtr, &volumePath[0], uint32(len(volumePath)))
			if err != nil {
				return nil, fmt.Errorf("failed to get volume of data path: %w", err)
			}

			var volumeFlags uint32
			err = windows.GetVolumeInformation( // https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getvolumeinformationa
				&volumePath[0],
				nil,          // optional, ignoring name
				0,            // No buffer
				nil,          // No volume serial
				nil,          // No need for volume component length
				&volumeFlags, // read volume flags
				nil,          // no need for FS name
				0,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to get volume information of data path: %w", err)
			}

			if volumeFlags&windows.FILE_NAMED_STREAMS != windows.FILE_NAMED_STREAMS {
				return nil, errors.New("source volume does not support named streams")
			}

			return &AlternateDataStreamHashDataAdapter{dataPath}, nil
		default:
			return osAgnosticBehavior(hashPath, dataPath, mode)
		}
	}
}

// AlternateDataStreamHashDataAdapter utilizes the Alternate Data Streams available on filesystems returning windows.FILE_NAMED_STREAMS from windows.GetVolumeInformation
type AlternateDataStreamHashDataAdapter struct {
	dataBasePath string
}

func (a *AlternateDataStreamHashDataAdapter) GetMode() HashStorageMode {
	return EHashStorageMode.AlternateDataStreams()
}

func (a *AlternateDataStreamHashDataAdapter) getDataPath(relativePath string) string {
	return filepath.Join(a.dataBasePath, relativePath)
}

func (a *AlternateDataStreamHashDataAdapter) getHashPath(relativePath string) string {
	return a.getDataPath(relativePath) + ":" + AzCopyHashDataStream
}

// GetHashData On Windows attempts to use Alternate Data Streams to read hash data
func (a *AlternateDataStreamHashDataAdapter) GetHashData(relativePath string) (*SyncHashData, error) {
	// get meta file name
	metaFile := a.getHashPath(relativePath)
	f, err := os.OpenFile(metaFile, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data stream: %w", err)
	}
	defer f.Close()

	buf, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read hash data: %w", err)
	}

	var out SyncHashData
	err = json.Unmarshal(buf, &out)

	return &out, err
}

// SetHashData on Windows attempts to use Alternate Data Streams to write hash data
func (a *AlternateDataStreamHashDataAdapter) SetHashData(relativePath string, data *SyncHashData) error {
	// get meta file name
	metaFile := a.getHashPath(relativePath)
	f, err := os.OpenFile(metaFile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open data stream: %w", err)
	}

	buf, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal hash data: %w", err)
	}

	_, err = f.Write(buf)
	if err != nil {
		return fmt.Errorf("failed to write hash data: %w", err)
	}

	_ = f.Close()

	// ensure LMT matches in case we overwrote it
	err = os.Chtimes(a.getDataPath(relativePath), time.Now(), data.LMT)
	if err != nil {
		return err
	}

	return nil
}

// ===== OS-Specific Hide File Function =====

func (a *HiddenFileDataAdapter) HideFile(fullPath string) error {
	pathPtr, err := syscall.UTF16PtrFromString(fullPath)
	if err != nil {
		return fmt.Errorf("failed to get UTF16 ptr from string: %w", err)
	}

	baseAttr, err := syscall.GetFileAttributes(pathPtr) // if there are already attributes, all we want to do is add an attribute.
	if err != nil {
		return fmt.Errorf("failed to read existing attributes: %w", err)
	}

	return syscall.SetFileAttributes(pathPtr, baseAttr|syscall.FILE_ATTRIBUTE_HIDDEN)
}
