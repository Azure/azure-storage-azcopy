//go:build linux || darwin

// XAttr APIs are compatible across linux & darwin; no need to duplicate code.

package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// ===== OS-Specific hash adapter changes =====

func (HashStorageMode) XAttr() HashStorageMode { return 11 } // It's OK if OS-specific options overlap, but we should leave some room for the agnostic options

func (e HashStorageMode) osDefault() HashStorageMode { return e.XAttr() }

func init() {
	osAgnosticBehavior := NewHashDataAdapter

	NewHashDataAdapter = func(hashPath, dataPath string, mode HashStorageMode) (adapter HashDataAdapter, err error) {
		switch mode {
		case EHashStorageMode.XAttr():
			// Checking the root directory of the source isn't technically correct, as a filesystem could be mounted under the source.
			lcm.Info("XAttr hash storage mode is selected. This assumes all files indexed on the source are on filesystem(s) that support user_xattr.")
			return &XAttrHashDataAdapter{dataBasePath: dataPath}, nil
		default: // fall back to OS-agnostic behaviors
			return osAgnosticBehavior(hashPath, dataPath, mode)
		}
	}
}

// XAttrHashDataAdapter stores hash data in the Xattr fields
type XAttrHashDataAdapter struct {
	dataBasePath string
}

func (a *XAttrHashDataAdapter) GetMode() HashStorageMode {
	return EHashStorageMode.XAttr()
}

func (a *XAttrHashDataAdapter) getDataPath(relativePath string) string {
	return filepath.Join(a.dataBasePath, relativePath)
}

func (a *XAttrHashDataAdapter) GetHashData(relativePath string) (*SyncHashData, error) {
	metaFile := a.getDataPath(relativePath)

	buf := make([]byte, 512) // 512 bytes should be plenty of space
retry:
	sz, err := unix.Getxattr(metaFile, strings.TrimPrefix(AzCopyHashDataStream, "."), buf) // MacOS doesn't take well to the dot(?)
	if err != nil {
		if err == unix.ERANGE { // But just in case, let's safeguard against it and re-call with a larger buffer.
			buf = make([]byte, len(buf)*2)
			goto retry
		}

		if err == unix.ENODATA { // There's no hash present; nothing to do.
			return nil, errors.New("no hash present")
		}

		return nil, fmt.Errorf("failed to read xattr: %w; consider utilizing an OS-agnostic hash storage mode", err)
	}

	buf = buf[:sz] // trim the ending bytes off

	var out SyncHashData
	err = json.Unmarshal(buf, &out)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal xattr: %w; buffer contents: \"%s\"", err, buf)
	}

	return &out, nil
}

func (a *XAttrHashDataAdapter) SetHashData(relativePath string, data *SyncHashData) error {
	if data == nil {
		return nil
	}

	metaFile := a.getDataPath(relativePath)

	buf, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal xattr: %w", err)
	}

	err = unix.Setxattr(metaFile, strings.TrimPrefix(AzCopyHashDataStream, "."), buf, 0) // Default flags == create or replace
	if err != nil {
		return fmt.Errorf("failed to write xattr: %w; consider utilizing an OS-agnostic hash storage mode", err)
	}

	return nil
}
