//go:build windows

package traverser

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

// createUnreadableFile holds the file open with no sharing, as another process locking it would.
func createUnreadableFile(t *testing.T, path string) {
	require.NoError(t, os.WriteFile(path, []byte(path), 0o644))
	p, err := windows.UTF16PtrFromString(path)
	require.NoError(t, err)
	h, err := windows.CreateFile(p, windows.GENERIC_READ, 0, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = windows.CloseHandle(h) })
}
