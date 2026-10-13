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
	h := openForTest(t, path, 0, 0)
	t.Cleanup(func() { _ = windows.CloseHandle(h) })
}

func openForTest(t *testing.T, path string, share, flags uint32) windows.Handle {
	t.Helper()
	p, err := windows.UTF16PtrFromString(path)
	require.NoError(t, err)
	h, err := windows.CreateFile(p, windows.GENERIC_READ, share, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL|flags, 0)
	require.NoError(t, err)
	return h
}
