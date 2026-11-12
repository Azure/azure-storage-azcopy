//go:build windows

package e2etest

import (
	"sync"

	"golang.org/x/sys/windows"
)

// makeFileUnreadable opens the file with no sharing, as another application holding it exclusively would.
// release closes the handle and is safe to call more than once.
func makeFileUnreadable(a Asserter, path string) (release func(), ok bool) {
	name, err := windows.UTF16PtrFromString(path)
	a.NoError("convert path", err, true)
	h, err := windows.CreateFile(name, windows.GENERIC_READ, 0, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	a.NoError("open file with no sharing", err, true)
	return sync.OnceFunc(func() { _ = windows.CloseHandle(h) }), true
}
