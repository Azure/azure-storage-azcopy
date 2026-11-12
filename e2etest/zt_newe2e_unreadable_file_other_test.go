//go:build !windows

package e2etest

import (
	"os"
	"sync"
)

// makeFileUnreadable removes all permissions from the file. It reports false when running as root, which can still read it.
// release restores the permissions and is safe to call more than once.
func makeFileUnreadable(a Asserter, path string) (release func(), ok bool) {
	if os.Geteuid() == 0 {
		return func() {}, false
	}
	a.NoError("remove file permissions", os.Chmod(path, 0), true)
	return sync.OnceFunc(func() { _ = os.Chmod(path, 0o644) }), true
}
