//go:build windows
// +build windows

package common

func getFileDescriptorLimits() (soft uint64, hard uint64, ok bool) {
	// Windows does not expose POSIX RLIMIT_NOFILE semantics.
	return 0, 0, false
}
