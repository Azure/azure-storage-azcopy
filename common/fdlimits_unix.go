//go:build !windows
// +build !windows

package common

import "syscall"

func getFileDescriptorLimits() (soft uint64, hard uint64, ok bool) {
	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		return 0, 0, false
	}

	return rlimit.Cur, rlimit.Max, true
}
