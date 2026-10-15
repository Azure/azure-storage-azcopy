//go:build !windows && !linux

package azcopy

func probeAzureVM() bool {
	return false
}
