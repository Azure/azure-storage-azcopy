//go:build !linux
// +build !linux

package ste

func (s *blobSymlinkSender) getExtraProperties() error {
	return nil
}
