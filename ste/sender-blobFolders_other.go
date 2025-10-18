//go:build !linux
// +build !linux

package ste

func (b *blobFolderSender) getExtraProperties() error {
	return nil
}
