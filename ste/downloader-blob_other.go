//go:build !linux
// +build !linux

package ste

func (bd *blobDownloader) SetFolderProperties(jptm IJobPartTransferMgr) error {
	return nil
}
