//go:build !linux
// +build !linux

package ste

func (bd *blobFSDownloader) SetFolderProperties(jptm IJobPartTransferMgr) error {
	return nil
}
