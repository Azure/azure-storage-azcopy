// +build linux

package ste

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
	"syscall"
	"time"
)

func (bd *blobFSDownloader) ApplyUnixProperties(adapter common.UnixStatAdapter) (stage string, err error) {
	// At this point, mode has already been applied. Let's work out what we need to apply, and apply the rest.
	destination := bd.txInfo.getDownloadPath()

	// First, grab our file descriptor and such.
	fi, err := os.Stat(destination)
	if err != nil {
		return "stat", err
	}

	// At this point, mode has already been applied. Let's work out what we need to apply, and apply the rest.
	if adapter.Extended() {
		stat := fi.Sys().(*syscall.Stat_t)
		mask := adapter.StatxMask()

		// stx_attributes is not persisted.

		mode := os.FileMode(common.DEFAULT_FILE_PERM)
		if common.StatXReturned(mask, common.STATX_MODE) {
			mode = os.FileMode(adapter.FileMode())
		}

		err = os.Chmod(destination, mode)
		if err != nil {
			return "chmod", err
		}

		uid := stat.Uid
		if common.StatXReturned(mask, common.STATX_UID) {
			uid = adapter.Owner()
		}

		gid := stat.Gid
		if common.StatXReturned(mask, common.STATX_GID) {
			gid = adapter.Group()
		}
		// set ownership
		err = os.Chown(destination, int(uid), int(gid))
		if err != nil {
			return "chown", err
		}

		atime := time.Unix(stat.Atim.Unix())
		if common.StatXReturned(mask, common.STATX_ATIME) || !adapter.ATime().IsZero() { // workaround for noatime when underlying fs supports atime
			atime = adapter.ATime()
		}

		mtime := time.Unix(stat.Mtim.Unix())
		if common.StatXReturned(mask, common.STATX_MTIME) {
			mtime = adapter.MTime()
		}

		// adapt times
		err = os.Chtimes(destination, atime, mtime)
		if err != nil {
			return "chtimes", err
		}
	} else {
		err = os.Chmod(destination, os.FileMode(adapter.FileMode())) // only write permissions
		if err != nil {
			return "chmod", err
		}
		err = os.Chown(destination, int(adapter.Owner()), int(adapter.Group()))
		if err != nil {
			return "chown", err
		}
		err = os.Chtimes(destination, adapter.ATime(), adapter.MTime())
		if err != nil {
			return "chtimes", err
		}
	}

	return
}

func (bd *blobFSDownloader) SetFolderProperties(jptm IJobPartTransferMgr) error {
	sip, err := newBlobSourceInfoProvider(jptm)
	if err != nil {
		return err
	}

	// inform the downloader
	bd.txInfo = jptm.Info()

	usip := sip.(IUNIXPropertyBearingSourceInfoProvider)
	if usip.HasUNIXProperties() {
		props, err := usip.GetUNIXProperties()
		if err != nil {
			return err
		}
		stage, err := bd.ApplyUnixProperties(props)

		if err != nil {
			return fmt.Errorf("set unix properties: %s; %w", stage, err)
		}
	}

	return nil
}