//go:build linux
// +build linux

package ste

import "github.com/Azure/azure-storage-azcopy/v10/common"

func (b *blobFolderSender) getExtraProperties() error {
	if b.jptm.Info().PreservePOSIXProperties {
		if sip, ok := b.sip.(*localFileSourceInfoProvider); ok { // has UNIX properties for sure; Blob metadata gets handled as expected.
			statAdapter, err := sip.GetUNIXProperties()

			if err != nil {
				return err
			}

			common.AddStatToBlobMetadata(statAdapter, &b.metadataToApply, b.jptm.Info().PosixPropertiesStyle)
		}
	}

	return nil
}
