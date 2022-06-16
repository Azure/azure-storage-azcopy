package ste

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func (s *blobSymlinkSender) getExtraProperties() error {
	if s.jptm.Info().PreservePOSIXProperties {
		if unixSIP, ok := s.sip.(IUNIXPropertyBearingSourceInfoProvider); ok {
			// Clone the metadata before we write to it, we shouldn't be writing to the same metadata as every other blob.
			s.metadataToApply = common.Metadata(s.metadataToApply).Clone().ToAzBlobMetadata()

			statAdapter, err := unixSIP.GetUNIXProperties()
			if err != nil {
				return err
			}

			common.AddStatToBlobMetadata(statAdapter, s.metadataToApply)
		}
	}

	return nil
}
