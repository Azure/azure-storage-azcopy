package ste

import (
	"errors"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func (s *blobSymlinkSender) getExtraProperties() error {
	if s.jptm.Info().PreservePOSIXProperties {
		if unixSIP, ok := s.sip.(IUNIXPropertyBearingSourceInfoProvider); ok {
			// Clone the metadata before we write to it, we shouldn't be writing to the same metadata as every other blob.
			s.metadataToApply = common.SafeMetadata{Metadata: s.metadataToApply.Metadata.Clone()}

			statAdapter, err := unixSIP.GetUNIXProperties()
			if err != nil {
				return err
			}

			s.jptm.Log(common.LogInfo, fmt.Sprintf("MODE: %b", statAdapter.FileMode()))
			if !(statAdapter.FileMode()&common.S_IFLNK == common.S_IFLNK) { // sanity check this is actually targeting the symlink
				return errors.New("sanity check: GetUNIXProperties did not return symlink properties")
			}

			common.AddStatToBlobMetadata(statAdapter, &s.metadataToApply, s.jptm.Info().PosixPropertiesStyle)
		}
	}

	return nil
}
