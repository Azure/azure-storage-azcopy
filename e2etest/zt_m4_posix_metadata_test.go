package e2etest

import (
	"strconv"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestPosixMetadataGroupWithoutOwner(t *testing.T) {
	group := uint32(2345)
	for _, style := range []common.PosixPropertiesStyle{common.StandardPosixPropertiesStyle, common.AMLFSPosixPropertiesStyle} {
		t.Run(style.String(), func(t *testing.T) {
			properties := objectUnixStatContainer{group: &group}
			metadata := make(map[string]*string)
			properties.AddToMetadata(metadata, style)
			key := common.POSIXGroupMeta
			if style == common.AMLFSPosixPropertiesStyle {
				key = common.AMLFSGroupMeta
			}
			assert.Equal(t, strconv.FormatUint(uint64(group), 10), DerefOrZero(metadata[key]))
		})
	}
}

func TestPosixMetadataKeepsDistinctOwnerAndGroup(t *testing.T) {
	owner, group := uint32(1234), uint32(2345)
	for _, style := range []common.PosixPropertiesStyle{common.StandardPosixPropertiesStyle, common.AMLFSPosixPropertiesStyle} {
		t.Run(style.String(), func(t *testing.T) {
			properties := objectUnixStatContainer{owner: &owner, group: &group}
			metadata := make(map[string]*string)
			properties.AddToMetadata(metadata, style)
			ownerKey, groupKey := common.POSIXOwnerMeta, common.POSIXGroupMeta
			if style == common.AMLFSPosixPropertiesStyle {
				ownerKey, groupKey = common.AMLFSOwnerMeta, common.AMLFSGroupMeta
			}
			assert.Equal(t, strconv.FormatUint(uint64(owner), 10), DerefOrZero(metadata[ownerKey]))
			assert.Equal(t, strconv.FormatUint(uint64(group), 10), DerefOrZero(metadata[groupKey]))
		})
	}
}
