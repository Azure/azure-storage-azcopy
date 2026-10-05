package common

import (
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/stretchr/testify/require"
)

func TestM4POSIXStyleRoundTrip(t *testing.T) {
	mtime := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.FixedZone("source", -7*60*60))
	for _, extended := range []bool{false, true} {
		for _, style := range []PosixPropertiesStyle{StandardPosixPropertiesStyle, AMLFSPosixPropertiesStyle} {
			t.Run(fmt.Sprintf("statx=%v/%s", extended, style), func(t *testing.T) {
				source := UnixStatContainer{
					statx: extended, mask: STATX_ALL, ownerUID: 123, groupGID: 456,
					mode: S_IFREG | 0755, numLinks: 1, modTime: mtime,
				}
				metadata := &SafeMetadata{Metadata: Metadata{"custom": to.Ptr("preserved")}}
				AddStatToBlobMetadata(source, metadata, style)
				result, err := ReadStatFromMetadata(metadata, 42)
				require.NoError(t, err)
				require.Equal(t, uint32(123), result.Owner())
				require.Equal(t, uint32(456), result.Group())
				expectedMode, expectedTime := source.mode, mtime
				if style == AMLFSPosixPropertiesStyle {
					expectedTime = mtime.Truncate(time.Second)
					require.Equal(t, "0755", *metadata.Metadata[POSIXModeMeta])
					require.Contains(t, metadata.Metadata, AMLFSOwnerMeta)
					require.NotContains(t, metadata.Metadata, POSIXOwnerMeta)
				} else {
					require.Contains(t, metadata.Metadata, POSIXOwnerMeta)
					require.NotContains(t, metadata.Metadata, AMLFSOwnerMeta)
				}
				require.Equal(t, expectedMode, result.FileMode())
				require.True(t, result.MTime().Equal(expectedTime))
				require.Equal(t, "preserved", *metadata.Metadata["custom"])
				gotTime, found, err := TryReadModTimeFromMetadata(metadata.Metadata)
				require.NoError(t, err)
				require.True(t, found)
				require.True(t, gotTime.Equal(expectedTime), "sync timestamp parsing must accept the stored style")
			})
		}
	}
}

func TestM4POSIXFileTypeMetadataIsExclusive(t *testing.T) {
	for _, mode := range []uint32{S_IFREG, S_IFDIR, S_IFLNK, S_IFSOCK, S_IFCHR, S_IFBLK, S_IFIFO} {
		t.Run(fmt.Sprintf("%o", mode), func(t *testing.T) {
			metadata := &SafeMetadata{Metadata: make(Metadata)}
			AddStatToBlobMetadata(UnixStatContainer{mode: mode | 0644}, metadata, AMLFSPosixPropertiesStyle)
			flags := map[uint32]string{
				S_IFDIR: POSIXFolderMeta, S_IFLNK: POSIXSymlinkMeta, S_IFSOCK: POSIXSocketMeta,
				S_IFCHR: POSIXCharDeviceMeta, S_IFBLK: POSIXBlockDeviceMeta, S_IFIFO: POSIXFIFOMeta,
			}
			for kind, key := range flags {
				_, present := metadata.Metadata[key]
				require.Equal(t, mode == kind, present, "incorrect %s flag for type %o", key, mode)
			}
			_, hasDevice := metadata.Metadata[POSIXRDevMeta]
			require.Equal(t, mode == S_IFCHR || mode == S_IFBLK, hasDevice)
			decoded, err := ReadStatFromMetadata(metadata, 0)
			require.NoError(t, err)
			require.Equal(t, uint32(0644), decoded.FileMode()&0777)
			require.Equal(t, mode, decoded.FileMode()&S_IFMT)
		})
	}
}

func TestM4InvalidAMLFSTimestampIsRejected(t *testing.T) {
	_, found, err := TryReadModTimeFromMetadata(Metadata{POSIXModTimeMeta: to.Ptr("not-a-timestamp")})
	require.Error(t, err)
	require.False(t, found)
}

func TestM4AMLFSSymlinkRoundTrip(t *testing.T) {
	for _, extended := range []bool{false, true} {
		t.Run(fmt.Sprintf("statx=%v", extended), func(t *testing.T) {
			source := UnixStatContainer{
				statx: extended, mask: STATX_ALL, mode: S_IFLNK | 0777,
				ownerUID: 123, groupGID: 456, numLinks: 1,
				modTime: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
			}
			metadata := &SafeMetadata{Metadata: make(Metadata)}
			AddStatToBlobMetadata(source, metadata, AMLFSPosixPropertiesStyle)
			require.Equal(t, "0777", *metadata.Metadata[POSIXModeMeta])
			require.Equal(t, "true", *metadata.Metadata[POSIXSymlinkMeta])

			decoded, err := ReadStatFromMetadata(metadata, 0)
			require.NoError(t, err)
			require.Equal(t, source.mode, decoded.FileMode())

			copied := &SafeMetadata{Metadata: make(Metadata)}
			AddStatToBlobMetadata(decoded, copied, AMLFSPosixPropertiesStyle)
			require.Equal(t, "0777", *copied.Metadata[POSIXModeMeta])
			require.Equal(t, "true", *copied.Metadata[POSIXSymlinkMeta])

			standard := &SafeMetadata{Metadata: make(Metadata)}
			AddStatToBlobMetadata(decoded, standard, StandardPosixPropertiesStyle)
			require.Equal(t, fmt.Sprintf("%d", source.mode), *standard.Metadata[POSIXModeMeta])
		})
	}
}

func TestM4AMLFSConflictingTypeMarkersAreRejected(t *testing.T) {
	for _, other := range []string{POSIXFolderMeta, POSIXCharDeviceMeta} {
		metadata := &SafeMetadata{Metadata: Metadata{
			AMLFSOwnerMeta: to.Ptr("123"), POSIXModeMeta: to.Ptr("0777"),
			POSIXSymlinkMeta: to.Ptr("true"), other: to.Ptr("true"),
		}}
		_, err := ReadStatFromMetadata(metadata, 0)
		require.ErrorContains(t, err, "conflicting AMLFS file type markers")
		require.ErrorContains(t, err, POSIXSymlinkMeta)
		require.ErrorContains(t, err, other)
	}
}

func TestM4AMLFSFalseTypeMarkersDescribeRegularFile(t *testing.T) {
	metadata := &SafeMetadata{Metadata: Metadata{
		AMLFSOwnerMeta: to.Ptr("123"), POSIXModeMeta: to.Ptr("0755"),
		POSIXSymlinkMeta: to.Ptr("false"), POSIXFolderMeta: to.Ptr("false"),
	}}
	decoded, err := ReadStatFromMetadata(metadata, 0)
	require.NoError(t, err)
	require.Equal(t, uint32(S_IFREG|0755), decoded.FileMode())
}

func TestM4AMLFSModeTypeConflictIsRejected(t *testing.T) {
	metadata := &SafeMetadata{Metadata: Metadata{
		AMLFSOwnerMeta: to.Ptr("123"), POSIXModeMeta: to.Ptr("0100755"),
		POSIXSymlinkMeta: to.Ptr("true"),
	}}
	_, err := ReadStatFromMetadata(metadata, 0)
	require.ErrorContains(t, err, "conflicts with metadata file type")
}

func TestM4StandardModePreservesLegacyTypeFlags(t *testing.T) {
	mode := uint32(S_IFLNK | 0777)
	metadata := &SafeMetadata{Metadata: Metadata{
		POSIXOwnerMeta: to.Ptr("123"), POSIXModeMeta: to.Ptr(fmt.Sprintf("%d", mode)),
		POSIXSymlinkMeta: to.Ptr("true"), POSIXCharDeviceMeta: to.Ptr("true"),
	}}
	decoded, err := ReadStatFromMetadata(metadata, 0)
	require.NoError(t, err)
	require.Equal(t, mode, decoded.FileMode())
}
