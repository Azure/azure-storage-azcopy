package e2etest

import (
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestLocalHardlinkFixture(t *testing.T) {
	asserter := NewFrameworkAsserter(t)
	container := &LocalContainerResourceManager{RootPath: t.TempDir()}
	original := container.GetObject(asserter, "fixture/original.txt", common.EEntityType.File())
	original.Create(asserter, NewZeroObjectContentContainer(0), ObjectProperties{EntityType: common.EEntityType.File()})
	older := time.Now().Add(-10 * time.Minute)
	require.NoError(t, os.Chtimes(original.URI(), older, older))
	linked := container.GetObject(asserter, "fixture/linked.txt", common.EEntityType.Hardlink())
	linked.Create(asserter, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: "fixture/original.txt",
	})
	originalInfo, err := os.Stat(original.URI())
	require.NoError(t, err)
	linkedInfo, err := os.Stat(linked.URI())
	require.NoError(t, err)
	require.True(t, os.SameFile(originalInfo, linkedInfo))
	require.WithinDuration(t, older, linkedInfo.ModTime(), time.Millisecond)
}
