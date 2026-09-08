package e2etest

import (
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestLocalFixtureTimeUsesReferenceClock(t *testing.T) {
	for _, clockSkew := range []time.Duration{-30 * time.Minute, 30 * time.Minute} {
		t.Run(clockSkew.String(), func(t *testing.T) {
			asserter := NewFrameworkAsserter(t)
			container := &LocalContainerResourceManager{RootPath: t.TempDir()}
			local := container.GetObject(asserter, "local", common.EEntityType.File())
			reference := container.GetObject(asserter, "reference", common.EEntityType.File())
			local.Create(asserter, NewZeroObjectContentContainer(0), ObjectProperties{})
			reference.Create(asserter, NewZeroObjectContentContainer(0), ObjectProperties{})
			referenceTime := time.Now().Add(clockSkew).Truncate(time.Second)
			require.NoError(t, os.Chtimes(reference.URI(), referenceTime, referenceTime))
			for _, offset := range []time.Duration{-time.Minute, time.Minute} {
				setLocalFixtureTimeRelativeTo(asserter, local, reference, offset)
				info, err := os.Stat(local.URI())
				require.NoError(t, err)
				require.WithinDuration(t, referenceTime.Add(offset), info.ModTime(), time.Millisecond)
			}
		})
	}
}
