package e2etest

import (
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

type fixtureTimeAsserter struct {
	Asserter
	failures []string
}

func (a *fixtureTimeAsserter) AssertNow(comment string, assertion Assertion, items ...any) {
	if !assertion.Assert(items...) {
		a.failures = append(a.failures, comment)
	}
}

type noTimeReference struct{ ObjectResourceManager }

func (noTimeReference) GetProperties(Asserter) ObjectProperties { return ObjectProperties{} }

func TestLocalFixtureTimeRequiresReferenceTime(t *testing.T) {
	asserter := &fixtureTimeAsserter{Asserter: NewFrameworkAsserter(t)}
	container := &LocalContainerResourceManager{RootPath: t.TempDir()}
	local := container.GetObject(asserter, "local", common.EEntityType.File())
	local.Create(asserter, NewZeroObjectContentContainer(0), ObjectProperties{})
	setLocalFixtureTimeRelativeTo(asserter, local, noTimeReference{}, time.Minute)
	require.Equal(t, []string{"reference fixture must report modification time"}, asserter.failures)
}

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
