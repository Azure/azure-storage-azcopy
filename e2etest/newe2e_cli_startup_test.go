package e2etest

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestAzCLIStartupGateScope(t *testing.T) {
	for _, platform := range []string{"darwin", "linux", "windows"} {
		for _, mode := range []string{"azcli", "AzCLI", "workload", "spn", ""} {
			require.Equal(t, platform == "darwin" && (mode == "azcli" || mode == "AzCLI"), needsAzCLIStartupGate(platform, mode, false))
			require.False(t, needsAzCLIStartupGate(platform, mode, true))
		}
	}
}

func TestAzCLIStartupGateReleasesOnJobID(t *testing.T) {
	slot := make(chan struct{}, 1)
	release, err := acquireAzCLIStartup(context.Background(), slot, time.Minute)
	require.NoError(t, err)
	defer release()
	capture := newAzCopyJobIDCapture(&AzCopyRawStdout{})
	capture.onJobID = release
	_, err = capture.Write([]byte("Job invalid has started\n"))
	require.NoError(t, err)
	require.Len(t, slot, 1)
	blocked, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = acquireAzCLIStartup(blocked, slot, time.Minute)
	require.ErrorIs(t, err, context.Canceled)
	_, err = capture.Write([]byte("Job " + common.NewJobID().String() + " has started\n"))
	require.NoError(t, err)
	require.Empty(t, slot)
	next, err := acquireAzCLIStartup(context.Background(), slot, time.Minute)
	require.NoError(t, err)
	defer next()
	release()
	require.Len(t, slot, 1, "duplicate release must not release the next process")
	_, err = capture.Write([]byte("Job " + common.NewJobID().String() + " has started\n"))
	require.NoError(t, err)
	require.Len(t, slot, 1)
}

func TestAzCLIStartupGateBoundsWaitAndLease(t *testing.T) {
	slot := make(chan struct{}, 1)
	slot <- struct{}{}
	_, err := acquireAzCLIStartup(context.Background(), slot, time.Millisecond)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	<-slot
	release, err := acquireAzCLIStartup(context.Background(), slot, time.Millisecond)
	require.NoError(t, err)
	defer release()
	next, err := acquireAzCLIStartup(context.Background(), slot, time.Second)
	require.NoError(t, err, "expired startup lease must allow another process")
	next()
	require.Empty(t, slot)
}
