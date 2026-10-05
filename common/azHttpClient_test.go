package common

import (
	"net/http"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func resetGlobalHTTPClientForTest(t *testing.T) {
	t.Helper()
	originalClient, originalOnce := GlobalHTTPClient, globalHTTPClientOnce
	GlobalHTTPClient, globalHTTPClientOnce = nil, new(sync.Once)
	t.Cleanup(func() {
		GlobalHTTPClient, globalHTTPClientOnce = originalClient, originalOnce
	})
}

func assertGlobalHTTPClientLimits(t *testing.T, client *http.Client, idleLimit int) {
	t.Helper()
	require.NotNil(t, client)
	if buildmode.HighPerf() {
		transport, ok := client.Transport.(*ShardedTransport)
		require.True(t, ok)
		require.Len(t, transport.transports, buildmode.TransportShards())
		for _, shard := range transport.transports {
			assert.Equal(t, 1024, shard.MaxConnsPerHost)
			assert.Equal(t, 1024, shard.MaxIdleConnsPerHost)
			assert.False(t, shard.ForceAttemptHTTP2)
			require.NotNil(t, shard.TLSClientConfig)
			assert.Equal(t, []string{"http/1.1"}, shard.TLSClientConfig.NextProtos)
		}
		return
	}
	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok, "default and mover-default profiles must not use sharding")
	assert.Equal(t, idleLimit, transport.MaxIdleConnsPerHost)
	assert.Equal(t, 0, transport.MaxIdleConns)
	assert.Equal(t, 10*runtime.NumCPU(), transport.MaxConnsPerHost)
	assert.True(t, transport.DisableCompression)
}

func TestGlobalHTTPClientLazyInitialization(t *testing.T) {
	resetGlobalHTTPClientForTest(t)
	t.Setenv(enum.EEnvironmentVariable.ConcurrencyValue().Name, "80")
	client := GetGlobalHTTPClient()
	require.Same(t, client, GlobalHTTPClient)
	require.Same(t, client, GetGlobalHTTPClient(nil))
	require.Same(t, client, InitGlobalHTTPClient())
	require.Same(t, client, InitGlobalHTTPClient(196))
	assertGlobalHTTPClientLimits(t, client, GetMaxIdleConnsPerHost())
}

func TestGlobalHTTPClientInitUsesConfiguredIdleConnLimit(t *testing.T) {
	resetGlobalHTTPClientForTest(t)
	client := InitGlobalHTTPClient(196)

	retrieved := GetGlobalHTTPClient()
	require.Same(t, client, retrieved)
	assertGlobalHTTPClientLimits(t, client, 196)

	// A second init call should be a no-op and keep the first configuration.
	clientAgain := InitGlobalHTTPClient(42)
	require.Same(t, client, clientAgain)
	assertGlobalHTTPClientLimits(t, clientAgain, 196)
}

func TestGlobalHTTPClientNoArgumentInit(t *testing.T) {
	resetGlobalHTTPClientForTest(t)
	t.Setenv(enum.EEnvironmentVariable.ConcurrencyValue().Name, "32")
	client := InitGlobalHTTPClient()
	require.Same(t, client, GetGlobalHTTPClient(nil))
	assertGlobalHTTPClientLimits(t, client, GetMaxIdleConnsPerHost())
}

func TestGlobalHTTPClientPreservesHostClient(t *testing.T) {
	resetGlobalHTTPClientForTest(t)
	hostTransport := &http.Transport{MaxIdleConnsPerHost: 17}
	hostClient := &http.Client{Transport: hostTransport, Timeout: 3 * time.Second}
	GlobalHTTPClient = hostClient

	require.Same(t, hostClient, InitGlobalHTTPClient(196))
	require.Same(t, hostClient, GetGlobalHTTPClient())
	require.Same(t, hostClient, GetGlobalHTTPClient(nil))
	require.Same(t, hostTransport, hostClient.Transport)
	assert.Equal(t, 17, hostTransport.MaxIdleConnsPerHost)
	assert.Equal(t, 3*time.Second, hostClient.Timeout)
}

func TestGlobalHTTPClientConcurrentInitialization(t *testing.T) {
	resetGlobalHTTPClientForTest(t)
	const callers = 32
	clients := make(chan *http.Client, callers)
	var workers sync.WaitGroup
	for i := 0; i < callers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			clients <- InitGlobalHTTPClient(196)
		}()
	}
	workers.Wait()
	close(clients)
	for client := range clients {
		require.Same(t, GlobalHTTPClient, client)
	}
	assertGlobalHTTPClientLimits(t, GlobalHTTPClient, 196)
}
