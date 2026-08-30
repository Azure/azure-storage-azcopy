package common

import (
	"net/http"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobalHTTPClientRequiresInit(t *testing.T) {
	assert.PanicsWithValue(t,
		"common.GetGlobalHTTPClient called before InitGlobalHTTPClient; InitGlobalHTTPClient must run during process startup (see azcopy.NewClient)",
		func() {
			_ = GetGlobalHTTPClient()
		})
}

func TestGlobalHTTPClientInitUsesConfiguredIdleConnLimit(t *testing.T) {
	client := InitGlobalHTTPClient(196)
	require.NotNil(t, client)

	retrieved := GetGlobalHTTPClient()
	require.Same(t, client, retrieved)

	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok, "global client transport should be an *http.Transport")
	assert.Equal(t, 196, transport.MaxIdleConnsPerHost)
	assert.Equal(t, 0, transport.MaxIdleConns)
	assert.Equal(t, 10*runtime.NumCPU(), transport.MaxConnsPerHost)

	// A second init call should be a no-op and keep the first configuration.
	clientAgain := InitGlobalHTTPClient(42)
	require.Same(t, client, clientAgain)
	assert.Equal(t, 196, transport.MaxIdleConnsPerHost)
}
