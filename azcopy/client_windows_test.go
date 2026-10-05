package azcopy

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestM6MinioProxyPropagatesFactoryFailure(t *testing.T) {
	failure := errors.New("transport initialization failed")
	wrapped := withMinioProxy(func(secure bool) (*http.Transport, error) {
		require.True(t, secure)
		return nil, failure
	})
	transport, err := wrapped(true)
	require.Nil(t, transport)
	require.ErrorIs(t, err, failure)
	require.ErrorContains(t, err, "MinIO transport")
}

func TestM6MinioProxyPreservesSuccessfulTransport(t *testing.T) {
	original := &http.Transport{MaxConnsPerHost: 17}
	wrapped := withMinioProxy(func(secure bool) (*http.Transport, error) {
		require.False(t, secure)
		return original, nil
	})
	transport, err := wrapped(false)
	require.NoError(t, err)
	require.Same(t, original, transport)
	require.NotNil(t, transport.Proxy)
	require.Equal(t, 17, transport.MaxConnsPerHost)
}

func TestM6MinioProxyRejectsNilTransport(t *testing.T) {
	wrapped := withMinioProxy(func(bool) (*http.Transport, error) { return nil, nil })
	transport, err := wrapped(true)
	require.Nil(t, transport)
	require.Error(t, err)
}
