package azcopy

import (
	"errors"
	"net/http"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

type hostInfoRoundTripper func(*http.Request) (*http.Response, error)

func (transport hostInfoRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return transport(req)
}

func TestBuildResourceAttributesDoesNotUseHTTP(t *testing.T) {
	t.Setenv(common.EEnvironmentVariable.UserDir().Name, t.TempDir())
	previous := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = previous })
	calls := 0
	http.DefaultTransport = hostInfoRoundTripper(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("resource collection must not use HTTP")
	})
	attributes := buildResourceAttributes()
	assert.Equal(t, probeAzureVM(), attributes.AzureVMDetected)
	assert.Zero(t, calls)
}
