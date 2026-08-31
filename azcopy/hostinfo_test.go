package azcopy

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type imdsRoundTripper func(*http.Request) (*http.Response, error)

func (f imdsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestProbeIMDSRequiresSuccessfulMetadataResponse(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		err        error
		wantAzure  bool
	}{
		{name: "success", statusCode: http.StatusOK, wantAzure: true},
		{name: "not found", statusCode: http.StatusNotFound},
		{name: "server error", statusCode: http.StatusInternalServerError},
		{name: "transport failure", err: errors.New("unreachable")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{Transport: imdsRoundTripper(func(req *http.Request) (*http.Response, error) {
				assert.Equal(t, "http://169.254.169.254/metadata/instance/compute/location", req.URL.Scheme+"://"+req.URL.Host+req.URL.Path)
				assert.Equal(t, "true", req.Header.Get("Metadata"))
				if test.err != nil {
					return nil, test.err
				}
				return &http.Response{
					StatusCode: test.statusCode,
					Body:       io.NopCloser(strings.NewReader("eastus")),
					Header:     make(http.Header),
				}, nil
			})}

			assert.Equal(t, test.wantAzure, probeIMDSWithClient(client).isAzureVM)
		})
	}
}

func TestIMDSTransportBypassesProxy(t *testing.T) {
	client := newIMDSHTTPClient()
	transport, ok := client.Transport.(*http.Transport)
	assert.True(t, ok)
	assert.Nil(t, transport.Proxy)
	assert.Equal(t, imdsTimeout, client.Timeout)
}
