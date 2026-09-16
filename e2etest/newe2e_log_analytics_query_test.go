package e2etest

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"testing/iotest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubTokenCredential struct{ err error }

func (c stubTokenCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	if c.err != nil {
		return azcore.AccessToken{}, c.err
	}
	return azcore.AccessToken{Token: "token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestOAuthCacheReleasesLockAfterTokenError(t *testing.T) {
	cache := NewOAuthCache(stubTokenCredential{err: errors.New("token service unavailable")}, "tenant")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 2 {
			_, err := cache.GetAccessToken(LogAnalyticsResource)
			assert.Error(t, err)
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a failed token request left the OAuth cache locked")
	}
}

func TestLogAnalyticsResponseReadFailuresAreRetryable(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Body: io.NopCloser(iotest.ErrReader(errors.New("connection reset")))}, nil
		})
		client := &logAnalyticsQueryClient{tokens: NewOAuthCache(stubTokenCredential{}, "tenant"), client: &http.Client{Transport: transport}}
		_, err := client.QueryTelemetryEvents(context.Background(), "workspace", "query")
		require.ErrorContains(t, err, "connection reset")
		assert.True(t, isRetryableQueryError(err), status)
	}
}
