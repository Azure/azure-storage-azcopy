package common

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testStorageAccountResourceID = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/account"

type metricsTestCredential struct {
	token azcore.AccessToken
	err   error
	opts  policy.TokenRequestOptions
}

func (c *metricsTestCredential) GetToken(_ context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	c.opts = opts
	return c.token, c.err
}

type metricsTestTransport func(*http.Request) (*http.Response, error)

func (t metricsTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return t(request)
}

func TestGetStorageAccountMetrics(t *testing.T) {
	startTime := time.Date(2026, time.October, 2, 22, 0, 0, 0, time.FixedZone("test", -7*60*60))
	endTime := startTime.Add(2 * time.Minute)
	credential := &metricsTestCredential{token: azcore.AccessToken{Token: "token"}}

	client := &http.Client{Transport: metricsTestTransport(func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.MethodGet, request.Method)
		assert.Equal(t, "management.azure.com", request.URL.Host)
		assert.Equal(t, testStorageAccountResourceID+"/providers/microsoft.insights/metrics", request.URL.Path)
		assert.Equal(t, "Bearer token", request.Header.Get("Authorization"))

		query := request.URL.Query()
		assert.Equal(t, azureMonitorMetricsAPIVersion, query.Get("api-version"))
		assert.Equal(t, "Microsoft.Storage/storageAccounts", query.Get("metricnamespace"))
		assert.Equal(t, "Ingress,Egress,Transactions", query.Get("metricnames"))
		assert.Equal(t, "Total", query.Get("aggregation"))
		assert.Equal(t, "PT1M", query.Get("interval"))
		assert.Equal(t, "2026-10-03T05:00:00Z/2026-10-03T05:02:00Z", query.Get("timespan"))

		body := `{
			"value": [
				{"name":{"value":"Ingress"},"timeseries":[{"data":[{"total":100},{"total":250}]}]},
				{"name":{"value":"Egress"},"timeseries":[{"data":[{"total":75},{"total":125}]}]},
				{"name":{"value":"Transactions"},"timeseries":[{"data":[{"total":600},{"total":1200}]}]}
			]
		}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	})}

	metrics, err := GetStorageAccountMetrics(context.Background(), client, credential, testStorageAccountResourceID, startTime, endTime)

	require.NoError(t, err)
	assert.Equal(t, float64(350), metrics.IngressBytes)
	assert.Equal(t, float64(200), metrics.EgressBytes)
	assert.Equal(t, float64(20), metrics.PeakTransactionsPerSecond)
	assert.Equal(t, []string{azureResourceManagerScope}, credential.opts.Scopes)
}

func TestGetStorageAccountMetricsReturnsHTTPError(t *testing.T) {
	credential := &metricsTestCredential{token: azcore.AccessToken{Token: "token"}}
	client := &http.Client{Transport: metricsTestTransport(func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusForbidden,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"AuthorizationFailed"}}`)),
		}, nil
	})}

	_, err := GetStorageAccountMetrics(
		context.Background(),
		client,
		credential,
		testStorageAccountResourceID,
		time.Now().Add(-time.Minute),
		time.Now(),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 403")
	assert.Contains(t, err.Error(), "AuthorizationFailed")
}

func TestGetStorageAccountMetricsValidatesInputs(t *testing.T) {
	credential := &metricsTestCredential{token: azcore.AccessToken{Token: "token"}}
	client := &http.Client{Transport: metricsTestTransport(func(_ *http.Request) (*http.Response, error) {
		return nil, errors.New("request should not be sent")
	})}
	now := time.Now()

	_, err := GetStorageAccountMetrics(context.Background(), client, credential, "/invalid", now.Add(-time.Minute), now)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage account resource ID")

	_, err = GetStorageAccountMetrics(context.Background(), client, credential, testStorageAccountResourceID, now, now)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start time must be before end time")
}

func TestGetStorageAccountMetricsRequiresAllMetrics(t *testing.T) {
	credential := &metricsTestCredential{token: azcore.AccessToken{Token: "token"}}
	client := &http.Client{Transport: metricsTestTransport(func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"value":[{"name":{"value":"Ingress"},"timeseries":[]}]}`)),
		}, nil
	})}

	_, err := GetStorageAccountMetrics(
		context.Background(),
		client,
		credential,
		testStorageAccountResourceID,
		time.Now().Add(-time.Minute),
		time.Now(),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "Egress, Transactions")
}
