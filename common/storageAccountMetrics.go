// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

const (
	azureMonitorMetricsAPIEndpoint = "https://management.azure.com"
	azureMonitorMetricsAPIVersion  = "2023-10-01"
	azureResourceManagerScope      = "https://management.azure.com/.default"
)

// StorageAccountMetrics contains observed traffic for a storage account over a
// requested time window. PeakTransactionsPerSecond is calculated from the
// busiest one-minute Transactions metric bucket.
type StorageAccountMetrics struct {
	IngressBytes              float64
	EgressBytes               float64
	PeakTransactionsPerSecond float64
}

type azureMonitorMetricsResponse struct {
	Value []azureMonitorMetric `json:"value"`
}

type azureMonitorMetric struct {
	Name       azureMonitorMetricName `json:"name"`
	TimeSeries []azureMonitorSeries   `json:"timeseries"`
}

type azureMonitorMetricName struct {
	Value string `json:"value"`
}

type azureMonitorSeries struct {
	Data []azureMonitorMetricValue `json:"data"`
}

type azureMonitorMetricValue struct {
	Total *float64 `json:"total"`
}

// GetStorageAccountMetrics gets observed ingress, egress, and transaction
// metrics from Azure Monitor. The storageAccountResourceID must be a complete
// ARM resource ID for a Microsoft.Storage/storageAccounts resource.
func GetStorageAccountMetrics(
	ctx context.Context,
	httpClient *http.Client,
	tokenCredential azcore.TokenCredential,
	storageAccountResourceID string,
	startTime time.Time,
	endTime time.Time,
) (StorageAccountMetrics, error) {
	if httpClient == nil {
		return StorageAccountMetrics{}, errors.New("HTTP client must not be nil")
	}
	if tokenCredential == nil {
		return StorageAccountMetrics{}, errors.New("token credential must not be nil")
	}
	if err := validateStorageAccountResourceID(storageAccountResourceID); err != nil {
		return StorageAccountMetrics{}, err
	}
	if startTime.IsZero() || endTime.IsZero() || !startTime.Before(endTime) {
		return StorageAccountMetrics{}, errors.New("metric start time must be before end time")
	}

	requestURL, err := buildStorageAccountMetricsURL(storageAccountResourceID, startTime, endTime)
	if err != nil {
		return StorageAccountMetrics{}, fmt.Errorf("building Azure Monitor metrics URL: %w", err)
	}

	token, err := tokenCredential.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{azureResourceManagerScope},
	})
	if err != nil {
		return StorageAccountMetrics{}, fmt.Errorf("acquiring Azure Resource Manager token: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return StorageAccountMetrics{}, fmt.Errorf("creating Azure Monitor metrics request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token.Token)

	response, err := httpClient.Do(request)
	if err != nil {
		return StorageAccountMetrics{}, fmt.Errorf("executing Azure Monitor metrics request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		if readErr != nil {
			return StorageAccountMetrics{}, fmt.Errorf("Azure Monitor metrics request returned status %d; reading error response: %w", response.StatusCode, readErr)
		}
		return StorageAccountMetrics{}, fmt.Errorf("Azure Monitor metrics request returned status %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}

	var metricsResponse azureMonitorMetricsResponse
	if err := json.NewDecoder(response.Body).Decode(&metricsResponse); err != nil {
		return StorageAccountMetrics{}, fmt.Errorf("decoding Azure Monitor metrics response: %w", err)
	}

	return aggregateStorageAccountMetrics(metricsResponse)
}

func validateStorageAccountResourceID(resourceID string) error {
	parts := strings.Split(strings.Trim(resourceID, "/"), "/")
	if len(parts) != 8 ||
		!strings.EqualFold(parts[0], "subscriptions") ||
		parts[1] == "" ||
		!strings.EqualFold(parts[2], "resourceGroups") ||
		parts[3] == "" ||
		!strings.EqualFold(parts[4], "providers") ||
		!strings.EqualFold(parts[5], "Microsoft.Storage") ||
		!strings.EqualFold(parts[6], "storageAccounts") ||
		parts[7] == "" {
		return errors.New("storage account resource ID must have the form /subscriptions/{subscription}/resourceGroups/{resourceGroup}/providers/Microsoft.Storage/storageAccounts/{account}")
	}
	return nil
}

func buildStorageAccountMetricsURL(resourceID string, startTime time.Time, endTime time.Time) (string, error) {
	endpoint, err := url.Parse(azureMonitorMetricsAPIEndpoint)
	if err != nil {
		return "", err
	}
	endpoint.Path = strings.TrimRight(resourceID, "/") + "/providers/microsoft.insights/metrics"

	query := endpoint.Query()
	query.Set("api-version", azureMonitorMetricsAPIVersion)
	query.Set("metricnamespace", "Microsoft.Storage/storageAccounts")
	query.Set("metricnames", "Ingress,Egress,Transactions")
	query.Set("aggregation", "Total")
	query.Set("interval", "PT1M")
	query.Set("timespan", startTime.UTC().Format(time.RFC3339)+"/"+endTime.UTC().Format(time.RFC3339))
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

func aggregateStorageAccountMetrics(response azureMonitorMetricsResponse) (StorageAccountMetrics, error) {
	var result StorageAccountMetrics
	found := map[string]bool{
		"Ingress":      false,
		"Egress":       false,
		"Transactions": false,
	}

	for _, metric := range response.Value {
		name := metric.Name.Value
		if _, expected := found[name]; !expected {
			continue
		}
		found[name] = true

		for _, series := range metric.TimeSeries {
			for _, dataPoint := range series.Data {
				if dataPoint.Total == nil {
					continue
				}
				switch name {
				case "Ingress":
					result.IngressBytes += *dataPoint.Total
				case "Egress":
					result.EgressBytes += *dataPoint.Total
				case "Transactions":
					tps := *dataPoint.Total / 60
					if tps > result.PeakTransactionsPerSecond {
						result.PeakTransactionsPerSecond = tps
					}
				}
			}
		}
	}

	var missing []string
	for _, name := range []string{"Ingress", "Egress", "Transactions"} {
		if !found[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return StorageAccountMetrics{}, fmt.Errorf("Azure Monitor metrics response omitted requested metrics: %s", strings.Join(missing, ", "))
	}

	return result, nil
}
