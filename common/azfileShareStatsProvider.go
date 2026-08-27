package common

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

// ShareStatsProvider fetches throttling statistics from the Azure Files GetShareStats API.
// It issues a raw HTTP GET with x-ms-file-return-throttling-stats: true,
// since the current Azure SDK does not model the ShareThrottlingStats response block.
type ShareStatsProvider struct {
	// shareURL is the base URL of the share (may include SAS token).
	shareURL string

	// httpClient is used for the raw request.
	httpClient *http.Client

	// logger for diagnostics.
	logger ILogger
}

// NewShareStatsProvider creates a provider that can fetch throttling stats for the given share.
// shareURL should be the full share URL as returned by share.Client.URL() (includes SAS if present).
func NewShareStatsProvider(shareURL string, httpClient *http.Client, logger ILogger) *ShareStatsProvider {
	// Add trace logging for initialization.
	logger.Log(LogInfo, fmt.Sprintf("Initializing ShareStatsProvider for %s", shareURL))
	return &ShareStatsProvider{
		shareURL:   shareURL,
		httpClient: httpClient,
		logger:     logger,
	}
}

// FetchStats calls the GetShareStats API with the throttling stats header and returns the parsed response.
func (p *ShareStatsProvider) FetchStats(ctx context.Context) (*ShareStatsResponse, error) {
	reqURL, err := p.buildRequestURL()
	if err != nil {
		p.logger.Log(LogError, fmt.Sprintf("building GetShareStats URL: %v", err))
		return nil, fmt.Errorf("building GetShareStats URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		p.logger.Log(LogError, fmt.Sprintf("creating GetShareStats request: %v", err))
		return nil, fmt.Errorf("creating GetShareStats request: %w", err)
	}

	// Required headers
	req.Header.Set("x-ms-file-return-throttling-stats", "true")	
	req.Header.Set("x-ms-version", enum.EEnvironmentVariable.DefaultServiceApiVersion().Get())
	req.Header.Set("x-ms-date", time.Now().UTC().Format(http.TimeFormat))

	resp, err := p.httpClient.Do(req)
	if err != nil {
		p.logger.Log(LogError, fmt.Sprintf("Error executing GetShareStats request %v",  err))
		return nil, fmt.Errorf("executing GetShareStats request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		p.logger.Log(LogError, fmt.Sprintf("GetShareStats returned status %d: %s", resp.StatusCode, string(body)))
		return nil, fmt.Errorf("GetShareStats returned status %d: %s", resp.StatusCode, string(body))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		p.logger.Log(LogError, fmt.Sprintf("Error reading GetShareStats response body: %v", err))
		return nil, fmt.Errorf("reading GetShareStats response body: %w", err)
	}

	var stats ShareStatsResponse
	if err := xml.Unmarshal(bodyBytes, &stats); err != nil {
		p.logger.Log(LogError, fmt.Sprintf("Error parsing GetShareStats XML response: %v", err))
		return nil, fmt.Errorf("parsing GetShareStats XML response: %w", err)
	}

	return &stats, nil
}

// buildRequestURL constructs the full URL with query params restype=share&comp=stats.
// If the share URL already contains a SAS token, those params are preserved.
func (p *ShareStatsProvider) buildRequestURL() (string, error) {
	parsed, err := url.Parse(p.shareURL)
	if err != nil {
		return "", err
	}

	// Remove any trailing slash
	parsed.Path = strings.TrimRight(parsed.Path, "/")

	q := parsed.Query()
	q.Set("restype", "share")
	q.Set("comp", "stats")
	parsed.RawQuery = q.Encode()

	return parsed.String(), nil
}
