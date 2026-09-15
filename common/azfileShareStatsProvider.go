package common

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

// Refresh the poll token once it has less than this left, matching ste.sourceAuthPolicy.
const shareStatsMinTokenValidDuration = 5 * time.Minute

// ShareStatsProvider fetches throttling statistics from the Azure Files GetShareStats API.
// It issues a raw HTTP GET with x-ms-file-return-throttling-stats: true,
// since the current Azure SDK does not model the ShareThrottlingStats response block.
type ShareStatsProvider struct {
	// shareURL is the base URL of the share (may include SAS token).
	shareURL string

	// httpClient is used for the raw request.
	httpClient *http.Client

	// tokenCred authenticates the request when shareURL carries no SAS. Nil for SAS/anonymous.
	tokenCred azcore.TokenCredential

	tokenMu sync.RWMutex
	token   *azcore.AccessToken

	// logger for diagnostics.
	logger ILogger
}

// NewShareStatsProvider creates a provider that can fetch throttling stats for the given share.
// shareURL should be the full share URL as returned by share.Client.URL() (includes SAS if present).
// tokenCred must be supplied when the URL has no SAS, and should be the same credential the
// data plane uses for this account.
// Diagnostics go through LogToJobLogWithPrefix rather than logger, because the
// injected logger is nil when this is constructed from MainSTE.
func NewShareStatsProvider(shareURL string, httpClient *http.Client, logger ILogger, tokenCred azcore.TokenCredential) *ShareStatsProvider {
	LogToJobLogWithPrefix(fmt.Sprintf("[sharestats] initializing provider for %s (oauth=%t)", shareURL, tokenCred != nil), LogInfo)
	return &ShareStatsProvider{
		shareURL:   shareURL,
		httpClient: httpClient,
		tokenCred:  tokenCred,
		logger:     logger,
	}
}

// FetchStats calls the GetShareStats API with the throttling stats header and returns the parsed response.
func (p *ShareStatsProvider) FetchStats(ctx context.Context) (*ShareStatsResponse, error) {
	reqURL, err := p.buildRequestURL()
	if err != nil {
		LogToJobLogWithPrefix(fmt.Sprintf("[sharestats] building GetShareStats URL: %v", err), LogError)
		return nil, fmt.Errorf("building GetShareStats URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		LogToJobLogWithPrefix(fmt.Sprintf("[sharestats] creating GetShareStats request: %v", err), LogError)
		return nil, fmt.Errorf("creating GetShareStats request: %w", err)
	}

	// Required headers
	req.Header.Set("x-ms-file-return-throttling-stats", "true")	
	req.Header.Set("x-ms-version", enum.EEnvironmentVariable.DefaultServiceApiVersion().Get())
	req.Header.Set("x-ms-date", time.Now().UTC().Format(http.TimeFormat))

	// Mirrors the data plane: OAuth against Azure Files also requires a request intent.
	if p.tokenCred != nil {
		tok, tokErr := p.bearerToken(ctx)
		if tokErr != nil {
			LogToJobLogWithPrefix(fmt.Sprintf("[sharestats] acquiring token for GetShareStats: %v", tokErr), LogError)
			return nil, fmt.Errorf("acquiring token for GetShareStats: %w", tokErr)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("x-ms-file-request-intent", "backup")
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		LogToJobLogWithPrefix(fmt.Sprintf("[sharestats] executing GetShareStats request: %v", err), LogError)
		return nil, fmt.Errorf("executing GetShareStats request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		LogToJobLogWithPrefix(fmt.Sprintf("[sharestats] GetShareStats returned status %d: %s", resp.StatusCode, string(body)), LogError)
		return nil, fmt.Errorf("GetShareStats returned status %d: %s", resp.StatusCode, string(body))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		LogToJobLogWithPrefix(fmt.Sprintf("[sharestats] reading GetShareStats response body: %v", err), LogError)
		return nil, fmt.Errorf("reading GetShareStats response body: %w", err)
	}

	var stats ShareStatsResponse
	if err := xml.Unmarshal(bodyBytes, &stats); err != nil {
		LogToJobLogWithPrefix(fmt.Sprintf("[sharestats] parsing GetShareStats XML response: %v", err), LogError)
		return nil, fmt.Errorf("parsing GetShareStats XML response: %w", err)
	}

	return &stats, nil
}

// bearerToken returns a cached access token, refreshing only when it is close to
// expiry. Mirrors ste.sourceAuthPolicy so the 30s-per-share poll loop does not
// hit the credential on every tick.
func (p *ShareStatsProvider) bearerToken(ctx context.Context) (string, error) {
	p.tokenMu.RLock()
	if p.token != nil && time.Until(p.token.ExpiresOn) >= shareStatsMinTokenValidDuration {
		tok := p.token.Token
		p.tokenMu.RUnlock()
		return tok, nil
	}
	p.tokenMu.RUnlock()

	p.tokenMu.Lock()
	defer p.tokenMu.Unlock()
	// Another goroutine may have refreshed while we waited for the write lock.
	if p.token != nil && time.Until(p.token.ExpiresOn) >= shareStatsMinTokenValidDuration {
		return p.token.Token, nil
	}
	tok, err := p.tokenCred.GetToken(ctx, policy.TokenRequestOptions{
		Scopes:    []string{cred.StorageScope},
		EnableCAE: true,
	})
	if err != nil {
		return "", err
	}
	p.token = &tok
	return tok.Token, nil
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
