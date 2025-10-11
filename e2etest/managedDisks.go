package e2etest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Compute/disks/{diskName}/beginGetAccess?api-version=2021-12-01

func (config *ManagedDiskConfig) GetMDURL() (*url.URL, error) {
	if config.DiskName == "" || config.ResourceGroupName == "" || config.SubscriptionID == "" {
		return nil, fmt.Errorf("one or more important details are missing in the config")
	}

	// the API is the same, but the provider is different
	uriFormat := common.Iff(config.isSnapshot,
		"https://management.azure.com/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/snapshots/%s?api-version=2023-04-02",
		"https://management.azure.com/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/%s?api-version=2023-04-02")

	uri := fmt.Sprintf(uriFormat, config.SubscriptionID, config.ResourceGroupName, config.DiskName)
	out, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URI (maybe some detail of the config was formatted invalid?)")
	}

	return out, nil
}

type ManagedDiskGetAccessResponse struct {
	AccessSAS             string `json:"accessSAS"`
	SecurityDataAccessSAS string `json:"securityDataAccessSAS"`
}

func (config *ManagedDiskConfig) GetAccess() (*url.URL, error) {
	mdURL, err := config.GetMDURL()
	if err != nil {
		return nil, fmt.Errorf("failed to get managed disk URL: %w", err)
	}

	mdURL.Path = path.Join(mdURL.Path, "beginGetAccess")

	var requestBody = map[string]interface{}{
		"access":            "Read", // for the moment, we'll only worry about download.
		"durationInSeconds": 3600,
	}

	body, _ := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body")
	}
	buf := bytes.NewBuffer(body)

	req, err := http.NewRequest("POST", mdURL.String(), buf)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize request: %w", err)
	}

	oauthToken, err := config.oauth.FreshToken()
	if err != nil {
		return nil, fmt.Errorf("failed to get fresh OAuth token: %w", err)
	}
	req.Header["Authorization"] = []string{"Bearer " + oauthToken}
	req.Header["Content-Type"] = []string{"application/json; charset=utf-8"}
	req.Header["Accept"] = []string{"application/json; charset=utf-8"}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	var getAccessResp ManagedDiskGetAccessResponse

	if resp.StatusCode != 200 {
		if resp.StatusCode == 202 { // async operation
			// Grab the azure-asyncoperation header
			newTarget := resp.Header.Get("Azure-Asyncoperation")
			_, err = ResolveAzureAsyncOperation(config.oauth, newTarget, &struct {
				Output *ManagedDiskGetAccessResponse `json:"output"`
			}{Output: &getAccessResp}) // no need to get the whole struct, json resolve will place data in our getAccessResp
			if err != nil {
				return nil, fmt.Errorf("failed to get access (async op): %w", err)
			}
		} else { // error
			rBody, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to read response body (resp code %d): %w", resp.StatusCode, err)
			}

			return nil, fmt.Errorf("failed to get access (resp code %d): %s", resp.StatusCode, string(rBody))
		}
	} else { // immediate response
		rBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response body: %w", err)
		}

		err = json.Unmarshal(rBody, &getAccessResp)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal response body")
		}
	}

	out, err := url.Parse(getAccessResp.AccessSAS)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SAS")
	}

	return out, nil
}

func (config *ManagedDiskConfig) RevokeAccess() error {
	url, err := config.GetMDURL()
	if err != nil {
		return fmt.Errorf("failed to get managed disk URL: %w", err)
	}

	url.Path = path.Join(url.Path, "endGetAccess")

	req, err := http.NewRequest("POST", url.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to initialize request: %w", err)
	}

	tok, err := config.oauth.FreshToken()
	if err != nil {
		return fmt.Errorf("failed to ensure OAuth token is fresh: %w", err)
	}
	req.Header["Authorization"] = []string{"Bearer " + tok}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	if resp.StatusCode != 200 {
		if resp.StatusCode == 202 {
			newTarget := resp.Header.Get("Azure-Asyncoperation")
			_, err := ResolveAzureAsyncOperation[any](config.oauth, newTarget, nil)

			return err
		}

		rBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body (resp code %d): %w", resp.StatusCode, err)
		}

		return fmt.Errorf("failed to revoke access (resp code %d): %s", resp.StatusCode, string(rBody))
	}

	return nil
}
