package e2etest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"time"
)

type ARMAsyncResponse[Props any] struct {
	ID              string  `json:"id"`
	Name            string  `json:"name"`
	Status          string  `json:"status"`
	StartTime       string  `json:"startTime"`
	EndTime         string  `json:"endTime"`
	PercentComplete float64 `json:"percentComplete"`
	// Set Properties to a pointer of your target struct, encoding/json will handle the magic.
	Properties *Props        `json:"properties"`
	Error      ARMAsyncError `json:"error"`
}

func (a ARMAsyncResponse[Props]) Validate() bool {
	return a.ID != "" &&
		a.Name != "" &&
		a.Status != "" &&
		a.StartTime != "" &&
		a.EndTime != "" // logical basic requirements
}

type ARMAsyncError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

const (
	ARMStatusSucceeded = "Succeeded"
	ARMStatusFailed    = "Failed"
	ARMStatusCanceled  = "Canceled"
)

func ResolveAzureAsyncOperation[Props any](OAuth AccessToken, uri string, properties *Props) (armResp *ARMAsyncResponse[Props], err error) {
	if properties != nil && reflect.TypeOf(properties).Kind() != reflect.Ptr {
		return nil, fmt.Errorf("properties must be a pointer (or nil)")
	}

	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var resp *http.Response
	for {
		oAuthToken, err := OAuth.FreshToken()
		if err != nil {
			return nil, fmt.Errorf("failed to get fresh token: %w", err)
		}
		// Update the OAuth token if we have to
		req.Header["Authorization"] = []string{"Bearer " + oAuthToken}

		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to send request: %w", err)
		}

		var buf []byte

		if resp.StatusCode != 200 && resp.StatusCode != 202 {
			buf, err = io.ReadAll(resp.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to read response body (resp code %d): %w", resp.StatusCode, err)
			}

			return nil, fmt.Errorf("failed to get access (resp code %d): %s", resp.StatusCode, string(buf))
		} else if resp.StatusCode == 200 {
			buf, err = io.ReadAll(resp.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to read response body (resp code 200): %w", err)
			}

			if len(buf) == 0 {
				return nil, nil
			}

			armResp = &ARMAsyncResponse[Props]{
				Properties: properties, // the user may have supplied a ptr to a struct, let encoding/json resolve that
			}

			err = json.Unmarshal(buf, armResp)
			if err != nil || !armResp.Validate() {
				if properties == nil {
					return nil, nil
				}

				// try parsing against just properties?
				err = json.Unmarshal(buf, properties)

				return nil, err
			}

			return armResp, err
		}

		if loc := resp.Header.Get("Location"); loc != "" {
			uri = loc
		}

		retryAfter := resp.Header.Get("Retry-after")
		waitSeconds := time.Second

		if retryAfter != "" {
			count, err := strconv.ParseInt(retryAfter, 10, 64)
			if err == nil {
				waitSeconds *= time.Duration(count)
			}
		}

		time.Sleep(waitSeconds)
	}
}
