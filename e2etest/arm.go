package e2etest

import (
	"encoding/json"
	"fmt"
	"github.com/Azure/go-autorest/autorest/adal"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"time"
)

type ARMAsyncResponse struct {
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
	Status    string `json:"status"`

	// Set Properties to a pointer of your target struct, encoding/json will handle the magic.
	Properties interface{}   `json:"properties"`
	Name       string        `json:"name"`
	Error      ARMAsyncError `json:"error"`
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

func ResolveAzureAsyncOperation(OAuth *adal.ServicePrincipalToken, uri string, properties interface{}) (armResp *ARMAsyncResponse, err error) {
	if properties != nil && reflect.TypeOf(properties).Kind() != reflect.Ptr {
		return nil, fmt.Errorf("properties must be a pointer (or nil)")
	}

	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header["Authorization"] = []string{"Bearer " + OAuth.OAuthToken()}

	var resp *http.Response
	for {
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to send request: %w", err)
		}

		var buf []byte
		buf, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response body (resp code 200): %w", err)
		}

		armResp = &ARMAsyncResponse{
			Properties: properties, // the user may have supplied a ptr to a struct, let encoding/json resolve that
		}

		err = json.Unmarshal(buf, armResp)
		if err != nil {
			return nil, fmt.Errorf("failed to parse response body: %w", err)
		}

		if resp.StatusCode != 200 {
			rBody, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to read response body (resp code %d): %w", resp.StatusCode, err)
			}

			return nil, fmt.Errorf("failed to get access (resp code %d): %s", resp.StatusCode, string(rBody))
		}

		if armResp.Status == ARMStatusSucceeded || armResp.Status == ARMStatusCanceled || armResp.Status == ARMStatusFailed {
			switch armResp.Status {
			case ARMStatusFailed:
				var code, message string
				if armResp.Error.Code != "" {
					code = fmt.Sprintf(" (code %s)", armResp.Error.Code)
				}

				if armResp.Error.Message != "" {
					message = fmt.Sprintf(": %s", armResp.Error.Message)
				}

				return nil, fmt.Errorf("ARM job exited with failed status%s%s", code, message)
			case ARMStatusCanceled:
				return nil, fmt.Errorf("ARM async job was canceled")
			default:
				return
			}
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
