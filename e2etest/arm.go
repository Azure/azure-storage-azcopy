package e2etest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
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
	return a.Name != "" &&
		a.Status != "" &&
		a.StartTime != "" // logical basic requirements
}

type ARMAsyncError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

const (
	ARMStatusSucceeded    = "Succeeded"
	ARMStatusFailed       = "Failed"
	ARMStatusCanceled     = "Canceled"
	ARMStatusInProgress   = "InProgress"
	ARMStatusRunning      = "Running"
	ARMStatusResolvingDNS = "ResolvingDNS"
)

// ResolveAzureAsyncOperation implements https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/async-operations
func ResolveAzureAsyncOperation[Props any](OAuth AccessToken, uri string, properties *Props) (armResp *ARMAsyncResponse[Props], err error) {
	if properties != nil && reflect.TypeOf(properties).Kind() != reflect.Ptr {
		return nil, fmt.Errorf("properties must be a pointer (or nil)")
	}

	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var resp *http.Response
	var lastWaitSeconds int64 = 0
	for {
		if lastWaitSeconds == 0 {
			lastWaitSeconds = 1 // pretend we waited, proceed to the initial request.
		} else {
			fmt.Println("Sleeping", lastWaitSeconds, "seconds")
			time.Sleep(time.Second * time.Duration(lastWaitSeconds))
		}

		oAuthToken, err := OAuth.FreshToken()
		if err != nil {
			return nil, fmt.Errorf("failed to get fresh token: %w", err)
		}
		// Update the OAuth token if we have to
		req.Header["Authorization"] = []string{"Bearer " + oAuthToken}

		armResp = &ARMAsyncResponse[Props]{
			Properties: properties, // the user may have supplied a ptr to a struct, let encoding/json resolve that
		}

		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to send request: %w", err)
		}

		/*
			Lessons learned from past attempts:
			- The body will not always be an ARMAsyncResponse
			- The body will not always be present.
			- The body not being present is not indicative that the job has finished.
			- The body not being present is not indicative that the job has not finished.
			- The response code shouldn't be trusted if there's a body.
			- The response code shouldn't be trusted if there's a followup location.
		*/

		// First things first. Let's pull our retry values.
		// There are two different places retry values can come back. `Location` and `Azure-AsyncOperation`
		followUpLoc := resp.Header.Get("Location")
		if followUpLoc == "" {
			followUpLoc = resp.Header.Get("Azure-AsyncOperation")
		}
		if followUpLoc != "" {
			uri = followUpLoc
		}

		// Let's see if we can find out how long to wait.
		// This can appear *sometimes*, but not always.
		retryAfterRaw := resp.Header.Get("Retry-After")
		var retryAfter int64
		if retryAfterRaw != "" {
			count, err := strconv.ParseInt(retryAfterRaw, 10, 32)
			if err != nil {
				retryAfter = count
			}
		}
		if retryAfter == 0 { // Fall back to our last wait, exponential, capped to 60s.
			retryAfter = lastWaitSeconds * 2
			retryAfter = common.Iff(retryAfter > 60, 60, retryAfter)
			lastWaitSeconds = retryAfter
		}

		// If the body is nonzero, we should read it.
		// This might contain status info that is more reliable than the response code (why? good question, that's why.)
		if resp.ContentLength != 0 {
			buf, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to read response body (resp code %d): %w", resp.StatusCode, err)
			}

			// Sometimes, this body *can* be an ARMAsyncResponse! If it is, this is great and useful information.
			// Other times, it may include "provisioningState":
			// Let's check!
			rawResp := map[string]any{}
			err = json.Unmarshal(buf, &rawResp)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal body to raw struct: %w", err)
			}

			search := func(data map[string]any, key string) (any, bool) {
				queue := strings.Split(key, "/")
				for k, v := range queue {
					value, ok := data[v]
					if !ok {
						return nil, false
					}

					if k+1 == len(queue) {
						return value, ok
					} else {
						data, ok = value.(map[string]any)
						if !ok {
							return nil, false
						}
					}
				}

				// Go can't properly detect that this is unreachable, but we'll always hit
				panic("unreachable code")
			}

			usesARMStatus := false
			if status, ok := rawResp["status"]; ok {
				usesARMStatus = true

				if status == ARMStatusInProgress || status == ARMStatusRunning || status == ARMStatusResolvingDNS {
					continue
				}
			} else if status, ok = search(rawResp, "properties/provisioningState"); ok {
				// workaround for storage accounts.
				// todo: this will probably burn us eventually, but it's the only exception listed on the docs page, and so far the only one we've encountered.
				strStatus, ok := status.(string)
				if ok && (strStatus == ARMStatusInProgress || strStatus == ARMStatusRunning || strStatus == ARMStatusResolvingDNS) {
					continue
				}
			}

			if usesARMStatus {
				err = json.Unmarshal(buf, &armResp)
				return armResp, err
			} else {
				err = json.Unmarshal(buf, &properties)
				return nil, err
			}
		} else {
			if followUpLoc != "" { // Continue if there's a follow-up location
				continue
			} else {
				// Quoth the documentation: If no value is returned for provisioningState, the operation finished and succeeded.
				return nil, nil
			}
		}
	}
}
