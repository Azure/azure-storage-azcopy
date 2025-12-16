package e2etest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	ARMRootURI = "https://management.azure.com/"
)

type ARMRequestSettings struct { // All values will be added to the request
	Method        string
	PathExtension string
	Query         url.Values
	Headers       http.Header
	Body          interface{}
}

func (s *ARMRequestSettings) CreateRequest(baseURI url.URL) (*http.Request, error) {
	query := baseURI.RawQuery
	if len(query) > 0 {
		query += "&"
	}
	query += s.Query.Encode()
	baseURI.RawQuery = query

	var body io.ReadSeeker
	if s.Body != nil {
		buf, err := json.Marshal(s.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal body: %w", err)
		}

		body = bytes.NewReader(buf)
	}

	newReq, err := http.NewRequest(s.Method, baseURI.String(), body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	newReq.Header = s.Headers

	if s.PathExtension != "" {
		newReq.URL = newReq.URL.JoinPath(s.PathExtension)
	}

	return newReq, nil
}

// PerformRequest will deserialize to target (which assumes the target is a pointer)
// If an LRO is required, an *ARMAsyncResponse will be returned. Otherwise, both armResp and err will be nil, and target will be written to.
func PerformRequest[Props any](subject ARMSubject, reqSettings ARMRequestSettings, target *Props) (armResp *ARMAsyncResponse[Props], err error) {
retry:
	var baseURI url.URL

	if uriGetter, ok := subject.(ARMCustomManagementURI); ok {
		baseURI = uriGetter.managementURI()
	} else {
		res, _ := url.Parse(ARMRootURI)
		res, err = res.Parse(subject.CanonicalPath())
		if err != nil {
			return nil, fmt.Errorf("failed to parse request uri: %w", err)
		}

		baseURI = *res
	}
	client := subject.httpClient()

	if prep, ok := subject.(ARMRequestPreparer); ok {
		prep.PrepareRequest(&reqSettings)
	}

	r, err := reqSettings.CreateRequest(baseURI)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare request: %w", err)
	}

	oAuthToken, err := subject.token().FreshToken()
	r.Header = make(http.Header)
	r.Header["Authorization"] = []string{"Bearer " + oAuthToken}
	r.Header["Content-Type"] = []string{"application/json; charset=utf-8"}
	r.Header["Accept"] = []string{"application/json; charset=utf-8"}

	resp, err := client.Do(r)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	switch resp.StatusCode {
	case 202: // LRO pattern; grab Azure-AsyncOperation and resolve it.
		newTarget := resp.Header.Get("Azure-Asyncoperation")
		if newTarget == "" {
			newTarget = resp.Header.Get("Location")
		}

		if newTarget != "" {
			return ResolveAzureAsyncOperation(subject.token(), newTarget, target)
		} else if resp.Header.Get("Content-Length") == "0" {
			return nil, fmt.Errorf("failed to handle async operation: no response data, Azure-Asyncoperation and Location are not found")
		}

		// If we don't have an asyncop to check against, pull the body
		fallthrough
	case 200, 201: // immediate response
		var buf []byte // Read the body
		buf, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response body (resp code 200): %w", err)
		}

		if len(buf) != 0 && target != nil {
			err = json.Unmarshal(buf, target)
			if err != nil {
				return nil, fmt.Errorf("failed to parse response body: %w", err)
			}
		}

		return nil, nil
	case 503: // This is usually just... retryable.
		time.Sleep(time.Second * 5)
		goto retry
	default:
		rBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response body (resp code %d): %w", resp.StatusCode, err)
		}

		return nil, fmt.Errorf("failed to get access (resp code %d): %s", resp.StatusCode, string(rBody))
	}
}

// ARMUnimplementedStruct is used to fill in the blanks in types when implementation doesn't seem like a good use of time.
// It can be explored with "Get", if you know precisely what you want.
type ARMUnimplementedStruct json.RawMessage

func (s ARMUnimplementedStruct) Get(Key []string, out interface{}) error {
	if reflect.TypeOf(out).Kind() != reflect.Pointer {
		return errors.New("")
	}

	object := s
	for len(Key) > 0 {
		dict := make(map[string]json.RawMessage)
		err := json.Unmarshal(object, &dict)
		if err != nil {
			return err
		}

		object = ARMUnimplementedStruct(dict[Key[0]])
	}

	return json.Unmarshal(object, out)
}

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
