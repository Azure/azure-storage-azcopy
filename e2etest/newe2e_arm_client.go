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
	"time"
)

type ARMSubject interface {
	Token() AccessToken
	Client() *ARMClient
	ManagementURI() url.URL
}

type ARMRequestPreparer interface {
	PrepareRequest(settings *ARMRequestSettings)
}

func CombineQuery(a, b url.Values) url.Values {
	out := make(url.Values)
	for k, v := range a {
		out[k] = append(out[k], v...)
	}
	for k, v := range b {
		out[k] = append(out[k], v...)
	}
	return out
}

// Ensure all types match interfaces
func init() {
	_ = []ARMSubject{&ARMClient{}, &ARMSubscription{}, &ARMResourceGroup{}}
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

type ARMClient struct {
	OAuth      AccessToken
	HttpClient *http.Client
}

func (c *ARMClient) Client() *ARMClient {
	return c
}

func (c *ARMClient) getHTTPClient() *http.Client {
	if c.HttpClient != nil {
		return c.HttpClient
	}

	return http.DefaultClient
}

func (c *ARMClient) Token() AccessToken {
	return c.OAuth
}

func (c *ARMClient) ManagementURI() url.URL {
	uri, err := url.Parse("https://management.azure.com/")
	common.PanicIfErr(err) // should never happen

	return *uri
}

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
	c := subject.Client()
	baseURI := subject.ManagementURI()
	client := c.getHTTPClient()

	if prep, ok := subject.(ARMRequestPreparer); ok {
		prep.PrepareRequest(&reqSettings)
	}

	r, err := reqSettings.CreateRequest(baseURI)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare request: %w", err)
	}

	oAuthToken, err := subject.Token().FreshToken()
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
			return ResolveAzureAsyncOperation(c.OAuth, newTarget, target)
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
