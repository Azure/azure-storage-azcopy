package pacer

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type (
	// pacerInjectKeyType is used to define pacerInjectKey to have a unique value for the context key
	pacerInjectKeyType struct{}
	// pacerInjectValue is the expected type of value behind pacerInjectKey.
	pacerInjectValue struct {
		pacer            Interface
		wrapMode         pacerInjectWrapMode
		expectedBodySize int64
	}
	// pacerInjectWrapMode defines how the pacer should be injected (
	pacerInjectWrapMode uint
)

const (
	// pacerInjectWrapModeNil is an invalid state, and will ignore the action if set, or pacer is nil.
	pacerInjectWrapModeNil pacerInjectWrapMode = iota
	// pacerInjectWrapModeRequest wraps the request body, and gates the request.
	pacerInjectWrapModeRequest
	// pacerInjectWrapModeResponse wraps the response body, and gates the request.
	pacerInjectWrapModeResponse
)

var (
	// pacerInjectKey see pacerInjectKeyType description
	pacerInjectKey = &pacerInjectKeyType{}
	// pacerInjectWarnOnce warns on LCM that something isn't right...
	pacerInjectWarnOnce = &sync.Once{}
)

type pacerInjectPolicy struct {
}

// NewPacerInjectPolicy creates a new policy (which should be added after retry policy). This policy, on it's own, does not wrap a request, but relies upon the pacer being injected.
func NewPacerInjectPolicy() policy.Policy {
	return &pacerInjectPolicy{}
}

func (p *pacerInjectPolicy) warn(warningText string) {
	pacerInjectWarnOnce.Do(func() {
		common.GetLifecycleMgr().Warn(warningText)
		common.AzcopyCurrentJobLogger.Log(common.LogWarning, warningText)
	})
}

func (p *pacerInjectPolicy) Do(req *policy.Request) (*http.Response, error) {
	if injectData, ok := req.Raw().Context().Value(pacerInjectKey).(pacerInjectValue); ok {

		if injectData.pacer == nil {
			p.warn("Sanity Check: Pacer inject key found, but pacer was nil. File a bug on AzCopy's github page if you see this.")
			return req.Next()
		}

		if injectData.wrapMode != pacerInjectWrapModeResponse && injectData.wrapMode != pacerInjectWrapModeRequest {
			p.warn("Sanity Check: Pacer inject key found, but wrap mode was undefined. File a bug on AzCopy's github page if you see this.")
			return req.Next()
		}

		pacerRequest := <-injectData.pacer.initiateRequest(injectData.expectedBodySize, req.Raw().Context())

		// for uploads, we wrap the request body.
		if injectData.wrapMode == pacerInjectWrapModeRequest {
			err := req.SetBody(pacerRequest.WrapRequestBody(req.Body()), req.Raw().Header.Get("Content-Type"))
			if err != nil {
				return nil, fmt.Errorf("error while wrapping body: %w", err)
			}
		}

		resp, err := req.Next()

		// for downloads, wrap the response body. Do not do this if the request failed.
		if err == nil && injectData.wrapMode == pacerInjectWrapModeResponse {
			resp.Body = pacerRequest.WrapResponseBody(resp.Body)
		}

		return resp, err
	}

	return req.Next()
}
