package ste

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"net/http"
)

func (r *requestPolicyPacer) GetPolicy() policy.Policy {
	return &pacerPolicy{
		parent: r,
	}
}

type pacerPolicy struct {
	parent *requestPolicyPacer
}

func (p pacerPolicy) Do(req *policy.Request) (*http.Response, error) {
	err := req.RewindBody()
	if err != nil {
		return nil, fmt.Errorf("failed to rewind body: %w", err)
	}

	raw := req.Raw()

	if reqBody := req.Body(); reqBody != nil {
		pacedBody := p.parent.GetPacedRequestBody(reqBody, uint64(max(req.Raw().ContentLength, 0)))

		err = req.SetBody(pacedBody, raw.Header.Get("Content-Type"))
		if err != nil {
			return nil, fmt.Errorf("failed to set new body: %w", err)
		}
	}

	resp, err := req.Next()
	if err != nil {
		_ = req.Body().Close()
		return nil, err
	}

	if resp.Body != nil {
		pacedBody := p.parent.GetPacedResponseBody(resp.Body, uint64(max(resp.ContentLength, 0)))
		resp.Body = pacedBody
	}

	return resp, err
}
