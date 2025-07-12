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
		pacedBody, err := p.parent.GetPacedRequestBody(reqBody)
		if err != nil {
			return nil, fmt.Errorf("failed to get paced body: %w", err)
		}

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
		pacedBody, err := p.parent.GetPacedResponseBody(resp.Body, uint64(resp.ContentLength))
		if err != nil {
			return nil, fmt.Errorf("failed to get paced body: %w", err)
		}

		resp.Body = pacedBody
	}

	return resp, err
}
