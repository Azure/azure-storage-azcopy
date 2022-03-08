package ste

import (
	"context"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type mdOAuthRequirementState uint8

const (
	mdOAuthRequirementStateUntested mdOAuthRequirementState = iota
	mdOAuthRequirementStateNotNeeded
	mdOAuthRequirementStateNeeded
)

type MDOAuthCredentialWrapper struct {
	mdOAuthCheck  *sync.Once
	mdOAuthResult mdOAuthRequirementState
	tokenInfo     azblob.Credential
}

func NewMDOAuthCredentialWrapper(token azblob.Credential) *MDOAuthCredentialWrapper {
	return &MDOAuthCredentialWrapper{
		mdOAuthCheck:  &sync.Once{},
		mdOAuthResult: mdOAuthRequirementStateUntested,
		tokenInfo:     token,
	}
}

// OverrideMDOAuthCheck forces MDOAuth on for the remaining lifecycle of the policy
func (m *MDOAuthCredentialWrapper) OverrideMDOAuthCheck() {
	m.mdOAuthCheck.Do(func() {
		m.mdOAuthResult = mdOAuthRequirementStateNeeded
	})
}

func (m *MDOAuthCredentialWrapper) New(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.Policy {
	pfunc := func(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
		var resp pipeline.Response
		var err error
		var didFunc bool

		m.mdOAuthCheck.Do(func() {
			resp, err = next.Do(ctx, request)

			if resp == nil {
				didFunc = true
				m.mdOAuthResult = mdOAuthRequirementStateNotNeeded
				return // this is what we can't handle.
			}

			if httpResp := resp.Response(); httpResp.StatusCode == 401 {
				challenge := httpResp.Header.Get("WWW-Authenticate")
				if challenge != "" {
					m.mdOAuthResult = mdOAuthRequirementStateNeeded
				} else {
					m.mdOAuthResult = mdOAuthRequirementStateNotNeeded
				}
			} else {
				didFunc = true
				m.mdOAuthResult = mdOAuthRequirementStateNotNeeded
			}
		})

		if didFunc { // No need to re-do the rest of the pipeline if the first one succeeded (or failed in an unhandled fashion)
			return resp, err
		}

		switch m.mdOAuthResult {
		case mdOAuthRequirementStateNotNeeded:
			return next.Do(ctx, request) // no action needed
		case mdOAuthRequirementStateNeeded:
			return m.tokenInfo.New(next, po).Do(ctx, request) // Inject the OAuth token into the pipeline
		default:
			panic("Invalid managed disk OAuth test state")
		}
	}

	return pipeline.PolicyFunc(pfunc)
}
