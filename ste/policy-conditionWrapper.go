package ste

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"net/http"
)

// PolicyInjector creates an open slot in a pipeline in which a
// policy can be retroactively injected into an existing pipeline
type PolicyInjector struct {
	policyKey any
}

func NewPolicyInjector(key any) policy.Policy {
	return &PolicyInjector{key}
}

func (p *PolicyInjector) Do(req *policy.Request) (*http.Response, error) {
	ctx := req.Raw().Context()

	newPolicy := ctx.Value(p.policyKey)

	if pol, ok := newPolicy.(policy.Policy); ok {
		return pol.Do(req)
	} else if pol != nil {
		return nil, fmt.Errorf("supplied policy for key %v was not a policy.Policy but a %v", p.policyKey, newPolicy)
	}

	return req.Next()
}
