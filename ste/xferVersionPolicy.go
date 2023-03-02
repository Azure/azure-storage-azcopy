package ste

import (
	"context"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"net/http"
)

type serviceAPIVersionOverride struct{}

// ServiceAPIVersionOverride is a global variable in package ste which is a key to Service Api Version Value set in the every Job's context.
var ServiceAPIVersionOverride = serviceAPIVersionOverride{}

// DefaultServiceApiVersion is the default value of service api version that is set as value to the ServiceAPIVersionOverride in every Job's context.
var DefaultServiceApiVersion = common.GetLifecycleMgr().GetEnvironmentVariable(common.EEnvironmentVariable.DefaultServiceApiVersion())

// NewVersionPolicyFactory creates a factory that can override the service version
// set in the request header.
// If the context has key overwrite-current-version set to false, then x-ms-version in
// request is not overwritten else it will set x-ms-version to 207-04-17
func NewVersionPolicyFactory() pipeline.Factory {
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		return func(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
			// get the service api version value using the ServiceAPIVersionOverride set in the context.
			if value := ctx.Value(ServiceAPIVersionOverride); value != nil {
				request.Header.Set("x-ms-version", value.(string))
			}
			resp, err := next.Do(ctx, request)
			return resp, err
		}
	})
}

type versionPolicy struct {
}

func newVersionPolicy() policy.Policy {
	return &versionPolicy{}
}

func (v *versionPolicy) Do(req *policy.Request) (*http.Response, error) {
	// get the service api version value using the ServiceAPIVersionOverride set in the context.
	if value := req.Raw().Context().Value(ServiceAPIVersionOverride); value != nil {
		req.Raw().Header.Set("x-ms-version", value.(string))
	}
	return req.Next()
}
