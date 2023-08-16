// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

type versionPolicy struct {
}

func newVersionPolicy() policy.Policy {
	return &versionPolicy{}
}

func (r *versionPolicy) Do(req *policy.Request) (*http.Response, error) {
	// get the service api version value using the ServiceAPIVersionOverride set in the context.
	if value := req.Raw().Context().Value(ServiceAPIVersionOverride); value != nil {
		req.Raw().Header["x-ms-version"] = []string{value.(string)}
	}
	return req.Next()
}

// TODO: Delete me when bumping the service version is no longer relevant.
type coldTierPolicy struct {
}

func newColdTierPolicy() policy.Policy {
	return &coldTierPolicy{}
}

func (r *coldTierPolicy) Do(req *policy.Request) (*http.Response, error) {
	if req.Raw().Header.Get("x-ms-access-tier") == common.EBlockBlobTier.Cold().String() {
		req.Raw().Header["x-ms-version"] = []string{"2021-12-02"}
	}
	return req.Next()
}

func NewTrailingDotPolicyFactory(trailingDot common.TrailingDotOption, from common.Location) pipeline.Factory {
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		return func(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
			if trailingDot == common.ETrailingDotOption.Enable() {
				request.Header.Set("x-ms-allow-trailing-dot", "true")
				if from == common.ELocation.File() {
					request.Header.Set("x-ms-source-allow-trailing-dot", "true")
				}
				request.Header.Set("x-ms-version", "2022-11-02")
			}
			return next.Do(ctx, request)
		}
	})
}

// TODO: Delete me when bumping the service version is no longer relevant.
type trailingDotPolicy struct {
	trailingDot *common.TrailingDotOption
	from *common.Location
}

func NewTrailingDotPolicy(trailingDot *common.TrailingDotOption, from *common.Location) policy.Policy {
	return &trailingDotPolicy{trailingDot: trailingDot, from: from}
}

func (r *trailingDotPolicy) Do(req *policy.Request) (*http.Response, error) {
	if r.trailingDot != nil && *r.trailingDot == common.ETrailingDotOption.Enable() {
		req.Raw().Header.Set("x-ms-allow-trailing-dot", "true")
		if r.from != nil && *r.from == common.ELocation.File() {
			req.Raw().Header.Set("x-ms-source-allow-trailing-dot", "true")
		}
		req.Raw().Header["x-ms-version"] = []string{"2022-11-02"}
	}
	return req.Next()
}