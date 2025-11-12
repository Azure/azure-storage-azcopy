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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"net/http"
)

var (
	// ServiceAPIVersionOverride is a global variable in package ste which is a key to Service Api Version Value set in the every Job's context.
	ServiceAPIVersionOverride = serviceAPIVersionOverride{}
	// DefaultServiceApiVersion is the default value of service api version that is set as value to the ServiceAPIVersionOverride in every Job's context.
	DefaultServiceApiVersion = common.GetEnvironmentVariable(common.EEnvironmentVariable.DefaultServiceApiVersion())
)

const (
	XMsVersion = "x-ms-version"
)

type serviceAPIVersionOverride struct{}

type versionPolicy struct{}

func NewVersionPolicy() policy.Policy {
	return &versionPolicy{}
}

func (r *versionPolicy) Do(req *policy.Request) (*http.Response, error) {
	// get the service api version value using the ServiceAPIVersionOverride set in the context.
	if value := req.Raw().Context().Value(ServiceAPIVersionOverride); value != nil {
		req.Raw().Header[XMsVersion] = []string{value.(string)}
	}
	return req.Next()
}
