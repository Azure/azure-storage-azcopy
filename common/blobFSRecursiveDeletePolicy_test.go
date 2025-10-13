package common

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/stretchr/testify/assert"
)

type testRecursive struct {
	recursive string
}

func (t testRecursive) Do(req *policy.Request) (*http.Response, error) {
	if req.Raw().URL.Query().Has("recursive") {
		if req.Raw().URL.Query().Get("recursive") == t.recursive {
			return &http.Response{}, nil
		}
	}
	return &http.Response{}, fmt.Errorf("recursive query parameter not found or does not match expected value. expected: %s, actual: %s", t.recursive, req.Raw().URL.Query().Get("recursive"))
}

func TestRecursivePolicyExpectTrue(t *testing.T) {
	a := assert.New(t)
	ctx := WithRecursive(context.Background(), true)
	policies := []policy.Policy{NewRecursivePolicy(), testRecursive{"true"}}
	p := runtime.NewPipeline("testmodule", "v0.1.0", runtime.PipelineOptions{}, &policy.ClientOptions{Transport: nil, PerCallPolicies: policies})

	endpoints := []string{"https://xxxx.dfs.core.windows.net/container/path?recursive=true",
		"https://xxxx.dfs.core.windows.net/container/path?recursive=true&sig=xxxxxx&snapshot=xxxxx&timeout=xxxx",
		"https://xxxx.dfs.core.windows.net/container/path?sig=xxxxxx&recursive=true&snapshot=xxxxx&timeout=xxxx",
		"https://xxxx.dfs.core.windows.net/container/path?sig=xxxxxx&snapshot=xxxxx&timeout=xxxx&recursive=true",
		"https://xxxx.dfs.core.windows.net/container/path?recursive=false",
		"https://xxxx.dfs.core.windows.net/container/path?recursive=false&sig=xxxxxx&snapshot=xxxxx&timeout=xxxx",
		"https://xxxx.dfs.core.windows.net/container/path?sig=xxxxxx&recursive=false&snapshot=xxxxx&timeout=xxxx",
		"https://xxxx.dfs.core.windows.net/container/path?sig=xxxxxx&snapshot=xxxxx&timeout=xxxx&recursive=false"}

	for _, e := range endpoints {
		req, err := runtime.NewRequest(ctx, "HEAD", e)
		a.NoError(err)
		_, err = p.Do(req)
		a.NoError(err)
	}
}

func TestRecursivePolicyExpectFalse(t *testing.T) {
	a := assert.New(t)
	ctx := WithRecursive(context.Background(), false)
	policies := []policy.Policy{NewRecursivePolicy(), testRecursive{"false"}}
	p := runtime.NewPipeline("testmodule", "v0.1.0", runtime.PipelineOptions{}, &policy.ClientOptions{Transport: nil, PerCallPolicies: policies})

	endpoints := []string{"https://xxxx.dfs.core.windows.net/container/path?recursive=true",
		"https://xxxx.dfs.core.windows.net/container/path?recursive=true&sig=xxxxxx&snapshot=xxxxx&timeout=xxxx",
		"https://xxxx.dfs.core.windows.net/container/path?sig=xxxxxx&recursive=true&snapshot=xxxxx&timeout=xxxx",
		"https://xxxx.dfs.core.windows.net/container/path?sig=xxxxxx&snapshot=xxxxx&timeout=xxxx&recursive=true",
		"https://xxxx.dfs.core.windows.net/container/path?recursive=false",
		"https://xxxx.dfs.core.windows.net/container/path?recursive=false&sig=xxxxxx&snapshot=xxxxx&timeout=xxxx",
		"https://xxxx.dfs.core.windows.net/container/path?sig=xxxxxx&recursive=false&snapshot=xxxxx&timeout=xxxx",
		"https://xxxx.dfs.core.windows.net/container/path?sig=xxxxxx&snapshot=xxxxx&timeout=xxxx&recursive=false"}

	for _, e := range endpoints {
		req, err := runtime.NewRequest(ctx, "HEAD", e)
		a.NoError(err)
		_, err = p.Do(req)
		a.NoError(err)
	}
}

type testEndpoint struct {
	endpoint string
}

func (t testEndpoint) Do(req *policy.Request) (*http.Response, error) {
	if req.Raw().URL.String() == t.endpoint {
		return &http.Response{}, nil
	}
	return &http.Response{}, fmt.Errorf("recursive query parameter not found or does not match expected value. expected: %s, actual: %s", t.endpoint, req.Raw().URL.String())
}

func TestRecursivePolicyExpectNoChange(t *testing.T) {
	a := assert.New(t)

	endpoints := []string{"https://xxxx.dfs.core.windows.net/container/path?recursive=true",
		"https://xxxx.dfs.core.windows.net/container/path?recursive=true&sig=xxxxxx&snapshot=xxxxx&timeout=xxxx",
		"https://xxxx.dfs.core.windows.net/container/path?sig=xxxxxx&recursive=true&snapshot=xxxxx&timeout=xxxx",
		"https://xxxx.dfs.core.windows.net/container/path?sig=xxxxxx&snapshot=xxxxx&timeout=xxxx&recursive=true",
		"https://xxxx.dfs.core.windows.net/container/path?recursive=false",
		"https://xxxx.dfs.core.windows.net/container/path?recursive=false&sig=xxxxxx&snapshot=xxxxx&timeout=xxxx",
		"https://xxxx.dfs.core.windows.net/container/path?sig=xxxxxx&recursive=false&snapshot=xxxxx&timeout=xxxx",
		"https://xxxx.dfs.core.windows.net/container/path?sig=xxxxxx&snapshot=xxxxx&timeout=xxxx&recursive=false",
		"https://xxxx.dfs.core.windows.net/container/path",
		"https://xxxx.dfs.core.windows.net/container/path?sig=xxxxxx&snapshot=xxxxx&timeout=xxxx"}

	for _, e := range endpoints {
		policies := []policy.Policy{NewRecursivePolicy(), testEndpoint{e}}
		p := runtime.NewPipeline("testmodule", "v0.1.0", runtime.PipelineOptions{}, &policy.ClientOptions{Transport: nil, PerCallPolicies: policies})
		req, err := runtime.NewRequest(context.Background(), "HEAD", e)
		a.NoError(err)
		_, err = p.Do(req)
		a.NoError(err)
	}

}
