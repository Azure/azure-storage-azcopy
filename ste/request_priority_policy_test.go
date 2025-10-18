package ste

import (
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/stretchr/testify/assert"
)

type FunctionTransporter struct {
	doFunc func(req *http.Request) (*http.Response, error)
}

func (f FunctionTransporter) Do(req *http.Request) (*http.Response, error) {
	return f.doFunc(req)
}

func TestNewRequestPriorityPolicy(t *testing.T) {
	reqPrio := 1

	c, err := blob.NewClientWithNoCredential("https://acct.blob.core.windows.net/ct/blob", &blob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			PerCallPolicies: []policy.Policy{requestPriorityPolicy{priorityData: &reqPrio}, NewVersionPolicy()},

			Transport: FunctionTransporter{
				doFunc: func(req *http.Request) (*http.Response, error) {
					assert.Equal(t, strconv.Itoa(reqPrio), req.Header.Get(XMsRequestPriority))

					dateList := req.Header[XMsVersion]
					assert.Equal(t, 1, len(dateList))
					assert.Equal(t, requestPriorityDateString, dateList[0])

					return &http.Response{}, nil // we don't really care
				},
			},
		},
	})
	assert.NoError(t, err)

	_, _ = c.GetProperties(context.Background(), &blob.GetPropertiesOptions{})
	reqPrio = 2
	_, _ = c.GetProperties(context.Background(), &blob.GetPropertiesOptions{})
}
