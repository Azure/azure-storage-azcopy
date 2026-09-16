package e2etest

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/require"
)

type fileListingFailureAsserter struct {
	Asserter
	errors []error
}

func (asserter *fileListingFailureAsserter) NoError(_ string, err error, _ ...bool) {
	if err != nil {
		asserter.errors = append(asserter.errors, err)
	}
}

func TestFileListingFailureDoesNotPanic(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("comp") != "list" {
			response.Header().Set("x-ms-share-quota", "1")
			response.WriteHeader(http.StatusOK)
			return
		}
		response.Header().Set("x-ms-error-code", "ServerBusy")
		response.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := share.NewClientWithNoCredential(server.URL+"/share", &share.ClientOptions{
		ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}},
	})
	require.NoError(t, err)
	manager := &FileShareResourceManager{InternalClient: client}
	asserter := &fileListingFailureAsserter{Asserter: NewFrameworkAsserter(t)}
	require.NotPanics(t, func() { require.Nil(t, manager.ListObjects(asserter, "", true)) })
	require.Len(t, asserter.errors, 1)
	var responseError *azcore.ResponseError
	require.ErrorAs(t, asserter.errors[0], &responseError)
	require.Equal(t, http.StatusServiceUnavailable, responseError.StatusCode)
	require.Equal(t, "ServerBusy", responseError.ErrorCode)
}
