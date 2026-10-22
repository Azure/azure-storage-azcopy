package cmd

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

// lookupTransport answers blob GetProperties with status/code and container
// listings with an empty result.
type lookupTransport struct {
	status int
	code   string
}

func (tr lookupTransport) Do(req *http.Request) (*http.Response, error) {
	resp := &http.Response{Request: req, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}
	if req.Method == http.MethodHead {
		resp.StatusCode = tr.status
		resp.Status = http.StatusText(tr.status)
		resp.Header.Set("x-ms-error-code", tr.code)
		return resp, nil
	}
	resp.StatusCode = http.StatusOK
	resp.Header.Set("Content-Type", "application/xml")
	resp.Body = io.NopCloser(strings.NewReader(`<?xml version="1.0" encoding="utf-8"?>` +
		`<EnumerationResults ServiceEndpoint="https://acct.blob.core.windows.net/" ContainerName="c"><Blobs /><NextMarker /></EnumerationResults>`))
	return resp, nil
}

func newLookupTestTraverser(t *testing.T, status int, code string) *blobTraverser {
	sc, err := service.NewClientWithNoCredential("https://acct.blob.core.windows.net/", &service.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: lookupTransport{status: status, code: code},
			Retry:     policy.RetryOptions{MaxRetries: -1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &blobTraverser{
		rawURL:                      "https://acct.blob.core.windows.net/c/dir/file.txt",
		serviceClient:               sc,
		ctx:                         context.Background(),
		incrementEnumerationCounter: enumerationCounterFuncNoop,
		failOnLookupError:           true,
	}
}

func TestBlobTraverserFailsOnLookupError(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{
		{http.StatusForbidden, "AuthorizationPermissionMismatch"},
		{http.StatusServiceUnavailable, "ServerBusy"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			err := newLookupTestTraverser(t, tc.status, tc.code).Traverse(nil, func(StoredObject) error { return nil }, nil)
			var respErr *azcore.ResponseError
			if !errors.As(err, &respErr) || respErr.StatusCode != tc.status || respErr.ErrorCode != tc.code {
				t.Fatalf("Traverse() = %v, want wrapped %d %s", err, tc.status, tc.code)
			}
		})
	}
}

func TestBlobTraverserLookupNotFoundIsNotAnError(t *testing.T) {
	var scheduled int
	err := newLookupTestTraverser(t, http.StatusNotFound, "BlobNotFound").Traverse(nil, func(StoredObject) error {
		scheduled++
		return nil
	}, nil)
	if err != nil || scheduled != 0 {
		t.Fatalf("Traverse() = %v with %d objects, want no error and nothing scheduled", err, scheduled)
	}
}
