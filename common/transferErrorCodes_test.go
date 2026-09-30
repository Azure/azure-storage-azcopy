package common

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

func TestTransferFailureCodes(t *testing.T) {
	copySourceHeader := http.Header{}
	copySourceHeader.Set("X-Ms-Copy-Source-Error-Code", "BlobNotFound")

	testCases := []struct {
		name               string
		err                error
		wantStatus         int32
		wantServiceCode    string
		wantSourceCode string
	}{
		{name: "nil"},
		{name: "plain error", err: errors.New("boom")},
		{
			name:            "service error",
			err:             &azcore.ResponseError{StatusCode: 409, ErrorCode: "BlobImmutableDueToPolicy"},
			wantStatus:      409,
			wantServiceCode: "BlobImmutableDueToPolicy",
		},
		{
			name:            "wrapped service error",
			err:             fmt.Errorf("get properties: %w", &azcore.ResponseError{StatusCode: 403, ErrorCode: "AuthorizationPermissionMismatch"}),
			wantStatus:      403,
			wantServiceCode: "AuthorizationPermissionMismatch",
		},
		{
			name: "copy source error",
			err: &azcore.ResponseError{StatusCode: 404, ErrorCode: "CannotVerifyCopySource",
				RawResponse: &http.Response{StatusCode: 404, Header: copySourceHeader}},
			wantStatus:         404,
			wantServiceCode:    "CannotVerifyCopySource",
			wantSourceCode: "BlobNotFound",
		},
		{
			name:            "azcopy coded error",
			err:             fmt.Errorf("epilogue: %w", NewCodedError(TransferErrorCodeSourceModifiedDuringTransfer, "source modified during transfer")),
			wantServiceCode: TransferErrorCodeSourceModifiedDuringTransfer,
		},
		{
			name:            "source request error",
			err:             NewSourceError(&azcore.ResponseError{StatusCode: 404, ErrorCode: "BlobNotFound"}),
			wantStatus:      404,
			wantServiceCode: "BlobNotFound",
			wantSourceCode:  "BlobNotFound",
		},
		{
			name:            "destination not found is not a source code",
			err:             &azcore.ResponseError{StatusCode: 404, ErrorCode: "BlobNotFound"},
			wantStatus:      404,
			wantServiceCode: "BlobNotFound",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status, serviceCode, sourceCode := TransferFailureCodes(tc.err)
			if status != tc.wantStatus || serviceCode != tc.wantServiceCode || sourceCode != tc.wantSourceCode {
				t.Fatalf("TransferFailureCodes() = (%d, %q, %q), want (%d, %q, %q)",
					status, serviceCode, sourceCode, tc.wantStatus, tc.wantServiceCode, tc.wantSourceCode)
			}
		})
	}
}

func TestCodedErrorKeepsMessage(t *testing.T) {
	err := NewCodedError(TransferErrorCodeBlockCountExceedsLimit, "Number of blocks will exceed the limit")
	if err.Error() != "Number of blocks will exceed the limit" {
		t.Fatalf("Error() = %q", err.Error())
	}
	if NewSourceError(nil) != nil {
		t.Fatal("NewSourceError(nil) != nil")
	}
}
