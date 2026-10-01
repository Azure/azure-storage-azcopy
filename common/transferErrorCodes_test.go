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
	copySourceHeader.Set("X-Ms-Copy-Source-Status-Code", "404")

	copySourceAuthHeader := http.Header{}
	copySourceAuthHeader.Set("X-Ms-Copy-Source-Error-Code", "InvalidAuthenticationInfo")
	copySourceAuthHeader.Set("X-Ms-Copy-Source-Status-Code", "401")

	badStatusHeader := http.Header{}
	badStatusHeader.Set("X-Ms-Copy-Source-Error-Code", "BlobNotFound")
	badStatusHeader.Set("X-Ms-Copy-Source-Status-Code", "not-a-number")

	testCases := []struct {
		name string
		err  error
		want TransferErrorCodes
	}{
		{name: "nil"},
		{name: "plain error", err: errors.New("boom")},
		{
			name: "service error",
			err:  &azcore.ResponseError{StatusCode: 409, ErrorCode: "BlobImmutableDueToPolicy"},
			want: TransferErrorCodes{StatusCode: 409, ExtendedErrorCode: "BlobImmutableDueToPolicy"},
		},
		{
			name: "wrapped service error",
			err:  fmt.Errorf("get properties: %w", &azcore.ResponseError{StatusCode: 403, ErrorCode: "AuthorizationPermissionMismatch"}),
			want: TransferErrorCodes{StatusCode: 403, ExtendedErrorCode: "AuthorizationPermissionMismatch"},
		},
		{
			name: "copy source error",
			err: &azcore.ResponseError{StatusCode: 404, ErrorCode: "CannotVerifyCopySource",
				RawResponse: &http.Response{StatusCode: 404, Header: copySourceHeader}},
			want: TransferErrorCodes{StatusCode: 404, ExtendedErrorCode: "CannotVerifyCopySource",
				S2SStatusCode: 404, S2SExtendedErrorCode: "BlobNotFound"},
		},
		{
			name: "copy source status differs from destination status",
			err: &azcore.ResponseError{StatusCode: 403, ErrorCode: "CannotVerifyCopySource",
				RawResponse: &http.Response{StatusCode: 403, Header: copySourceAuthHeader}},
			want: TransferErrorCodes{StatusCode: 403, ExtendedErrorCode: "CannotVerifyCopySource",
				S2SStatusCode: 401, S2SExtendedErrorCode: "InvalidAuthenticationInfo"},
		},
		{
			name: "copy source status unparsable",
			err: &azcore.ResponseError{StatusCode: 404, ErrorCode: "CannotVerifyCopySource",
				RawResponse: &http.Response{StatusCode: 404, Header: badStatusHeader}},
			want: TransferErrorCodes{StatusCode: 404, ExtendedErrorCode: "CannotVerifyCopySource",
				S2SExtendedErrorCode: "BlobNotFound"},
		},
		{
			name: "azcopy coded error",
			err:  fmt.Errorf("epilogue: %w", NewCodedError(TransferErrorCodeSourceModifiedDuringTransfer, "source modified during transfer")),
			want: TransferErrorCodes{ExtendedErrorCode: TransferErrorCodeSourceModifiedDuringTransfer},
		},
		{
			name: "source request error",
			err:  NewSourceError(&azcore.ResponseError{StatusCode: 404, ErrorCode: "BlobNotFound"}),
			want: TransferErrorCodes{StatusCode: 404, ExtendedErrorCode: "BlobNotFound",
				S2SStatusCode: 404, S2SExtendedErrorCode: "BlobNotFound"},
		},
		{
			name: "destination not found is not a source code",
			err:  &azcore.ResponseError{StatusCode: 404, ErrorCode: "BlobNotFound"},
			want: TransferErrorCodes{StatusCode: 404, ExtendedErrorCode: "BlobNotFound"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := TransferFailureCodes(tc.err); got != tc.want {
				t.Fatalf("TransferFailureCodes() = %+v, want %+v", got, tc.want)
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
