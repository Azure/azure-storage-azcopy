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
			want: TransferErrorCodes{StatusCode: 404, ExtendedErrorCode: "CannotVerifyCopySource", FailedRequestTarget: FailedRequestTargetDestination,
				S2SSourceStatusCode: 404, S2SSourceExtendedErrorCode: "BlobNotFound"},
		},
		{
			name: "copy source status differs from destination status",
			err: &azcore.ResponseError{StatusCode: 403, ErrorCode: "CannotVerifyCopySource",
				RawResponse: &http.Response{StatusCode: 403, Header: copySourceAuthHeader}},
			want: TransferErrorCodes{StatusCode: 403, ExtendedErrorCode: "CannotVerifyCopySource", FailedRequestTarget: FailedRequestTargetDestination,
				S2SSourceStatusCode: 401, S2SSourceExtendedErrorCode: "InvalidAuthenticationInfo"},
		},
		{
			name: "copy source status unparsable",
			err: &azcore.ResponseError{StatusCode: 404, ErrorCode: "CannotVerifyCopySource",
				RawResponse: &http.Response{StatusCode: 404, Header: badStatusHeader}},
			want: TransferErrorCodes{StatusCode: 404, ExtendedErrorCode: "CannotVerifyCopySource", FailedRequestTarget: FailedRequestTargetDestination,
				S2SSourceExtendedErrorCode: "BlobNotFound"},
		},
		{
			name: "azcopy coded error",
			err:  fmt.Errorf("epilogue: %w", NewCodedError(TransferErrorCodeSourceModifiedDuringTransfer, "source modified during transfer")),
			want: TransferErrorCodes{ExtendedErrorCode: TransferErrorCodeSourceModifiedDuringTransfer},
		},
		{
			name: "source request error",
			err:  NewSourceError(&azcore.ResponseError{StatusCode: 404, ErrorCode: "BlobNotFound"}),
			want: TransferErrorCodes{StatusCode: 404, ExtendedErrorCode: "BlobNotFound", FailedRequestTarget: FailedRequestTargetSource},
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

func TestTransferFailureCodesForAttributesByURL(t *testing.T) {
	const (
		src = "https://srcacct.blob.core.windows.net/c1/dir/a.txt?sv=x&sig=y"
		dst = "https://dstacct.blob.core.windows.net/c2/dir/a.txt"
	)
	respErr := func(method, rawURL string, status int, code string, header http.Header) error {
		req, err := http.NewRequest(method, rawURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		if header == nil {
			header = http.Header{}
		}
		return &azcore.ResponseError{StatusCode: status, ErrorCode: code,
			RawResponse: &http.Response{StatusCode: status, Header: header, Request: req}}
	}
	copySourceHeader := http.Header{}
	copySourceHeader.Set("X-Ms-Copy-Source-Error-Code", "BlobNotFound")

	testCases := []struct {
		name     string
		err      error
		src, dst string
		want     FailedRequestTarget
	}{
		{"source blob", respErr("HEAD", "https://srcacct.blob.core.windows.net/c1/dir/a.txt", 404, "BlobNotFound", nil), src, dst, FailedRequestTargetSource},
		{"source via dfs endpoint", respErr("HEAD", "https://srcacct.dfs.core.windows.net/c1/dir/a.txt?action=getAccessControl", 403, "AuthorizationPermissionMismatch", nil), src, dst, FailedRequestTargetSource},
		{"destination block", respErr("PUT", "https://dstacct.blob.core.windows.net/c2/dir/a.txt?comp=blocklist", 409, "BlobImmutableDueToPolicy", nil), src, dst, FailedRequestTargetDestination},
		{"destination parent directory", respErr("PUT", "https://dstacct.dfs.core.windows.net/c2/dir?resource=directory", 403, "AuthorizationPermissionMismatch", nil), src, dst, FailedRequestTargetDestination},
		{"other blob in the account", respErr("HEAD", "https://srcacct.blob.core.windows.net/c1/dir/b.txt", 404, "BlobNotFound", nil), src, dst, FailedRequestTargetUnknown},
		{"same account, ambiguous container", respErr("GET", "https://acct.blob.core.windows.net/c", 403, "AuthorizationFailure", nil),
			"https://acct.blob.core.windows.net/c/a", "https://acct.blob.core.windows.net/c/b", FailedRequestTargetUnknown},
		{"source error wrapper wins", NewSourceError(respErr("HEAD", "https://dstacct.blob.core.windows.net/c2/dir/a.txt", 404, "BlobNotFound", nil)), src, dst, FailedRequestTargetSource},
		{"copy source error is the destination", respErr("PUT", "https://dstacct.blob.core.windows.net/c2/dir/a.txt", 404, "CannotVerifyCopySource", copySourceHeader), src, dst, FailedRequestTargetDestination},
		{"no request", &azcore.ResponseError{StatusCode: 500, ErrorCode: "InternalError"}, src, dst, FailedRequestTargetUnknown},
		{"local source", respErr("PUT", "https://dstacct.blob.core.windows.net/c2/dir/a.txt", 403, "AuthorizationPermissionMismatch", nil), "/mnt/data/a.txt", dst, FailedRequestTargetDestination},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := TransferFailureCodesFor(tc.err, tc.src, tc.dst).FailedRequestTarget; got != tc.want {
				t.Fatalf("FailedRequestTarget = %q, want %q", got, tc.want)
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
