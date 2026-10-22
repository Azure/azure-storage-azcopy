package common

import (
	"errors"
	"net/url"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

// AzCopy-defined transfer failure codes, reported in
// TransferDetail.ExtendedErrorCode when AzCopy itself fails a transfer rather
// than a storage service request. The "AzCopy." prefix keeps them apart from
// storage service error codes.
const (
	// The source's last-modified time changed between enumeration and the
	// start of the transfer.
	TransferErrorCodeSourceModifiedBeforeTransfer = "AzCopy.SourceModifiedBeforeTransfer"

	// The source's last-modified time changed while the transfer ran.
	TransferErrorCodeSourceModifiedDuringTransfer = "AzCopy.SourceModifiedDuringTransfer"

	// The source is too large to fit in the maximum number of blocks at the
	// configured block size.
	TransferErrorCodeBlockCountExceedsLimit = "AzCopy.BlockCountExceedsLimit"
)

// CodedError is an error AzCopy raised itself, tagged with a
// TransferErrorCode* value so callers need not parse its message.
type CodedError struct {
	Code string
	Err  error
}

func (e *CodedError) Error() string { return e.Err.Error() }
func (e *CodedError) Unwrap() error { return e.Err }

// NewCodedError returns an error with message msg tagged with code.
func NewCodedError(code, msg string) error {
	return &CodedError{Code: code, Err: errors.New(msg)}
}

// SourceError marks an error returned by a request made directly to a
// transfer's source, so its FailedRequestTarget is the source.
type SourceError struct {
	Err error
}

func (e *SourceError) Error() string { return e.Err.Error() }
func (e *SourceError) Unwrap() error { return e.Err }

// NewSourceError marks err as coming from the transfer's source. nil stays nil.
func NewSourceError(err error) error {
	if err == nil {
		return nil
	}
	return &SourceError{Err: err}
}

// FailedRequestTarget is the account a failed request was sent to.
type FailedRequestTarget string

const (
	FailedRequestTargetUnknown     FailedRequestTarget = ""
	FailedRequestTargetSource      FailedRequestTarget = "Source"
	FailedRequestTargetDestination FailedRequestTarget = "Destination"
)

// TransferErrorCodes are the codes of the failure that ended a transfer, as
// reported in the matching TransferDetail fields.
type TransferErrorCodes struct {
	StatusCode                 int32
	ExtendedErrorCode          string
	FailedRequestTarget        FailedRequestTarget
	S2SSourceStatusCode        int32
	S2SSourceExtendedErrorCode string
}

// IsZero reports whether no codes are set.
func (c TransferErrorCodes) IsZero() bool {
	return c == TransferErrorCodes{}
}

// TransferFailureCodes extracts the codes of err, as recorded for a failed
// transfer in TransferDetail. A storage service error yields its status and
// x-ms-error-code. For CannotVerifyCopySource the request went to the
// destination, and the x-ms-copy-source-* headers give the source's status
// and code. A SourceError's request went to the source. A CodedError yields
// its AzCopy code. Anything else, including nil, yields zero values.
func TransferFailureCodes(err error) TransferErrorCodes {
	var codes TransferErrorCodes

	var srcErr *SourceError
	if errors.As(err, &srcErr) {
		codes.FailedRequestTarget = FailedRequestTargetSource
	}

	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		codes.StatusCode = int32(respErr.StatusCode)
		codes.ExtendedErrorCode = respErr.ErrorCode
		if codes.ExtendedErrorCode == string(bloberror.CannotVerifyCopySource) {
			codes.FailedRequestTarget = FailedRequestTargetDestination
			if respErr.RawResponse != nil {
				header := respErr.RawResponse.Header
				codes.S2SSourceExtendedErrorCode = header.Get("x-ms-copy-source-error-code")
				if status, parseErr := strconv.ParseInt(header.Get("x-ms-copy-source-status-code"), 10, 32); parseErr == nil {
					codes.S2SSourceStatusCode = int32(status)
				}
			}
		}
		return codes
	}

	var codedErr *CodedError
	if errors.As(err, &codedErr) {
		codes.ExtendedErrorCode = codedErr.Code
	}
	return codes
}

// TransferFailureCodesFor is TransferFailureCodes for a transfer from source
// to destination. When err doesn't say which account its request went to,
// the request's URL is matched against source and destination.
func TransferFailureCodesFor(err error, source, destination string) TransferErrorCodes {
	codes := TransferFailureCodes(err)
	if codes.FailedRequestTarget != FailedRequestTargetUnknown {
		return codes
	}
	var respErr *azcore.ResponseError
	if !errors.As(err, &respErr) || respErr.RawResponse == nil || respErr.RawResponse.Request == nil {
		return codes
	}
	reqURL := respErr.RawResponse.Request.URL
	isSource := requestIsFor(reqURL, source)
	isDestination := requestIsFor(reqURL, destination)
	switch {
	case isSource && !isDestination:
		codes.FailedRequestTarget = FailedRequestTargetSource
	case isDestination && !isSource:
		codes.FailedRequestTarget = FailedRequestTargetDestination
	}
	return codes
}

// requestIsFor reports whether reqURL addresses resource, or one of its
// parent directories or its container, in the same account. The blob and
// dfs endpoints of an account count as the same account.
func requestIsFor(reqURL *url.URL, resource string) bool {
	if reqURL == nil {
		return false
	}
	resURL, err := url.Parse(resource)
	if err != nil || resURL.Host == "" {
		return false
	}
	if accountHost(reqURL.Host) != accountHost(resURL.Host) {
		return false
	}
	reqPath := strings.TrimSuffix(reqURL.Path, "/")
	resPath := strings.TrimSuffix(resURL.Path, "/")
	return reqPath == resPath || (reqPath != "" && strings.HasPrefix(resPath, reqPath+"/"))
}

func accountHost(host string) string {
	return strings.Replace(strings.ToLower(host), ".dfs.", ".blob.", 1)
}
