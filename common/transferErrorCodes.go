package common

import (
	"errors"
	"strconv"

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
// transfer's source, so TransferFailureCodes reports its status and code as
// the source's.
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

// TransferErrorCodes are the codes of the failure that ended a transfer, as
// reported in the matching TransferDetail fields.
type TransferErrorCodes struct {
	// StatusCode is the HTTP status of the failed request; 0 if none.
	StatusCode int32

	// ExtendedErrorCode is the failed request's x-ms-error-code, or a
	// TransferErrorCode* value when AzCopy itself failed the transfer.
	ExtendedErrorCode string

	// S2SSourceStatusCode and S2SSourceExtendedErrorCode are the HTTP status and error
	// code the source returned, when the failure came from the source.
	S2SSourceStatusCode        int32
	S2SSourceExtendedErrorCode string
}

// IsZero reports whether no codes are set.
func (c TransferErrorCodes) IsZero() bool {
	return c == TransferErrorCodes{}
}

// TransferFailureCodes extracts the HTTP status and error codes of err, as
// recorded for a failed transfer in TransferDetail. A storage service error
// yields its status and x-ms-error-code, plus the source's status and code:
// x-ms-copy-source-status-code and x-ms-copy-source-error-code for
// CannotVerifyCopySource, or the error's own when err is a SourceError. A
// CodedError yields its AzCopy code. Anything else, including nil, yields
// zero values.
func TransferFailureCodes(err error) TransferErrorCodes {
	var codes TransferErrorCodes

	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		codes.StatusCode = int32(respErr.StatusCode)
		codes.ExtendedErrorCode = respErr.ErrorCode
		var srcErr *SourceError
		switch {
		case codes.ExtendedErrorCode == string(bloberror.CannotVerifyCopySource) && respErr.RawResponse != nil:
			header := respErr.RawResponse.Header
			codes.S2SSourceExtendedErrorCode = header.Get("x-ms-copy-source-error-code")
			if status, parseErr := strconv.ParseInt(header.Get("x-ms-copy-source-status-code"), 10, 32); parseErr == nil {
				codes.S2SSourceStatusCode = int32(status)
			}
		case errors.As(err, &srcErr):
			codes.S2SSourceStatusCode = codes.StatusCode
			codes.S2SSourceExtendedErrorCode = codes.ExtendedErrorCode
		}
		return codes
	}

	var codedErr *CodedError
	if errors.As(err, &codedErr) {
		codes.ExtendedErrorCode = codedErr.Code
	}
	return codes
}
