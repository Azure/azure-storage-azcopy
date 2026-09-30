package common

import (
	"errors"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

// AzCopy-defined transfer failure codes, reported in
// TransferDetail.ServiceErrorCode when AzCopy itself fails a transfer rather
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
// transfer's source, so TransferFailureCodes reports its code as the
// source's.
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

// TransferFailureCodes extracts the HTTP status and error codes of err, as
// recorded for a failed transfer in TransferDetail. A storage service error
// yields its status and x-ms-error-code, and a source error code: the copy
// source's (x-ms-copy-source-error-code) for CannotVerifyCopySource, or the
// error's own code when err is a SourceError. A CodedError yields its AzCopy
// code. Anything else, including nil, yields zero values.
func TransferFailureCodes(err error) (httpStatus int32, serviceCode, sourceCode string) {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		httpStatus = int32(respErr.StatusCode)
		serviceCode = respErr.ErrorCode
		var srcErr *SourceError
		switch {
		case serviceCode == string(bloberror.CannotVerifyCopySource) && respErr.RawResponse != nil:
			sourceCode = respErr.RawResponse.Header.Get("x-ms-copy-source-error-code")
		case errors.As(err, &srcErr):
			sourceCode = serviceCode
		}
		return
	}

	var codedErr *CodedError
	if errors.As(err, &codedErr) {
		serviceCode = codedErr.Code
	}
	return
}
