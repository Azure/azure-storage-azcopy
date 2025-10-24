package common

import (
	"encoding/xml"
	"fmt"
	"net/http"
)

// ==============================================================================================
// S3 Error Response Parsing
// ==============================================================================================

// S3ErrorResponse represents the XML error response from S3-compatible APIs
//
// This structure is compatible with both:
// - AWS S3: Full support for all fields
// - GCP Cloud Storage (S3-compatible API): Supports most fields but may have different error codes
//
// Key differences:
// - AWS S3 uses specific error codes like "NoSuchBucket", "AccessDenied", etc.
// - GCP may return different error codes or HTTP status codes directly
// - Both use XML format but GCP may include additional fields or omit some
type S3ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`      // Error code (AWS and GCP both use this)
	Message   string   `xml:"Message"`   // Human-readable error message
	Resource  string   `xml:"Resource"`  // Resource that caused the error (may be empty in GCP)
	RequestID string   `xml:"RequestId"` // Request ID for tracking (AWS: RequestId, GCP may use different field)
}

// S3-compatible error codes
// These are used by both AWS S3 and GCP Cloud Storage (S3-compatible API)
// However, there are important differences in implementation:
//
// COMMON ERROR CODES (Both AWS S3 and GCP):
// - AccessDenied: Permission denied
// - NoSuchBucket: Bucket doesn't exist
// - NoSuchKey: Object/key doesn't exist
// - InternalError: Server-side error
// - InvalidAccessKeyId: Invalid credentials
// - SignatureDoesNotMatch: Authentication failure
//
// AWS S3 SPECIFIC (may not be returned by GCP):
// - AccountProblem: AWS account issue
// - RequestTimeTooSkewed: Clock skew issue (AWS is strict about this)
// - SlowDown: Rate limiting (AWS specific error code)
// - TemporaryRedirect: Bucket in different region
//
// GCP CLOUD STORAGE NOTES:
// - GCP may return HTTP status codes (403, 404, etc.) without detailed XML error codes
// - GCP may use "Forbidden" instead of "AccessDenied" in some cases
// - GCP rate limiting typically returns HTTP 429 without specific error code
// - GCP may include additional fields in error XML not present in AWS
const (
	// Common to both AWS S3 and GCP Cloud Storage
	S3ErrorAccessDenied          = "AccessDenied"
	S3ErrorNoSuchBucket          = "NoSuchBucket"
	S3ErrorNoSuchKey             = "NoSuchKey"
	S3ErrorInternalError         = "InternalError"
	S3ErrorInvalidAccessKeyId    = "InvalidAccessKeyId"
	S3ErrorSignatureDoesNotMatch = "SignatureDoesNotMatch"

	// Primarily AWS S3 (may not be used by GCP)
	S3ErrorAccountProblem       = "AccountProblem"
	S3ErrorBucketNotEmpty       = "BucketNotEmpty"
	S3ErrorRequestTimeout       = "RequestTimeout"
	S3ErrorServiceUnavailable   = "ServiceUnavailable"
	S3ErrorSlowDown             = "SlowDown"
	S3ErrorTemporaryRedirect    = "TemporaryRedirect"
	S3ErrorRequestTimeTooSkewed = "RequestTimeTooSkewed"

	// GCP may use these alternative codes
	GCPErrorForbidden = "Forbidden" // GCP alternative to AccessDenied
)

// IsRetryableS3Error checks if an S3 error code indicates a retryable condition
func IsRetryableS3Error(code string) bool {
	switch code {
	case S3ErrorInternalError,
		S3ErrorServiceUnavailable,
		S3ErrorSlowDown,
		S3ErrorRequestTimeout:
		return true
	default:
		return false
	}
}

// IsCriticalS3Error checks if an S3 error code indicates a critical (non-retryable) condition
func IsCriticalS3Error(code string) bool {
	switch code {
	case S3ErrorAccessDenied,
		S3ErrorInvalidAccessKeyId,
		S3ErrorSignatureDoesNotMatch,
		S3ErrorAccountProblem,
		GCPErrorForbidden:
		return true
	default:
		return false
	}
}

// ==============================================================================================
// S3 HTTP Error Wrapper (Modular Design)
// ==============================================================================================

// S3HTTPError wraps HTTPStatusError with S3-specific error information
// This provides modular, protocol-specific error handling without coupling HTTPStatusError to S3
type S3HTTPError struct {
	*HTTPStatusError                  // Embedded generic HTTP error
	S3Error          *S3ErrorResponse // S3-specific XML error details
}

// DetectS3HTTPStatusError checks if the HTTP response indicates an S3 error and parses it
// Returns nil if the status code is 2xx (success)
// This is an S3-specific version of DetectHTTPStatusError that includes XML parsing
func DetectS3HTTPStatusError(resp *http.Response) *S3HTTPError {
	// First get the generic HTTP error
	httpErr := DetectHTTPStatusError(resp)
	if httpErr == nil {
		return nil
	}

	s3Err := &S3HTTPError{
		HTTPStatusError: httpErr,
	}

	// Try to parse S3 XML error from the raw body
	if httpErr.RawBody != "" && isLikelyXML([]byte(httpErr.RawBody)) {
		var s3XmlErr S3ErrorResponse
		if err := xml.Unmarshal([]byte(httpErr.RawBody), &s3XmlErr); err == nil && s3XmlErr.Code != "" {
			s3Err.S3Error = &s3XmlErr
		}
	}

	return s3Err
}

// isLikelyXML performs a fast check if the bytes look like XML
// This avoids expensive XML parsing for non-XML responses
func isLikelyXML(data []byte) bool {
	if len(data) < 2 {
		return false
	}
	// Check for XML declaration or opening tag
	return data[0] == '<' && (data[1] == '?' || data[1] == 'E' || (data[1] >= 'a' && data[1] <= 'z') || (data[1] >= 'A' && data[1] <= 'Z'))
}

// String formats the S3HTTPError for logging, including S3-specific details
func (e *S3HTTPError) String() string {
	if e == nil {
		return ""
	}

	// If we have S3-specific error details, include them
	if e.S3Error != nil {
		errType := "Unknown"
		if e.IsClientErr {
			errType = "Client Error"
		} else if e.IsServerErr {
			errType = "Server Error"
		}
		return fmt.Sprintf("HTTP %d (%s) - S3 Error: Code=%s, Message=%s, Resource=%s, RequestID=%s",
			e.StatusCode, errType, e.S3Error.Code, e.S3Error.Message, e.S3Error.Resource, e.S3Error.RequestID)
	}

	// Fall back to generic HTTP error formatting
	return e.HTTPStatusError.String()
}

// GetS3ErrorCode returns the S3 error code if available, otherwise returns empty string
func (e *S3HTTPError) GetS3ErrorCode() string {
	if e == nil || e.S3Error == nil {
		return ""
	}
	return e.S3Error.Code
}

// GetS3ErrorMessage returns the S3 error message if available
func (e *S3HTTPError) GetS3ErrorMessage() string {
	if e == nil || e.S3Error == nil {
		return e.GetErrorMessage()
	}
	if e.S3Error.Message != "" {
		return e.S3Error.Message
	}
	return e.GetErrorMessage()
}

// IsS3ErrorCode checks if the error matches a specific S3 error code
func (e *S3HTTPError) IsS3ErrorCode(code string) bool {
	return e != nil && e.S3Error != nil && e.S3Error.Code == code
}
