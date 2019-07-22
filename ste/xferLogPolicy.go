package ste

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

// This file is copied and extended from Azure Storage Blob Go SDK.
// Because V10 SDK supports flexibility for injecting customized logging policy,
// and considering redact x-amz-signature's request header for logging is not a general demand for Azure Storage Blob Go SDK.
// TODO: Further discuss whether to add callback into RequestLogOptions for Azure Storage Blob Go SDK.
// TODO: (new) consider also the relationship between the above comment and todos, and the new LogSanitizer
//    Do we really need this copied version of the blob Storage SDK file now?

// RequestLogOptions configures the retry policy's behavior.
type RequestLogOptions struct {
	// LogWarningIfTryOverThreshold logs a warning if a tried operation takes longer than the specified
	// duration (-1=no logging; 0=default threshold).
	LogWarningIfTryOverThreshold time.Duration
}

func (o RequestLogOptions) defaults() RequestLogOptions {
	if o.LogWarningIfTryOverThreshold == 0 {
		// It would be good to relate this to https://azure.microsoft.com/en-us/support/legal/sla/storage/v1_2/
		// But this monitors the time to get the HTTP response; NOT the time to download the response body.
		o.LogWarningIfTryOverThreshold = 3 * time.Second // Default to 3 seconds
	}
	return o
}

// NewRequestLogPolicyFactory creates a RequestLogPolicyFactory object configured using the specified options.
func NewRequestLogPolicyFactory(o RequestLogOptions) pipeline.Factory {
	o = o.defaults() // Force defaults to be calculated
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		// These variables are per-policy; shared by multiple calls to Do
		var try int32
		operationStart := time.Now() // If this is the 1st try, record the operation state time
		return func(ctx context.Context, request pipeline.Request) (response pipeline.Response, err error) {
			try++ // The first try is #1 (not #0)

			// Log the outgoing request if at debug log level
			if po.ShouldLog(pipeline.LogDebug) {
				b := &bytes.Buffer{}
				fmt.Fprintf(b, "==> OUTGOING REQUEST (Try=%d)\n", try)
				pipeline.WriteRequestWithResponse(b, prepareRequestForLogging(request), nil, nil)
				po.Log(pipeline.LogInfo, b.String())
			}

			// Set the time for this particular retry operation and then Do the operation.
			tryStart := time.Now()
			response, err = next.Do(ctx, request) // Make the request
			tryEnd := time.Now()
			tryDuration := tryEnd.Sub(tryStart)
			opDuration := tryEnd.Sub(operationStart)

			logLevel, forceLog, httpError := pipeline.LogInfo, false, false // Default logging information

			// If the response took too long, we'll upgrade to warning.
			if o.LogWarningIfTryOverThreshold > 0 && tryDuration > o.LogWarningIfTryOverThreshold {
				// Log a warning if the try duration exceeded the specified threshold
				logLevel, forceLog = pipeline.LogWarning, true
			}

			if err == nil { // We got a response from the service
				sc := response.Response().StatusCode
				if ((sc >= 400 && sc <= 499) && sc != http.StatusNotFound && sc != http.StatusConflict && sc != http.StatusPreconditionFailed && sc != http.StatusRequestedRangeNotSatisfiable) || (sc >= 500 && sc <= 599) {
					logLevel, forceLog, httpError = pipeline.LogError, true, true // Promote to Error any 4xx (except those listed is an error) or any 5xx
				} else if sc == http.StatusNotFound || sc == http.StatusConflict || sc == http.StatusPreconditionFailed || sc == http.StatusRequestedRangeNotSatisfiable {
					httpError = true
				}
			} else { // This error did not get an HTTP response from the service; upgrade the severity to Error
				logLevel, forceLog = pipeline.LogError, true
			}

			if shouldLog := po.ShouldLog(logLevel); forceLog || shouldLog {
				// We're going to log this; build the string to log
				b := &bytes.Buffer{}
				slow := ""
				if o.LogWarningIfTryOverThreshold > 0 && tryDuration > o.LogWarningIfTryOverThreshold {
					slow = fmt.Sprintf("[SLOW >%v]", o.LogWarningIfTryOverThreshold)
				}
				fmt.Fprintf(b, "==> REQUEST/RESPONSE (Try=%d/%v%s, OpTime=%v) -- ", try, tryDuration, slow, opDuration)
				if err != nil { // This HTTP request did not get a response from the service
					fmt.Fprint(b, "REQUEST ERROR\n")
				} else {
					if logLevel == pipeline.LogError {
						fmt.Fprint(b, "RESPONSE STATUS CODE ERROR\n")
					} else {
						fmt.Fprint(b, "RESPONSE SUCCESSFULLY RECEIVED\n")
					}
				}

				if forceLog || err != nil || po.ShouldLog(pipeline.LogDebug) {
					pipeline.WriteRequestWithResponse(b, prepareRequestForLogging(request), response.Response(), err) // only write full headers if debugging or error
				} else {
					writeRequestAsOneLine(b, prepareRequestForLogging(request))
				}

				//Dropping HTTP errors as grabbing the stack is an expensive operation & fills the log too much
				//for a set of harmless errors. HTTP requests ultimately will be retried.
				if logLevel <= pipeline.LogError && !httpError {
					b.Write(stack())
				}
				msg := b.String()

				if forceLog {
					pipeline.ForceLog(logLevel, msg)
				}
				if shouldLog {
					po.Log(logLevel, msg)
				}
			}
			return response, err
		}
	})
}

func writeRequestAsOneLine(b *bytes.Buffer, request *http.Request) {
	fmt.Fprint(b, "   "+request.Method+" "+request.URL.String()+"\n")
}

func prepareRequestForLogging(request pipeline.Request) *http.Request {
	req := request
	rawQuery := req.URL.RawQuery
	sigRedacted, rawQuery := common.RedactSecretQueryParam(rawQuery, common.SigAzure)

	if sigRedacted {
		// Make copy so we don't destroy the query parameters we actually need to send in the request
		req = request.Copy()
		req.Request.URL.RawQuery = rawQuery
	}

	return prepareRequestForServiceLogging(req)
}

func stack() []byte {
	buf := make([]byte, 1024)
	for {
		n := runtime.Stack(buf, false)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}

///////////////////////////////////////////////////////////////////////////////////////
// Redact phase useful for blob and file service only. For other services,
// this method can directly return request.Request.
///////////////////////////////////////////////////////////////////////////////////////
func prepareRequestForServiceLogging(request pipeline.Request) *http.Request {
	req := request

	// As CopyBlob https://docs.microsoft.com/en-us/rest/api/storageservices/copy-blob and
	// PutBlockFromURL/PutPageFromURL/AppendBlobFromURL https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-from-url
	// contains header x-ms-copy-source which could contains secrets for authentication.
	// Prepare the headers for logging, with redact secrets in x-ms-copy-source header.
	if exist, key := doesHeaderExistCaseInsensitive(req.Header, xMsCopySourceHeader); exist {
		req = request.Copy()
		url, err := url.Parse(req.Header.Get(key))
		if err == nil {
			rawQuery := url.RawQuery
			sigRedacted, rawQuery := common.RedactSecretQueryParam(rawQuery, common.SigAzure)
			xAmzSignatureRedacted, rawQuery := common.RedactSecretQueryParam(rawQuery, common.SigXAmzForAws)

			if sigRedacted || xAmzSignatureRedacted {
				url.RawQuery = rawQuery
				req.Header.Set(xMsCopySourceHeader, url.String())
			}
		}
	}
	return req.Request
}

const xMsCopySourceHeader = "x-ms-copy-source"

func doesHeaderExistCaseInsensitive(header http.Header, key string) (bool, string) {
	for keyInHeader := range header {
		if strings.EqualFold(keyInHeader, key) {
			return true, keyInHeader
		}
	}
	return false, ""
}
