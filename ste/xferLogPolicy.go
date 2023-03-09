package ste

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
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

	// SyslogDisabled is a flag to check if logging to Syslog/Windows-Event-Logger is enabled or not
	// We by default print to Syslog/Windows-Event-Logger.
	// If SyslogDisabled is not provided explicitly, the default value will be false.
	SyslogDisabled bool
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
			// The time we gather here is a measure of service responsiveness, and as such it shouldn't
			// include the time taken to transfer the body. For downloads, that's easy,
			// since Do returns before the body is processed.  For uploads, its trickier, because
			// the body transferring is inside Do. So we use an http trace, so we can time
			// from the time we finished sending the request (including any body).
			var endRequestWrite time.Time
			haveEndWrite := false
			tracedContext := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
				WroteRequest: func(w httptrace.WroteRequestInfo) {
					endRequestWrite = time.Now()
					haveEndWrite = true
				},
			})
			tryBeginAwaitResponse := time.Now()

			response, err = next.Do(tracedContext, request) // Make the request

			tryEnd := time.Now()
			if haveEndWrite {
				tryBeginAwaitResponse = endRequestWrite // adjust to the time we really started waiting for the response
			}
			tryDuration := tryEnd.Sub(tryBeginAwaitResponse)
			opDuration := tryEnd.Sub(operationStart)

			logLevel, forceLog, httpError := pipeline.LogInfo, false, false // Default logging information

			// If the response took too long, we'll upgrade to warning.
			if o.LogWarningIfTryOverThreshold > 0 && tryDuration > o.LogWarningIfTryOverThreshold {
				// Log a warning if the try duration exceeded the specified threshold
				logLevel, forceLog = pipeline.LogWarning, !o.SyslogDisabled
			}

			if err == nil { // We got a response from the service
				sc := response.Response().StatusCode
				if ((sc >= 400 && sc <= 499) && sc != http.StatusNotFound && sc != http.StatusConflict && sc != http.StatusPreconditionFailed && sc != http.StatusRequestedRangeNotSatisfiable) || (sc >= 500 && sc <= 599) {
					logLevel, forceLog, httpError = pipeline.LogError, !o.SyslogDisabled, true // Promote to Error any 4xx (except those listed is an error) or any 5xx
				} else if sc == http.StatusNotFound || sc == http.StatusConflict || sc == http.StatusPreconditionFailed || sc == http.StatusRequestedRangeNotSatisfiable {
					httpError = true
				}
			} else if isContextCancelledError(err) {
				// No point force-logging these, and probably, for clarity of the log, no point in even logging unless at debug level
				// Otherwise, when lots of go-routines are running, and one fails with a real error, the rest obscure the log with their
				// context canceled logging. If there's no real error, just user-requested cancellation,
				// that's is visible by cancelled status shown in end-of-log summary.
				logLevel, forceLog = pipeline.LogDebug, false
			} else {
				// This error did not get an HTTP response from the service; upgrade the severity to Error
				logLevel, forceLog = pipeline.LogError, !o.SyslogDisabled
			}

			logBody := false
			if shouldLog := po.ShouldLog(logLevel); forceLog || shouldLog {
				// We're going to log this; build the string to log
				b := &bytes.Buffer{}
				slow := ""
				if o.LogWarningIfTryOverThreshold > 0 && tryDuration > o.LogWarningIfTryOverThreshold {
					slow = fmt.Sprintf("[SLOW >%v]", o.LogWarningIfTryOverThreshold)
				}
				fmt.Fprintf(b, "==> REQUEST/RESPONSE (Try=%d/%v%s, OpTime=%v) -- ", try, tryDuration, slow, opDuration)
				if err != nil { // This HTTP request did not get a response from the service (note, this assumes that we are running lower in the pipeline (closer to the wire) that the method factory, since SDK method factories DO create Storage Errors when (error) responses were received from Service)
					fmt.Fprint(b, "REQUEST ERROR\n")
				} else {
					if logLevel == pipeline.LogError {
						fmt.Fprint(b, "RESPONSE STATUS CODE ERROR\n")
						logBody = true
					} else {
						fmt.Fprint(b, "RESPONSE SUCCESSFULLY RECEIVED\n")
					}
				}

				if forceLog || err != nil || po.ShouldLog(pipeline.LogDebug) {
					pipeline.WriteRequestWithResponse(b, prepareRequestForLogging(request), response.Response(), err) // only write full headers if debugging or error
				} else {
					writeRequestAsOneLine(b, prepareRequestForLogging(request))
					writeActivityId(b, response.Response())
				}

				if logBody {
					body := transparentlyReadBody(response.Response())
					fmt.Fprint(b, "Response Details: ", formatBody(body), "\n") // simple logging of response body, as raw XML (better than not logging it at all!)
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

func isContextCancelledError(err error) bool {
	if err == nil {
		return false
	}

	if err == context.Canceled {
		return true
	}

	cause := pipeline.Cause(err)
	if cause == context.Canceled {
		return true
	}

	if uErr, ok := cause.(*url.Error); ok {
		return isContextCancelledError(uErr.Err)
	}

	return false
}

func writeRequestAsOneLine(b *bytes.Buffer, request *http.Request) {
	fmt.Fprint(b, "   "+request.Method+" "+request.URL.String()+"\n")
}

func writeActivityId(b *bytes.Buffer, response *http.Response) {
	if response == nil {
		return
	}
	const key = "X-Ms-Request-Id" // use this, rather than client ID, because this one is easier to search by in Service logs
	value, ok := response.Header[key]
	if ok {
		fmt.Fprintf(b, "   %s: %+v\n", key, value)
	}
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

var errorBodyRemovalRegex = regexp.MustCompile("RequestId:.*?</Message>")

func formatBody(rawBody string) string {
	//Turn something like this:
	//    <?xml version="1.0" encoding="utf-8"?><Error><Code>ServerBusy</Code><Message>Ingress is over the account limit.
	//    RequestId:99909524-001e-006f-1fb1-67ad25000000
	//    Time:2019-01-01T01:00:00.000000Z</Message><Foo>bar</Foo></Error>
	// into something a little less verbose, like this:
	//    <Code>ServerBusy</Code><Message>Ingress is over the account limit. </Message><Foo>bar</Foo>
	const start = `<?xml version="1.0" encoding="utf-8"?><Error>`
	b := strings.Replace(rawBody, start, "", -1)
	b = strings.Replace(b, "</Error>", "", -1)
	b = strings.Replace(b, "\n", " ", -1)
	b = errorBodyRemovalRegex.ReplaceAllString(b, "</Message>") // strip out the RequestID and Time, which we log separately in the headers
	return b
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

// logPolicyOpValues is the struct containing the per-operation values
type logPolicyOpValues struct {
	try   int32
	start time.Time
}

type LogOptions struct {
	RequestLogOptions  RequestLogOptions
	LogOptions         pipeline.LogOptions
	AllowedHeaders     []string
	AllowedQueryParams []string
}

type logPolicy struct {
	LogOptions         LogOptions
	allowedHeaders     map[string]struct{}
	allowedQueryParams map[string]struct{}
}

func (p logPolicy) Do(req *policy.Request) (*http.Response, error) {
	// Get the per-operation values. These are saved in the Message's map so that they persist across each retry calling into this policy object.
	var opValues logPolicyOpValues
	if req.OperationValue(&opValues); opValues.start.IsZero() {
		opValues.start = time.Now() // If this is the 1st try, record this operation's start time
	}
	opValues.try++ // The first try is #1 (not #0)
	req.SetOperationValue(opValues)

	if p.LogOptions.LogOptions.ShouldLog(pipeline.LogDebug) {
		b := &bytes.Buffer{}
		fmt.Fprintf(b, "==> OUTGOING REQUEST (Try=%d)\n", opValues.try)
		p.writeRequestWithResponse(b, req, nil, nil)
		p.LogOptions.LogOptions.Log(pipeline.LogInfo, b.String())
	}

	// Set the time for this particular retry operation and then Do the operation.
	// The time we gather here is a measure of service responsiveness, and as such it shouldn't
	// include the time taken to transfer the body. For downloads, that's easy,
	// since Do returns before the body is processed.  For uploads, its trickier, because
	// the body transferring is inside Do. So we use an http trace, so we can time
	// from the time we finished sending the request (including any body).
	var endRequestWrite time.Time
	haveEndWrite := false
	tracedContext := httptrace.WithClientTrace(req.Raw().Context(), &httptrace.ClientTrace{
		WroteRequest: func(w httptrace.WroteRequestInfo) {
			endRequestWrite = time.Now()
			haveEndWrite = true
		},
	})
	tryBeginAwaitResponse := time.Now()
	response, err := req.Clone(tracedContext).Next()
	tryEnd := time.Now()
	if haveEndWrite {
		tryBeginAwaitResponse = endRequestWrite // adjust to the time we really started waiting for the response
	}
	tryDuration := tryEnd.Sub(tryBeginAwaitResponse)
	opDuration := tryEnd.Sub(opValues.start)

	logLevel, forceLog, httpError := pipeline.LogInfo, false, false // Default logging information

	// If the response took too long, we'll upgrade to warning.
	if p.LogOptions.RequestLogOptions.LogWarningIfTryOverThreshold > 0 && tryDuration > p.LogOptions.RequestLogOptions.LogWarningIfTryOverThreshold {
		// Log a warning if the try duration exceeded the specified threshold
		logLevel, forceLog = pipeline.LogWarning, !p.LogOptions.RequestLogOptions.SyslogDisabled
	}

	if err == nil { // We got a response from the service
		sc := response.StatusCode
		if ((sc >= 400 && sc <= 499) && sc != http.StatusNotFound && sc != http.StatusConflict && sc != http.StatusPreconditionFailed && sc != http.StatusRequestedRangeNotSatisfiable) || (sc >= 500 && sc <= 599) {
			logLevel, forceLog, httpError = pipeline.LogError, !p.LogOptions.RequestLogOptions.SyslogDisabled, true // Promote to Error any 4xx (except those listed is an error) or any 5xx
		} else if sc == http.StatusNotFound || sc == http.StatusConflict || sc == http.StatusPreconditionFailed || sc == http.StatusRequestedRangeNotSatisfiable {
			httpError = true
		}
	} else if isContextCancelledError(err) {
		// No point force-logging these, and probably, for clarity of the log, no point in even logging unless at debug level
		// Otherwise, when lots of go-routines are running, and one fails with a real error, the rest obscure the log with their
		// context canceled logging. If there's no real error, just user-requested cancellation,
		// that's is visible by cancelled status shown in end-of-log summary.
		logLevel, forceLog = pipeline.LogDebug, false
	} else {
		// This error did not get an HTTP response from the service; upgrade the severity to Error
		logLevel, forceLog = pipeline.LogError, !p.LogOptions.RequestLogOptions.SyslogDisabled
	}

	logBody := false
	if shouldLog := p.LogOptions.LogOptions.ShouldLog(logLevel); forceLog || shouldLog {
		// We're going to log this; build the string to log
		b := &bytes.Buffer{}
		slow := ""
		if p.LogOptions.RequestLogOptions.LogWarningIfTryOverThreshold > 0 && tryDuration > p.LogOptions.RequestLogOptions.LogWarningIfTryOverThreshold {
			slow = fmt.Sprintf("[SLOW >%v]", p.LogOptions.RequestLogOptions.LogWarningIfTryOverThreshold)
		}
		fmt.Fprintf(b, "==> REQUEST/RESPONSE (Try=%d/%v%s, OpTime=%v) -- ", opValues.try, tryDuration, slow, opDuration)
		if err != nil { // This HTTP request did not get a response from the service
			fmt.Fprint(b, "REQUEST ERROR\n")
		} else {
			if logLevel == pipeline.LogError {
				fmt.Fprint(b, "RESPONSE STATUS CODE ERROR\n")
				logBody = true
			} else {
				fmt.Fprint(b, "RESPONSE SUCCESSFULLY RECEIVED\n")
			}
		}
		if forceLog || err != nil || p.LogOptions.LogOptions.ShouldLog(pipeline.LogDebug) {
			p.writeRequestWithResponse(b, req, response, err)
		} else {
			p.writeRequestAsOneLine(b, req)
			writeActivityId(b, response)
		}

		if logBody {
			body := transparentlyReadBody(response)
			fmt.Fprint(b, "Response Details: ", formatBody(body), "\n") // simple logging of response body, as raw XML (better than not logging it at all!)
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
			p.LogOptions.LogOptions.Log(logLevel, msg)
		}

	}

	return response, err
}

func newLogPolicy(options LogOptions) policy.Policy {
	options.RequestLogOptions = options.RequestLogOptions.defaults()
	// TODO : Possibly add to default set?
	// construct default hash set of allowed headers
	allowedHeaders := map[string]struct{}{
		"accept":                        {},
		"cache-control":                 {},
		"connection":                    {},
		"content-length":                {},
		"content-type":                  {},
		"date":                          {},
		"etag":                          {},
		"expires":                       {},
		"if-match":                      {},
		"if-modified-since":             {},
		"if-none-match":                 {},
		"if-unmodified-since":           {},
		"last-modified":                 {},
		"ms-cv":                         {},
		"pragma":                        {},
		"request-id":                    {},
		"retry-after":                   {},
		"server":                        {},
		"traceparent":                   {},
		"transfer-encoding":             {},
		"user-agent":                    {},
		"www-authenticate":              {},
		"x-ms-request-id":               {},
		"x-ms-client-request-id":        {},
		"x-ms-return-client-request-id": {},
	}
	// add any caller-specified allowed headers to the set
	for _, ah := range options.AllowedHeaders {
		allowedHeaders[strings.ToLower(ah)] = struct{}{}
	}
	// now do the same thing for query params
	allowedQP := map[string]struct{}{
		"api-version": {},
	}
	for _, qp := range options.AllowedQueryParams {
		allowedQP[strings.ToLower(qp)] = struct{}{}
	}
	return logPolicy{LogOptions: options, allowedHeaders: allowedHeaders, allowedQueryParams: allowedQP}
}

const redactedValue = "REDACTED"

func (p *logPolicy) writeRequestAsOneLine(b *bytes.Buffer, req *policy.Request) {
	cpURL := *req.Raw().URL
	qp := cpURL.Query()
	for k := range qp {
		if _, ok := p.allowedQueryParams[strings.ToLower(k)]; !ok {
			qp.Set(k, redactedValue)
		}
	}
	cpURL.RawQuery = qp.Encode()
	fmt.Fprint(b, "   "+req.Raw().Method+" "+cpURL.String()+"\n")
}

// writeRequestWithResponse appends a formatted HTTP request into a Buffer. If request and/or err are
// not nil, then these are also written into the Buffer.
func (p *logPolicy) writeRequestWithResponse(b *bytes.Buffer, req *policy.Request, resp *http.Response, err error) {
	// redact applicable query params
	cpURL := *req.Raw().URL
	qp := cpURL.Query()
	for k := range qp {
		if _, ok := p.allowedQueryParams[strings.ToLower(k)]; !ok {
			qp.Set(k, redactedValue)
		}
	}
	cpURL.RawQuery = qp.Encode()
	// Write the request into the buffer.
	fmt.Fprint(b, "   "+req.Raw().Method+" "+cpURL.String()+"\n")
	p.writeHeader(b, req.Raw().Header)
	if resp != nil {
		fmt.Fprintln(b, "   --------------------------------------------------------------------------------")
		fmt.Fprint(b, "   RESPONSE Status: "+resp.Status+"\n")
		p.writeHeader(b, resp.Header)
	}
	if err != nil {
		fmt.Fprintln(b, "   --------------------------------------------------------------------------------")
		fmt.Fprint(b, "   ERROR:\n"+err.Error()+"\n")
	}
}

// formatHeaders appends an HTTP request's or response's header into a Buffer.
func (p *logPolicy) writeHeader(b *bytes.Buffer, header http.Header) {
	if len(header) == 0 {
		b.WriteString("   (no headers)\n")
		return
	}
	keys := make([]string, 0, len(header))
	// Alphabetize the headers
	for k := range header {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		value := header.Get(k)
		// redact all header values not in the allow-list
		if _, ok := p.allowedHeaders[strings.ToLower(k)]; !ok {
			value = redactedValue
		}
		fmt.Fprintf(b, "   %s: %+v\n", k, value)
	}
}


