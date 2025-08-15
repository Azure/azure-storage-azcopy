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

func isContextCancelledError(err error) bool {
	if err == nil {
		return false
	}

	if err == context.Canceled {
		return true
	}

	cause := common.Cause(err)
	if cause == context.Canceled {
		return true
	}

	if uErr, ok := cause.(*url.Error); ok {
		return isContextCancelledError(uErr.Err)
	}

	return false
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

var errorBodyRemovalRegex = regexp.MustCompile("RequestId:.*?</Message>")

func formatBody(rawBody string) string {
	//Turn something like this:
	//    <?xml version="1.0" encoding="utf-8"?><OnError><Code>ServerBusy</Code><Message>Ingress is over the account limit.
	//    RequestId:99909524-001e-006f-1fb1-67ad25000000
	//    Time:2019-01-01T01:00:00.000000Z</Message><Foo>bar</Foo></OnError>
	// into something a little less verbose, like this:
	//    <Code>ServerBusy</Code><Message>Ingress is over the account limit. </Message><Foo>bar</Foo>
	const start = `<?xml version="1.0" encoding="utf-8"?><OnError>`
	b := strings.Replace(rawBody, start, "", -1)
	b = strings.Replace(b, "</OnError>", "", -1)
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

// logPolicyOpValues is the struct containing the per-operation values
type logPolicyOpValues struct {
	try   int32
	start time.Time
}

type LogOptions struct {
	// TODO : Unravel LogOptions and RequestLogOptions
	RequestLogOptions RequestLogOptions
	Log               func(level common.LogLevel, message string)
	// ShouldLog is called periodically allowing you to return whether the specified LogLevel should be logged or not.
	// An application can return different values over the its lifetime; this allows the application to dynamically
	// alter what is logged. NOTE: This method can be called by multiple goroutines simultaneously so make sure
	// you implement it in a goroutine-safe way. If nil, nothing is logged (the equivalent of returning LogNone).
	// Usually, the function will be implemented simply like this: return level <= LogWarning
	ShouldLog func(level common.LogLevel) bool
}

type logPolicy struct {
	LogOptions            LogOptions
	disallowedHeaders     map[string]struct{}
	sanitizedUrlHeaders   map[string]struct{}
	disallowedQueryParams map[string]struct{}
}

func (p logPolicy) Do(req *policy.Request) (*http.Response, error) {
	// Get the per-operation values. These are saved in the Message's map so that they persist across each retry calling into this policy object.
	var opValues logPolicyOpValues
	if req.OperationValue(&opValues); opValues.start.IsZero() {
		opValues.start = time.Now() // If this is the 1st try, record this operation's start time
	}
	opValues.try++ // The first try is #1 (not #0)
	req.SetOperationValue(opValues)

	if p.LogOptions.ShouldLog(common.LogDebug) {
		b := &bytes.Buffer{}
		fmt.Fprintf(b, "==> OUTGOING REQUEST (Try=%d)\n", opValues.try)
		p.writeRequestWithResponse(b, req, nil, nil)
		p.LogOptions.Log(common.LogInfo, b.String())
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

	logLevel, forceLog, httpError := common.LogInfo, false, false // Default logging information

	// If the response took too long, we'll upgrade to warning.
	if p.LogOptions.RequestLogOptions.LogWarningIfTryOverThreshold > 0 && tryDuration > p.LogOptions.RequestLogOptions.LogWarningIfTryOverThreshold {
		// Log a warning if the try duration exceeded the specified threshold
		logLevel, forceLog = common.LogWarning, !p.LogOptions.RequestLogOptions.SyslogDisabled
	}

	if err == nil { // We got a response from the service
		sc := response.StatusCode
		if ((sc >= 400 && sc <= 499) && sc != http.StatusNotFound && sc != http.StatusConflict && sc != http.StatusPreconditionFailed && sc != http.StatusRequestedRangeNotSatisfiable) || (sc >= 500 && sc <= 599) {
			logLevel, forceLog, httpError = common.LogError, !p.LogOptions.RequestLogOptions.SyslogDisabled, true // Promote to OnError any 4xx (except those listed is an error) or any 5xx
		} else if sc == http.StatusNotFound || sc == http.StatusConflict || sc == http.StatusPreconditionFailed || sc == http.StatusRequestedRangeNotSatisfiable {
			httpError = true
		}
	} else if isContextCancelledError(err) {
		// No point force-logging these, and probably, for clarity of the log, no point in even logging unless at debug level
		// Otherwise, when lots of go-routines are running, and one fails with a real error, the rest obscure the log with their
		// context canceled logging. If there's no real error, just user-requested cancellation,
		// that's is visible by cancelled status shown in end-of-log summary.
		logLevel, forceLog = common.LogDebug, false
	} else {
		// This error did not get an HTTP response from the service; upgrade the severity to OnError
		logLevel, forceLog = common.LogError, !p.LogOptions.RequestLogOptions.SyslogDisabled
	}

	logBody := false
	if shouldLog := p.LogOptions.ShouldLog(logLevel); forceLog || shouldLog {
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
			if logLevel == common.LogError {
				fmt.Fprint(b, "RESPONSE STATUS CODE ERROR\n")
				logBody = true
			} else {
				fmt.Fprint(b, "RESPONSE SUCCESSFULLY RECEIVED\n")
			}
		}
		if forceLog || err != nil || p.LogOptions.ShouldLog(common.LogDebug) {
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
		if logLevel <= common.LogError && !httpError {
			b.Write(stack())
		}
		msg := b.String()

		if forceLog {
			common.ForceLog(logLevel, msg)
		}
		if shouldLog {
			p.LogOptions.Log(logLevel, msg)
		}

	}

	return response, err
}

func newLogPolicy(options LogOptions) policy.Policy {
	options.RequestLogOptions = options.RequestLogOptions.defaults()
	if options.ShouldLog == nil {
		options.ShouldLog = func(common.LogLevel) bool { return false } // No-op logger
	}
	if options.Log == nil {
		options.Log = func(common.LogLevel, string) {} // No-op logger
	}
	disallowedHeaders := map[string]struct{}{
		"authorization":                  {},
		"x-ms-encryption-key":            {},
		"x-ms-copy-source-authorization": {},
	}
	sanitizedUrlHeaders := map[string]struct{}{
		"x-ms-copy-source": {},
	}

	// now do the same thing for query params
	disallowedQP := map[string]struct{}{
		"sig": {},
	}
	return logPolicy{LogOptions: options, disallowedHeaders: disallowedHeaders, disallowedQueryParams: disallowedQP, sanitizedUrlHeaders: sanitizedUrlHeaders}
}

const redactedValue = "REDACTED"

func (p *logPolicy) writeRequestAsOneLine(b *bytes.Buffer, req *policy.Request) {
	cpURL := *req.Raw().URL
	qp := cpURL.Query()
	for k := range qp {
		if _, ok := p.disallowedQueryParams[strings.ToLower(k)]; ok {
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
		if _, ok := p.disallowedQueryParams[strings.ToLower(k)]; ok {
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
		value := header[k][0]
		// sanitize or redact certain headers
		// redact all header values in the disallow-list
		if _, ok := p.disallowedHeaders[strings.ToLower(k)]; ok {
			value = redactedValue
		} else if _, ok := p.sanitizedUrlHeaders[strings.ToLower(k)]; ok {
			u, err := url.Parse(value)
			if err == nil {
				rawQuery := u.RawQuery
				sigRedacted, rawQuery := common.RedactSecretQueryParam(rawQuery, common.SigAzure)
				xAmzSignatureRedacted, rawQuery := common.RedactSecretQueryParam(rawQuery, common.SigXAmzForAws)

				if sigRedacted || xAmzSignatureRedacted {
					u.RawQuery = rawQuery
				}
				value = u.String()
			}
		}
		fmt.Fprintf(b, "   %s: %+v\n", k, value)
	}
}
