package ste

import (
	"context"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// XferRetryPolicy tells the pipeline what kind of retry policy to use. See the XferRetryPolicy* constants.
// Added a new retry policy and not using the existing policy azblob.zc_retry_policy.go since there are some changes
// in the retry policy.
// Retry on all the type of network errors instead of retrying only in case of temporary or timeout errors.
type XferRetryPolicy int32

const (
	// RetryPolicyExponential tells the pipeline to use an exponential back-off retry policy
	RetryPolicyExponential XferRetryPolicy = 0

	// RetryPolicyFixed tells the pipeline to use a fixed back-off retry policy
	RetryPolicyFixed XferRetryPolicy = 1
)

// XferRetryOptions configures the retry policy's behavior.
type XferRetryOptions struct {
	// Policy tells the pipeline what kind of retry policy to use. See the XferRetryPolicy* constants.\
	// A value of zero means that you accept our default policy.
	Policy XferRetryPolicy

	// MaxTries specifies the maximum number of attempts an operation will be tried before producing an error (0=default).
	// A value of zero means that you accept our default policy. A value of 1 means 1 try and no retries.
	MaxTries int32

	// TryTimeout indicates the maximum time allowed for any single try of an HTTP request.
	// A value of zero means that you accept our default timeout. NOTE: When transferring large amounts
	// of data, the default TryTimeout will probably not be sufficient. You should override this value
	// based on the bandwidth available to the host machine and proximity to the Storage service. A good
	// starting point may be something like (60 seconds per MB of anticipated-payload-size).
	TryTimeout time.Duration

	// RetryDelay specifies the amount of delay to use before retrying an operation (0=default).
	// The delay increases (exponentially or linearly) with each retry up to a maximum specified by
	// MaxRetryDelay. If you specify 0, then you must also specify 0 for MaxRetryDelay.
	RetryDelay time.Duration

	// MaxRetryDelay specifies the maximum delay allowed before retrying an operation (0=default).
	// If you specify 0, then you must also specify 0 for RetryDelay.
	MaxRetryDelay time.Duration

	// RetryReadsFromSecondaryHost specifies whether the retry policy should retry a read operation against another host.
	// If RetryReadsFromSecondaryHost is "" (the default) then operations are not retried against another host.
	// NOTE: Before setting this field, make sure you understand the issues around reading stale & potentially-inconsistent
	// data at this webpage: https://docs.microsoft.com/en-us/azure/storage/common/storage-designing-ha-apps-with-ragrs
	RetryReadsFromSecondaryHost string // Comment this our for non-Blob SDKs
}

func (o XferRetryOptions) retryReadsFromSecondaryHost() string {
	return o.RetryReadsFromSecondaryHost // This is for the Blob SDK only
	//return "" // This is for non-blob SDKs
}

func (o XferRetryOptions) defaults() XferRetryOptions {
	if o.Policy != RetryPolicyExponential && o.Policy != RetryPolicyFixed {
		panic("XferRetryPolicy must be RetryPolicyExponential or RetryPolicyFixed")
	}
	if o.MaxTries < 0 {
		panic("MaxTries must be >= 0")
	}
	if o.TryTimeout < 0 || o.RetryDelay < 0 || o.MaxRetryDelay < 0 {
		panic("TryTimeout, RetryDelay, and MaxRetryDelay must all be >= 0")
	}
	if o.RetryDelay > o.MaxRetryDelay {
		panic("RetryDelay must be <= MaxRetryDelay")
	}
	if (o.RetryDelay == 0 && o.MaxRetryDelay != 0) || (o.RetryDelay != 0 && o.MaxRetryDelay == 0) {
		panic("Both RetryDelay and MaxRetryDelay must be 0 or neither can be 0")
	}

	IfDefault := func(current *time.Duration, desired time.Duration) {
		if *current == time.Duration(0) {
			*current = desired
		}
	}

	// Set defaults if unspecified
	if o.MaxTries == 0 {
		o.MaxTries = 4
	}
	switch o.Policy {
	case RetryPolicyExponential:
		IfDefault(&o.TryTimeout, 1*time.Minute)
		IfDefault(&o.RetryDelay, 4*time.Second)
		IfDefault(&o.MaxRetryDelay, 120*time.Second)

	case RetryPolicyFixed:
		IfDefault(&o.TryTimeout, 1*time.Minute)
		IfDefault(&o.RetryDelay, 30*time.Second)
		IfDefault(&o.MaxRetryDelay, 120*time.Second)
	}
	return o
}

func (o XferRetryOptions) calcDelay(try int32) time.Duration { // try is >=1; never 0
	pow := func(number int64, exponent int32) int64 { // pow is nested helper function
		var result int64 = 1
		for n := int32(0); n < exponent; n++ {
			result *= number
		}
		return result
	}

	delay := time.Duration(0)
	switch o.Policy {
	case RetryPolicyExponential:
		delay = time.Duration(pow(2, try-1)-1) * o.RetryDelay

	case RetryPolicyFixed:
		if try > 1 { // Any try after the 1st uses the fixed delay
			delay = o.RetryDelay
		}
	}

	// Introduce some jitter:  [0.0, 1.0) / 2 = [0.0, 0.5) + 0.8 = [0.8, 1.3)
	// For casts and rounding - be careful, as per https://github.com/golang/go/issues/20757
	delay = time.Duration(float32(delay) * (rand.Float32()/2 + 0.8)) // NOTE: We want math/rand; not crypto/rand
	if delay > o.MaxRetryDelay {
		delay = o.MaxRetryDelay
	}
	return delay
}

// TODO fix the separate retry policies
// NewBFSXferRetryPolicyFactory creates a RetryPolicyFactory object configured using the specified options.
func NewBFSXferRetryPolicyFactory(o XferRetryOptions) pipeline.Factory {
	o = o.defaults() // Force defaults to be calculated
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		return func(ctx context.Context, request pipeline.Request) (response pipeline.Response, err error) {
			// Before each try, we'll select either the primary or secondary URL.
			primaryTry := int32(0) // This indicates how many tries we've attempted against the primary DC

			// We only consider retrying against a secondary if we have a read request (GET/HEAD) AND this policy has a Secondary URL it can use
			considerSecondary := (request.Method == http.MethodGet || request.Method == http.MethodHead) && o.retryReadsFromSecondaryHost() != ""

			// Exponential retry algorithm: ((2 ^ attempt) - 1) * delay * random(0.8, 1.2)
			// When to retry: connection failure or temporary/timeout. NOTE: StorageError considers HTTP 500/503 as temporary & is therefore retryable
			// If using a secondary:
			//    Even tries go against primary; odd tries go against the secondary
			//    For a primary wait ((2 ^ primaryTries - 1) * delay * random(0.8, 1.2)
			//    If secondary gets a 404, don't fail, retry but future retries are only against the primary
			//    When retrying against a secondary, ignore the retry count and wait (.1 second * random(0.8, 1.2))
			for try := int32(1); try <= o.MaxTries; try++ {
				logf("\n=====> Try=%d\n", try)

				// Determine which endpoint to try. It's primary if there is no secondary or if it is an add # attempt.
				tryingPrimary := !considerSecondary || (try%2 == 1)
				// Select the correct host and delay
				if tryingPrimary {
					primaryTry++
					delay := o.calcDelay(primaryTry)
					logf("Primary try=%d, Delay=%v\n", primaryTry, delay)
					time.Sleep(delay) // The 1st try returns 0 delay
				} else {
					// For casts and rounding - be careful, as per https://github.com/golang/go/issues/20757
					delay := time.Duration(float32(time.Second) * (rand.Float32()/2 + 0.8))
					logf("Secondary try=%d, Delay=%v\n", try-primaryTry, delay)
					time.Sleep(delay) // Delay with some jitter before trying secondary
				}

				// Clone the original request to ensure that each try starts with the original (unmutated) request.
				requestCopy := request.Copy()

				// For each try, seek to the beginning of the body stream. We do this even for the 1st try because
				// the stream may not be at offset 0 when we first get it and we want the same behavior for the
				// 1st try as for additional tries.
				err = requestCopy.RewindBody()
				common.PanicIfErr(err)

				if !tryingPrimary {
					requestCopy.URL.Host = o.retryReadsFromSecondaryHost()
					requestCopy.Host = o.retryReadsFromSecondaryHost()
				}

				// Set the server-side timeout query parameter "timeout=[seconds]"
				timeout := int32(o.TryTimeout.Seconds()) // Max seconds per try
				if deadline, ok := ctx.Deadline(); ok {  // If user's ctx has a deadline, make the timeout the smaller of the two
					t := int32(deadline.Sub(time.Now()).Seconds()) // Duration from now until user's ctx reaches its deadline
					logf("MaxTryTimeout=%d secs, TimeTilDeadline=%d sec\n", timeout, t)
					if t < timeout {
						timeout = t
					}
					if timeout < 0 {
						timeout = 0 // If timeout ever goes negative, set it to zero; this happen while debugging
					}
					logf("TryTimeout adjusted to=%d sec\n", timeout)
				}
				q := requestCopy.Request.URL.Query()
				q.Set("timeout", strconv.Itoa(int(timeout+1))) // Add 1 to "round up"
				requestCopy.Request.URL.RawQuery = q.Encode()
				logf("Url=%s\n", requestCopy.Request.URL.String())

				// Set the time for this particular retry operation and then Do the operation.
				tryCtx, tryCancel := context.WithTimeout(ctx, time.Second*time.Duration(timeout))
				//requestCopy.body = &deadlineExceededReadCloser{r: requestCopy.Request.body}
				response, err = next.Do(tryCtx, requestCopy) // Make the request
				/*err = improveDeadlineExceeded(err)
				if err == nil {
					response.Response().body = &deadlineExceededReadCloser{r: response.Response().body}
				}*/
				logf("Err=%v, response=%v\n", err, response)

				action := "" // This MUST get changed within the switch code below
				switch {
				case err == nil:
					action = "NoRetry: successful HTTP request" // no error

				case !tryingPrimary && response != nil && response.Response() != nil && response.Response().StatusCode == http.StatusNotFound:
					// If attempt was against the secondary & it returned a StatusNotFound (404), then
					// the resource was not found. This may be due to replication delay. So, in this
					// case, we'll never try the secondary again for this operation.
					considerSecondary = false
					action = "Retry: Secondary URL returned 404"

				case ctx.Err() != nil:
					action = "NoRetry: Op timeout"

				case err != nil:
					// NOTE: Protocol Responder returns non-nil if REST API returns invalid status code for the invoked operation
					// retry on all the network errors.
					// zc_policy_retry perform the retries on Temporary and Timeout Errors only.
					// some errors like 'connection reset by peer' or 'transport connection broken' does not implement the Temporary interface
					// but they should be retried. So redefined the retry policy for azcopy to retry for such errors as well.

					// TODO make sure Storage error can be cast to different package's error object
					if stErr, ok := err.(azbfs.StorageError); ok {
						// retry only in case of temporary storage errors.
						if stErr.Temporary() {
							action = "Retry: StorageError with error service code and Temporary()"
						} else if stErr.Response() != nil && isSuccessStatusCode(stErr.Response()) { // This is a temporarily work around.
							action = "Retry: StorageError with success status code"
						} else {
							action = "NoRetry: StorageError not Temporary() and without retriable status code"
						}
					} else if _, ok := err.(net.Error); ok {
						action = "Retry: net.Error and Temporary() or Timeout()"
					} else {
						action = "NoRetry: unrecognized error"
					}

				default:
					action = "NoRetry: successful HTTP request" // no error
				}

				logf("Action=%s\n", action)
				if action[0] != 'R' { // Retry only if action starts with 'R'
					if err != nil {
						tryCancel() // If we're returning an error, cancel this current/last per-retry timeout context
					} else {
						// We wrap the last per-try context in a body and overwrite the Response's Body field with our wrapper.
						// So, when the user closes the Body, the our per-try context gets closed too.
						// Another option, is that the Last Policy do this wrapping for a per-retry context (not for the user's context)
						if response == nil || response.Response() == nil {
							// We do panic in the case response or response.Response() is nil,
							// as for client, the response should not be nil if request is sent and the operations is executed successfully.
							// Another option, is that execute the cancel function when response or response.Response() is nil,
							// as in this case, current per-try has nothing to do in future.
							panic("invalid state, response should not be nil when the operation is executed successfully")
						}

						response.Response().Body = &contextCancelReadCloser{cf: tryCancel, body: response.Response().Body}
					}
					break // Don't retry
				}
				if response.Response() != nil {
					// If we're going to retry and we got a previous response, then flush its body to avoid leaking its TCP connection
					io.Copy(ioutil.Discard, response.Response().Body)
					response.Response().Body.Close()
				}
				// If retrying, cancel the current per-try timeout context
				tryCancel()
			}
			return response, err // Not retryable or too many retries; return the last response/error
		}
	})
}

var retrySuppressionContextKey = contextKey{"retrySuppression"}

// withNoRetryForBlob returns a context that contains a marker to say we don't want any retries to happen
// Is only implemented for blob pipelines at present
func withNoRetryForBlob(ctx context.Context) context.Context {
	return context.WithValue(ctx, retrySuppressionContextKey, struct{}{})
	// TODO: this is fragile, in the sense that we have no way to check, here, that we are running in a pipeline that
	//    actually knows how to check the context for the value.  Maybe add a check here, if/when we rationalize
	//    all our retry policies into one
}

// TODO: Fix the separate retry policies, use Azure blob's retry policy after blob SDK with retry optimization get released.
// NewBlobXferRetryPolicyFactory creates a RetryPolicyFactory object configured using the specified options.
func NewBlobXferRetryPolicyFactory(o XferRetryOptions) pipeline.Factory {
	o = o.defaults() // Force defaults to be calculated
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		return func(ctx context.Context, request pipeline.Request) (response pipeline.Response, err error) {
			// Before each try, we'll select either the primary or secondary URL.
			primaryTry := int32(0) // This indicates how many tries we've attempted against the primary DC

			// We only consider retrying against a secondary if we have a read request (GET/HEAD) AND this policy has a Secondary URL it can use
			considerSecondary := (request.Method == http.MethodGet || request.Method == http.MethodHead) && o.retryReadsFromSecondaryHost() != ""

			// Exponential retry algorithm: ((2 ^ attempt) - 1) * delay * random(0.8, 1.2)
			// When to retry: connection failure or temporary/timeout. NOTE: StorageError considers HTTP 500/503 as temporary & is therefore retryable
			// If using a secondary:
			//    Even tries go against primary; odd tries go against the secondary
			//    For a primary wait ((2 ^ primaryTries - 1) * delay * random(0.8, 1.2)
			//    If secondary gets a 404, don't fail, retry but future retries are only against the primary
			//    When retrying against a secondary, ignore the retry count and wait (.1 second * random(0.8, 1.2))
			maxTries := o.MaxTries
			if _, ok := ctx.Value(retrySuppressionContextKey).(struct{}); ok {
				maxTries = 1 // retries are suppressed by the context
			}
			for try := int32(1); try <= maxTries; try++ {
				logf("\n=====> Try=%d\n", try)

				// Determine which endpoint to try. It's primary if there is no secondary or if it is an add # attempt.
				tryingPrimary := !considerSecondary || (try%2 == 1)
				// Select the correct host and delay
				if tryingPrimary {
					primaryTry++
					delay := o.calcDelay(primaryTry)
					logf("Primary try=%d, Delay=%f s\n", primaryTry, delay.Seconds())
					time.Sleep(delay) // The 1st try returns 0 delay
				} else {
					// For casts and rounding - be careful, as per https://github.com/golang/go/issues/20757
					delay := time.Duration(float32(time.Second) * (rand.Float32()/2 + 0.8))
					logf("Secondary try=%d, Delay=%f s\n", try-primaryTry, delay.Seconds())
					time.Sleep(delay) // Delay with some jitter before trying secondary
				}

				// Clone the original request to ensure that each try starts with the original (unmutated) request.
				requestCopy := request.Copy()

				// For each try, seek to the beginning of the body stream. We do this even for the 1st try because
				// the stream may not be at offset 0 when we first get it and we want the same behavior for the
				// 1st try as for additional tries.
				err = requestCopy.RewindBody()
				common.PanicIfErr(err)

				if !tryingPrimary {
					requestCopy.URL.Host = o.retryReadsFromSecondaryHost()
					requestCopy.Host = o.retryReadsFromSecondaryHost()
				}

				// Set the server-side timeout query parameter "timeout=[seconds]"
				timeout := int32(o.TryTimeout.Seconds()) // Max seconds per try
				if deadline, ok := ctx.Deadline(); ok {  // If user's ctx has a deadline, make the timeout the smaller of the two
					t := int32(deadline.Sub(time.Now()).Seconds()) // Duration from now until user's ctx reaches its deadline
					logf("MaxTryTimeout=%d secs, TimeTilDeadline=%d sec\n", timeout, t)
					if t < timeout {
						timeout = t
					}
					if timeout < 0 {
						timeout = 0 // If timeout ever goes negative, set it to zero; this happen while debugging
					}
					logf("TryTimeout adjusted to=%d sec\n", timeout)
				}
				q := requestCopy.Request.URL.Query()
				q.Set("timeout", strconv.Itoa(int(timeout+1))) // Add 1 to "round up"
				requestCopy.Request.URL.RawQuery = q.Encode()
				logf("Url=%s\n", requestCopy.Request.URL.String())

				// Set the time for this particular retry operation and then Do the operation.
				tryCtx, tryCancel := context.WithTimeout(ctx, time.Second*time.Duration(timeout))
				//requestCopy.body = &deadlineExceededReadCloser{r: requestCopy.Request.body}
				response, err = next.Do(tryCtx, requestCopy) // Make the request
				/*err = improveDeadlineExceeded(err)
				if err == nil {
					response.Response().body = &deadlineExceededReadCloser{r: response.Response().body}
				}*/
				logf("Err=%v, response=%v\n", err, response)

				action := "" // This MUST get changed within the switch code below
				switch {
				case err == nil:
					action = "NoRetry: successful HTTP request" // no error

				case !tryingPrimary && response != nil && response.Response() != nil && response.Response().StatusCode == http.StatusNotFound:
					// If attempt was against the secondary & it returned a StatusNotFound (404), then
					// the resource was not found. This may be due to replication delay. So, in this
					// case, we'll never try the secondary again for this operation.
					considerSecondary = false
					action = "Retry: Secondary URL returned 404"

				case ctx.Err() != nil:
					action = "NoRetry: Op timeout"

				case err != nil:
					// NOTE: Protocol Responder returns non-nil if REST API returns invalid status code for the invoked operation
					// retry on all the network errors.
					// zc_policy_retry perform the retries on Temporary and Timeout Errors only.
					// some errors like 'connection reset by peer' or 'transport connection broken' does not implement the Temporary interface
					// but they should be retried. So redefined the retry policy for azcopy to retry for such errors as well.

					// TODO make sure Storage error can be cast to different package's error object
					// TODO: Discuss the error handling of Go Blob SDK.
					if stErr, ok := err.(azblob.StorageError); ok {
						// retry only in case of temporary storage errors.
						if stErr.Temporary() {
							action = "Retry: StorageError with error service code and Temporary()"
						} else if stErr.Response() != nil && isSuccessStatusCode(stErr.Response()) { // This is a temporarily work around.
							action = "Retry: StorageError with success status code"
						} else {
							action = "NoRetry: StorageError not Temporary() and without retriable status code"
						}
					} else if _, ok := err.(net.Error); ok {
						action = "Retry: net.Error"
					} else {
						action = "NoRetry: unrecognized error"
					}

				default:
					action = "NoRetry: successful HTTP request" // no error
				}

				logf("Action=%s\n", action)
				if action[0] != 'R' { // Retry only if action starts with 'R'
					if err != nil {
						tryCancel() // If we're returning an error, cancel this current/last per-retry timeout context
					} else {
						// We wrap the last per-try context in a body and overwrite the Response's Body field with our wrapper.
						// So, when the user closes the Body, the our per-try context gets closed too.
						// Another option, is that the Last Policy do this wrapping for a per-retry context (not for the user's context)
						if response == nil || response.Response() == nil {
							// We do panic in the case response or response.Response() is nil,
							// as for client, the response should not be nil if request is sent and the operations is executed successfully.
							// Another option, is that execute the cancel function when response or response.Response() is nil,
							// as in this case, current per-try has nothing to do in future.
							panic("invalid state, response should not be nil when the operation is executed successfully")
						}

						response.Response().Body = &contextCancelReadCloser{cf: tryCancel, body: response.Response().Body}
					}
					break // Don't retry
				}
				if response.Response() != nil {
					// If we're going to retry and we got a previous response, then flush its body to avoid leaking its TCP connection
					io.Copy(ioutil.Discard, response.Response().Body)
					response.Response().Body.Close()
				}
				// If retrying, cancel the current per-try timeout context
				tryCancel()
			}
			return response, err // Not retryable or too many retries; return the last response/error
		}
	})
}

var successStatusCodes = []int{http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent, http.StatusPartialContent}

func isSuccessStatusCode(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	for _, i := range successStatusCodes {
		if i == resp.StatusCode {
			return true
		}
	}
	return false
}

// contextCancelReadCloser helps to invoke context's cancelFunc properly when the ReadCloser is closed.
type contextCancelReadCloser struct {
	cf   context.CancelFunc
	body io.ReadCloser
}

func (rc *contextCancelReadCloser) Read(p []byte) (n int, err error) {
	return rc.body.Read(p)
}

func (rc *contextCancelReadCloser) Close() error {
	err := rc.body.Close()
	if rc.cf != nil {
		rc.cf()
	}
	return err
}

// According to https://github.com/golang/go/wiki/CompilerOptimizations, the compiler will inline this method and hopefully optimize all calls to it away
var logf = func(format string, a ...interface{}) {}
