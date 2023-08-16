package ste

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"math/rand"
	"time"
)

// XferRetryPolicy tells the pipeline what kind of retry policy to use. See the XferRetryPolicy* constants.
// Added a new retry policy and not using the existing policy zc_retry_policy.go since there are some changes
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

// withNoRetryForBlob returns a context that contains a marker to say we don't want any retries to happen
// Is only implemented for blob pipelines at present
func withNoRetryForBlob(ctx context.Context) context.Context {
	return runtime.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: 1})
}
