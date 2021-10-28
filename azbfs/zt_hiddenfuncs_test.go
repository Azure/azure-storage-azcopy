package azbfs

// This file isn't normally compiled when not testing.
// Therefore, since we're in the azbfs package, we can export a method to help us construct a failing retry reader options.

func InjectErrorInRetryReaderOptions(err error) RetryReaderOptions {
	return RetryReaderOptions{
		MaxRetryRequests:       1,
		doInjectError:          true,
		doInjectErrorRound:     0,
		injectedError:          err,
		NotifyFailedRead:       nil,
		TreatEarlyCloseAsError: false,
	}
}
