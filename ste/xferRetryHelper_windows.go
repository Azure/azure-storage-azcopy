//go:build windows

package ste

import (
	"errors"
	"golang.org/x/sys/windows"
	"net/http"
	"strings"
)

func init() {

	platformRetryPolicy = func(response *http.Response, err error) bool {
		if err == nil {
			return false // we have no tests to run against this
		}

		// It's entirely possible something in between closed our connection.
		if errors.Is(err, windows.WSAECONNRESET) || // Catch it in the idiomatic way
			strings.Contains(strings.ToLower(err.Error()), strings.ToLower(windows.WSAECONNRESET.Error())) { // But just in case something funny happened along the line, let's listen for the string we expect.
			return true
		}

		// This can sometimes happen if we're trying too many connections. It usually resolves itself quickly.
		if errors.Is(err, windows.WSAEADDRINUSE) ||
			strings.Contains(strings.ToLower(err.Error()), strings.ToLower(windows.WSAEADDRINUSE.Error())) {
			return true
		}

		// It's possible something in between might time us out too, in which case windows will respond WSAETIMEDOUT
		if errors.Is(err, windows.WSAETIMEDOUT) ||
			strings.Contains(strings.ToLower(err.Error()), strings.ToLower(windows.WSAETIMEDOUT.Error())) {
			return true
		}

		return false
	}
}
