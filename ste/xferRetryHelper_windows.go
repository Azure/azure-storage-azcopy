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
			strings.Contains(strings.ToLower(err.Error()), "an existing connection was forcibly closed by the remote host.") { // But just in case something funny happened along the line, let's listen for the string we expect.
			return true
		}

		return false
	}
}
