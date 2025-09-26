//go:build linux

package ste

import (
	"errors"
	"golang.org/x/sys/unix"
	"net/http"
	"strings"
)

func init() {
	platformRetryPolicy = func(response *http.Response, err error) bool {
		if err == nil {
			return false
		}

		if errors.Is(err, unix.EADDRNOTAVAIL) ||
			strings.Contains(strings.ToLower(err.Error()), strings.ToLower(unix.EADDRNOTAVAIL.Error())) {
			return true
		}

		return false
	}
}
