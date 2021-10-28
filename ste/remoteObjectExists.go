// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ste

import (
	"net/http"
	"time"
)

// an error with an HTTP Response
type responseError interface {
	Response() *http.Response
}

type lastModifiedTimerProvider interface {
	LastModified() time.Time
}

// remoteObjectExists takes the error returned when trying to access a remote object, sees whether is
// a "not found" error.  If the object exists (i.e. error is nil) it returns (true, nil).  If the
// error is a "not found" error, it returns (false, nil). Else it returns false and the original error.
// The initial, dummy, parameter, is to allow callers to conveniently call it with functions that return a tuple
// - even though we only need the error.
func remoteObjectExists(props lastModifiedTimerProvider, errWhenAccessingRemoteObject error) (bool, time.Time, error) {

	if typedErr, ok := errWhenAccessingRemoteObject.(responseError); ok && typedErr.Response().StatusCode == http.StatusNotFound {
		return false, time.Time{}, nil // 404 error, so it does NOT exist
	} else if errWhenAccessingRemoteObject != nil {
		return false, time.Time{}, errWhenAccessingRemoteObject // some other error happened, so we return it
	} else {
		return true, props.LastModified(), nil // If err equals nil, the file exists
	}
}
