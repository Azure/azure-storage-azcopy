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
	"bytes"
	"errors"
	"github.com/Azure/azure-storage-azcopy/common"
)

type md5Comparsion struct {
	expected      []byte
	actualAsSaved []byte
	logger        common.ILogger
}

// TODO: let's add an aka.ms link to the message, that gives more info
var errMd5Mismatch = errors.New("the MD5 hash of the data, as we received it, did not match the expected value, as found in the Blob/File Service. " +
	"This means that either there is a data integrity error OR another tool has failed to keep the stored hash up to date")

// TODO: let's add an aka.ms link to the message, that gives more info
var errExpectedMd5Missing = errors.New("no MD5 was stored in the Blob/File service against this file. This application is currently configured to treat missing MD5 hashes as errors")

var errActualMd5NotComputed = errors.New("no MDB was computed within this application. This indicates a logic error in this application")

// Check compares the two MD5s, and returns any error if applicable
// Any informational logging will be done within Check, so all the caller needs to do
// is respond to non-nil errors
func (c *md5Comparsion) Check() error {

	if c.actualAsSaved == nil || len(c.actualAsSaved) == 0 {
		return errActualMd5NotComputed // Should never happen
	}

	if c.expected == nil || len(c.expected) == 0 {
		return errExpectedMd5Missing
	}

	match := bytes.Equal(c.expected, c.actualAsSaved)
	if !match {
		return errMd5Mismatch
	}

	return nil

	// TODO: add logging-only options. E.g. 3-levels of processing: FailIfDifferentOrMissing, FailIfDifferent, LogOnly
}
