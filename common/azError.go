// Copyright © Microsoft <wastore@microsoft.com>
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

package common

import "fmt"

// AzError is to handle AzCopy internal errors in a fine way
type AzError struct {
	code          uint64
	msg           string
	additonalInfo string
}

// NewAzError composes an AzError with given code and message
func NewAzError(base AzError, additionalInfo string) AzError {
	base.additonalInfo = additionalInfo
	return base
}

func (err AzError) ErrorCode() uint64 {
	return err.code
}

func (lhs AzError) Equals(rhs AzError) bool {
	return lhs.code == rhs.code
}

func (err AzError) Error() string {
	return err.msg + err.additonalInfo
}

var EAzError AzError

func (err AzError) LoginCredMissing() AzError {
	return AzError{uint64(1), "Login Credentials missing. ", ""}
}

func (err AzError) InvalidBlobName() AzError {
	return AzError{uint64(2), "Invalid Blob Name.", ""}
}

func (err AzError) InvalidBlobOrWindowsName() AzError {
	return AzError{uint64(3), "Invalid Blob or Windows Name. ", ""}
}

func (err AzError) InvalidServiceClient() AzError {
	return AzError{uint64(4), "Invalid Service Client. ", ""}
}

func ErrInvalidClient(msg string) AzError {
	return NewAzError(EAzError.InvalidServiceClient(), fmt.Sprintf("Expecting %s client", msg))
}
