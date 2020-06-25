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

package e2etest

import (
	"fmt"
	"os"
	"reflect"
	"strconv"

	"github.com/JeffreyRichter/enum/enum"
)

// clearly define all the inputs to the end-to-end tests
// it's ok to panic if the inputs are absolutely required
// the general guidance is to take in as few parameters as possible
type GlobalInputManager struct{}

func (GlobalInputManager) GetAccountAndKey(accountType AccountType) (string, string) {
	var name, key string

	switch accountType {
	case EAccountType.Standard():
		name = os.Getenv("AZCOPY_E2E_ACCOUNT_NAME")
		key = os.Getenv("AZCOPY_E2E_ACCOUNT_KEY")
	default:
		panic("Only the standard account type is supported for the moment.")
	}

	if name == "" || key == "" {
		panic(fmt.Sprintf("Name and key for %s account must be set before running tests", accountType))
	}

	return name, key
}

func (GlobalInputManager) GetExecutablePath() string {
	path := os.Getenv("AZCOPY_E2E_EXECUTABLE_PATH")
	if path == "" {
		panic("Cannot test AzCopy if AZCOPY_E2E_EXECUTABLE_PATH is not provided")
	}

	return path
}

func (GlobalInputManager) KeepFailedData() bool {
	raw := os.Getenv("AZCOPY_E2E_KEEP_FAILED_DATA")
	if raw == "" {
		return false
	}

	result, err := strconv.ParseBool(raw)
	if err != nil {
		panic("If AZCOPY_E2E_KEEP_FAILED_DATA is set, it must be a boolean")
	}

	return result
}

var EAccountType = AccountType(0)

type AccountType uint8

func (AccountType) Standard() AccountType                     { return AccountType(0) }
func (AccountType) Premium() AccountType                      { return AccountType(1) }
func (AccountType) HierarchicalNamespaceEnabled() AccountType { return AccountType(2) }

func (o AccountType) String() string {
	return enum.StringInt(o, reflect.TypeOf(o))
}
