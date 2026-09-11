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

package ste

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestClassifyAzureFilesThrottle(t *testing.T) {
	cases := []struct {
		name string
		code int
		body string
		want common.ThrottleKind
	}{
		{"ok", 200, "", common.ThrottleNone},
		{"iops", 429, "Operations per second is over the account limit.", common.ThrottleIops},
		{"egress", 503, "Egress is over the account limit.", common.ThrottleBandwidth},
		{"ingress", 429, "Ingress is over the account limit.", common.ThrottleBandwidth},
		{"unknown", 503, "Server busy.", common.ThrottleUnknown},
		{"unknown-empty", 429, "", common.ThrottleUnknown},
	}
	for _, c := range cases {
		if got := classifyAzureFilesThrottle(c.code, c.body); got != c.want {
			t.Errorf("%s: classify(%d,%q)=%v, want %v", c.name, c.code, c.body, got, c.want)
		}
	}
}
