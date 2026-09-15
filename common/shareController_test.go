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

import "testing"

func TestShareKeyFromRawURL(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"files with sas and path", "https://acct.file.core.windows.net/share1/dir/file.txt?sig=redacted", "acct.file.core.windows.net/share1"},
		{"files host case-insensitive", "https://ACCT.FILE.core.windows.net/Share1", "acct.file.core.windows.net/Share1"},
		{"blob is not a files url", "https://acct.blob.core.windows.net/container/blob", ""},
		{"files root has no share", "https://acct.file.core.windows.net/", ""},
		{"garbage", "::::", ""},
	}
	for _, c := range cases {
		if got := ShareKeyFromRawURL(c.raw); got != c.want {
			t.Errorf("%s: ShareKeyFromRawURL(%q)=%q, want %q", c.name, c.raw, got, c.want)
		}
	}
}

// TestGetOrCreateShareControl_Idempotent verifies the registry returns the same
// pacer for the same share key and nil for non-Files URLs.
func TestGetOrCreateShareControl_Idempotent(t *testing.T) {
	const shareURL = "https://acct.file.core.windows.net/testshare-idempotent"
	p1 := GetOrCreateSharePacer(shareURL, 1)
	if p1 == nil {
		t.Fatal("expected a pacer to be created for a Files URL")
	}
	p2 := GetOrCreateSharePacer(shareURL, 1)
	if p1 != p2 {
		t.Fatal("expected the same pacer instance for the same share key")
	}
	if GetOrCreateSharePacer("https://acct.blob.core.windows.net/container", 1) != nil {
		t.Fatal("expected nil pacer for a non-Files URL")
	}
}

// TestHandleShareResponse_NoopForUnknownShare verifies feeding a response for a
// share with no registered controller is a safe no-op.
func TestHandleShareResponse_NoopForUnknownShare(t *testing.T) {
	HandleShareResponse("acct.file.core.windows.net/never-registered", ThrottleUnknown, 0)
	HandleShareResponse("", ThrottleNone, 0)
}
