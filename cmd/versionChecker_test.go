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

package cmd

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestVersionEquality(t *testing.T) {
	a := assert.New(t)
	// simple equal
	v1, _ := NewVersion("10.0.0")
	v2, _ := NewVersion("10.0.0")
	a.Zero(v1.compare(*v2))

	// preview version equal
	v1, _ = NewVersion("10.0.0-preview")
	v2, _ = NewVersion("10.0.0-preview")
	a.Zero(v1.compare(*v2))

	// future version equal
	v1, _ = NewVersion("10.0.0-preview")
	v2, _ = NewVersion("10.0.0-beta5")
	a.Zero(v1.compare(*v2))
}

func TestVersionSuperiority(t *testing.T) {
	a := assert.New(t)
	// major version bigger
	v1, _ := NewVersion("11.3.0")
	v2, _ := NewVersion("10.8.3")
	a.Equal(1, v1.compare(*v2))

	// minor version bigger
	v1, _ = NewVersion("15.5.6")
	v2, _ = NewVersion("15.3.5")
	a.Equal(1, v1.compare(*v2))

	// patch version bigger
	v1, _ = NewVersion("15.5.6")
	v2, _ = NewVersion("15.5.5")
	a.Equal(1, v1.compare(*v2))

	// preview bigger
	v1, _ = NewVersion("15.5.5")
	v2, _ = NewVersion("15.5.5-preview")
	a.Equal(1, v1.compare(*v2))
}

func TestVersionInferiority(t *testing.T) {
	a := assert.New(t)
	// major version smaller
	v1, _ := NewVersion("10.5.6")
	v2, _ := NewVersion("11.8.3")
	a.Equal(-1, v1.compare(*v2))

	// minor version smaller
	v1, _ = NewVersion("15.3.6")
	v2, _ = NewVersion("15.5.5")
	a.Equal(-1, v1.compare(*v2))

	// patch version smaller
	v1, _ = NewVersion("15.5.5")
	v2, _ = NewVersion("15.5.6")
	a.Equal(-1, v1.compare(*v2))

	// preview smaller
	v1, _ = NewVersion("15.5.5-preview")
	v2, _ = NewVersion("15.5.5")
	a.Equal(-1, v1.compare(*v2))
}