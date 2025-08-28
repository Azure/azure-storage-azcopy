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

import (
	"strings"
	"sync"

	"github.com/pkg/errors"
)

type ExclusiveStringMap struct {
	lock          *sync.Mutex
	m             map[string]struct{}
	caseSensitive bool
}

func NewExclusiveStringMap(fromTo FromTo, goos string) *ExclusiveStringMap {

	caseInsenstiveDownload := fromTo.IsDownload() &&
		(strings.EqualFold(goos, "windows") || strings.EqualFold(goos, "darwin")) // download to case insensitive OS
	caseSensitiveToRemote := fromTo.To() == ELocation.FileSMB() // upload to Windows-like cloud file system
	insensitive := caseInsenstiveDownload || caseSensitiveToRemote
	sensitive := !insensitive

	return &ExclusiveStringMap{
		lock:          &sync.Mutex{},
		m:             make(map[string]struct{}),
		caseSensitive: sensitive,
	}
}

var exclusiveStringMapCollisionError = errors.New("cannot simultaneously send two files to same destination name")

// Add succeeds if and only if key is not currently in the map
func (e *ExclusiveStringMap) Add(key string) error {
	key = e.convertCase(key)

	e.lock.Lock()
	defer e.lock.Unlock()

	_, alreadyThere := e.m[key]
	if alreadyThere {
		return exclusiveStringMapCollisionError
	}
	e.m[key] = struct{}{}
	return nil
}

func (e *ExclusiveStringMap) Remove(key string) {
	key = e.convertCase(key)

	e.lock.Lock()
	defer e.lock.Unlock()

	delete(e.m, key)
}

func (e *ExclusiveStringMap) convertCase(s string) string {
	if e.caseSensitive {
		return s
	} else {
		// technically, toLower is a bad way to make something case insensitive,
		// but, in our case, the collisions we are concerned about are
		// ones that are caused due to US (our code) mutating characters that fall within the ASCII
		// section of the code space (specifically escaping or unescaping special characters not
		// supported by Windows or Azure Files); rather than general case sensitivity issues in the broader sense.
		// So we think this is OK.
		// TODO: double check, is it really? Should we replace by going through rune-by-rune converting to unicode "fold case"?
		return strings.ToLower(s)
	}
}
