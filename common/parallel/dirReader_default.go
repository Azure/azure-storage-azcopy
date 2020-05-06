// +build !linux

// TODO: this currently applies on Darwin. Should it? Or should we use the Linux implementation there?

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

package parallel

import (
	"os"
)

func NewDirReader(parallelism int) DirReader {
	return defaultDirReader{}
}

type defaultDirReader struct{}

// Readdir in the default reader just makes the normal OS read call
// On Windows, this is performant because Go does not have to make any additional OS calls to hydrate the raw results into os.FileInfos.
func (_ defaultDirReader) Readdir(dir *os.File, n int) ([]os.FileInfo, error) {
	return dir.Readdir(n)
}

func (_ defaultDirReader) Close() {
	// noop
}
