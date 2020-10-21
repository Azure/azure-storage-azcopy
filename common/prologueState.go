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

package common

type cutdownJptm interface {
	ResourceDstData(dataFileToXfer []byte) (headers ResourceHTTPHeaders, metadata Metadata)
}

// PrologueState contains info necessary for different sending operations' prologue.
type PrologueState struct {
	// Leading bytes are the early bytes of the file, to be used
	// for mime-type detection (or nil if file is empty or the bytes code
	// not be read).
	LeadingBytes []byte
}

//func (ps PrologueState) CanInferContentType() bool {
//	return len(ps.LeadingBytes) > 0 // we can have a go, if we have some leading bytes
//}

func (ps PrologueState) GetInferredContentType(jptm cutdownJptm) string {
	headers, _ := jptm.ResourceDstData(ps.LeadingBytes)
	return headers.ContentType
}
