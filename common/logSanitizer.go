// Copyright Microsoft <wastore@microsoft.com>
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
	"github.com/Azure/azure-pipeline-go/pipeline"
)

// azCopyLogSanitizer performs string-replacement based log redaction
// This serves as a backstop, to help make sure that secrets don't get logged.
// It does search and replace of the types of credentials that are expected to exist in an
// azCopy application (which are typically auth tokens inside URLs).
// While we already have code that redacts these from known request headers and request URLs,
// they can still be put into errors (e.g. HTTP errors) and if those errors are logged then those
// secrets will leak into the logs if we don't have this class to filter them out.
// The alternative would be to filter at all sites where such errors (and other things) may be
// logged, but that's less maintainable in the long term.
type azCopyLogSanitizer struct {
}

func NewAzCopyLogSanitizer() pipeline.LogSanitizer {
	return &azCopyLogSanitizer{}
}

// SanitizeLogLine removes credentials and credential-like strings that are expected to exist
// in material logged by this application.
// Note: it does not remove whole headers. Specifically, it does not remove
// Authorization headers.  Those are assumed to be removed by azure-pipeline-go.
// It does however remove signatures of the type found in SAS tokens and AWS presigned URLs.
// The implementation uses a 'to lower' of the raw string, because the alternative (of
// using case-insensitive regexs) was surprisingly measured as 36 times slower in testing.
func (s *azCopyLogSanitizer) SanitizeLogLine(raw string) string {
	return raw // TODO implement
}
