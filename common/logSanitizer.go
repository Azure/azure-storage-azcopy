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
	"regexp"
	"strings"
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

func NewAzCopyLogSanitizer() LogSanitizer {
	return &azCopyLogSanitizer{}
}

var sensitiveQueryStringKeys = []string{
	"sig", // was strings.ToLower(SigAzure), but that isn't fully init-order-safe, see init() method below
	//omitted because covered by "signature" below strings.ToLower(SigXAmzForAws),
	"signature",  // covers both "signature" and x-amz-signature. The former may be used in AWS authorization headers. Not sure if we ever log those, but may as well redact them if we do
	"token",      // seems worth removing in case something uses it one day (e.g. if we support a new backend)
	"credential", // covers redacting x-amz-credential from logs
}

// SanitizeLogLine removes credentials and credential-like strings that are expected to exist
// in material logged by this application.
// Note: it does not remove whole headers. Specifically, it does not remove the entire value of
// Authorization headers.  Those are assumed to be removed by azure-pipeline-go.
// It does however remove signatures of the type found in SAS tokens and AWS presigned URLs,
// plus several other things.
// The implementation uses a 'to lower' of the raw string, because the alternative (of
// using case-insensitive regexs) was surprisingly measured as 36 times slower in testing.
func (s *azCopyLogSanitizer) SanitizeLogMessage(msg string) string {
	lowerMsg := strings.ToLower(msg)

	for _, key := range sensitiveQueryStringKeys {
		// take a quick look, using contains, and then get fancy only if we
		// find something in the quick look
		if strings.Contains(lowerMsg, key) {
			msg = s.redact(msg, key) // must redact from the real (original case) msg, not lowerMsg
		}
	}

	return msg
}

func (s *azCopyLogSanitizer) redact(msg, key string) string {

	// The leading and trailing '-' chars are not in our older redaction code (which will continue to run,
	// at the point of logging requests etc.)
	// By including the dashes here we can examine logs (if we ever want to)
	// to see how many redactions are from here (with the -'s) and how many
	// are from the old code (without -'s).
	const redacted = "-REDACTED-"

	return sensitiveRegexMap[key].ReplaceAllString(msg, "$1"+redacted)
}

// as per https://groups.google.com/forum/#!topic/golang-nuts/3FVAs9dPR8k, this map should be
// safe for concurrent reads
var sensitiveRegexMap = make(map[string]*regexp.Regexp)

// init a map of pre-prepared regexes, one for each key
func init() {
	mapContainsAzureSig := false
	for _, key := range sensitiveQueryStringKeys {
		// We don't care what's before the key (in a query string it will always be ? or &, but that's not
		// the case in say, an auth header).
		// Also, for flexibility and robustness we allow : or = as the delimiter, and allow space around it.
		// We do ASSUME that the value to be redacted will never contain a &. We _think_
		// that assumption is reasonably for all anticipated values. (Without that
		// assumption, we'd have to redact query strings all the way to the end of the whole query string.)
		// Regex has two groups: first gets key and delimiter.
		// Second group gets as many chars as possible that do not terminate the value.
		sensitiveRegexMap[key] = regexp.MustCompile("(?i)(?P<key>" + key + "[ \t]*[:=][ \t]*)(?P<value>[^& ,;\t\n\r]+)")

		// see comment below
		if key == strings.ToLower(SigAzure) {
			mapContainsAzureSig = true
		}
	}

	// Double check.
	// We can't directly do strings.ToLower(SigAzure) in the declaration of
	// sensitiveRegexMap itself, since it won't be ready if something else logs before that
	// initialization has happened (as can be the case e.g. init of lifecylemanager).
	// So we just check here to make sure we have something that matches that constant.
	if !mapContainsAzureSig {
		panic("sensitiveQueryStrings is misconfigured and does not contain Azure sign")
	}
}
