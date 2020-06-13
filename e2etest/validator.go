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
	"net/url"
	"runtime"
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"

	chk "gopkg.in/check.v1"
)

type Validator struct{}

func (Validator) ValidateCopyTransfersAreScheduled(c *chk.C, isSrcEncoded bool, isDstEncoded bool,
	sourcePrefix string, destinationPrefix string, expectedTransfers []string, actualTransfers []common.TransferDetail) {

	normalizeSlashes := func(s string) string {
		return strings.Replace(s, "\\", "/", -1)
	}

	if len(expectedTransfers) > 0 && !common.IsShortPath(actualTransfers[0].Src) {
		sourcePrefix = common.ToExtendedPath(sourcePrefix)
	}
	if len(expectedTransfers) > 0 && !common.IsShortPath(actualTransfers[0].Dst) {
		destinationPrefix = common.ToExtendedPath(destinationPrefix)
	}
	sourcePrefix = normalizeSlashes(sourcePrefix)
	destinationPrefix = normalizeSlashes(destinationPrefix)

	// validate that the right number of transfers were scheduled
	c.Assert(len(actualTransfers), chk.Equals, len(expectedTransfers))

	// validate that the right transfers were sent
	lookupMap := scenarioHelper{}.convertListToMap(expectedTransfers)
	for _, transfer := range actualTransfers {
		srcRelativeFilePath := strings.TrimPrefix(strings.TrimPrefix(normalizeSlashes(transfer.Src), sourcePrefix), "/")
		dstRelativeFilePath := strings.TrimPrefix(strings.TrimPrefix(normalizeSlashes(transfer.Dst), destinationPrefix), "/")

		if isSrcEncoded {
			srcRelativeFilePath, _ = url.PathUnescape(srcRelativeFilePath)

			if runtime.GOOS == "windows" {
				// Decode unsafe dst characters on windows
				pathParts := strings.Split(dstRelativeFilePath, "/")
				invalidChars := `<>\/:"|?*` + string(0x00)

				for _, c := range strings.Split(invalidChars, "") {
					for k, p := range pathParts {
						pathParts[k] = strings.ReplaceAll(p, url.PathEscape(c), c)
					}
				}

				dstRelativeFilePath = strings.Join(pathParts, "/")
			}
		}

		if isDstEncoded {
			dstRelativeFilePath, _ = url.PathUnescape(dstRelativeFilePath)
		}

		// the relative paths should be equal
		c.Assert(srcRelativeFilePath, chk.Equals, dstRelativeFilePath)

		// look up the path from the expected transfers, make sure it exists
		_, transferExist := lookupMap[srcRelativeFilePath]
		c.Assert(transferExist, chk.Equals, true)
	}
}
