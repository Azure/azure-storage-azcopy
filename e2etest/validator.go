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
	"net/url"
	"runtime"
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
)

type Validator struct{}

func (Validator) ValidateCopyTransfersAreScheduled(c asserter, isSrcEncoded bool, isDstEncoded bool,
	sourcePrefix string, destinationPrefix string, expectedTransfers []*testObject, actualTransfers []common.TransferDetail, statusToTest common.TransferStatus) {

	normalizeSlashes := func(s string) string {
		return strings.Replace(s, "\\", "/", -1)
	}

	if len(actualTransfers) > 0 && !common.IsShortPath(actualTransfers[0].Src) {
		sourcePrefix = common.ToExtendedPath(sourcePrefix)
	}
	if len(actualTransfers) > 0 && !common.IsShortPath(actualTransfers[0].Dst) {
		destinationPrefix = common.ToExtendedPath(destinationPrefix)
	}
	sourcePrefix = normalizeSlashes(sourcePrefix)
	destinationPrefix = normalizeSlashes(destinationPrefix)

	// validate that the right number of transfers were scheduled
	c.Assert(len(actualTransfers), equals(), len(expectedTransfers),
		fmt.Sprintf("Number of actual and expected transfers should match, for status %s", statusToTest.String()))

	// validate that the right transfers were sent
	addFolderSuffix := func(s string) string {
		if strings.HasSuffix(s, "/") {
			panic("folder suffix already present")
		}
		return s + "/"
	}
	lookupMap := scenarioHelper{}.convertListToMap(expectedTransfers, func(to *testObject) string {
		if to.isFolder {
			return addFolderSuffix(to.name)
		} else {
			return to.name
		}
	})
	for _, transfer := range actualTransfers {
		srcRelativeFilePath := strings.Trim(strings.TrimPrefix(normalizeSlashes(transfer.Src), sourcePrefix), "/")
		dstRelativeFilePath := strings.Trim(strings.TrimPrefix(normalizeSlashes(transfer.Dst), destinationPrefix), "/")

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
		c.Assert(srcRelativeFilePath, equals(), dstRelativeFilePath)

		// look up the path from the expected transfers, make sure it exists
		folderMessage := ""
		lookupKey := srcRelativeFilePath
		if transfer.IsFolderProperties {
			lookupKey = addFolderSuffix(lookupKey)
			folderMessage = ".\n    The transfer was for a folder. Have you forgotten to include folders in your testFiles? (Use the folder() function)"
		}
		_, transferExist := lookupMap[lookupKey]
		c.Assert(transferExist, equals(), true,
			fmt.Sprintf("Transfer '%s' ended with status '%s' but was not expected to end in that status%s",
				lookupKey,
				statusToTest.String(),
				folderMessage))

		// TODO: do we also want to output specific filenames for ones that were expected to have that status, but did not get it?
	}
}
