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
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type Validator struct{}

// Use this to ensure that source and dest strings can be compared with each other
func makeSlashesComparable(s string) string {
	return strings.Replace(s, "\\", "/", -1)
}

// Use this to ensure slashes are correct for the location, loc
func fixSlashes(s string, loc common.Location) string {
	if loc == common.ELocation.Local() {
		// replace all slashes with the one that right for the local OS
		s = strings.Replace(s, "/", common.OS_PATH_SEPARATOR, -1)
		s = strings.Replace(s, "\\", common.OS_PATH_SEPARATOR, -1)
	} else {
		// replace all backslashes with web-style forward slash
		s = strings.Replace(s, "\\", common.AZCOPY_PATH_SEPARATOR_STRING, -1)
	}
	return s
}

// versionIDRegex is intended to capture variations of the destination version ID.
var versionIDRegex = regexp.MustCompile("^\\d{4}-\\d{2}-\\d{2}T\\d{2}[-:]\\d{2}[-:]\\d{2}\\.\\d{7}Z")

func trimBaseSnapshotDetails(c asserter, url *url.URL, location common.Location, acctType AccountType) (trimmed, snapshot string) {
	switch {
	case location == common.ELocation.File(), location == common.ELocation.FileNFS():
		snapshot = url.Query().Get("sharesnapshot")
		if snapshot != "" {
			query := url.Query()
			query.Del("sharesnapshot")
			url.RawQuery = query.Encode()
			trimmed = url.String()
		} else {
			trimmed = url.String()
		}
	case location == common.ELocation.Blob() && acctType.IsManagedDisk():
		snapshot = url.Query().Get("snapshot")
		if snapshot != "" {
			query := url.Query()
			query.Del("snapshot")
			url.RawQuery = query.Encode()
			trimmed = url.String()
		} else {
			trimmed = url.String()
		}
	default:
		trimmed = url.String()
	}

	return
}

func (Validator) ValidateRemoveTransfer(c asserter, isSrcEncoded bool, isDstEncoded bool,
	sourcePrefix string, destinationPrefix string, expectedTransfers []*testObject, actualTransfers []common.TransferDetail, statusToTest common.TransferStatus) {
	// TODO: Think of how to validate files in case of remove
}
func (Validator) ValidateCopyTransfersAreScheduled(s *scenario, isSrcEncoded bool, isDstEncoded bool,
	sourcePrefix string, destinationPrefix string, expectedTransfers []*testObject, actualTransfers []common.TransferDetail, statusToTest common.TransferStatus, expectFolders bool) {
	c := s.a
	tf := s.GetTestFiles()

	sourcePrefix = makeSlashesComparable(sourcePrefix)
	destinationPrefix = makeSlashesComparable(destinationPrefix)
	snapshotID := ""
	if isSrcEncoded {
		// i.e. source is a URL
		srcPrefixURL, err := url.Parse(sourcePrefix)
		if err == nil {
			sourcePrefix, snapshotID = trimBaseSnapshotDetails(c, srcPrefixURL, s.fromTo.From(), s.srcAccountType)
		}
	}

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
		if to.isFolder() && expectFolders {
			return addFolderSuffix(to.name)
		} else {
			return to.name
		}
	})

	for _, transfer := range actualTransfers {
		if snapshotID != "" {
			c.Assert(strings.Contains(transfer.Src, snapshotID), equals(), true)
			uri, err := url.Parse(transfer.Src)
			c.AssertNoErr(err, "url must parse, sanity check")
			transfer.Src, _ = trimBaseSnapshotDetails(c, uri, s.fromTo.From(), s.srcAccountType)
		}

		srcRelativeFilePath := strings.Trim(strings.TrimPrefix(makeSlashesComparable(transfer.Src), sourcePrefix), "/")
		dstRelativeFilePath := strings.Trim(strings.TrimPrefix(makeSlashesComparable(transfer.Dst), destinationPrefix), "/")

		if isSrcEncoded {
			srcRelativeFilePath, _ = url.PathUnescape(srcRelativeFilePath)

			if runtime.GOOS == "windows" {
				// Decode unsafe dst characters on windows
				pathParts := strings.Split(dstRelativeFilePath, "/")
				invalidChars := `<>\/:"|?*` + string(rune(0x00))

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

		if tf.isListOfVersions() { // Append the appropriate version for the lookup
			versionID := versionIDRegex.FindString(filepath.Base(dstRelativeFilePath))
			c.Assert(versionID, notEquals(), "", "expected to find a version attached to the file name")
			// flatten the version ID
			versionID = strings.ReplaceAll(versionID, ":", "-")

			srcRelativeFilePath = filepath.Join(filepath.Dir(srcRelativeFilePath), versionID+"-"+filepath.Base(srcRelativeFilePath))
		}

		if transfer.Dst != os.DevNull { // Don't check if the destination is NUL-- It won't be correct.
			// the relative paths should be equal
			c.Assert(dstRelativeFilePath, equals(), srcRelativeFilePath)
		}

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
