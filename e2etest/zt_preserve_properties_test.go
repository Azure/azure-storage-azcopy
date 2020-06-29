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
	"testing"
)

// Purpose: Tests for preserving transferred properties, info and ACLs.  Both those possessed by the original source file/folder,
//   and those specified on the command line

func TestProperties_NameValueMetadataIsPreservedS2S(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllAzureS2S(),
		eValidate.Auto(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				f("filea", with{nameValueMetadata: map[string]string{"foo": "abc", "bar": "def"}}),
				folder("fold1", with{nameValueMetadata: map[string]string{"other": "xyz"}}),
			},
		})
}

func TestProperties_NameValueMetadataCanBeUploaded(t *testing.T) {
	expectedMap := map[string]string{"foo": "abc", "bar": "def"}

	RunScenarios(
		t,
		eOperation.Copy(), // Sync doesn't support the command-line metadata flag
		eTestFromTo.AllUploads(),
		eValidate.Auto(),
		params{
			recursive: true,
			metadata:  "foo=abc;bar=def",
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder("", verifyOnly{with{nameValueMetadata: expectedMap}}), // root folder
				f("filea", verifyOnly{with{nameValueMetadata: expectedMap}}),
			},
		})
}

// TODO: add some tests (or modify the above) to make assertions about case preservation (or not) in metadata
//    See https://github.com/Azure/azure-storage-azcopy/issues/113 (which incidentally, I'm not observing in the tests above, for reasons unknown)

/* todo
func TestProperties_SMBDates(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllSMBPairs(), // these are the only pairs where we preserve last write time AND creation time
		eValidate.CreationTime() && eValidate.LastWriteTimeTime(),
		params{
			recursive:       true,
			preserveSmbInfo: true,
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				// Pause then re-write all the files, so that their LastWriteTime is different from their creation time
				// So that when validating, our validation can be sure that the right datetime has ended up in the right
				// field
				time.Sleep(5 * time.Second)
				h.CreateFiles(h.GetTestFiles(), true)
				// And pause again, so that that the write times at the destination wont' just _automatically_ match the source times
				// (due to there being < 1 sec delay between creation and completion of copy). With this delay, we know they only match
				// if AzCopy really did preserve them
				time.Sleep(10 * time.Second) // we are assuming here, that the clock skew between source and dest is less than 10 secs
			},
		},
		testFiles{
			size: "1K",
			// no need to set specific dates on these. Instead, we just mess with the write times in
			// beforeRunJob
			// TODO: is that what we really want, or do we want to set write times here?
			shouldTransfer: []string{
				"filea",
				folder("fold1"),
				"fold1/fileb",
			},
		})
}

func TestProperties_SMBDates(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllSMBPairs(), // these are the only pairs where we preserve last write time AND creation time
		eValidate.SMBAttributes(),
		params{
			recursive:       true,
			preserveSmbInfo: true,
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				// Pause then re-write all the files, so that their LastWriteTime is different from their creation time
				// So that when validating, our validation can be sure that the right datetime has ended up in the right
				// field
				time.Sleep(5 * time.Second)
				h.CreateFiles(h.GetTestFiles(), true)
				// And pause again, so that that the write times at the destination wont' just _automatically_ match the source times
				// (due to there being < 1 sec delay between creation and completion of copy). With this delay, we know they only match
				// if AzCopy really did preserve them
				time.Sleep(10 * time.Second) // we are assuming here, that the clock skew between source and dest is less than 10 secs
			},
		},
		testFiles{
			size: "1K",
			shouldTransfer: []string{
				f("filea", with{smbAttributes: ABTC}),
				folder("fold1"),
				"fold1/fileb",
			},
		})
}
*/
