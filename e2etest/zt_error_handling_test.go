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

// Purpose: Tests for how we respond to errors. Maybe also resume?

// TODO: include how we clean up destination files/blobs after errors

// This test runs a transfer partially, then cancel the job, then resumes it
/*
 *
func TestError_CanResume(t *testing.T) {
	// TODO: as at 1 July 2020, this test doesn't pass, but if you run it, it will tell you why (all to do with missing parts of the framework)
	//   It's here as one possible approach for testing resume.
	// TODO: the current test data volume might be a bit big
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllSourcesToOneDest(), // TODO: or should we make this AllPairs?
		eValidate.AutoPlusContent(),       // important to validate file content after a resume  // TODO: as at 1 July 2020, content validation isn't supported yet, so this test will fail
		params{
			recursive:       true,
			capMbps:         50,   // at this speed the payload should take about 50 seconds to move
			cancelFromStdin: true, // needed to use hookHelper.CancelAndResume()
		},
		&hooks{beforeRunJob: func(h hookHelper) {
			go func() {
				// wait a while, until we are probably somewhere the middle of the transfer, and then
				// kill AzCopy and resume it
				// Must do this in a separate GoRoutine, since we are (ab)using the beforeRunJob hook, and the job won't start
				// unit our hook func returns
				time.Sleep(20 * time.Second)
				h.CancelAndResume()
			}()
		}},
		testFiles{
			// Make a payload this is big enough to last for a minute or so, at our capped speed as per params above.
			// Make the files in it big enough to have multiple chunks, since that's the more interesting case for resume.
			defaultSize: "50M",
			shouldTransfer: []interface{}{
				folder(""),
				"a",
				"b",
				folder("fold1"),
				"fold1/f",
				"fold1/g",
				folder("fold1/fold2"),
				"fold1/j",
				"fold1/k",
			},
		})
}


*/
///* Go fmt is messing things
//				"fold1/k",
//			},
//		})
//}
