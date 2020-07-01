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
	"time"
)

// Purpose: Tests our "Resume" functionality

// Unlike the other tests in this file, this one tests resume of a job that has completed, but with failures
func TestResume_AfterCompletion(t *testing.T) {
	doTestResume(t, "failedFile", "")
}

// This is the most basic resume-after-non-completion scenario.
func TestResume_AfterCancel(t *testing.T) {
	doTestResume(t, "cancel", "")
}

// This is a more drastic test of resume-after-non-completion, in which the process is killed instead of CTRL-C'd.
func TestResume_AfterKillProcess(t *testing.T) {
	doTestResume(t, "kill", "")
}

// This is the hardest test of all, in which we must correctly respond to overwrite processing after a killed process
func TestResume_AfterKillProcess_NoOverwrite(t *testing.T) {
	doTestResume(t, "kill", "false")
}

// Note that, if this test fails, it may do so "randomly", due to the non-repeatability of the timing of exactly when the process
// goes down.  So any failure should be investigated (to the extent that is possible), and not dismissed just because its not repeatable.
func doTestResume(t *testing.T, method string, overwrite string) {
	props := with{smbAttributes: "H"} // need to test that SMB properties are handled correctly in resume for both files and folders
	didResume := false

	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllSourcesToOneDest(), // TODO: or should we make this AllPairs?
		eValidate.AutoPlusContent(),       // important to validate file content after a resume  // TODO: as at 1 July 2020, content validation isn't supported yet, so this test will fail
		params{
			recursive:       true,
			capMbps:         50, // at this speed the payload should take about 50 seconds to move
			overwrite:       overwrite,
			cancelFromStdin: true, // needed to use hookHelper.Cancel()
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				h.RequestResumeAfterFailure()
				if method == "failedFile" {
					// with this failure method, we don't kill the process. We let it run to completion, but we ensure that one
					// file fails.
					// TODO implement some way of forcing AzCopy to fail on a particular file
					//    Maybe a command line parameter: e.g. h.GetModifiableParameters().forceFailureOnFile = "a"
					//    Or maybe something that sends a message to stdin like h.CancelProcess does (but that's more complicated)
					return
				}

				go func() {
					// wait a while, until we are probably somewhere the middle of the transfer, and then
					// take action to trigger whatever kind of process exit has been requested.
					// Must do this in a separate GoRoutine, since we are (ab)using the beforeRunJob hook, and the job won't start
					// unit our hook func returns
					time.Sleep(20 * time.Second)
					switch method {
					case "cancel":
						h.CancelProcess() // graceful, user-initiated resume
					case "kill":
						h.KillProcess() // abrupt/forced process termination
					default:
						panic("unexpected failure method")
					}
				}()
			},
			beforeResume: func(h hookHelper) {
				// record that the resume really did happen, because we want to check that later (because the test framework doesn't
				// check this for us)
				didResume = true
			},
			afterRunScenario: func(h hookHelper) {
				// We have to assert that the resume really did happen
				// if this assertion fails when method=="failedFile", maybe you have not implemented the way to force
				// AzCopy to fail on a particular file. See the TO DO above.
				h.Assert(didResume, equals(), true, "Expect that the resume test should have actually _done_ a resume")
			}},
		testFiles{
			// Make a payload this is big enough to last for a minute or so, at our capped speed as per params above.
			// Make the files in it big enough to have multiple chunks, since that's the more interesting case for resume.
			defaultSize: "50M",
			shouldTransfer: []interface{}{
				folder("", props),
				f("a", props),
				f("b", props),
				folder("fold1", props),
				f("fold1/f", props),
				f("fold1/g", props),
				folder("fold1/fold2", props),
				f("fold1/j", props),
				f("fold1/k", props),
			},
		})
}
