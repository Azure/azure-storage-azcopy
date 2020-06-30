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
	"io"
)

func beginAzCopyDebugging(stdin io.Writer) {
	// to debug AzCopy from within a running test, you must
	// 0. Make sure that gops is available on your PATH. (i.e. running "gops" will run it).  See https://github.com/google/gops
	//    Gops is needed by TestRunner.isLaunchedByDebugger(), and it's also needed by GoLand's Attach To Process (step 2 below)
	// then
	// 1. Set a breakpoint on the line below
	// 2. When the breakpoint is hit, use Attach To Process to manually attach your debugger to the running AzCopy process
	// 3. Then you MUST continue past the breakpoint that you set here (i.e. on the line below). Any breakpoints you've set in AzCopy will then be hit.
	//    (Note that some IDEs, e.g. GoLand, will give you two separate debug tabs/contexts, one for the tests exe, and one for AzCopy. So make sure you hit run/continue in the right tab)
	_, _ = stdin.Write([]byte("continue\n")) // tell AzCopy to start its work
}
