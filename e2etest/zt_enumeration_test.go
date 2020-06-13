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
	"github.com/Azure/azure-storage-azcopy/common"
	chk "gopkg.in/check.v1"
	"os"
	"path/filepath"
	"time"
)

// Purpose: Tests for enumeration of sources, including filtering
type enumerationSuite struct{}

var _ = chk.Suite(&enumerationSuite{})

// Please leave the following test at the top of this file, where it can serve as an easy-to-find annotated example.
// We won't normally put this many comments in a test, but this one has the verbose comments to explain the declarative test
// framework
// TestIncludePath_Folder tests the includePath parameter in the case where it lists folders.
func (s *enumerationSuite) TestFilter_IncludePath_Folder(c *chk.C) {
	// This will test IncludePath once for each source resource type.
	// For source resource types that support both Copy and Sync, it will run the test twice, once with Copy and once with Sync.
	//  Copy and Sync: Blob -> Blob
	//  Copy and Sync: Local -> Blob
	//  Copy and Sync: Files -> Files
	//  Copy: AWS -> Blob
	//  Copy: ADLS Gen2 -> Local
	// That's 8 scenarios in total, but we only need to specify the test declaratively _once_.  The eOperation and eTestFromTo
	// parameters automatically cause this test to expand out to the 8 scenarios.

	RunTests( // RunTests is the method that does all the work.  We pass it params to define that test that should be run
		c,                                 // Pass the chk object in, so that RunTests can make assertions with it
		eOperation.CopyAndSync(),          // Should the test be run for copy only, sync only, or both?
		eTestFromTo.AllSourcesToOneDest(), // What range of source/dest pairs should this test be run on
		nil,                               // Here nil == block blobs only; or eBlobTypes.All() == test on all blob types
		nil,                               // Here nil == use one (default) auth type only. To repeat the test with different auth types, use eAuthTypes.<something>.
		params{ // Pass flag values that the test requires. The params struct is a superset of Copy and Sync params
			recursive:   true,
			includePath: "sub/subsub",
		},
		nil, // For advanced usage, can pass a hooks struct here, to hook funcs into different stage of the testing process to customize it
		testFiles{ // Source files specifies details of the files to test on
			size: "1K", // An indication of what size of files should be created
			shouldIgnore: []string{ // A list of files which should be created, but which are expected to be ignored by the job
				"filea",
				"fileb",
				"filec",
				"sub/filea",
				"sub/fileb",
				"sub/filec",
				"sub/somethingelse/subsub/filex", // should not be included because sub/subsub is not contiguous here
				"othersub/sub/subsub/filey",      // should not be included because sub/subsub is not at root here
			},
			shouldTransfer: []string{ // A list of files which should be created an which should indeed be transferred
				"sub/subsub/filea",
				"sub/subsub/fileb",
				"sub/subsub/filec",
			},
		})

}

func (s *enumerationSuite) TestFilter_IncludePath_Folder_Temp_Imperative(c *chk.C) {
	// set up the source
	files := []string{
		"filea",
		"fileb",
		"filec",
		"sub/filea",
		"sub/fileb",
		"sub/filec",
		"sub/somethingelse/subsub/filex", // should not be included because sub/subsub is not contiguous here
		"othersub/sub/subsub/filey",      // should not be included because sub/subsub is not at root here
	}

	filesToInclude := []string{
		"sub/subsub/filea",
		"sub/subsub/fileb",
		"sub/subsub/filec",
	}

	dirPath := TestResourceFactory{}.CreateLocalDirectory(c)
	defer os.RemoveAll(dirPath)
	scenarioHelper{}.generateLocalFilesFromList(c, dirPath, files)
	scenarioHelper{}.generateLocalFilesFromList(c, dirPath, filesToInclude)

	// set up the destination
	containerURL, _, containerURLWithSAS := TestResourceFactory{}.CreateNewContainer(c, EAccountType.Standard())
	defer deleteContainer(c, containerURL)

	// invoke the executable and get results
	runner := newTestRunner()
	runner.SetRecursiveFlag(true)
	runner.SetIncludePathFlag("sub/subsub")

	result, err := runner.ExecuteCopyCommand(dirPath, containerURLWithSAS.String())
	c.Assert(err, chk.IsNil, chk.Commentf("Error: %s", err))
	c.Assert(int(result.finalStatus.TransfersCompleted), chk.Equals, len(filesToInclude))

	transfers := result.GetTransferList(common.ETransferStatus.Success())
	srcRoot := dirPath
	dstRoot := fmt.Sprintf("%s/%s", containerURL.String(), filepath.Base(dirPath))
	Validator{}.ValidateCopyTransfersAreScheduled(c, false, true, srcRoot, dstRoot,
		filesToInclude, transfers)
}

// TestFilter_IncludeAfter test the include-after parameter
func (s *enumerationSuite) TestFilter_IncludeAfter(c *chk.C) {
	RunTests(c,
		eOperation.Copy(), // IncludeAfter is not applicable for sync
		eTestFromTo.AllSourcesToOneDest(),
		nil,
		nil,
		params{},
		&hooks{
			betweenCreateFilesToIgnoreAndToTransfer: func(h hookHelper) {
				// Put a gap in time between creation of the "to ignore" and "to transfer" files, and then set includeAfterDate
				// See comments on definition of betweenCreateFilesToIgnoreAndToTransfer for acknowledgment that this approach is a bit ugly, but it's the best we have for now.
				time.Sleep(5 * time.Second)
				scenarioParams := h.GetModifyableParameters() // must get the right params instance, because RunTests operates multiple scenarios in parallel
				scenarioParams.includeAfter = time.Now().Format(time.RFC3339)
			},
		},
		testFiles{
			size: "1K",
			shouldIgnore: []string{
				"filea",
			},
			shouldTransfer: []string{
				"fileb",
			},
		})
}

func (s *enumerationSuite) TestFilter_IncludePath_File(c *chk.C) {
	c.Skip("TODO")
}
