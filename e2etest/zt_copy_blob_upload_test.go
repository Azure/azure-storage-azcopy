// Copyright © 2017 Microsoft <wastore@microsoft.com>
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
	"os"
	"path/filepath"

	"github.com/Azure/azure-storage-azcopy/common"

	chk "gopkg.in/check.v1"
)

func (s *cmdIntegrationSuite) TestIncludeDir(c *chk.C) {
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
	c.Assert(err, chk.IsNil)
	c.Assert(int(result.finalStatus.TransfersCompleted), chk.Equals, len(filesToInclude))

	transfers := result.GetTransferList(common.ETransferStatus.Success())
	srcRoot := dirPath
	dstRoot := fmt.Sprintf("%s/%s", containerURL.String(), filepath.Base(dirPath))
	Validator{}.ValidateCopyTransfersAreScheduled(c, false, true, srcRoot, dstRoot,
		filesToInclude, transfers)
}
