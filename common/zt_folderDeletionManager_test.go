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
	"context"
	chk "gopkg.in/check.v1"
)

type folderDeletionManagerSuite struct{}

var _ = chk.Suite(&folderDeletionManagerSuite{})

func (s *folderDeletionManagerSuite) TestFolderDeletion_NoChildren(c *chk.C) {
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionCalled := false

	// ask for deletion of folder first
	f.RequestDeletion("foo/bar", func(context.Context, ILogger) bool { deletionCalled = true; return true })
	c.Assert(deletionCalled, chk.Equals, true)
}

func (s *folderDeletionManagerSuite) TestFolderDeletion_WithChildren(c *chk.C) {
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionCallCount := 0
	lastDeletionFolder := ""

	// register one for deletion before first children (order shouldn't matter)
	f.RequestDeletion("foo/bar", func(context.Context, ILogger) bool { deletionCallCount++; lastDeletionFolder = "foo/bar"; return true })

	f.RecordChildExists("foo/bar/a")
	f.RecordChildExists("foo/bar/b")
	f.RecordChildExists("other/x?SAS") // test SAS's get stripped (since we don't provide this below, in the folder name)

	// register one for deletion after first children (order shouldn't matter)
	f.RequestDeletion("other", func(context.Context, ILogger) bool { deletionCallCount++; lastDeletionFolder = "other"; return true })
	c.Assert(deletionCallCount, chk.Equals, 0) // deletion doesn't happen right now

	f.RecordChildDeleted("other/x?SASStuff") // this is the last one in this parent, so deletion of that parent should happen now
	c.Assert(deletionCallCount, chk.Equals, 1)
	c.Assert(lastDeletionFolder, chk.Equals, "other")

	f.RecordChildDeleted("foo/bar/a")
	c.Assert(deletionCallCount, chk.Equals, 1) // no change
	f.RecordChildDeleted("foo/bar/b")          // last one in its parent
	c.Assert(deletionCallCount, chk.Equals, 2) // now deletion happens, since last child gone
	c.Assert(lastDeletionFolder, chk.Equals, "foo/bar")
}

func (s *folderDeletionManagerSuite) TestFolderDeletion_WithMultipleDeletionCallsOnOneFolder(c *chk.C) {
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionResult := false
	deletionCallCount := 0

	// run a deletion that where the deletion func returns false
	f.RecordChildExists("foo/bar/a")
	f.RequestDeletion("foo/bar", func(context.Context, ILogger) bool { deletionCallCount++; return deletionResult })
	c.Assert(deletionCallCount, chk.Equals, 0)
	f.RecordChildDeleted("foo/bar/a")
	c.Assert(deletionCallCount, chk.Equals, 1)

	// Now find and process more children. When all are processed,
	// deletion should be automatically retried, because it didn't
	// succeed last time.
	// (May happen in AzCopy due to highly asynchronous nature and
	// fact that folders may be enumerated well before all their children)
	f.RecordChildExists("foo/bar/b")
	c.Assert(deletionCallCount, chk.Equals, 1)
	deletionResult = true // our next deletion should work
	f.RecordChildDeleted("foo/bar/b")
	c.Assert(deletionCallCount, chk.Equals, 2) // deletion was called again, when count again dropped to zero

	// Now find and process even more children.
	// This time, here should be no deletion, because the deletion func _suceeded_ last time.
	// We don't expect ever to find another child after successful deletion, but may as well test it
	f.RecordChildExists("foo/bar/c")
	f.RecordChildDeleted("foo/bar/c")
	c.Assert(deletionCallCount, chk.Equals, 2) // no change from above
}

func (s *folderDeletionManagerSuite) TestFolderDeletion_WithMultipleFolderLevels(c *chk.C) {
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionCallCount := 0

	f.RecordChildExists("base/a.txt")
	f.RecordChildExists("base/childfolder")
	f.RecordChildExists("base/childfolder/grandchildfolder")
	f.RecordChildExists("base/childfolder/grandchildfolder/ggcf")
	f.RecordChildExists("base/childfolder/grandchildfolder/ggcf/b.txt")

	f.RequestDeletion("base", func(context.Context, ILogger) bool { deletionCallCount++; return true })
	f.RequestDeletion("base/childfolder", func(context.Context, ILogger) bool { deletionCallCount++; return true })
	f.RequestDeletion("base/childfolder/grandchildfolder", func(context.Context, ILogger) bool { deletionCallCount++; return true })
	f.RequestDeletion("base/childfolder/grandchildfolder/ggcf", func(context.Context, ILogger) bool { deletionCallCount++; return true })

	f.RecordChildDeleted("base/childfolder/grandchildfolder/ggcf/b.txt")
	c.Assert(deletionCallCount, chk.Equals, 3) // everything except base

	f.RecordChildDeleted("base/a.txt")
	c.Assert(deletionCallCount, chk.Equals, 4) // base is gone now too
}
