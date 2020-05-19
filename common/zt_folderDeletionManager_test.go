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
	"net/url"
)

type folderDeletionManagerSuite struct{}

var _ = chk.Suite(&folderDeletionManagerSuite{})

func (s *folderDeletionManagerSuite) u(str string) *url.URL {
	u, _ := url.Parse("http://example.com/" + str)
	return u
}

func (s *folderDeletionManagerSuite) TestFolderDeletion_BeforeChildrenSeen(c *chk.C) {
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionCallCount := 0

	// ask for deletion of folder first
	f.RequestDeletion(s.u("foo/bar"), func(context.Context, ILogger) bool { deletionCallCount++; return false })
	c.Assert(deletionCallCount, chk.Equals, 1)

	// deletion should be attempted again after children seen and processed (if deletion returned false first time)
	f.RecordChildExists(s.u("foo/bar/a"))
	c.Assert(deletionCallCount, chk.Equals, 1)
	f.RecordChildDeleted(s.u("foo/bar/a"))
	c.Assert(deletionCallCount, chk.Equals, 2)

}

func (s *folderDeletionManagerSuite) TestFolderDeletion_WithChildren(c *chk.C) {
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionCallCount := 0
	lastDeletionFolder := ""

	f.RecordChildExists(s.u("foo/bar/a"))
	f.RecordChildExists(s.u("foo/bar/b"))
	f.RecordChildExists(s.u("other/x"))

	f.RequestDeletion(s.u("foo/bar"), func(context.Context, ILogger) bool { deletionCallCount++; lastDeletionFolder = "foo/bar"; return true })
	f.RequestDeletion(s.u("other"), func(context.Context, ILogger) bool { deletionCallCount++; lastDeletionFolder = "other"; return true })
	c.Assert(deletionCallCount, chk.Equals, 0) // deletion doesn't happen right now

	f.RecordChildDeleted(s.u("other/x")) // this is the last one in this parent, so deletion of that parent should happen now
	c.Assert(deletionCallCount, chk.Equals, 1)
	c.Assert(lastDeletionFolder, chk.Equals, "other")

	f.RecordChildDeleted(s.u("foo/bar/a"))
	c.Assert(deletionCallCount, chk.Equals, 1) // no change
	f.RecordChildDeleted(s.u("foo/bar/b"))     // last one in its parent
	c.Assert(deletionCallCount, chk.Equals, 2) // now deletion happens, since last child gone
	c.Assert(lastDeletionFolder, chk.Equals, "foo/bar")
}

func (s *folderDeletionManagerSuite) TestFolderDeletion_IsUnaffectedByQueryStringsAndPathEscaping(c *chk.C) {
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionCallCount := 0
	lastDeletionFolder := ""

	f.RecordChildExists(s.u("foo/bar%2Fa?SAS"))
	f.RecordChildExists(s.u("foo/bar/b"))
	f.RecordChildExists(s.u("other/x"))

	f.RequestDeletion(s.u("foo%2fbar"), func(context.Context, ILogger) bool { deletionCallCount++; lastDeletionFolder = "foo/bar"; return true })
	f.RequestDeletion(s.u("other?SAS"), func(context.Context, ILogger) bool { deletionCallCount++; lastDeletionFolder = "other"; return true })
	c.Assert(deletionCallCount, chk.Equals, 0) // deletion doesn't happen right now

	f.RecordChildDeleted(s.u("other%2fx")) // this is the last one in this parent, so deletion of that parent should happen now
	c.Assert(deletionCallCount, chk.Equals, 1)
	c.Assert(lastDeletionFolder, chk.Equals, "other")

	f.RecordChildDeleted(s.u("foo/bar/a"))
	c.Assert(deletionCallCount, chk.Equals, 1) // no change
	f.RecordChildDeleted(s.u("foo/bar/b?SAS")) // last one in its parent
	c.Assert(deletionCallCount, chk.Equals, 2) // now deletion happens, since last child gone
	c.Assert(lastDeletionFolder, chk.Equals, "foo/bar")
}

func (s *folderDeletionManagerSuite) TestFolderDeletion_WithMultipleDeletionCallsOnOneFolder(c *chk.C) {
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionResult := false
	deletionCallCount := 0

	// run a deletion that where the deletion func returns false
	f.RecordChildExists(s.u("foo/bar/a"))
	f.RequestDeletion(s.u("foo/bar"), func(context.Context, ILogger) bool { deletionCallCount++; return deletionResult })
	c.Assert(deletionCallCount, chk.Equals, 0)
	f.RecordChildDeleted(s.u("foo/bar/a"))
	c.Assert(deletionCallCount, chk.Equals, 1)

	// Now find and process more children. When all are processed,
	// deletion should be automatically retried, because it didn't
	// succeed last time.
	// (May happen in AzCopy due to highly asynchronous nature and
	// fact that folders may be enumerated well before all their children)
	f.RecordChildExists(s.u("foo/bar/b"))
	c.Assert(deletionCallCount, chk.Equals, 1)
	deletionResult = true // our next deletion should work
	f.RecordChildDeleted(s.u("foo/bar/b"))
	c.Assert(deletionCallCount, chk.Equals, 2) // deletion was called again, when count again dropped to zero

	// Now find and process even more children.
	// This time, here should be no deletion, because the deletion func _suceeded_ last time.
	// We don't expect ever to find another child after successful deletion, but may as well test it
	f.RecordChildExists(s.u("foo/bar/c"))
	f.RecordChildDeleted(s.u("foo/bar/c"))
	c.Assert(deletionCallCount, chk.Equals, 2) // no change from above
}

func (s *folderDeletionManagerSuite) TestFolderDeletion_WithMultipleFolderLevels(c *chk.C) {
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionCallCount := 0

	f.RecordChildExists(s.u("base/a.txt"))
	f.RecordChildExists(s.u("base/childfolder"))
	f.RecordChildExists(s.u("base/childfolder/grandchildfolder"))
	f.RecordChildExists(s.u("base/childfolder/grandchildfolder/ggcf"))
	f.RecordChildExists(s.u("base/childfolder/grandchildfolder/ggcf/b.txt"))

	f.RequestDeletion(s.u("base"), func(context.Context, ILogger) bool { deletionCallCount++; return true })
	f.RequestDeletion(s.u("base/childfolder"), func(context.Context, ILogger) bool { deletionCallCount++; return true })
	f.RequestDeletion(s.u("base/childfolder/grandchildfolder"), func(context.Context, ILogger) bool { deletionCallCount++; return true })
	f.RequestDeletion(s.u("base/childfolder/grandchildfolder/ggcf"), func(context.Context, ILogger) bool { deletionCallCount++; return true })

	f.RecordChildDeleted(s.u("base/childfolder/grandchildfolder/ggcf/b.txt"))
	c.Assert(deletionCallCount, chk.Equals, 3) // everything except base

	f.RecordChildDeleted(s.u("base/a.txt"))
	c.Assert(deletionCallCount, chk.Equals, 4) // base is gone now too
}

func (s *folderDeletionManagerSuite) TestGetParent(c *chk.C) {
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	test := func(child string, expectedParent string) {
		u, _ := url.Parse(child)
		p, ok := f.(*standardFolderDeletionManager).getParent(u)
		if expectedParent == "" {
			c.Assert(ok, chk.Equals, false)
		} else {
			c.Assert(ok, chk.Equals, true)
			c.Assert(p, chk.Equals, expectedParent)
		}
	}

	test("http://example.com", "")
	test("http://example.com/foo", "http://example.com")
	test("http://example.com/foo/bar", "http://example.com/foo")
	test("http://example.com/foo%2Fbar", "http://example.com/foo")
	test("http://example.com/foo/bar?ooo", "http://example.com/foo")
}
