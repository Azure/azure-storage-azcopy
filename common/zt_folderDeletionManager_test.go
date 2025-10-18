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
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func u(str string) *url.URL {
	u, _ := url.Parse("http://example.com/" + str)
	return u
}

func TestFolderDeletion_BeforeChildrenSeen(t *testing.T) {
	a := assert.New(t)
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionCallCount := 0

	// ask for deletion of folder first
	f.RequestDeletion(u("foo/bar"), func(context.Context, ILogger) bool { deletionCallCount++; return false })
	a.Equal(1, deletionCallCount)

	// deletion should be attempted again after children seen and processed (if deletion returned false first time)
	f.RecordChildExists(u("foo/bar/a"))
	a.Equal(1, deletionCallCount)
	f.RecordChildDeleted(u("foo/bar/a"))
	a.Equal(2, deletionCallCount)

}

func TestFolderDeletion_WithChildren(t *testing.T) {
	a := assert.New(t)
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionCallCount := 0
	lastDeletionFolder := ""

	f.RecordChildExists(u("foo/bar/a"))
	f.RecordChildExists(u("foo/bar/b"))
	f.RecordChildExists(u("other/x"))

	f.RequestDeletion(u("foo/bar"), func(context.Context, ILogger) bool { deletionCallCount++; lastDeletionFolder = "foo/bar"; return true })
	f.RequestDeletion(u("other"), func(context.Context, ILogger) bool { deletionCallCount++; lastDeletionFolder = "other"; return true })
	a.Equal(0, deletionCallCount) // deletion doesn't happen right now

	f.RecordChildDeleted(u("other/x")) // this is the last one in this parent, so deletion of that parent should happen now
	a.Equal(1, deletionCallCount)
	a.Equal("other", lastDeletionFolder)

	f.RecordChildDeleted(u("foo/bar/a"))
	a.Equal(1, deletionCallCount)        // no change
	f.RecordChildDeleted(u("foo/bar/b")) // last one in its parent
	a.Equal(2, deletionCallCount)        // now deletion happens, since last child gone
	a.Equal("foo/bar", lastDeletionFolder)
}

func TestFolderDeletion_IsUnaffectedByQueryStringsAndPathEscaping(t *testing.T) {
	a := assert.New(t)
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionCallCount := 0
	lastDeletionFolder := ""

	f.RecordChildExists(u("foo/bar%2Fa?SAS"))
	f.RecordChildExists(u("foo/bar/b"))
	f.RecordChildExists(u("other/x"))

	f.RequestDeletion(u("foo%2fbar"), func(context.Context, ILogger) bool { deletionCallCount++; lastDeletionFolder = "foo/bar"; return true })
	f.RequestDeletion(u("other?SAS"), func(context.Context, ILogger) bool { deletionCallCount++; lastDeletionFolder = "other"; return true })
	a.Equal(0, deletionCallCount) // deletion doesn't happen right now

	f.RecordChildDeleted(u("other%2fx")) // this is the last one in this parent, so deletion of that parent should happen now
	a.Equal(1, deletionCallCount)
	a.Equal("other", lastDeletionFolder)

	f.RecordChildDeleted(u("foo/bar/a"))
	a.Equal(1, deletionCallCount)            // no change
	f.RecordChildDeleted(u("foo/bar/b?SAS")) // last one in its parent
	a.Equal(2, deletionCallCount)            // now deletion happens, since last child gone
	a.Equal("foo/bar", lastDeletionFolder)
}

func TestFolderDeletion_WithMultipleDeletionCallsOnOneFolder(t *testing.T) {
	a := assert.New(t)
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionResult := false
	deletionCallCount := 0

	// run a deletion that where the deletion func returns false
	f.RecordChildExists(u("foo/bar/a"))
	f.RequestDeletion(u("foo/bar"), func(context.Context, ILogger) bool { deletionCallCount++; return deletionResult })
	a.Equal(0, deletionCallCount)
	f.RecordChildDeleted(u("foo/bar/a"))
	a.Equal(1, deletionCallCount)

	// Now find and process more children. When all are processed,
	// deletion should be automatically retried, because it didn't
	// succeed last time.
	// (May happen in AzCopy due to highly asynchronous nature and
	// fact that folders may be enumerated well before all their children)
	f.RecordChildExists(u("foo/bar/b"))
	a.Equal(1, deletionCallCount)
	deletionResult = true // our next deletion should work
	f.RecordChildDeleted(u("foo/bar/b"))
	a.Equal(2, deletionCallCount) // deletion was called again, when count again dropped to zero

	// Now find and process even more children.
	// This time, here should be no deletion, because the deletion func _succeeded_ last time.
	// We don't expect ever to find another child after successful deletion, but may as well test it
	f.RecordChildExists(u("foo/bar/c"))
	f.RecordChildDeleted(u("foo/bar/c"))
	a.Equal(2, deletionCallCount) // no change from above
}

func TestFolderDeletion_WithMultipleFolderLevels(t *testing.T) {
	a := assert.New(t)
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	deletionCallCount := 0

	f.RecordChildExists(u("base/a.txt"))
	f.RecordChildExists(u("base/childfolder"))
	f.RecordChildExists(u("base/childfolder/grandchildfolder"))
	f.RecordChildExists(u("base/childfolder/grandchildfolder/ggcf"))
	f.RecordChildExists(u("base/childfolder/grandchildfolder/ggcf/b.txt"))

	f.RequestDeletion(u("base"), func(context.Context, ILogger) bool { deletionCallCount++; return true })
	f.RequestDeletion(u("base/childfolder"), func(context.Context, ILogger) bool { deletionCallCount++; return true })
	f.RequestDeletion(u("base/childfolder/grandchildfolder"), func(context.Context, ILogger) bool { deletionCallCount++; return true })
	f.RequestDeletion(u("base/childfolder/grandchildfolder/ggcf"), func(context.Context, ILogger) bool { deletionCallCount++; return true })

	f.RecordChildDeleted(u("base/childfolder/grandchildfolder/ggcf/b.txt"))
	a.Equal(3, deletionCallCount) // everything except base

	f.RecordChildDeleted(u("base/a.txt"))
	a.Equal(4, deletionCallCount) // base is gone now too
}

func TestGetParent(t *testing.T) {
	a := assert.New(t)
	f := NewFolderDeletionManager(context.Background(), EFolderPropertiesOption.AllFolders(), nil)

	test := func(child string, expectedParent string) {
		u, _ := url.Parse(child)
		p, ok := f.(*standardFolderDeletionManager).getParent(u)
		if expectedParent == "" {
			a.False(ok)
		} else {
			a.True(ok)
			a.Equal(expectedParent, p.String())
		}
	}

	test("http://example.com", "")
	test("http://example.com/foo", "http://example.com")
	test("http://example.com/foo/bar", "http://example.com/foo")
	test("http://example.com/foo%2Fbar", "http://example.com/foo")
	test("http://example.com/foo/bar?ooo", "http://example.com/foo")
}
