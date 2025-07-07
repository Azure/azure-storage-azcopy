// Copyright Microsoft <wastore@microsoft.com>
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
	"strings"
	"sync"
)

// folderDeletionFunc should delete the folder IF IT IS EMPTY, and return true.
// If it is not empty, false must be returned.
// FolderDeletionManager is allowed to call this on a folder that is not yet empty.
// In that case, FolderDeletionManager may call it again later.
// Errors are not returned because of the delay to when deletion might happen, so
// it's up to the func to do its own logging
type FolderDeletionFunc func(context.Context, ILogger) bool

// FolderDeletionManager handles the fact that (in most locations) we can't delete folders that
// still contain files.  So it allows us to request deletion of a folder, and have that be attempted
// after the last file is removed.  Note that maybe the apparent last file isn't the last (e.g.
// there are other files, still to be deleted, in future job parts), in which case any failed deletion
// will be retried if there's a new "candidate last child" removed.
// Takes URLs rather than strings because that ensures correct (un)escaping, and makes it clear that we
// don't support Windows & MacOS local paths (which have cases insensitivity that we don't support here).
type FolderDeletionManager interface {

	// RecordChildExists takes a child name and counts it against the child's immediate parent
	// Should be called for both types of child: folders and files.
	// Only counts it against the immediate parent (that's all that's necessary, because we recurse in tryDeletion)
	RecordChildExists(childFileOrFolder *url.URL)

	// RecordChildDelete records that a file, previously passed to RecordChildExists, has now been deleted
	// Only call for files, not folders
	RecordChildDeleted(childFile *url.URL)

	// RequestDeletion registers a function that will be called to delete the given folder, when that
	// folder has no more known children.  May be called before, after or during the time that
	// the folder's children are being passed to RecordChildExists and RecordChildDeleted
	//
	// Warning: only pass in deletionFuncs that will do nothing and return FALSE if the
	// folder is not yet empty. If they return false, they may be called again later.
	RequestDeletion(folder *url.URL, deletionFunc FolderDeletionFunc)
	// TODO: do we want this to report, so that we can log, any folders at the very end which still are not deleted?
	//     or will we just leave such folders there, with no logged message other than any "per attempt" logging?
}

type FolderDeletionManagerOptions struct {
	recursive bool
}

func NewDefaultFolderDeletionManagerOptions() FolderDeletionManagerOptions {
	return FolderDeletionManagerOptions{
		recursive: false, // default to non-recursive deletion
	}
}

type folderDeletionState struct {
	childCount int64
	deleter    FolderDeletionFunc
}

func (f *folderDeletionState) shouldDeleteNow() bool {
	deletionRequested := f.deleter != nil
	return deletionRequested && f.childCount == 0
}

func NewFolderDeletionManager(ctx context.Context, fpo FolderPropertyOption, logger ILogger, opts ...FolderDeletionManagerOptions) FolderDeletionManager {

	options := NewDefaultFolderDeletionManagerOptions()
	if len(opts) > 0 {
		options = opts[0]
	}

	mgr := standardFolderDeletionManager{
		mu:       &sync.Mutex{},
		contents: make(map[string]*folderDeletionState),
		logger:   logger,
		ctx:      ctx,
	}

	switch fpo {
	case EFolderPropertiesOption.AllFolders(),
		EFolderPropertiesOption.AllFoldersExceptRoot():
		return &mgr

	case EFolderPropertiesOption.NoFolders():
		if options.recursive {
			// if we are doing recursive deletion, we need to provide a folder deletion manager
			// even if location is not folder-aware
			return &mgr
		}

		// no point in using a real implementation here, since it will just use memory and take time for no benefit
		return &nullFolderDeletionManager{}

	default:
		panic("unknown folderPropertiesOption")
	}
}

// Note: the current implementation assumes that names are either case sensitive, or at least
// consistently capitalized.  If it receives inconsistently capitalized things, it will think they are
// distinct, and so may try deletion prematurely and fail
type standardFolderDeletionManager struct {
	mu       *sync.Mutex                     // mutex is simpler than RWMutex because folderDeletionState has multiple mutable elements
	contents map[string]*folderDeletionState // pointer so no need to put back INTO map after reading from map and mutating a field value
	// have our own logger and context, because our deletions don't necessarily run when RequestDeletion is called
	logger ILogger
	ctx    context.Context
}

func (s standardFolderDeletionManager) copyURL(u *url.URL) *url.URL {
	out := *u
	if u.User != nil {
		user := *u.User
		out.User = &user
	}

	return &out
}

func (s *standardFolderDeletionManager) clean(u *url.URL) *url.URL {
	out := s.copyURL(u)
	out.RawQuery = "" // no SAS

	return out
}

// getParent drops final part of path (not using use path.Dir because it messes with the // in URLs)
func (s *standardFolderDeletionManager) getParent(u *url.URL) (*url.URL, bool) {
	if len(u.Path) == 0 || u.Path == "/" {
		return u, false // path is already empty, so we can't go up another level
	}

	// trim off last portion of path (or all of the path, if it only has one component)
	out := s.clean(u)
	out.Path = out.Path[:strings.LastIndex(out.Path, "/")]
	if out.RawPath != "" {
		out.RawPath = out.RawPath[:strings.LastIndex(out.RawPath, "/")]
	}
	return out, true
}

func (s standardFolderDeletionManager) getMapKey(u *url.URL) string {
	return url.PathEscape(u.Path)
}

// getStateAlreadyLocked assumes the lock is already held
func (s *standardFolderDeletionManager) getStateAlreadyLocked(folder *url.URL) *folderDeletionState {
	fmapKey := s.getMapKey(folder)
	state, alreadyKnown := s.contents[fmapKey]
	if alreadyKnown {
		return state
	} else {
		state = &folderDeletionState{}
		s.contents[fmapKey] = state
		return state
	}
}

func (s *standardFolderDeletionManager) RecordChildExists(childFileOrFolder *url.URL) {
	folder, ok := s.getParent(childFileOrFolder)
	if !ok {
		return // this is not a child of any parent, so there is nothing for us to do
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	folderStatePtr := s.getStateAlreadyLocked(folder)
	folderStatePtr.childCount++
}

func (s *standardFolderDeletionManager) RecordChildDeleted(childFile *url.URL) {
	folder, ok := s.getParent(childFile)
	if !ok {
		return // this is not a child of any parent, so there is nothing for us to do
	}

	s.mu.Lock()
	folderStatePtr, alreadyKnown := s.contents[s.getMapKey(folder)]
	if !alreadyKnown {
		// we are not tracking this child, so there is nothing that we should do in response
		// to its deletion (may happen in the recursive calls from tryDeletion, when they recurse up to parent dirs)
		s.mu.Unlock()
		return
	}
	folderStatePtr.childCount--
	if folderStatePtr.childCount < 0 {
		// should never happen. If it does it means someone called RequestDeletion and Recorded a child as deleted, without ever registering the child as known
		folderStatePtr.childCount = 0
	}
	deletionFunc := folderStatePtr.deleter
	shouldDel := folderStatePtr.shouldDeleteNow()
	s.mu.Unlock() // unlock before network calls for deletion

	if shouldDel {
		s.tryDeletion(folder, deletionFunc)
	}
}

func (s *standardFolderDeletionManager) RequestDeletion(folder *url.URL, deletionFunc FolderDeletionFunc) {
	folder = s.clean(folder)

	s.mu.Lock()
	folderStatePtr := s.getStateAlreadyLocked(folder)
	folderStatePtr.deleter = deletionFunc
	shouldDel := folderStatePtr.shouldDeleteNow() // test now in case there are no children
	s.mu.Unlock()                                 // release lock before expensive deletion attempt

	if shouldDel {
		s.tryDeletion(folder, deletionFunc)
	}
}

func (s *standardFolderDeletionManager) tryDeletion(folder *url.URL, deletionFunc FolderDeletionFunc) {
	success := deletionFunc(s.ctx, s.logger) // for safety, deletionFunc should be coded to do nothing, and return false, if the directory is not empty

	if success {
		s.mu.Lock()
		delete(s.contents, s.getMapKey(folder))
		s.mu.Unlock()

		// folder is, itself, a child of its parent. So recurse.  This is the only place that RecordChildDeleted should be called with a FOLDER parameter
		s.RecordChildDeleted(folder)
	}
}

///////////////////////////////////////

type nullFolderDeletionManager struct{}

func (f *nullFolderDeletionManager) RecordChildExists(child *url.URL) {
	// no-op
}

func (f *nullFolderDeletionManager) RecordChildDeleted(child *url.URL) {
	// no-op
}

func (f *nullFolderDeletionManager) RequestDeletion(folder *url.URL, deletionFunc FolderDeletionFunc) {
	// There's no way this should ever be called, because we only create the null deletion manager if we are
	// NOT transferring folder info.
	panic("wrong type of folder deletion manager has been instantiated. This type does not do anything")
}
