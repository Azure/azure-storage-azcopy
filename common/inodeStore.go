// Copyright © 2026 Microsoft <azcopydev@microsoft.com>
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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// inodeMeta tracks the position and allocated size of an inode's record in the store file.
type inodeMeta struct {
	offset   int64 // byte offset of the record in the file
	capacity int   // total bytes allocated for this record (including trailing \n)
}

// InodeStore tracks hardlink relationships by inode.
// Data is stored on disk with exactly one fixed-size record per inode.
// A small in-memory index (inode → offset + capacity, ~24 bytes per inode)
// enables O(1) seeks and in-place updates without scanning the file.
type InodeStore struct {
	mu       sync.RWMutex
	index    map[string]*inodeMeta // inode → file metadata
	file     *os.File
	fileSize int64 // current logical end of file (avoids Seek calls)
}

// recordPadding is extra bytes reserved per record to accommodate anchor path changes.
// If a new anchor exceeds the capacity, the record is relocated to the end of the file.
const recordPadding = 64

var inodeStoreInstance *InodeStore
var inodeStoreOnce sync.Once

// InitInodeStore initializes the global InodeStore singleton with the given jobID.
// Must be called once before any GetInodeStore calls (typically right after the job ID is created).
func InitInodeStore(jobID JobID) error {
	var err error
	inodeStoreOnce.Do(func() {
		inodeStoreInstance, err = NewInodeStore(jobID)
	})
	return err
}

func GetInodeStore() (*InodeStore, error) {
	if inodeStoreInstance == nil {
		return nil, fmt.Errorf("InodeStore not initialized; call InitInodeStore first")
	}
	return inodeStoreInstance, nil
}

func NewInodeStore(jobID JobID) (*InodeStore, error) {
	f, err := os.OpenFile(
		fmt.Sprintf("%s/inodeStore-%s.txt", filepath.Join(AzcopyJobPlanFolder), jobID.String()),
		os.O_CREATE|os.O_RDWR, 0644,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open inode store file: %w", err)
	}

	return &InodeStore{
		index:    make(map[string]*inodeMeta),
		file:     f,
		fileSize: 0,
	}, nil
}

// writeRecord writes a padded record at the current end of the file.
// Returns the offset and capacity. Must be called with s.mu write lock held.
func (s *InodeStore) writeRecord(inode, firstPath, anchor string) (int64, int, error) {
	content := fmt.Sprintf("%s %s %s", inode, firstPath, anchor)
	capacity := len(content) + recordPadding + 1 // +1 for trailing \n
	padded := content + strings.Repeat(" ", capacity-len(content)-1) + "\n"

	offset := s.fileSize
	if _, err := s.file.WriteAt([]byte(padded), offset); err != nil {
		return 0, 0, fmt.Errorf("failed to write record: %w", err)
	}
	s.fileSize += int64(capacity)
	return offset, capacity, nil
}

// overwriteRecord updates the anchor of an existing record in place.
// If the new content exceeds the record's capacity, relocates to the end of the file.
// Must be called with s.mu write lock held.
func (s *InodeStore) overwriteRecord(meta *inodeMeta, inode, firstPath, newAnchor string) error {
	content := fmt.Sprintf("%s %s %s", inode, firstPath, newAnchor)

	if len(content)+1 > meta.capacity {
		// New content doesn't fit — relocate record to end of file (old slot becomes dead space)
		offset, capacity, err := s.writeRecord(inode, firstPath, newAnchor)
		if err != nil {
			return err
		}
		meta.offset = offset
		meta.capacity = capacity
		return nil
	}

	padded := content + strings.Repeat(" ", meta.capacity-len(content)-1) + "\n"
	if _, err := s.file.WriteAt([]byte(padded), meta.offset); err != nil {
		return fmt.Errorf("failed to overwrite record: %w", err)
	}
	return nil
}

// readRecord reads and parses the record at the given metadata location.
// Uses position-independent ReadAt so it's safe under RLock with concurrent readers.
func (s *InodeStore) readRecord(meta *inodeMeta) (firstPath, anchor string, err error) {
	buf := make([]byte, meta.capacity)
	if _, err := s.file.ReadAt(buf, meta.offset); err != nil {
		return "", "", fmt.Errorf("failed to read record at offset %d: %w", meta.offset, err)
	}
	line := strings.TrimRight(string(buf), " \n")
	parts := strings.SplitN(line, " ", 3)
	if len(parts) != 3 {
		return "", "", fmt.Errorf("corrupt record at offset %d: %q", meta.offset, line)
	}
	return parts[1], parts[2], nil
}

// GetOrAdd registers a path for the given inode.
// If the inode is new, writes a record to disk and returns ("", false, nil).
// If the inode was seen before, returns (firstPath, true, nil) and
// updates the anchor in place on disk if the new path is lexicographically smaller.
func (s *InodeStore) GetOrAdd(inode, path string) (existingPath string, exists bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if meta, ok := s.index[inode]; ok {
		// Inode seen before → read current record from disk
		firstPath, anchor, err := s.readRecord(meta)
		if err != nil {
			return "", false, err
		}
		// Update anchor deterministically: keep the lexicographically smallest path.
		if path < anchor {
			if err := s.overwriteRecord(meta, inode, firstPath, path); err != nil {
				return "", false, err
			}
		}
		return firstPath, true, nil
	}

	// New inode → write first record to disk
	offset, capacity, err := s.writeRecord(inode, path, path)
	if err != nil {
		return "", false, err
	}
	s.index[inode] = &inodeMeta{offset: offset, capacity: capacity}
	return "", false, nil
}

// GetAnchor returns the current anchor path for the given inode by reading from disk.
func (s *InodeStore) GetAnchor(inode string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, ok := s.index[inode]
	if !ok {
		return "", fmt.Errorf("anchor for inode %s not found", inode)
	}
	_, anchor, err := s.readRecord(meta)
	if err != nil {
		return "", err
	}
	return anchor, nil
}
