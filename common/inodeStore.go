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
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type InodeStore struct {
	mu   sync.RWMutex
	set  map[string]struct{}
	file *os.File
}

var inodeStoreInstance *InodeStore
var inodeStoreOnce sync.Once

func GetInodeStore() (*InodeStore, error) {
	var err error
	inodeStoreOnce.Do(func() {
		inodeStoreInstance, err = NewInodeStore()
	})
	return inodeStoreInstance, err
}

func NewInodeStore() (*InodeStore, error) {
	f, err := os.OpenFile(fmt.Sprintf("%s/inodeStore-%s.txt", filepath.Join(AzcopyJobPlanFolder), NewJobID()), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open inode store file: %w", err)
	}

	return &InodeStore{
		set:  make(map[string]struct{}),
		file: f,
	}, nil
}

func (s *InodeStore) GetOrAdd(inode, path string) (existingPath string, exists bool, err error) {
	// Fast path: shared lock
	s.mu.RLock()
	_, ok := s.set[inode]
	s.mu.RUnlock()

	if ok {
		// inode seen before → lookup from file
		p, err := s.lookupFromFile(inode)
		if err != nil {
			return "", false, err
		}
		err = s.UpdateAnchor(inode, path)
		return p, true, err
	}

	// Slow path: exclusive lock
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if _, ok := s.set[inode]; ok {
		p, err := s.lookupFromFile(inode)
		if err != nil {
			return "", false, err
		}
		err = s.UpdateAnchor(inode, path)
		return p, true, err
	}

	// New inode → record it
	s.set[inode] = struct{}{}
	_, err = fmt.Fprintf(s.file, "%s %s %s\n", inode, path, path)
	if err != nil {
		return "", false, err
	}

	// Return empty path for a new inode
	return "", false, nil
}

func (s *InodeStore) lookupFromFile(inode string) (string, error) {
	// We must sync file access
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(s.file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 3)
		if len(parts) != 3 {
			continue
		}
		if parts[0] == inode {
			return parts[1], nil
		}
	}

	return "", fmt.Errorf("inode %s not found in store", inode)
}

func (s *InodeStore) UpdateAnchor(inode string, newAnchor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Create a temporary file to write the updated content
	tempFileName := s.file.Name() + ".tmp"
	tempFile, err := os.OpenFile(tempFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create temp file for update: %w", err)
	}
	defer tempFile.Close()

	// 2. Reset pointer of current file to start scanning
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek original file: %w", err)
	}

	updated := false
	scanner := bufio.NewScanner(s.file)
	writer := bufio.NewWriter(tempFile)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 3)

		if len(parts) == 3 && parts[0] == inode {
			currentAnchor := parts[2]

			// DETERMINISTIC COMPARISON:
			// Update only if the new anchor is lexicographically smaller than the current one.
			// This ensures the "alphabetically first" path always wins.
			fmt.Println("-------Idnode, currentAnchor, newAnchor:", inode, currentAnchor, newAnchor)
			if newAnchor < currentAnchor {
				_, err = fmt.Fprintf(writer, "%s %s %s\n", parts[0], parts[1], newAnchor)
				updated = true
				fmt.Println("Updated anchor for inode", inode, "to", newAnchor)
			} else {
				// Keep existing anchor if it's already "smaller"
				_, err = fmt.Fprintln(writer, line)
			}
		} else {
			// Write the line as is
			_, err = fmt.Fprintln(writer, line)
		}

		if err != nil {
			return fmt.Errorf("failed to write to temp file: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	if !updated {
		os.Remove(tempFileName)
		return fmt.Errorf("inode %s not found; nothing to update", inode)
	}

	// 3. Swap files
	oldName := s.file.Name()
	s.file.Close() // Must close before renaming on some OSs (Windows)

	if err := os.Rename(tempFileName, oldName); err != nil {
		return fmt.Errorf("failed to replace old store file: %w", err)
	}

	// 4. Reopen the file handle for the InodeStore
	newFile, err := os.OpenFile(oldName, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen store file: %w", err)
	}
	s.file = newFile

	return nil
}

// GetAnchor fetches the current anchor path for a given inode from the disk store.
func (s *InodeStore) GetAnchor(inode string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("failed to seek inode store: %w", err)
	}

	scanner := bufio.NewScanner(s.file)
	for scanner.Scan() {
		line := scanner.Text()
		// Format is: <inode> <path> <anchor>
		parts := strings.SplitN(line, " ", 3)

		if len(parts) == 3 && parts[0] == inode {
			// Return the anchor (the 3rd field)
			return parts[2], nil
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error reading inode store: %w", err)
	}

	return "", fmt.Errorf("anchor for inode %s not found", inode)
}
