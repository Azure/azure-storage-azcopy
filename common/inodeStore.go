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
		return nil, err
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
		return p, true, err
	}

	// Slow path: exclusive lock
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if _, ok := s.set[inode]; ok {
		p, err := s.lookupFromFile(inode)
		return p, true, err
	}

	// New inode → record it
	s.set[inode] = struct{}{}
	_, err = fmt.Fprintf(s.file, "%s %s\n", inode, path)
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
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			continue
		}
		if parts[0] == inode {
			return parts[1], nil
		}
	}

	return "", fmt.Errorf("inode %s not found in store", inode)
}
