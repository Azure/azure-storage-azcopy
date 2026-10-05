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
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// TargetedReadWriteCloser is the minimal I/O surface needed by InodeStore.
// *os.File satisfies this interface; tests can substitute an in-memory implementation.
type TargetedReadWriteCloser interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
}

// inodeMeta locates the latest complete record for an inode.
type inodeMeta struct {
	offset   int64 // byte offset of the record in the file
	capacity int   // record length, including its newline
}

// InodeStore tracks hardlink relationships by inode.
// Records are append-only and encode arbitrary path bytes without delimiter loss.
// The in-memory index points to the latest complete record for each inode.
type InodeStore struct {
	mu            sync.RWMutex
	index         map[string]*inodeMeta // inode → file metadata
	file          TargetedReadWriteCloser
	fileSize      int64 // current logical end of file (avoids Seek calls)
	writeErr      error
	closed        bool
	closeErr      error
	removeOnClose string
}

// NewInodeStore opens (or creates) the inode store file for the given jobID.
// If the file already exists and is non-empty (e.g. a resumed job), it scans
// all records and rebuilds the in-memory index so that subsequent writes
// continue from the correct end-of-file offset rather than overwriting
// existing data from offset 0.
func NewInodeStore(jobID JobID) (*InodeStore, error) {
	if jobID.IsEmpty() {
		return nil, errors.New("inode store requires a job ID")
	}
	f, err := os.OpenFile(
		filepath.Join(AzcopyJobPlanFolder, fmt.Sprintf("inodeStore-%s.txt", jobID.String())),
		os.O_CREATE|os.O_RDWR, 0644,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open inode store file: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("failed to stat inode store file: %w", err)
	}

	fileSize := fi.Size()
	var index map[string]*inodeMeta
	if fileSize > 0 {
		// Existing file: scan all records to rebuild the index.
		// Last occurrence of each inode wins because relocated records are appended.
		var validEnd int64
		index, validEnd, err = scanInodeStore(f, fileSize)
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("failed to rehydrate inode store: %w", err)
		}
		if validEnd != fileSize {
			if err := f.Truncate(validEnd); err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("failed to remove incomplete inode record: %w", err)
			}
			fileSize = validEnd
		}
	} else {
		index = make(map[string]*inodeMeta)
	}

	return &InodeStore{
		index:    index,
		file:     f,
		fileSize: fileSize,
	}, nil
}

// NewInodeStoreForNewJob exclusively creates inode state for a new preserve-mode
// scan. Existing state belongs to its original scan and is never overwritten.
func NewInodeStoreForNewJob(jobID JobID) (*InodeStore, error) {
	if jobID.IsEmpty() {
		return nil, errors.New("inode store requires a job ID")
	}
	file, err := os.OpenFile(
		filepath.Join(AzcopyJobPlanFolder, fmt.Sprintf("inodeStore-%s.txt", jobID.String())),
		os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644,
	)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("inode state already exists for job %s; use ResumeJob to resume the existing job or a fresh JobID for a new scan: %w", jobID, err)
		}
		return nil, fmt.Errorf("failed to create inode store for new job %s: %w", jobID, err)
	}
	return NewInodeStoreFromBackend(file), nil
}

// NewInodeStoreFromBackend starts an empty store on a fresh backend.
// Tests can supply an in-memory implementation instead of a real file.
func NewInodeStoreFromBackend(backend TargetedReadWriteCloser) *InodeStore {
	return &InodeStore{
		index:    make(map[string]*inodeMeta),
		file:     backend,
		fileSize: 0,
	}
}

// NewTemporaryInodeStore creates disposable job-local state for dry-run scans.
// It is removed by Close and never reuses a persisted job's inode records.
func NewTemporaryInodeStore() (*InodeStore, error) {
	if AzcopyJobPlanFolder == "" {
		return nil, errors.New("a job plan folder is required for temporary inode state")
	}
	file, err := os.CreateTemp(AzcopyJobPlanFolder, "inodeStore-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary inode store: %w", err)
	}
	store := NewInodeStoreFromBackend(file)
	store.removeOnClose = file.Name()
	return store, nil
}

func rehydrateInodeStore(f *os.File, size int64) (map[string]*inodeMeta, error) {
	index, _, err := scanInodeStore(f, size)
	return index, err
}

func scanInodeStore(f io.ReaderAt, size int64) (map[string]*inodeMeta, int64, error) {
	index := make(map[string]*inodeMeta)
	reader := bufio.NewReader(io.NewSectionReader(f, 0, size))
	var offset int64
	for {
		record, err := reader.ReadString('\n')
		if errors.Is(err, io.EOF) {
			// A failed append cannot replace an earlier complete record.
			return index, offset, nil
		}
		if err != nil {
			return nil, offset, err
		}
		inode, _, _, err := decodeInodeRecord(record)
		if err != nil {
			return nil, offset, fmt.Errorf("invalid inode record at offset %d: %w", offset, err)
		}
		index[inode] = &inodeMeta{offset: offset, capacity: len(record)}
		offset += int64(len(record))
	}
}

func encodeInodeRecord(inode, firstPath, anchor string) []byte {
	encode := base64.RawStdEncoding.EncodeToString
	return []byte("i1\t" + encode([]byte(inode)) + "\t" + encode([]byte(firstPath)) + "\t" + encode([]byte(anchor)) + "\n")
}

func decodeInodeRecord(record string) (inode, firstPath, anchor string, err error) {
	if !strings.HasSuffix(record, "\n") {
		return "", "", "", io.ErrUnexpectedEOF
	}
	fields := strings.Split(strings.TrimSuffix(record, "\n"), "\t")
	if len(fields) != 4 || fields[0] != "i1" {
		return "", "", "", errors.New("unsupported or corrupt inode store encoding")
	}
	values := make([]string, 3)
	for i, field := range fields[1:] {
		decoded, decodeErr := base64.RawStdEncoding.DecodeString(field)
		if decodeErr != nil {
			return "", "", "", decodeErr
		}
		values[i] = string(decoded)
	}
	if values[0] == "" {
		return "", "", "", errors.New("empty inode key")
	}
	return values[0], values[1], values[2], nil
}

// writeRecord only publishes a complete append. A failed write poisons further
// writes until the file is reopened and its incomplete tail has been discarded.
func (s *InodeStore) writeRecord(inode, firstPath, anchor string) (offset int64, capacity int, err error) {
	if s.writeErr != nil {
		return 0, 0, s.writeErr
	}
	record := encodeInodeRecord(inode, firstPath, anchor)
	capacity = len(record)
	offset = s.fileSize
	n, err := s.file.WriteAt(record, offset)
	if err == nil && n != len(record) {
		err = io.ErrShortWrite
	}
	if err != nil {
		s.writeErr = fmt.Errorf("failed to write record: %w", err)
		return 0, 0, s.writeErr
	}
	s.fileSize += int64(capacity)
	return offset, capacity, nil
}

// overwriteRecord appends a replacement so partial writes cannot corrupt the
// previous anchor. Must be called with s.mu write lock held.
func (s *InodeStore) overwriteRecord(meta *inodeMeta, inode, firstPath, newAnchor string) error {
	offset, capacity, err := s.writeRecord(inode, firstPath, newAnchor)
	if err != nil {
		return err
	}
	meta.offset, meta.capacity = offset, capacity
	return nil
}

// readRecord reads and parses the record at the given metadata location.
// Uses position-independent ReadAt so it's safe under RLock with concurrent readers.
func (s *InodeStore) readRecord(meta *inodeMeta) (firstPath, anchor string, err error) {
	buf := make([]byte, meta.capacity)
	n, err := s.file.ReadAt(buf, meta.offset)
	if err == nil && n != len(buf) {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return "", "", fmt.Errorf("failed to read record at offset %d: %w", meta.offset, err)
	}
	_, firstPath, anchor, err = decodeInodeRecord(string(buf))
	return
}

// GetOrAdd registers a path for the given inode.
// If the inode is new, writes a record to disk and returns ("", false, nil).
// If the inode was seen before, returns (firstPath, true, nil) and
// appends a replacement record if the new path is lexicographically smaller.
func (s *InodeStore) GetOrAdd(inode, path string) (existingPath string, exists bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.file == nil {
		return "", false, os.ErrClosed
	}
	if inode == "" {
		return "", false, errors.New("empty inode key")
	}
	if s.writeErr != nil {
		return "", false, s.writeErr
	}

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
	if s.closed || s.file == nil {
		return "", os.ErrClosed
	}

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

// Flush persists complete records when the backend supports Sync.
func (s *InodeStore) Flush() error {
	if s == nil {
		return os.ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.file == nil {
		return os.ErrClosed
	}
	return s.flush()
}

func (s *InodeStore) flush() error {
	if backend, ok := s.file.(interface{ Sync() error }); ok {
		return errors.Join(s.writeErr, backend.Sync())
	}
	return s.writeErr
}

// Close flushes the store and closes its backend exactly once.
func (s *InodeStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.file == nil {
		return s.closeErr
	}
	s.closeErr = errors.Join(s.flush(), s.file.Close())
	if s.removeOnClose != "" {
		s.closeErr = errors.Join(s.closeErr, os.Remove(s.removeOnClose))
	}
	s.closed = true
	return s.closeErr
}
