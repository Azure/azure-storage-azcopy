// +build linux
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

/*
#cgo LDFLAGS: -larchive
#include <archive.h>
#include <archive_entry.h>

typedef const void *const_void_ptr;
extern ssize_t pipeWrite(struct archive *a, void *client_data,  const void *buff, size_t len);

static inline int 
archive_write_open_pipe(struct archive *a, uintptr_t writerHandle) {
	return archive_write_open2(a, (void*)writerHandle, NULL, pipeWrite, NULL, NULL);
};

*/
import "C"

import (
	"errors"
	"io"
	"runtime/cgo"
	"time"
	"unsafe"
)

type ArchivingReader struct {
	archiver    *archiver
	rawDataSize int64
	workerError chan error
	pipeReader  *io.PipeReader
}

// TODO be smarter about this size
const archivingReaderCopyBufferSize = 8 * 1024 * 1024

var archivingReaderBufferPool = NewMultiSizeSlicePool(archivingReaderCopyBufferSize)

func NewArchivingReader(sourcePath string, sourceFile CloseableReaderAt, rawDataSize int64) (*ArchivingReader, error) {
	pipeReader, pipeWriter := io.Pipe()
	a, err := newArchiver(sourcePath, pipeWriter);
	if err != nil {
		return nil, err
	}

	c := &ArchivingReader{
		archiver:    a,
		rawDataSize: rawDataSize,
		workerError: make(chan error, 1),
		pipeReader:  pipeReader,
	}

	// start the output processor worker
	go c.worker(sourceFile, pipeWriter, c.workerError)

	return c, nil
}

func (c *ArchivingReader) Read(p []byte) (n int, err error) {
	return c.pipeReader.Read(p)
}

func (c *ArchivingReader) worker(sourceFile CloseableReaderAt, resultWriter *io.PipeWriter, workerError chan error) {
	buffer := archivingReaderBufferPool.RentSlice(archivingReaderCopyBufferSize)
	var err error

	defer func() {
		c.archiver.Close()
		resultWriter.Close()
		sourceFile.Close()
		archivingReaderBufferPool.ReturnSlice(buffer)
		workerError <- err
	}()

	archiveSize := int64(0)
	for position := int64(0); position < c.rawDataSize; {
		n, readErr := sourceFile.ReadAt(buffer, position)

		if n != 0 {
			// write the buffered content completely, which requires calling write until the entire content is ingested
			m, archiveErr := c.archiver.Write(buffer)
			if archiveErr != nil {
				err = archiveErr
				return
			}

			archiveSize += int64(m)
		}

		if readErr != nil && readErr != io.EOF {
			err = readErr
			return
		}

		position += int64(n)
	}
}

func (c *ArchivingReader) Close() error {
	// the worker will get ErrClosedPipe if it continues to write, resulting in it quitting
	closeErr := c.pipeReader.Close()
	if closeErr != nil {
		return closeErr
	}

	select {
	case workerErr := <-c.workerError:
		if workerErr == nil || workerErr == io.ErrClosedPipe {
			return nil
		}
		return errors.New("error in compression worker when closing: " + workerErr.Error())
	case <-time.After(time.Minute * 15): // should never take THIS long to flush, but better to wait too long than too short and stop it before it's closed
		return errors.New("timed out closing compression worker")
	}
}

//====================================================================================================
const archiveOk = 0

type archiver struct {
	writerHandle  cgo.Handle //this will hold pointer to the io.Writer given to archiver
	dst     *C.struct_archive
	src     *C.struct_archive
	entry   *C.struct_archive_entry
	headerWritten bool
}

func newArchiver(srcPath string, w io.Writer) (*archiver, error) {
	a := archiver{
		writerHandle: cgo.NewHandle(w),
		headerWritten: false,
	}

	err := (&a).Init(srcPath)
	if err != nil {
		return nil, err
	}

	return &a, nil
}

func (a *archiver) Init(srcPath string) error {
	dst := C.archive_write_new();
	toError := func (a *C.struct_archive) (error) {
		return errors.New(C.GoString(C.archive_error_string(a)))
	}

	if err := C.archive_write_set_format_pax_restricted(dst); err != archiveOk {
		return toError(dst)
	}

	if err := C.archive_write_open_pipe(dst, C.uintptr_t(a.writerHandle)); err != archiveOk {
		return toError(dst)
	}

	C.archive_write_set_bytes_in_last_block(dst,1)

	disk  := C.archive_read_disk_new();

	if err := C.archive_read_disk_open(disk, C.CString(srcPath)); err != archiveOk {
		return toError(disk)
	}

	entry := C.archive_entry_new()

	if err := C.archive_read_next_header2(disk, entry); err != archiveOk {
		return toError(disk)
	}

	a.dst, a.src, a.entry = dst, disk, entry

	return nil
}

func (a *archiver) Write(p []byte) (int, error) {
	if (!a.headerWritten) {
		ret := C.archive_write_header(a.dst, a.entry)
		if ret != 0 {
			return 0, a.Error()
		}
	}

	ret := C.archive_write_data(a.dst, unsafe.Pointer(&p[0]), C.ulong(len(p)))
	if  ret < 0 {
		return 0, a.Error()
	}

	if (!a.headerWritten) {
		a.headerWritten = true
		return int(ret+512), nil
	}

	return int(ret), nil
}

func (a *archiver) Close() error {
	C.archive_entry_free(a.entry)
	C.archive_write_close(a.dst)
	C.archive_write_free(a.dst)
	C.archive_read_close(a.src)
	C.archive_read_free(a.src)
	a.writerHandle.Delete()
	return nil
}

func (a *archiver) Error() error {
	s := C.GoString(C.archive_error_string(a.dst))
	return errors.New(s)
}

//export pipeWrite
func pipeWrite(_ *C.struct_archive, clientData unsafe.Pointer, data C.const_void_ptr, length C.size_t) (C.ssize_t) {
	w := cgo.Handle(clientData).Value().(io.Writer)
	src := unsafe.Slice((*byte)(data), length);

	l, err := w.Write(src)
	PanicIfErr(err) //Our writer is a pipewriter and should not fail

	return C.ssize_t(l)

}
//====================================================================================================

