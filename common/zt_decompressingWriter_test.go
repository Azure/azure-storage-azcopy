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
	"bytes"
	"compress/zlib"
	chk "gopkg.in/check.v1"
	"io"
	"math/rand"
)

type decompressingWriterSuite struct{}

type closeableDecorator struct {
	io.Writer
	closeWasCalled bool
}

func (c closeableDecorator) Close() error {
	c.closeWasCalled = true
	return nil
}

var _ = chk.Suite(&decompressingWriterSuite{})

func (d *decompressingWriterSuite) TestDecompressingWriter(c *chk.C) {
	// given:
	// we have original uncompressed data
	compressionType := ECompressionType.GZip() // todo: both types
	originalSize := int64(10 * 1024 * 1024)    // TODO: do an range of szies
	originalData := make([]byte, originalSize)
	_, err := NewRandomDataGenerator(originalSize).ReadAt(originalData, 0)
	c.Assert(err, chk.IsNil)
	// and we have original compressed data
	buf := &bytes.Buffer{}
	comp := zlib.NewWriter(buf)
	_, err = io.Copy(comp, bytes.NewReader(originalData)) // write into buf by way of comp
	c.Assert(err, chk.IsNil)
	c.Assert(comp.Close(), chk.IsNil)
	compressedData := buf.Bytes()

	// when:
	underlyingDest := &bytes.Buffer{} // will be a file in real usage, but just a buffer in this test
	decWriter := NewDecompressingWriter(closeableDecorator{underlyingDest, false}, compressionType)
	copyBuf := make([]byte, rand.Intn(1024*1024)+1) // copy using a random-sized buffer, to make sure that, over time, we exercise a range of buffer sizes
	// we decompress using a decompressing writer
	_, err = io.CopyBuffer(decWriter, bytes.NewReader(compressedData), copyBuf)
	c.Assert(err, chk.IsNil)
	err = decWriter.Close()
	c.Assert(err, chk.IsNil)

	// then:
	// the data that was written to the underlying destination is correctly decompressed
	dataWritten := underlyingDest.Bytes()
	c.Assert(dataWritten, chk.DeepEquals, originalData)
}
