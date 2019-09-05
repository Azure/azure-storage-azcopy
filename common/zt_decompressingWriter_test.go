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
	"compress/gzip"
	"compress/zlib"
	chk "gopkg.in/check.v1"
	"io"
	"math/rand"
)

type decompressingWriterSuite struct{}

type closeableBuffer struct {
	*bytes.Buffer
	closeWasCalled bool
}

func (c *closeableBuffer) Close() error {
	c.closeWasCalled = true
	return nil
}

var _ = chk.Suite(&decompressingWriterSuite{})

func (d *decompressingWriterSuite) TestDecompressingWriter_SuccessCases(c *chk.C) {
	cases := []struct {
		desc            string
		tp              CompressionType
		originalSize    int
		writeBufferSize int
	}{
		// we use random write buffer sizes in some cases, so that over time we'll test a wide variety of sizes
		// The other size we use is 1 byte, so we'll get a case where all bytes have been processed,
		// (exactly), but Close has not been called. To make sure EOF is picked up when Close is called.
		{"big gzip", ECompressionType.GZip(), 10 * 1024 * 1024, rand.Intn(1024*1024) + 1},
		{"sml gzip", ECompressionType.GZip(), 1024, rand.Intn(1024*1024) + 1},
		{"1bytgzip", ECompressionType.GZip(), 1234, 1},

		{"big zlib", ECompressionType.ZLib(), 10 * 1024 * 1024, rand.Intn(1024*1024) + 1},
		{"sml zlib", ECompressionType.ZLib(), 1024, rand.Intn(1024*1024) + 1},
		{"1bytzlib", ECompressionType.ZLib(), 1234, 1},
	}

	for _, cs := range cases {
		// given:
		originalData, compressedData := d.getTestData(c, cs.tp, cs.originalSize)

		// when:
		// we decompress using a decompressing writer
		destFile := &closeableBuffer{&bytes.Buffer{}, false} // will be a file in real usage, but just a buffer in this test
		decWriter := NewDecompressingWriter(destFile, cs.tp)
		copyBuf := make([]byte, cs.writeBufferSize)
		_, err := io.CopyBuffer(decWriter, bytes.NewReader(compressedData), copyBuf) // write compressed data to decWriter
		c.Assert(err, chk.IsNil)
		err = decWriter.Close()
		c.Assert(err, chk.IsNil)

		// then:
		// the data that was written to the underlying destination is correctly decompressed
		dataWritten := destFile.Bytes()
		c.Assert(dataWritten, chk.DeepEquals, originalData)
		// the dest is closed
		c.Assert(destFile.closeWasCalled, chk.Equals, true)
	}
}

func (d *decompressingWriterSuite) TestDecompressingWriter_EarlyClose(c *chk.C) {

	cases := []CompressionType{
		ECompressionType.GZip(),
		ECompressionType.ZLib(),
	}
	for _, tp := range cases {
		// given:
		dataSize := int(rand.Int31n(1024*1024) + 100)
		_, compressedData := d.getTestData(c, tp, dataSize)
		sizeBeforeEarlyClose := int64(len(compressedData) / 2)

		// when:
		// we close the decompressing writer before we have processed everything
		destFile := &closeableBuffer{&bytes.Buffer{}, false} // will be a file in real usage, but just a buffer in this test
		decWriter := NewDecompressingWriter(destFile, tp)
		n, err := io.CopyN(decWriter, bytes.NewReader(compressedData), sizeBeforeEarlyClose) // process only some of the data
		c.Assert(err, chk.IsNil)
		err = decWriter.Close()

		// then:
		// the amount processed was as expected, the dest file is closed, and an error was returned from close (because decompressor never sees the expected footer)
		c.Assert(n, chk.Equals, sizeBeforeEarlyClose)
		c.Assert(destFile.closeWasCalled, chk.Equals, true)
		c.Assert(err, chk.NotNil)
	}
}

func (d *decompressingWriterSuite) getTestData(c *chk.C, tp CompressionType, originalSize int) (original []byte, compressed []byte) {
	// we have original uncompressed data
	originalData := d.genCompressibleTestData(originalSize)
	// and from that we have original compressed data
	compBuf := &bytes.Buffer{}
	var comp io.WriteCloser = zlib.NewWriter(compBuf)
	if tp == ECompressionType.GZip() {
		comp = gzip.NewWriter(compBuf)
	}
	_, err := io.Copy(comp, bytes.NewReader(originalData))
	// write into buf by way of comp
	c.Assert(err, chk.IsNil)
	c.Assert(comp.Close(), chk.IsNil)
	compressedData := compBuf.Bytes()
	return originalData, compressedData
}

/* Manual sanity check of compressible data gen
func (d *decompressingWriterSuite) TestDecompressingWriter_GenTestData(c *chk.C) {
	f, _ := os.Create("<yourfoldergoeshere>\\testGen4373462.dat")
	dat := d.genCompressibleTestData(20 * 1024)
	f.Write(dat)
	f.Close()
}*/

func (d *decompressingWriterSuite) genCompressibleTestData(size int) []byte {
	phrases := make([][]byte, rand.Intn(50)+1)
	for i := range phrases {
		phrases[i] = make([]byte, rand.Intn(100)+1)
		rand.Read(phrases[i])
	}
	b := bytes.Buffer{}
	for n := 0; n < size; {
		delta, _ := b.Write(phrases[rand.Intn(len(phrases))])
		n += delta
	}
	return b.Bytes()[:size]
}
