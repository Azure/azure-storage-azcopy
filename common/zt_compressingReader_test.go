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
	chk "gopkg.in/check.v1"
	"io"
	"math/rand"
	"os"
	"sync/atomic"
)

type closeableReaderAt struct {
	atomicCloseWasCalled int32
	io.ReaderAt
}

func (c *closeableReaderAt) Close() error {
	atomic.StoreInt32(&c.atomicCloseWasCalled, 1)
	return nil
}

func (c *closeableReaderAt) closeWasCalled() bool {
	return atomic.LoadInt32(&c.atomicCloseWasCalled) == 1
}

func (d *compressionSuite) TestCompressingReader_SuccessCases(c *chk.C) {
	cases := []struct {
		desc             string
		rawDataSize      int
		incomingReadSize int
	}{
		// we use random read sizes in some cases, so that over time we'll test a wide variety of sizes
		{"big gzip", 50 * 1024 * 1024, rand.Intn(1024*1024) + 1},
		{"sml gzip", 1024, rand.Intn(1024*1024) + 1},
		{"1 byte gzip", 1234, 1},
	}

	for _, cs := range cases {
		// given:
		originalData, compressedData := d.getTestData(c, ECompressionType.GZip(), cs.rawDataSize)

		// when:
		// we compress using a compressing reader
		closeableSourceData := &closeableReaderAt{ReaderAt: bytes.NewReader(originalData)}
		compReader := NewCompressingReader(closeableSourceData, int64(cs.rawDataSize))
		//copyBuf := make([]byte, cs.incomingReadSize)

		//result := &bytes.Buffer{}
		//_, err := io.Copy(result, compReader) // retrieve compressed data from compReader
		result := make([]byte, len(compressedData)*2)
		n, err := io.ReadFull(compReader, result) // retrieve compressed data from compReader
		c.Assert(err, chk.Equals, io.ErrUnexpectedEOF)
		err = compReader.Close()
		c.Assert(err, chk.IsNil)

		// then:
		// the data is correctly compressed
		c.Assert(n, chk.Equals, len(compressedData))
		dataWritten := result[0:len(compressedData)]
		c.Assert(dataWritten, chk.DeepEquals, compressedData)
		// the src is closed
		c.Assert(closeableSourceData.closeWasCalled(), chk.Equals, true)
	}
}

// stop reading before all the data is compressed
// make sure the reader exits gracefully
func (d *compressionSuite) TestCompressingReader_EarlyClose(c *chk.C) {
	// given:
	originalData, _ := d.getTestData(c, ECompressionType.GZip(), int(rand.Int31n(1024*1024)+100))

	// when:
	// we compress using a compressing reader
	closeableSourceData := &closeableReaderAt{ReaderAt: bytes.NewReader(originalData)}
	compReader := NewCompressingReader(closeableSourceData, int64(len(originalData)))

	result := &bytes.Buffer{}
	n, err := io.CopyN(result, compReader, 1) // retrieve only 1 byte
	c.Assert(err, chk.IsNil)
	err = compReader.Close()
	c.Assert(err, chk.IsNil)

	// then:
	// the data is correctly compressed
	c.Assert(n, chk.Equals, int64(1))
	// the src is closed
	c.Assert(closeableSourceData.closeWasCalled(), chk.Equals, true)
}

func (d *compressionSuite) TestCompressingReader_File(c *chk.C) {
	f, err := os.Open("/Users/devexp/work/go/azure-storage-azcopy/temp_50MB_file.txt")
	c.Assert(err, chk.IsNil)

	compBuf := &bytes.Buffer{}
	comp := gzip.NewWriter(compBuf)
	_, err = io.Copy(comp, f)
	// write into buf by way of comp
	c.Assert(err, chk.IsNil)
	c.Assert(comp.Close(), chk.IsNil)
	compressedData := compBuf.Bytes()

	// when:
	// we compress using a compressing reader
	fi, err := f.Stat()
	if err != nil {
		// Could not obtain stat, handle error
	}
	closeableSourceData := &closeableReaderAt{ReaderAt: f}
	compReader := NewCompressingReader(closeableSourceData, fi.Size())
	//copyBuf := make([]byte, cs.incomingReadSize)

	//result := &bytes.Buffer{}
	//_, err := io.Copy(result, compReader) // retrieve compressed data from compReader
	result := make([]byte, len(compressedData)*2)
	n, err := io.ReadFull(compReader, result) // retrieve compressed data from compReader
	c.Assert(err, chk.Equals, io.ErrUnexpectedEOF)
	err = compReader.Close()
	c.Assert(err, chk.IsNil)

	// then:
	// the data is correctly compressed
	c.Assert(n, chk.Equals, len(compressedData))
	dataWritten := result[0:len(compressedData)]
	c.Assert(dataWritten, chk.DeepEquals, compressedData)
	// the src is closed
	c.Assert(closeableSourceData.closeWasCalled(), chk.Equals, true)
}
