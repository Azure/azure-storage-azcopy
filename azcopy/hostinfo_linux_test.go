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

//go:build linux

package azcopy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClassifyFSType(t *testing.T) {
	a := assert.New(t)
	a.Equal("nas-nfs", classifyFSType("nfs"))
	a.Equal("nas-nfs", classifyFSType("nfs4"))
	a.Equal("nas-smb", classifyFSType("cifs"))
	a.Equal("nas-smb", classifyFSType("smb3"))
	a.Equal("nas-smb", classifyFSType("smbfs"))
	a.Equal("local-disk", classifyFSType("ext4"))
	a.Equal("local-disk", classifyFSType("xfs"))
	a.Equal("", classifyFSType(""))
}

func TestParseMountinfoLine(t *testing.T) {
	a := assert.New(t)
	mp, fs, ok := parseMountinfoLine("36 35 98:0 / /mnt/nas rw,noatime - nfs4 1.2.3.4:/export rw")
	a.True(ok)
	a.Equal("/mnt/nas", mp)
	a.Equal("nfs4", fs)

	mp, fs, ok = parseMountinfoLine("22 30 0:21 / / rw,relatime shared:1 - ext4 /dev/root rw")
	a.True(ok)
	a.Equal("/", mp)
	a.Equal("ext4", fs)

	_, _, ok = parseMountinfoLine("garbage line without separator")
	a.False(ok)
}

func TestPathHasMountPrefix(t *testing.T) {
	a := assert.New(t)
	a.True(pathHasMountPrefix("/mnt/nas/data", "/mnt/nas"))
	a.True(pathHasMountPrefix("/mnt/nas", "/mnt/nas"))
	a.True(pathHasMountPrefix("/anything", "/"))
	a.False(pathHasMountPrefix("/mnt/nasextra", "/mnt/nas"))
	a.False(pathHasMountPrefix("/home/user", "/mnt/nas"))
}
