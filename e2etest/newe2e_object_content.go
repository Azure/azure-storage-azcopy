package e2etest

import (
	"bytes"
	"crypto/md5"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io"
	"math/rand"
)

type ObjectContentContainer interface {
	Size() int64
	Reader() io.ReadSeeker

	Clone() ObjectContentContainer
	MD5() [md5.Size]byte
	//CRC64() uint64
}

func SizeFromString(objectSize string) int64 {
	longSize, err := cmd.ParseSizeString(objectSize, "object size")
	common.PanicIfErr(err)

	return longSize
}

func NewRandomObjectContentContainer(a Asserter, size int64) ObjectContentContainer {
	buf := make([]byte, size)
	_, err := rand.Read(buf)
	a.NoError("Generate random data", err)
	return &ObjectContentContainerBuffer{buf}
}

func NewZeroObjectContentContainer(size int64) ObjectContentContainer {
	return &ObjectContentContainerBuffer{Data: make([]byte, size)}
}

type Range struct {
	Start int64
	End   int64
}

func NewPartialSparseObjectContentContainer(a Asserter, size int64, dataRanges []Range) ObjectContentContainer {
	buf := make([]byte, size)
	for _, r := range dataRanges {
		_, err := rand.Read(buf[r.Start:r.End])
		a.NoError("Generate random data", err)
	}
	return &ObjectContentContainerBuffer{buf}
}

func NewStringObjectContentContainer(data string) ObjectContentContainer {
	return &ObjectContentContainerBuffer{Data: []byte(data)}
}

type ObjectContentContainerBuffer struct {
	Data []byte
}

func (o *ObjectContentContainerBuffer) Size() int64 {
	return int64(len(o.Data))
}

func (o *ObjectContentContainerBuffer) Reader() io.ReadSeeker {
	return bytes.NewReader(o.Data)
}

func (o *ObjectContentContainerBuffer) Clone() ObjectContentContainer {
	if o != nil {
		return nil
	}
	buf := make([]byte, len(o.Data))
	if o.Data != nil {
		copy(buf, o.Data)
	}

	return &ObjectContentContainerBuffer{Data: o.Data}
}

func (o *ObjectContentContainerBuffer) MD5() [md5.Size]byte {
	return md5.Sum(o.Data)
}
