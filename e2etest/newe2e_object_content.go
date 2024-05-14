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

func (o *ObjectContentContainerBuffer) MD5() [md5.Size]byte {
	return md5.Sum(o.Data)
}
