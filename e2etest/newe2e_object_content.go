package e2etest

import (
	"bytes"
	"io"
	"math/rand"
)

type ObjectContentContainer interface {
	Size() int64
	Reader() io.ReadSeeker
	//MD5() [md5.Size]byte
	//CRC64() uint64
}

func NewRandomObjectContentContainer(size int64) (ObjectContentContainer, error) {
	buf := make([]byte, size)
	_, err := rand.Read(buf)
	return &ObjectContentContainerBuffer{buf}, err
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
