package e2etest

import (
	"bytes"
	"crypto/md5"
	"io"
	"math/rand"
	"strconv"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
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

var megaSize = []string{
	"B",
	"KB",
	"MB",
	"GB",
	"TB",
	"PB",
	"EB",
}

func SizeToString(size int64, megaUnits bool) string {
	units := []string{
		"B",
		"KiB",
		"MiB",
		"GiB",
		"TiB",
		"PiB",
		"EiB", // Let's face it, a file, account, or container probably won't be more than 1000 exabytes in YEARS.
		// (and int64 literally isn't large enough to handle too many exbibytes. 128 bit processors when)
	}
	unit := 0
	floatSize := float64(size)
	gigSize := 1024

	if megaUnits {
		gigSize = 1000
		units = megaSize
	}

	for floatSize/float64(gigSize) >= 1 {
		unit++
		floatSize /= float64(gigSize)
	}

	return strconv.FormatFloat(floatSize, 'f', 2, 64) + " " + units[unit]
}

func NewRandomObjectContentContainer(size int64) ObjectContentContainer {
	buf := make([]byte, size)
	_, _ = rand.New(rand.NewSource(time.Now().Unix())).Read(buf)
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
