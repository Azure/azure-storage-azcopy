package azbfs

import (
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	// CountToEnd indicates a flag for count parameter. It means the count of bytes
	// from start offset to the end of file.
	CountToEnd = 0
)

// httpRange defines a range of bytes within an HTTP resource, starting at offset and
// ending at offset+count-1 inclusively.
// An httpRange which has a zero-value offset, and a count with value CountToEnd indicates the entire resource.
// An httpRange which has a non zero-value offset but a count with value CountToEnd indicates from the offset to the resource's end.
type httpRange struct {
	offset int64
	count  int64
}

func (r httpRange) pointers() *string {
	if r.offset == 0 && r.count == CountToEnd { // Do common case first for performance
		return nil // No specified range
	}
	if r.offset < 0 {
		panic("The range offset must be >= 0")
	}
	if r.count <= 0 && r.count != CountToEnd {
		panic("The range count must be either equal to CountToEnd (0) or > 0")
	}

	return toRange(r.offset, r.count)
}

// toRange makes range string adhere to REST API.
// A count with value CountToEnd means count of bytes from offset to the end of file.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/specifying-the-range-header-for-file-service-operations.
func toRange(offset int64, count int64) *string {
	// No additional validation by design. API can validate parameter by case, and use this method.
	endRange := ""
	if count != CountToEnd {
		endRange = strconv.FormatInt(offset+count-1, 10)
	}
	r := fmt.Sprintf("bytes=%d-%s", offset, endRange)
	return &r
}

func validateSeekableStreamAt0AndGetCount(body io.ReadSeeker) int64 {
	if body == nil { // nil body's are "logically" seekable to 0 and are 0 bytes long
		return 0
	}
	validateSeekableStreamAt0(body)
	count, err := body.Seek(0, io.SeekEnd)
	if err != nil {
		panic("failed to seek stream")
	}
	body.Seek(0, io.SeekStart)
	return count
}

func validateSeekableStreamAt0(body io.ReadSeeker) {
	if body == nil { // nil body's are "logically" seekable to 0
		return
	}
	if pos, err := body.Seek(0, io.SeekCurrent); pos != 0 || err != nil {
		if err != nil {
			panic(err)
		}
		panic(errors.New("stream must be set to position 0"))
	}
}
