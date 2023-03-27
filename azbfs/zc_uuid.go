package azbfs

import (
	"crypto/rand"
	"fmt"
)

// The UUID reserved variants.
const (
	reservedRFC4122 byte = 0x40
)

// A UUID representation compliant with specification in RFC 4122 document.
type uuid [16]byte

// NewUUID returns a new uuid using RFC 4122 algorithm.
func newUUID() (u uuid) {
	u = uuid{}
	// Set all bits to randomly (or pseudo-randomly) chosen values.
	_, err := rand.Read(u[:])
	if err != nil {
		panic("ran.Read failed")
	}
	u[8] = (u[8] | reservedRFC4122) & 0x7F // u.setVariant(ReservedRFC4122)

	var version byte = 4
	u[6] = (u[6] & 0xF) | (version << 4) // u.setVersion(4)
	return
}

// String returns an unparsed version of the generated UUID sequence.
func (u uuid) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:])
}
