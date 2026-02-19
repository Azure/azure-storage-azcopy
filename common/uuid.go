package common

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"unsafe"
)

// The JobID reserved variants.
const (
	reservedRFC4122 byte = 0x40
	guidFormat           = "%08x-%04x-%04x-%02x%02x-%02x%02x%02x%02x%02x%02x"
)

// A UUID representation compliant with specification in RFC 4122 document.
type UUID struct {
	D1 uint32
	D2 uint16
	D3 uint16
	D4 [8]uint8
}

// NewUUID returns a new UUID using RFC 4122 algorithm.
func NewUUID() (u UUID) {
	u = UUID{}
	// Set all bits to randomly (or pseudo-randomly) chosen values.
	uuid := (*[16]byte)(unsafe.Pointer(&u))[:]
	_, err := rand.Read(uuid)
	if err != nil {
		panic("rand.Read failed")
	}
	uuid[8] = (uuid[8] | reservedRFC4122) & 0x7F // u.setVariant(ReservedRFC4122)

	var version byte = 4
	uuid[6] = (uuid[6] & 0xF) | (version << 4) // u.setVersion(4)
	return
}

// String returns an unparsed version of the generated UUID sequence.
func (u UUID) String() string {
	return fmt.Sprintf(guidFormat, u.D1, u.D2, u.D3, u.D4[0], u.D4[1], u.D4[2], u.D4[3], u.D4[4], u.D4[5], u.D4[6], u.D4[7])
}

// Implementing MarshalJSON() method for type UUID
func (u UUID) MarshalJSON() ([]byte, error) {
	s := u.String()
	return json.Marshal(s)
}

// Implementing UnmarshalJSON() method for type UUID
func (u *UUID) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	uuid, err := ParseUUID(s)
	if err != nil {
		return err
	}
	*u = uuid
	return nil
}

// ParseUUID parses a string formatted as "003020100-0504-0706-0809-0a0b0c0d0e0f"
// or "{03020100-0504-0706-0809-0a0b0c0d0e0f}" into a UUID.
func ParseUUID(uuidStr string) (UUID, error) {
	if uuidStr[0] == '{' {
		uuidStr = uuidStr[1:] // Skip over the '{'
	}
	uuid := UUID{}
	_, err := fmt.Sscanf(uuidStr, guidFormat,
		&uuid.D1, &uuid.D2, &uuid.D3,
		&uuid.D4[0], &uuid.D4[1], &uuid.D4[2], &uuid.D4[3], &uuid.D4[4], &uuid.D4[5], &uuid.D4[6], &uuid.D4[7])
	if err != nil {
		return UUID{}, err
	}
	return uuid, nil
}
