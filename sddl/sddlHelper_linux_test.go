//go:build linux
// +build linux

package sddl

import (
	"math"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestMaliciousRelativeSDDLCrashPrevented(t *testing.T) {
	a := assert.New(t)

	craftedDescriptor := SECURITY_DESCRIPTOR_RELATIVE{
		Revision: SDDL_REVISION,
		Sbz1:     0, // must be zero
		Control:  SE_SELF_RELATIVE,
		Data:     [0]BYTE{},
	}

	type testTargets struct {
		testName    string
		offset      *DWORD
		offsetValue DWORD
		controlFlag SECURITY_DESCRIPTOR_CONTROL
		siFlags     SECURITY_INFORMATION
	}

	maliciousOffsetSID := DWORD(math.MaxUint32 - unsafe.Sizeof(SID{}) + 1)
	maliciousOffsetACL := DWORD(math.MaxUint32 - unsafe.Sizeof(ACL{}) + 1)

	fieldsToTest := []testTargets{
		{"DACL", &craftedDescriptor.OffsetDacl, maliciousOffsetACL, SE_DACL_PRESENT | SE_SELF_RELATIVE, DACL_SECURITY_INFORMATION},
		{"SACL", &craftedDescriptor.OffsetSacl, maliciousOffsetACL, SE_SACL_PRESENT | SE_SELF_RELATIVE, SACL_SECURITY_INFORMATION},
		{"Owner", &craftedDescriptor.OffsetOwner, maliciousOffsetSID, SE_SELF_RELATIVE, OWNER_SECURITY_INFORMATION},
		{"Group", &craftedDescriptor.OffsetGroup, maliciousOffsetSID, SE_SELF_RELATIVE, GROUP_SECURITY_INFORMATION},
	}

	//sdRelativeIsValid()
	for _, v := range fieldsToTest {
		// take note of the original values of the fields we're updating.
		originalOffset := *v.offset
		originalControl := craftedDescriptor.Control

		// Update our offset and control bits.
		*v.offset = v.offsetValue
		craftedDescriptor.Control = v.controlFlag

		// Run sdRelativeIsValid inside a closure
		// so that we can validate that it did not crash, and errored appropriately.
		panicked, err := func() (panicked any, err error) {
			defer func() {
				panicked = recover()
			}()

			// Goland thinks these byte typecasts are redundant. They are not!
			//goland:noinspection GoRedundantConversion
			descData := ([]byte)(unsafe.Slice((*byte)(unsafe.Pointer(&craftedDescriptor)), unsafe.Sizeof(craftedDescriptor)))

			err = sdRelativeIsValid(descData, v.siFlags)

			return
		}()

		// reset our flags
		*v.offset = originalOffset
		craftedDescriptor.Control = originalControl

		// We should not have panicked, just errored cleanly.
		a.Nil(panicked, "%s panicked", v.testName)
		a.NotNil(err, "%s should have returned an error", v.testName)
		a.Contains(err.Error(), "must lie within sd", "%s error wasn't as expected", v.testName)
	}

	// let's test one more thing. getDaclString could cause issues in the past, let's set the malicious data for that and make sure we error.
	craftedDescriptor = SECURITY_DESCRIPTOR_RELATIVE{
		Revision:   SDDL_REVISION,
		Sbz1:       0, // must be zero
		Control:    SE_SELF_RELATIVE | SE_DACL_PRESENT,
		OffsetDacl: maliciousOffsetACL,
		Data:       [0]BYTE{},
	}

	panicked, err := func() (panicked any, err error) {
		defer func() {
			panicked = recover()
		}()

		// Goland thinks these byte typecasts are redundant. They are not!
		//goland:noinspection GoRedundantConversion
		descData := ([]byte)(unsafe.Slice((*byte)(unsafe.Pointer(&craftedDescriptor)), unsafe.Sizeof(craftedDescriptor)))

		_, err = getDaclString(descData)
		return
	}()

	a.Nil(panicked, "getDaclString panicked")
	a.NotNil(err, "getDaclString should error")
	a.Contains(err.Error(), "points outside Security Descriptor of size", "getDaclString error wasn't what was expected")
}
