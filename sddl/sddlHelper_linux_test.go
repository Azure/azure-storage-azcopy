//go:build linux
// +build linux

// Copyright Microsoft <wastore@microsoft.com>
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

package sddl

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-world SDDL from a lab system with a callback ACE (XA) and inherited ACEs.
const realWorldSDDL = `O:S-1-5-21-2127521184-1604012920-1887927527-5560896G:DUD:AI(XA;;0x1200a9;;;AU;(Member_of{SID(S-1-5-21-72051607-1745760036-109187956-363937)}))(A;ID;FA;;;BA)(A;ID;FA;;;SY)(A;ID;FA;;;S-1-5-21-2127521184-1604012920-1887927527-5560896)(A;ID;0x1200a9;;;BU)`

// =============================================================================
// sidToString / stringToSid — used by SecurityDescriptorToString and
// SecurityDescriptorFromString to convert SIDs between binary and string forms.
// =============================================================================

func TestSidToString(t *testing.T) {
	tests := []struct {
		name     string
		sid      []byte
		expected string
		wantErr  bool
	}{
		{
			name: "LocalSystem SID S-1-5-18",
			sid: func() []byte {
				b := make([]byte, 12)
				b[0] = 1 // Revision
				b[1] = 1 // SubAuthorityCount
				b[2], b[3], b[4], b[5], b[6], b[7] = 0, 0, 0, 0, 0, 5
				binary.LittleEndian.PutUint32(b[8:12], 18)
				return b
			}(),
			expected: "S-1-5-18",
		},
		{
			name: "Builtin Admins S-1-5-32-544",
			sid: func() []byte {
				b := make([]byte, 16)
				b[0] = 1
				b[1] = 2
				b[2], b[3], b[4], b[5], b[6], b[7] = 0, 0, 0, 0, 0, 5
				binary.LittleEndian.PutUint32(b[8:12], 32)
				binary.LittleEndian.PutUint32(b[12:16], 544)
				return b
			}(),
			expected: "S-1-5-32-544",
		},
		{
			name:    "Too short",
			sid:     []byte{1, 2, 3},
			wantErr: true,
		},
		{
			name: "SubAuthorityCount exceeds buffer",
			sid: func() []byte {
				b := make([]byte, 12)
				b[0] = 1
				b[1] = 5
				b[2], b[3], b[4], b[5], b[6], b[7] = 0, 0, 0, 0, 0, 5
				binary.LittleEndian.PutUint32(b[8:12], 18)
				return b
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sidToString(tt.sid)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestStringToSid(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "Numeric SID", input: "S-1-5-18"},
		{name: "Numeric SID with multiple SubAuthorities", input: "S-1-5-32-544"},
		{name: "Well-known shortcut BA", input: "BA"},
		{name: "Well-known shortcut SY", input: "SY"},
		{name: "Invalid SID string", input: "INVALID_SID", wantErr: true},
		{name: "Invalid revision", input: "S-2-5-18", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := stringToSid(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotEmpty(t, result)
			}
		})
	}
}

func TestSidRoundTrip(t *testing.T) {
	sids := []string{
		"S-1-5-18",
		"S-1-5-32-544",
		"S-1-5-21-1234567890-987654321-111111111-1001",
		"S-1-1-0",
		"S-1-5-21-2127521184-1604012920-1887927527-5560896",
		"S-1-5-21-72051607-1745760036-109187956-363937",
	}

	for _, sid := range sids {
		t.Run(sid, func(t *testing.T) {
			bin, err := stringToSid(sid)
			require.NoError(t, err)

			result, err := sidToString(bin)
			require.NoError(t, err)
			assert.Equal(t, sid, result)
		})
	}
}

func TestWellKnownSidShortcutRoundTrip(t *testing.T) {
	shortcuts := []struct {
		shortcut    string
		expectedSid string
	}{
		{"SY", "S-1-5-18"},
		{"BA", "S-1-5-32-544"},
		{"BU", "S-1-5-32-545"},
		{"AU", "S-1-5-11"},
	}

	for _, tt := range shortcuts {
		t.Run(tt.shortcut, func(t *testing.T) {
			bin, err := stringToSid(tt.shortcut)
			require.NoError(t, err)

			result, err := sidToString(bin)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedSid, result)
		})
	}
}

// =============================================================================
// aceToString / aceRightsToString — used by getDaclString when reading binary
// ACEs from a security descriptor queried via QuerySecurityObject.
// =============================================================================

func TestAceToString(t *testing.T) {
	tests := []struct {
		name    string
		ace     []byte
		expect  string
		wantErr bool
	}{
		{
			name: "ACCESS_ALLOWED with GA to S-1-1-0",
			ace: func() []byte {
				b := make([]byte, 20)
				b[0] = ACCESS_ALLOWED_ACE_TYPE
				b[1] = 0
				binary.LittleEndian.PutUint16(b[2:4], 20)
				binary.LittleEndian.PutUint32(b[4:8], GENERIC_ALL)
				b[8] = 1
				b[9] = 1
				b[10], b[11], b[12], b[13], b[14], b[15] = 0, 0, 0, 0, 0, 1
				binary.LittleEndian.PutUint32(b[16:20], 0)
				return b
			}(),
			expect: "(A;;GA;;;S-1-1-0)",
		},
		{
			name: "ACCESS_DENIED with inheritance flags",
			ace: func() []byte {
				b := make([]byte, 20)
				b[0] = ACCESS_DENIED_ACE_TYPE
				b[1] = CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE | INHERITED_ACE
				binary.LittleEndian.PutUint16(b[2:4], 20)
				binary.LittleEndian.PutUint32(b[4:8], GENERIC_ALL)
				b[8] = 1
				b[9] = 1
				b[10], b[11], b[12], b[13], b[14], b[15] = 0, 0, 0, 0, 0, 1
				binary.LittleEndian.PutUint32(b[16:20], 0)
				return b
			}(),
			expect: "(D;CIOIID;GA;;;S-1-1-0)",
		},
		{
			name:    "Too short",
			ace:     []byte{0, 0, 0},
			wantErr: true,
		},
		{
			name: "Unsupported ACE type is rejected",
			ace: func() []byte {
				b := make([]byte, 20)
				b[0] = SYSTEM_AUDIT_ACE_TYPE
				b[1] = 0
				binary.LittleEndian.PutUint16(b[2:4], 20)
				binary.LittleEndian.PutUint32(b[4:8], GENERIC_ALL)
				b[8] = 1
				b[9] = 1
				b[10], b[11], b[12], b[13], b[14], b[15] = 0, 0, 0, 0, 0, 1
				binary.LittleEndian.PutUint32(b[16:20], 0)
				return b
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := aceToString(tt.ace, ACL_REVISION)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expect, result)
			}
		})
	}
}

func TestAceRightsToString(t *testing.T) {
	tests := []struct {
		name     string
		rights   uint32
		expected string
	}{
		{"GENERIC_ALL", GENERIC_ALL, "GA"},
		{"GENERIC_READ", GENERIC_READ, "GR"},
		{"DELETE", DELETE, "SD"},
		{"READ_CONTROL", READ_CONTROL, "RC"},
		{"FILE_ALL_ACCESS falls back to hex", FILE_ALL_ACCESS, "0x1f01ff"},
		{"0x1200a9 falls back to hex", 0x1200a9, "0x1200a9"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := aceRightsToString(tt.rights)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// getDaclString — used by SecurityDescriptorToString to extract the DACL from
// a binary security descriptor returned by QuerySecurityObject.
// =============================================================================

func TestGetDaclStringACLRevisionAcceptsAnyRevision(t *testing.T) {
	// Various SMB servers (e.g. NetApp) may return non-standard ACL revision values.
	for _, rev := range []byte{2, 3, 4, 5} {
		sd := make([]byte, 36)
		sd[0] = 1
		sd[1] = 0
		binary.LittleEndian.PutUint16(sd[2:4], uint16(SE_SELF_RELATIVE|SE_DACL_PRESENT))
		binary.LittleEndian.PutUint32(sd[16:20], 20)

		sd[20] = rev
		sd[21] = 0
		binary.LittleEndian.PutUint16(sd[22:24], 8)
		binary.LittleEndian.PutUint32(sd[24:28], 0)

		result, err := getDaclString(sd)
		assert.NoError(t, err, "revision %d should be accepted", rev)
		assert.Equal(t, "D:", result)
	}
}

func TestGetDaclStringNoAccessControl(t *testing.T) {
	sd := make([]byte, 28)
	sd[0] = 1
	sd[1] = 0
	binary.LittleEndian.PutUint16(sd[2:4], uint16(SE_SELF_RELATIVE|SE_DACL_PRESENT))

	result, err := getDaclString(sd)
	assert.NoError(t, err)
	assert.Equal(t, "D:NO_ACCESS_CONTROL", result)
}

func TestGetDaclStringWithProtectedAndAutoInheritFlags(t *testing.T) {
	sd := make([]byte, 36)
	sd[0] = 1
	sd[1] = 0
	binary.LittleEndian.PutUint16(sd[2:4], uint16(SE_SELF_RELATIVE|SE_DACL_PRESENT|SE_DACL_PROTECTED|SE_DACL_AUTO_INHERITED))
	binary.LittleEndian.PutUint32(sd[16:20], 20)

	sd[20] = ACL_REVISION
	sd[21] = 0
	binary.LittleEndian.PutUint16(sd[22:24], 8)
	binary.LittleEndian.PutUint32(sd[24:28], 0)

	result, err := getDaclString(sd)
	assert.NoError(t, err)
	assert.Contains(t, result, "P")
	assert.Contains(t, result, "AI")
}

func TestGetDaclStringWithACEs(t *testing.T) {
	sd := make([]byte, 56)
	sd[0] = 1
	sd[1] = 0
	binary.LittleEndian.PutUint16(sd[2:4], uint16(SE_SELF_RELATIVE|SE_DACL_PRESENT))
	binary.LittleEndian.PutUint32(sd[16:20], 20)

	sd[20] = ACL_REVISION
	sd[21] = 0
	binary.LittleEndian.PutUint32(sd[24:28], 1)

	aceOffset := 28
	sd[aceOffset] = ACCESS_ALLOWED_ACE_TYPE
	sd[aceOffset+1] = 0
	binary.LittleEndian.PutUint16(sd[aceOffset+2:aceOffset+4], 20)
	binary.LittleEndian.PutUint32(sd[aceOffset+4:aceOffset+8], GENERIC_ALL)
	sd[aceOffset+8] = 1
	sd[aceOffset+9] = 1
	sd[aceOffset+10], sd[aceOffset+11], sd[aceOffset+12], sd[aceOffset+13], sd[aceOffset+14], sd[aceOffset+15] = 0, 0, 0, 0, 0, 1
	binary.LittleEndian.PutUint32(sd[aceOffset+16:aceOffset+20], 0)
	binary.LittleEndian.PutUint16(sd[22:24], 28)

	result, err := getDaclString(sd)
	assert.NoError(t, err)
	assert.Contains(t, result, "(A;;GA;;;S-1-1-0)")
}

// =============================================================================
// sdRelativeIsValid — validates binary security descriptors; used by both
// SecurityDescriptorToString (read path) and SetSecurityObject (write path).
// =============================================================================

func TestSdRelativeIsValid(t *testing.T) {
	tests := []struct {
		name    string
		sd      []byte
		flags   SECURITY_INFORMATION
		wantErr bool
	}{
		{
			name:    "Too small",
			sd:      []byte{1, 2, 3},
			flags:   DACL_SECURITY_INFORMATION,
			wantErr: true,
		},
		{
			name: "Invalid revision",
			sd: func() []byte {
				b := make([]byte, 28)
				b[0] = 2
				binary.LittleEndian.PutUint16(b[2:4], uint16(SE_SELF_RELATIVE|SE_DACL_PRESENT))
				return b
			}(),
			flags:   DACL_SECURITY_INFORMATION,
			wantErr: true,
		},
		{
			name: "SE_SELF_RELATIVE not set",
			sd: func() []byte {
				b := make([]byte, 28)
				b[0] = 1
				binary.LittleEndian.PutUint16(b[2:4], uint16(SE_DACL_PRESENT))
				return b
			}(),
			flags:   DACL_SECURITY_INFORMATION,
			wantErr: true,
		},
		{
			name: "SE_DACL_PRESENT not set when DACL requested",
			sd: func() []byte {
				b := make([]byte, 28)
				b[0] = 1
				binary.LittleEndian.PutUint16(b[2:4], uint16(SE_SELF_RELATIVE))
				return b
			}(),
			flags:   DACL_SECURITY_INFORMATION,
			wantErr: true,
		},
		{
			name: "Valid minimal SD",
			sd: func() []byte {
				b := make([]byte, 28)
				b[0] = 1
				binary.LittleEndian.PutUint16(b[2:4], uint16(SE_SELF_RELATIVE|SE_DACL_PRESENT))
				return b
			}(),
			flags: DACL_SECURITY_INFORMATION,
		},
		{
			name: "OffsetOwner is 0 when OWNER requested",
			sd: func() []byte {
				b := make([]byte, 28)
				b[0] = 1
				binary.LittleEndian.PutUint16(b[2:4], uint16(SE_SELF_RELATIVE|SE_DACL_PRESENT))
				return b
			}(),
			flags:   OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sdRelativeIsValid(tt.sd, tt.flags)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// =============================================================================
// GetControl / SetControl — used by PutSDDL in the write path to read and
// modify inheritance/protection control bits before calling SetSecurityObject.
// =============================================================================

func TestGetControl(t *testing.T) {
	tests := []struct {
		name    string
		sd      []byte
		expect  SECURITY_DESCRIPTOR_CONTROL
		wantErr bool
	}{
		{
			name: "Valid control",
			sd: func() []byte {
				b := make([]byte, 20)
				binary.LittleEndian.PutUint16(b[2:4], uint16(SE_SELF_RELATIVE|SE_DACL_PRESENT))
				return b
			}(),
			expect: SECURITY_DESCRIPTOR_CONTROL(SE_SELF_RELATIVE | SE_DACL_PRESENT),
		},
		{
			name:    "Too short",
			sd:      []byte{1, 2},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl, err := GetControl(tt.sd)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expect, ctrl)
			}
		})
	}
}

func TestSetControl(t *testing.T) {
	sd := make([]byte, 20)
	binary.LittleEndian.PutUint16(sd[2:4], uint16(SE_SELF_RELATIVE))

	err := SetControl(sd, SECURITY_DESCRIPTOR_CONTROL(SE_DACL_PRESENT), SECURITY_DESCRIPTOR_CONTROL(SE_DACL_PRESENT))
	require.NoError(t, err)

	ctrl, err := GetControl(sd)
	require.NoError(t, err)
	assert.Equal(t, SECURITY_DESCRIPTOR_CONTROL(SE_SELF_RELATIVE|SE_DACL_PRESENT), ctrl)
}

func TestSetControlInheritanceBits(t *testing.T) {
	// Mirrors the PutSDDL logic: set SE_DACL_AUTO_INHERITED and SE_DACL_AUTO_INHERIT_REQ.
	sd := make([]byte, 20)
	binary.LittleEndian.PutUint16(sd[2:4], uint16(SE_SELF_RELATIVE|SE_DACL_PRESENT))

	bits := SECURITY_DESCRIPTOR_CONTROL(SE_DACL_AUTO_INHERITED | SE_DACL_AUTO_INHERIT_REQ)
	err := SetControl(sd, bits, bits)
	require.NoError(t, err)

	ctrl, err := GetControl(sd)
	require.NoError(t, err)
	assert.NotEqual(t, SECURITY_DESCRIPTOR_CONTROL(0), ctrl&SECURITY_DESCRIPTOR_CONTROL(SE_DACL_AUTO_INHERITED))
	assert.NotEqual(t, SECURITY_DESCRIPTOR_CONTROL(0), ctrl&SECURITY_DESCRIPTOR_CONTROL(SE_DACL_AUTO_INHERIT_REQ))
}

func TestSetControlProtectedBit(t *testing.T) {
	// Mirrors the PutSDDL logic: set SE_DACL_PROTECTED for root folders.
	sd := make([]byte, 20)
	binary.LittleEndian.PutUint16(sd[2:4], uint16(SE_SELF_RELATIVE|SE_DACL_PRESENT))

	bits := SECURITY_DESCRIPTOR_CONTROL(SE_DACL_PROTECTED)
	err := SetControl(sd, bits, bits)
	require.NoError(t, err)

	ctrl, err := GetControl(sd)
	require.NoError(t, err)
	assert.NotEqual(t, SECURITY_DESCRIPTOR_CONTROL(0), ctrl&SECURITY_DESCRIPTOR_CONTROL(SE_DACL_PROTECTED))
}

// =============================================================================
// SecurityDescriptorToString / SecurityDescriptorFromString — the top-level
// exported functions used by GetSDDL (read) and PutSDDL (write) respectively.
// =============================================================================

func TestSecurityDescriptorRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		sddl  string
		check func(t *testing.T, result string)
	}{
		{
			name: "Owner and Group with no ACL",
			sddl: "O:S-1-5-18G:S-1-5-32-544D:NO_ACCESS_CONTROL",
			check: func(t *testing.T, result string) {
				assert.Contains(t, result, "O:S-1-5-18")
				assert.Contains(t, result, "G:S-1-5-32-544")
				assert.Contains(t, result, "D:")
			},
		},
		{
			name: "Simple allow ACE",
			sddl: "O:S-1-5-18G:S-1-5-18D:(A;;GA;;;S-1-1-0)",
			check: func(t *testing.T, result string) {
				assert.Contains(t, result, "O:S-1-5-18")
				assert.Contains(t, result, "G:S-1-5-18")
				assert.Contains(t, result, "(A;;GA;;;S-1-1-0)")
			},
		},
		{
			name: "Allow and deny ACEs",
			sddl: "O:S-1-5-18G:S-1-5-18D:(A;;GA;;;S-1-1-0)(D;;GA;;;S-1-5-7)",
			check: func(t *testing.T, result string) {
				assert.Contains(t, result, "(A;;GA;;;S-1-1-0)")
				assert.Contains(t, result, "(D;;GA;;;S-1-5-7)")
			},
		},
		{
			name: "ACE with inheritance flags",
			sddl: "O:S-1-5-18G:S-1-5-18D:(A;OICI;GA;;;S-1-1-0)",
			check: func(t *testing.T, result string) {
				assert.Contains(t, result, "(A;CIOI;GA;;;S-1-1-0)")
			},
		},
		{
			name: "DACL protected flag",
			sddl: "O:S-1-5-18G:S-1-5-18D:P(A;;GA;;;S-1-1-0)",
			check: func(t *testing.T, result string) {
				assert.Contains(t, result, "D:P")
				assert.Contains(t, result, "(A;;GA;;;S-1-1-0)")
			},
		},
		{
			name: "DACL auto-inherited flag",
			sddl: "O:S-1-5-18G:S-1-5-18D:AI(A;;GA;;;S-1-1-0)",
			check: func(t *testing.T, result string) {
				assert.Contains(t, result, "D:AI")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sd, err := SecurityDescriptorFromString(tt.sddl)
			require.NoError(t, err)

			result, err := SecurityDescriptorToString(sd)
			require.NoError(t, err)
			tt.check(t, result)
		})
	}
}

func TestSecurityDescriptorFromStringErrors(t *testing.T) {
	_, err := SecurityDescriptorFromString("X:INVALID")
	assert.Error(t, err)
}

// =============================================================================
// Real-world SDDL tests — exercises the full read/write path with production
// data including long domain SIDs, inherited ACEs, and callback ACEs.
// =============================================================================

func TestParseRealWorldSDDL(t *testing.T) {
	parsed, err := ParseSDDL(realWorldSDDL)
	require.NoError(t, err)

	assert.Equal(t, "S-1-5-21-2127521184-1604012920-1887927527-5560896", parsed.OwnerSID)
	assert.Equal(t, "DU", parsed.GroupSID)
	assert.Equal(t, "AI", parsed.DACL.Flags)
	assert.Len(t, parsed.DACL.ACLEntries, 5)

	// XA (callback) ACE with conditional expression
	assert.Equal(t, "XA", parsed.DACL.ACLEntries[0].Sections[0])
	assert.Equal(t, "0x1200a9", parsed.DACL.ACLEntries[0].Sections[2])
	assert.Equal(t, "AU", parsed.DACL.ACLEntries[0].Sections[5])

	// Standard inherited ACEs
	assert.Equal(t, "A", parsed.DACL.ACLEntries[1].Sections[0])
	assert.Equal(t, "ID", parsed.DACL.ACLEntries[1].Sections[1])
	assert.Equal(t, "FA", parsed.DACL.ACLEntries[1].Sections[2])
	assert.Equal(t, "BA", parsed.DACL.ACLEntries[1].Sections[5])

	assert.Equal(t, "SY", parsed.DACL.ACLEntries[2].Sections[5])
	assert.Equal(t, "S-1-5-21-2127521184-1604012920-1887927527-5560896", parsed.DACL.ACLEntries[3].Sections[5])
	assert.Equal(t, "BU", parsed.DACL.ACLEntries[4].Sections[5])
}

func TestRealWorldSupportedACEsRoundTrip(t *testing.T) {
	// Use only the supported ACEs (type A) from the real-world SDDL for binary round-trip.
	supportedSDDL := "O:S-1-5-21-2127521184-1604012920-1887927527-5560896G:S-1-5-21-2127521184-1604012920-1887927527-5560896D:AI(A;ID;FA;;;BA)(A;ID;FA;;;SY)(A;ID;FA;;;S-1-5-21-2127521184-1604012920-1887927527-5560896)(A;ID;0x1200a9;;;BU)"

	sd, err := SecurityDescriptorFromString(supportedSDDL)
	require.NoError(t, err)

	result, err := SecurityDescriptorToString(sd)
	require.NoError(t, err)

	assert.Contains(t, result, "O:S-1-5-21-2127521184-1604012920-1887927527-5560896")
	assert.Contains(t, result, "D:AI")

	// FA (FILE_ALL_ACCESS=0x1f01ff) round-trips as hex since aceRightsToStringMap
	// doesn't contain it. Well-known SIDs canonicalize to numeric form.
	assert.Contains(t, result, "(A;ID;0x1f01ff;;;S-1-5-32-544)")
	assert.Contains(t, result, "(A;ID;0x1f01ff;;;S-1-5-18)")
	assert.Contains(t, result, "(A;ID;0x1f01ff;;;S-1-5-21-2127521184-1604012920-1887927527-5560896)")
	assert.Contains(t, result, "(A;ID;0x1200a9;;;S-1-5-32-545)")
}

func TestRealWorldCallbackACERejected(t *testing.T) {
	// XA (callback) ACE has a conditional expression producing 7 sections;
	// aclEntryToSlice expects 6, so SecurityDescriptorFromString should error.
	xaSDDL := `O:S-1-5-21-2127521184-1604012920-1887927527-5560896G:S-1-5-18D:AI(XA;;0x1200a9;;;AU;(Member_of{SID(S-1-5-21-72051607-1745760036-109187956-363937)}))(A;ID;FA;;;BA)`
	_, err := SecurityDescriptorFromString(xaSDDL)
	assert.Error(t, err)
}

func TestRealWorldSidRoundTrips(t *testing.T) {
	sids := []string{
		"S-1-5-21-2127521184-1604012920-1887927527-5560896",
		"S-1-5-21-72051607-1745760036-109187956-363937",
	}

	for _, sid := range sids {
		t.Run(sid, func(t *testing.T) {
			bin, err := stringToSid(sid)
			require.NoError(t, err)

			result, err := sidToString(bin)
			require.NoError(t, err)
			assert.Equal(t, sid, result)
		})
	}
}
