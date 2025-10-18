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
	"fmt"
	"strconv"
	"unsafe"

	"github.com/Azure/azure-storage-azcopy/v10/common"

	"github.com/pkg/xattr"
)

/*
 * Following constants are used by various Windows functions that deal with SECURITY_DESCRIPTORs and SIDs.
 * Most of these constants are originally defined in winnt.h
 */

/*
 * Valid/supported revision numbers for various object types.
 *
 * TODO: Do we need to support ACL_REVISION_DS (4) with support for Object ACEs?
 *       Are they used for filesystem objects?
 */
const (
	SDDL_REVISION   = 1 // SDDL Revision MUST always be 1.
	SID_REVISION    = 1 // SID Revision MUST always be 1.
	ACL_REVISION    = 2 // ACL revision for support basic ACE type used for filesystem ACLs.
	ACL_REVISION_DS = 4 // ACL revision for supporting stuff like Object ACE. This should ideally not be used with the ACE
	// types we support, but I've seen some objects like that.
)

type SECURITY_INFORMATION uint32

// Valid bitmasks contained in type SECURITY_INFORMATION.
const (
	OWNER_SECURITY_INFORMATION            = 0x00000001
	GROUP_SECURITY_INFORMATION            = 0x00000002
	DACL_SECURITY_INFORMATION             = 0x00000004
	SACL_SECURITY_INFORMATION             = 0x00000008
	LABEL_SECURITY_INFORMATION            = 0x00000010
	ATTRIBUTE_SECURITY_INFORMATION        = 0x00000020
	SCOPE_SECURITY_INFORMATION            = 0x00000040
	BACKUP_SECURITY_INFORMATION           = 0x00010000
	PROTECTED_DACL_SECURITY_INFORMATION   = 0x80000000
	PROTECTED_SACL_SECURITY_INFORMATION   = 0x40000000
	UNPROTECTED_DACL_SECURITY_INFORMATION = 0x20000000
	UNPROTECTED_SACL_SECURITY_INFORMATION = 0x10000000
)

// Valid bitmasks contained in type SECURITY_DESCRIPTOR_CONTROL.
const (
	SE_OWNER_DEFAULTED       = 0x0001
	SE_GROUP_DEFAULTED       = 0x0002
	SE_DACL_PRESENT          = 0x0004
	SE_DACL_DEFAULTED        = 0x0008
	SE_SACL_PRESENT          = 0x0010
	SE_SACL_DEFAULTED        = 0x0020
	SE_DACL_AUTO_INHERIT_REQ = 0x0100
	SE_SACL_AUTO_INHERIT_REQ = 0x0200
	SE_DACL_AUTO_INHERITED   = 0x0400
	SE_SACL_AUTO_INHERITED   = 0x0800
	SE_DACL_PROTECTED        = 0x1000
	SE_SACL_PROTECTED        = 0x2000
	SE_RM_CONTROL_VALID      = 0x4000
	SE_SELF_RELATIVE         = 0x8000
)

// Valid AceType values present in ACE_HEADER.
const (
	ACCESS_MIN_MS_ACE_TYPE                  = 0x0
	ACCESS_ALLOWED_ACE_TYPE                 = 0x0
	ACCESS_DENIED_ACE_TYPE                  = 0x1
	SYSTEM_AUDIT_ACE_TYPE                   = 0x2
	SYSTEM_ALARM_ACE_TYPE                   = 0x3
	ACCESS_MAX_MS_V2_ACE_TYPE               = 0x3
	ACCESS_ALLOWED_COMPOUND_ACE_TYPE        = 0x4
	ACCESS_MAX_MS_V3_ACE_TYPE               = 0x4
	ACCESS_MIN_MS_OBJECT_ACE_TYPE           = 0x5
	ACCESS_ALLOWED_OBJECT_ACE_TYPE          = 0x5
	ACCESS_DENIED_OBJECT_ACE_TYPE           = 0x6
	SYSTEM_AUDIT_OBJECT_ACE_TYPE            = 0x7
	SYSTEM_ALARM_OBJECT_ACE_TYPE            = 0x8
	ACCESS_MAX_MS_OBJECT_ACE_TYPE           = 0x8
	ACCESS_MAX_MS_V4_ACE_TYPE               = 0x8
	ACCESS_MAX_MS_ACE_TYPE                  = 0x8
	ACCESS_ALLOWED_CALLBACK_ACE_TYPE        = 0x9
	ACCESS_DENIED_CALLBACK_ACE_TYPE         = 0xA
	ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE = 0xB
	ACCESS_DENIED_CALLBACK_OBJECT_ACE_TYPE  = 0xC
	SYSTEM_AUDIT_CALLBACK_ACE_TYPE          = 0xD
	SYSTEM_ALARM_CALLBACK_ACE_TYPE          = 0xE
	SYSTEM_AUDIT_CALLBACK_OBJECT_ACE_TYPE   = 0xF
	SYSTEM_ALARM_CALLBACK_OBJECT_ACE_TYPE   = 0x10
	SYSTEM_MANDATORY_LABEL_ACE_TYPE         = 0x11
	SYSTEM_RESOURCE_ATTRIBUTE_ACE_TYPE      = 0x12
	SYSTEM_SCOPED_POLICY_ID_ACE_TYPE        = 0x13
	SYSTEM_PROCESS_TRUST_LABEL_ACE_TYPE     = 0x14
	SYSTEM_ACCESS_FILTER_ACE_TYPE           = 0x15
	ACCESS_MAX_MS_V5_ACE_TYPE               = 0x15
)

var aceTypeStringMap = map[string]BYTE{
	"A":  ACCESS_ALLOWED_ACE_TYPE,
	"D":  ACCESS_DENIED_ACE_TYPE,
	"OA": ACCESS_ALLOWED_OBJECT_ACE_TYPE,
	"OD": ACCESS_DENIED_OBJECT_ACE_TYPE,
	"AU": SYSTEM_AUDIT_ACE_TYPE,
	"AL": SYSTEM_ALARM_ACE_TYPE,
	"OU": SYSTEM_AUDIT_OBJECT_ACE_TYPE,
	"OL": SYSTEM_ALARM_OBJECT_ACE_TYPE,
	"ML": SYSTEM_MANDATORY_LABEL_ACE_TYPE,
	"XA": ACCESS_ALLOWED_CALLBACK_ACE_TYPE,
	"XD": ACCESS_DENIED_CALLBACK_ACE_TYPE,
	"RA": SYSTEM_RESOURCE_ATTRIBUTE_ACE_TYPE,
	"SP": SYSTEM_SCOPED_POLICY_ID_ACE_TYPE,
	"XU": SYSTEM_AUDIT_CALLBACK_OBJECT_ACE_TYPE,
	"ZA": ACCESS_ALLOWED_CALLBACK_ACE_TYPE,
	"TL": SYSTEM_PROCESS_TRUST_LABEL_ACE_TYPE,
	"FL": SYSTEM_ACCESS_FILTER_ACE_TYPE,
}

// Valid bitmasks contained in AceFlags present in ACE_HEADER.
const (
	OBJECT_INHERIT_ACE       = 0x01
	CONTAINER_INHERIT_ACE    = 0x02
	NO_PROPAGATE_INHERIT_ACE = 0x04
	INHERIT_ONLY_ACE         = 0x08
	INHERITED_ACE            = 0x10
	VALID_INHERIT_FLAGS      = 0x1F
	CRITICAL_ACE_FLAG        = 0x20

	// AceFlags mask for what events we (should) audit. Used by SACL.
	SUCCESSFUL_ACCESS_ACE_FLAG = 0x40
	FAILED_ACCESS_ACE_FLAG     = 0x80

	TRUST_PROTECTED_FILTER_ACE_FLAG = 0x40
)

// Valid bitmasks contained in AccessMask present in type ACCESS_ALLOWED_ACE.
const (
	// Generic access rights.
	GENERIC_READ             = 0x80000000
	GENERIC_WRITE            = 0x40000000
	GENERIC_EXECUTE          = 0x20000000
	GENERIC_ALL              = 0x10000000
	DELETE                   = 0x00010000
	READ_CONTROL             = 0x00020000
	WRITE_DAC                = 0x00040000
	WRITE_OWNER              = 0x00080000
	SYNCHRONIZE              = 0x00100000
	STANDARD_RIGHTS_REQUIRED = 0x000F0000
	STANDARD_RIGHTS_READ     = READ_CONTROL
	STANDARD_RIGHTS_WRITE    = READ_CONTROL
	STANDARD_RIGHTS_EXECUTE  = READ_CONTROL
	STANDARD_RIGHTS_ALL      = 0x001F0000
	SPECIFIC_RIGHTS_ALL      = 0x0000FFFF

	// Access rights for files and directories.
	FILE_READ_DATA        = 0x0001 /* file & pipe */
	FILE_READ_ATTRIBUTES  = 0x0080 /* all */
	FILE_READ_EA          = 0x0008 /* file & directory */
	FILE_WRITE_DATA       = 0x0002 /* file & pipe */
	FILE_WRITE_ATTRIBUTES = 0x0100 /* all */
	FILE_WRITE_EA         = 0x0010 /* file & directory */
	FILE_APPEND_DATA      = 0x0004 /* file */
	FILE_EXECUTE          = 0x0020 /* file */

	FILE_ALL_ACCESS      = (STANDARD_RIGHTS_REQUIRED | SYNCHRONIZE | 0x1FF)
	FILE_GENERIC_READ    = (STANDARD_RIGHTS_READ | FILE_READ_DATA | FILE_READ_ATTRIBUTES | FILE_READ_EA | SYNCHRONIZE)
	FILE_GENERIC_WRITE   = (STANDARD_RIGHTS_WRITE | FILE_WRITE_DATA | FILE_WRITE_ATTRIBUTES | FILE_WRITE_EA | FILE_APPEND_DATA | SYNCHRONIZE)
	FILE_GENERIC_EXECUTE = (STANDARD_RIGHTS_EXECUTE | FILE_READ_ATTRIBUTES | FILE_EXECUTE | SYNCHRONIZE)

	// Access rights for DS objects.
	ADS_RIGHT_DS_CREATE_CHILD   = 0x0001
	ADS_RIGHT_DS_DELETE_CHILD   = 0x0002
	ADS_RIGHT_ACTRL_DS_LIST     = 0x0004
	ADS_RIGHT_DS_SELF           = 0x0008
	ADS_RIGHT_DS_READ_PROP      = 0x0010
	ADS_RIGHT_DS_WRITE_PROP     = 0x0020
	ADS_RIGHT_DS_DELETE_TREE    = 0x0040
	ADS_RIGHT_DS_LIST_OBJECT    = 0x0080
	ADS_RIGHT_DS_CONTROL_ACCESS = 0x0100

	// Registry Specific Access Rights.
	KEY_QUERY_VALUE        = 0x0001
	KEY_SET_VALUE          = 0x0002
	KEY_CREATE_SUB_KEY     = 0x0004
	KEY_ENUMERATE_SUB_KEYS = 0x0008
	KEY_NOTIFY             = 0x0010
	KEY_CREATE_LINK        = 0x0020
	KEY_WOW64_32KEY        = 0x0200
	KEY_WOW64_64KEY        = 0x0100
	KEY_WOW64_RES          = 0x0300

	KEY_READ       = ((STANDARD_RIGHTS_READ | KEY_QUERY_VALUE | KEY_ENUMERATE_SUB_KEYS | KEY_NOTIFY) & (^SYNCHRONIZE))
	KEY_WRITE      = ((STANDARD_RIGHTS_WRITE | KEY_SET_VALUE | KEY_CREATE_SUB_KEY) & (^SYNCHRONIZE))
	KEY_EXECUTE    = ((KEY_READ) & (^SYNCHRONIZE))
	KEY_ALL_ACCESS = ((STANDARD_RIGHTS_ALL | KEY_QUERY_VALUE | KEY_SET_VALUE | KEY_CREATE_SUB_KEY | KEY_ENUMERATE_SUB_KEYS | KEY_NOTIFY | KEY_CREATE_LINK) & (^SYNCHRONIZE))

	// SYSTEM_ACCESS_FILTER_ACE Access rights.
	SYSTEM_MANDATORY_LABEL_NO_WRITE_UP   = 0x1
	SYSTEM_MANDATORY_LABEL_NO_READ_UP    = 0x2
	SYSTEM_MANDATORY_LABEL_NO_EXECUTE_UP = 0x4
)

// Access mask exactly matching the value here will be mapped to the key.
var aceStringToRightsMap = map[string]uint32{
	"GA": GENERIC_ALL,
	"GR": GENERIC_READ,
	"GW": GENERIC_WRITE,
	"GX": GENERIC_EXECUTE,

	"RC": READ_CONTROL,
	"SD": DELETE,
	"WD": WRITE_DAC,
	"WO": WRITE_OWNER,

	"RP": ADS_RIGHT_DS_READ_PROP,
	"WP": ADS_RIGHT_DS_WRITE_PROP,
	"CC": ADS_RIGHT_DS_CREATE_CHILD,
	"DC": ADS_RIGHT_DS_DELETE_CHILD,
	"LC": ADS_RIGHT_ACTRL_DS_LIST,
	"SW": ADS_RIGHT_DS_SELF,
	"LO": ADS_RIGHT_DS_LIST_OBJECT,
	"DT": ADS_RIGHT_DS_DELETE_TREE,
	"CR": ADS_RIGHT_DS_CONTROL_ACCESS,

	"FA": FILE_ALL_ACCESS,
	"FR": FILE_GENERIC_READ,
	"FW": FILE_GENERIC_WRITE,
	"FX": FILE_GENERIC_EXECUTE,

	"KA": KEY_ALL_ACCESS,
	"KR": KEY_READ,
	"KW": KEY_WRITE,
	"KX": KEY_EXECUTE,

	"NR": SYSTEM_MANDATORY_LABEL_NO_READ_UP,
	"NW": SYSTEM_MANDATORY_LABEL_NO_WRITE_UP,
	"NX": SYSTEM_MANDATORY_LABEL_NO_EXECUTE_UP,
}

// Access rights to their corresponding friendly names.
// Note that this intentionally has some of the fields left out from aceStringToRightsMap.
var aceRightsToStringMap = map[uint32]string{
	GENERIC_ALL:                 "GA",
	GENERIC_READ:                "GR",
	GENERIC_WRITE:               "GW",
	GENERIC_EXECUTE:             "GX",
	READ_CONTROL:                "RC",
	DELETE:                      "SD",
	WRITE_DAC:                   "WD",
	WRITE_OWNER:                 "WO",
	ADS_RIGHT_DS_READ_PROP:      "RP",
	ADS_RIGHT_DS_WRITE_PROP:     "WP",
	ADS_RIGHT_DS_CREATE_CHILD:   "CC",
	ADS_RIGHT_DS_DELETE_CHILD:   "DC",
	ADS_RIGHT_ACTRL_DS_LIST:     "LC",
	ADS_RIGHT_DS_SELF:           "SW",
	ADS_RIGHT_DS_LIST_OBJECT:    "LO",
	ADS_RIGHT_DS_DELETE_TREE:    "DT",
	ADS_RIGHT_DS_CONTROL_ACCESS: "CR",
}

var (
	SECURITY_NULL_SID_AUTHORITY         = [6]byte{0, 0, 0, 0, 0, 0}
	SECURITY_WORLD_SID_AUTHORITY        = [6]byte{0, 0, 0, 0, 0, 1}
	SECURITY_LOCAL_SID_AUTHORITY        = [6]byte{0, 0, 0, 0, 0, 2}
	SECURITY_CREATOR_SID_AUTHORITY      = [6]byte{0, 0, 0, 0, 0, 3}
	SECURITY_NON_UNIQUE_AUTHORITY       = [6]byte{0, 0, 0, 0, 0, 4}
	SECURITY_NT_AUTHORITY               = [6]byte{0, 0, 0, 0, 0, 5}
	SECURITY_APP_PACKAGE_AUTHORITY      = [6]byte{0, 0, 0, 0, 0, 15}
	SECURITY_MANDATORY_LABEL_AUTHORITY  = [6]byte{0, 0, 0, 0, 0, 16}
	SECURITY_SCOPED_POLICY_ID_AUTHORITY = [6]byte{0, 0, 0, 0, 0, 17}
	SECURITY_AUTHENTICATION_AUTHORITY   = [6]byte{0, 0, 0, 0, 0, 18}
)

const (
	SECURITY_NULL_RID                   = 0
	SECURITY_WORLD_RID                  = 0
	SECURITY_LOCAL_RID                  = 0
	SECURITY_CREATOR_OWNER_RID          = 0
	SECURITY_CREATOR_GROUP_RID          = 1
	SECURITY_DIALUP_RID                 = 1
	SECURITY_NETWORK_RID                = 2
	SECURITY_BATCH_RID                  = 3
	SECURITY_INTERACTIVE_RID            = 4
	SECURITY_LOGON_IDS_RID              = 5
	SECURITY_SERVICE_RID                = 6
	SECURITY_LOCAL_SYSTEM_RID           = 18
	SECURITY_BUILTIN_DOMAIN_RID         = 32
	SECURITY_PRINCIPAL_SELF_RID         = 10
	SECURITY_CREATOR_OWNER_SERVER_RID   = 0x2
	SECURITY_CREATOR_GROUP_SERVER_RID   = 0x3
	SECURITY_LOGON_IDS_RID_COUNT        = 0x3
	SECURITY_ANONYMOUS_LOGON_RID        = 0x7
	SECURITY_PROXY_RID                  = 0x8
	SECURITY_ENTERPRISE_CONTROLLERS_RID = 0x9
	SECURITY_SERVER_LOGON_RID           = SECURITY_ENTERPRISE_CONTROLLERS_RID
	SECURITY_AUTHENTICATED_USER_RID     = 0xb
	SECURITY_RESTRICTED_CODE_RID        = 0xc
	SECURITY_NT_NON_UNIQUE_RID          = 0x15

	SECURITY_CREATOR_OWNER_RIGHTS_RID  = 0x00000004
	SECURITY_LOCAL_SERVICE_RID         = 0x00000013
	SECURITY_NETWORK_SERVICE_RID       = 0x00000014
	SECURITY_WRITE_RESTRICTED_CODE_RID = 0x00000021

	SECURITY_MANDATORY_LOW_RID         = 0x00001000
	SECURITY_MANDATORY_MEDIUM_RID      = 0x00002000
	SECURITY_MANDATORY_MEDIUM_PLUS_RID = (SECURITY_MANDATORY_MEDIUM_RID + 0x100)
	SECURITY_MANDATORY_HIGH_RID        = 0x00003000
	SECURITY_MANDATORY_SYSTEM_RID      = 0x00004000

	SECURITY_APP_PACKAGE_BASE_RID        = 0x00000002
	SECURITY_BUILTIN_PACKAGE_ANY_PACKAGE = 0x00000001
)

// Predefined domain-relative RIDs for local groups.
// See https://msdn.microsoft.com/en-us/library/windows/desktop/aa379649(v=vs.85).aspx
const (
	DOMAIN_ALIAS_RID_ADMINS                         = 0x220
	DOMAIN_ALIAS_RID_USERS                          = 0x221
	DOMAIN_ALIAS_RID_GUESTS                         = 0x222
	DOMAIN_ALIAS_RID_POWER_USERS                    = 0x223
	DOMAIN_ALIAS_RID_ACCOUNT_OPS                    = 0x224
	DOMAIN_ALIAS_RID_SYSTEM_OPS                     = 0x225
	DOMAIN_ALIAS_RID_PRINT_OPS                      = 0x226
	DOMAIN_ALIAS_RID_BACKUP_OPS                     = 0x227
	DOMAIN_ALIAS_RID_REPLICATOR                     = 0x228
	DOMAIN_ALIAS_RID_RAS_SERVERS                    = 0x229
	DOMAIN_ALIAS_RID_PREW2KCOMPACCESS               = 0x22A
	DOMAIN_ALIAS_RID_REMOTE_DESKTOP_USERS           = 0x22B
	DOMAIN_ALIAS_RID_NETWORK_CONFIGURATION_OPS      = 0x22C
	DOMAIN_ALIAS_RID_INCOMING_FOREST_TRUST_BUILDERS = 0x22D
	DOMAIN_ALIAS_RID_MONITORING_USERS               = 0x22E
	DOMAIN_ALIAS_RID_LOGGING_USERS                  = 0x22F
	DOMAIN_ALIAS_RID_AUTHORIZATIONACCESS            = 0x230
	DOMAIN_ALIAS_RID_TS_LICENSE_SERVERS             = 0x231
	DOMAIN_ALIAS_RID_DCOM_USERS                     = 0x232
	DOMAIN_ALIAS_RID_IUSERS                         = 0x238
	DOMAIN_ALIAS_RID_CRYPTO_OPERATORS               = 0x239
	DOMAIN_ALIAS_RID_CACHEABLE_PRINCIPALS_GROUP     = 0x23B
	DOMAIN_ALIAS_RID_NON_CACHEABLE_PRINCIPALS_GROUP = 0x23C
	DOMAIN_ALIAS_RID_EVENT_LOG_READERS_GROUP        = 0x23D
	DOMAIN_ALIAS_RID_CERTSVC_DCOM_ACCESS_GROUP      = 0x23E
	DOMAIN_ALIAS_RID_RDS_REMOTE_ACCESS_SERVERS      = 0x23F
	DOMAIN_ALIAS_RID_RDS_ENDPOINT_SERVERS           = 0x240
	DOMAIN_ALIAS_RID_RDS_MANAGEMENT_SERVERS         = 0x241
	DOMAIN_ALIAS_RID_HYPER_V_ADMINS                 = 0x242
	DOMAIN_ALIAS_RID_ACCESS_CONTROL_ASSISTANCE_OPS  = 0x243
	DOMAIN_ALIAS_RID_REMOTE_MANAGEMENT_USERS        = 0x244
	DOMAIN_ALIAS_RID_DEFAULT_ACCOUNT                = 0x245
	DOMAIN_ALIAS_RID_STORAGE_REPLICA_ADMINS         = 0x246
	DOMAIN_ALIAS_RID_DEVICE_OWNERS                  = 0x247
)

const (
	DOMAIN_GROUP_RID_ENTERPRISE_READONLY_DOMAIN_CONTROLLERS = 0x1F2 // 498
	DOMAIN_USER_RID_ADMIN                                   = 0x1F4 // 500
	DOMAIN_USER_RID_GUEST                                   = 0x1F5
	DOMAIN_GROUP_RID_ADMINS                                 = 0x200 // 512
	DOMAIN_GROUP_RID_USERS                                  = 0x201
	DOMAIN_GROUP_RID_GUESTS                                 = 0x202
	DOMAIN_GROUP_RID_COMPUTERS                              = 0x203
	DOMAIN_GROUP_RID_CONTROLLERS                            = 0x204
	DOMAIN_GROUP_RID_CERT_ADMINS                            = 0x205
	DOMAIN_GROUP_RID_SCHEMA_ADMINS                          = 0x206
	DOMAIN_GROUP_RID_ENTERPRISE_ADMINS                      = 0x207
	DOMAIN_GROUP_RID_POLICY_ADMINS                          = 0x208
	DOMAIN_GROUP_RID_READONLY_CONTROLLERS                   = 0x209
	DOMAIN_GROUP_RID_CLONEABLE_CONTROLLERS                  = 0x20A
	DOMAIN_GROUP_RID_CDC_RESERVED                           = 0x20C
	DOMAIN_GROUP_RID_PROTECTED_USERS                        = 0x20D
	DOMAIN_GROUP_RID_KEY_ADMINS                             = 0x20E
	DOMAIN_GROUP_RID_ENTERPRISE_KEY_ADMINS                  = 0x20F
)

const (
	SECURITY_AUTHENTICATION_AUTHORITY_ASSERTED_RID       = 0x1
	SECURITY_AUTHENTICATION_SERVICE_ASSERTED_RID         = 0x2
	SECURITY_AUTHENTICATION_FRESH_KEY_AUTH_RID           = 0x3
	SECURITY_AUTHENTICATION_KEY_TRUST_RID                = 0x4
	SECURITY_AUTHENTICATION_KEY_PROPERTY_MFA_RID         = 0x5
	SECURITY_AUTHENTICATION_KEY_PROPERTY_ATTESTATION_RID = 0x6
)

/*
 * Define some Windows type names for increased readability of various Windows structs we use here.
 */
type BYTE byte
type WORD uint16
type DWORD uint32

/****************************************************************************
 * Various binary structures used for conveying SMB objects.
 *
 * ALL MULTI-BYTE VALUES ARE IN LITTLE ENDIAN FORMAT.
 *
 * We don't use these structures in the code but they are there to help reader
 * understand the code.
 ****************************************************************************/

/*
 * This is NT Security Descriptor in "Self Relative" format.
 * This is returned when common.CIFS_XATTR_CIFS_NTSD xattr is queried for a file.
 * The Linux equivalent struct is "struct cifs_ntsd".
 */
type SECURITY_DESCRIPTOR_CONTROL WORD
type SECURITY_DESCRIPTOR_RELATIVE struct {
	// Revision number of this SECURITY_DESCRIPTOR. Must be 1.
	Revision BYTE
	// Zero byte.
	Sbz1 BYTE
	// Flag bits describing this SECURITY_DESCRIPTOR.
	Control SECURITY_DESCRIPTOR_CONTROL
	// Offset of owner sid. There's a SID structure at this offset.
	OffsetOwner DWORD
	// Offset of primary group sid. There's a SID structure at this offset.
	OffsetGroup DWORD
	// Offset of SACL. There's an ACL structure at this offset.
	OffsetSacl DWORD
	// Offset of DACL. There's an ACL structure at this offset.
	OffsetDacl DWORD
	// 0 or more bytes (depending on the various offsets above) follow this structure.
	Data [0]BYTE
}

// Maximum sub authority values present in a SID.
const SID_MAX_SUB_AUTHORITIES = 15

/*
 * SID structure.
 * The Linux equivalent struct is "struct cifs_sid".
 */
type SID struct {
	Revision BYTE
	// How many DWORD SubAuthority values? Cannot be 0, max possible value is SID_MAX_SUB_AUTHORITIES.
	SubAuthorityCount BYTE
	// IdentifierAuthority is in big endian format.
	IdentifierAuthority [6]BYTE
	// SubAuthorityCount SubAuthority DWORDs.
	SubAuthority [1]DWORD
}

/*
 * Header at the beginning of every ACE.
 */
type ACE_HEADER struct {
	AceType  BYTE
	AceFlags BYTE
	AceSize  WORD
}

/*
 * Single ACE (Access Check Entry).
 * One or more of these are contained in ACL.
 * The Linux equivalent struct is "struct cifs_ace".
 */
type ACCESS_ALLOWED_ACE struct {
	Header ACE_HEADER
	// What permissions is this ACE controlling?
	AccessMask DWORD
	// SID to which these permissions apply.
	Sid SID
}

/*
 * Binary ACL format. Used for both DACL and SACL.
 * The Linux equivalent struct is "struct cifs_acl".
 */
type ACL struct {
	AclRevision BYTE
	Sbz1        BYTE
	AclSize     WORD
	AceCount    WORD
	Sbz2        WORD
}

type AnySID struct {
	Revision            byte
	SubAuthorityCount   byte
	IdentifierAuthority [6]byte
	SubAuthority        []uint32
}

// TODO: Validate completeness/correctness.
var wellKnownSidShortcuts = map[string]AnySID{
	"WD": {SID_REVISION, 1, SECURITY_WORLD_SID_AUTHORITY, []uint32{SECURITY_NULL_RID}},

	"CO": {SID_REVISION, 1, SECURITY_CREATOR_SID_AUTHORITY, []uint32{SECURITY_CREATOR_OWNER_RID}},
	"CG": {SID_REVISION, 1, SECURITY_CREATOR_SID_AUTHORITY, []uint32{SECURITY_CREATOR_GROUP_RID}},
	"OW": {SID_REVISION, 1, SECURITY_CREATOR_SID_AUTHORITY, []uint32{SECURITY_CREATOR_OWNER_RIGHTS_RID}},

	"NU": {SID_REVISION, 1, SECURITY_NT_AUTHORITY, []uint32{SECURITY_NETWORK_RID}},
	"IU": {SID_REVISION, 1, SECURITY_NT_AUTHORITY, []uint32{SECURITY_INTERACTIVE_RID}},
	"SU": {SID_REVISION, 1, SECURITY_NT_AUTHORITY, []uint32{SECURITY_SERVICE_RID}},
	"AN": {SID_REVISION, 1, SECURITY_NT_AUTHORITY, []uint32{SECURITY_ANONYMOUS_LOGON_RID}},
	"ED": {SID_REVISION, 1, SECURITY_NT_AUTHORITY, []uint32{SECURITY_ENTERPRISE_CONTROLLERS_RID}},
	"PS": {SID_REVISION, 1, SECURITY_NT_AUTHORITY, []uint32{SECURITY_PRINCIPAL_SELF_RID}},
	"AU": {SID_REVISION, 1, SECURITY_NT_AUTHORITY, []uint32{SECURITY_AUTHENTICATED_USER_RID}},
	"RC": {SID_REVISION, 1, SECURITY_NT_AUTHORITY, []uint32{SECURITY_RESTRICTED_CODE_RID}},
	"SY": {SID_REVISION, 1, SECURITY_NT_AUTHORITY, []uint32{SECURITY_LOCAL_SYSTEM_RID}},
	"LS": {SID_REVISION, 1, SECURITY_NT_AUTHORITY, []uint32{SECURITY_LOCAL_SERVICE_RID}},
	"NS": {SID_REVISION, 1, SECURITY_NT_AUTHORITY, []uint32{SECURITY_NETWORK_SERVICE_RID}},
	"WR": {SID_REVISION, 1, SECURITY_NT_AUTHORITY, []uint32{SECURITY_WRITE_RESTRICTED_CODE_RID}},

	"BA": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_ADMINS}},
	"BU": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_USERS}},
	"BG": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_GUESTS}},
	"PU": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_POWER_USERS}},
	"AO": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_ACCOUNT_OPS}},
	"SO": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_SYSTEM_OPS}},
	"PO": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_PRINT_OPS}},
	"BO": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_BACKUP_OPS}},
	"RE": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_REPLICATOR}},
	"RU": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_PREW2KCOMPACCESS}},
	"RD": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_REMOTE_DESKTOP_USERS}},
	"NO": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_NETWORK_CONFIGURATION_OPS}},

	"MU": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_MONITORING_USERS}},
	"LU": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_LOGGING_USERS}},
	"IS": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_IUSERS}},
	"CY": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_CRYPTO_OPERATORS}},
	"ER": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_EVENT_LOG_READERS_GROUP}},
	"CD": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_CERTSVC_DCOM_ACCESS_GROUP}},
	"RA": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_RDS_REMOTE_ACCESS_SERVERS}},
	"ES": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_RDS_ENDPOINT_SERVERS}},
	"MS": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_RDS_MANAGEMENT_SERVERS}},
	"HA": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_HYPER_V_ADMINS}},
	"AA": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_ACCESS_CONTROL_ASSISTANCE_OPS}},
	"RM": {SID_REVISION, 2, SECURITY_NT_AUTHORITY, []uint32{SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_REMOTE_MANAGEMENT_USERS}},

	"LW": {SID_REVISION, 1, SECURITY_MANDATORY_LABEL_AUTHORITY, []uint32{SECURITY_MANDATORY_LOW_RID}},
	"ME": {SID_REVISION, 1, SECURITY_MANDATORY_LABEL_AUTHORITY, []uint32{SECURITY_MANDATORY_MEDIUM_RID}},
	"MP": {SID_REVISION, 1, SECURITY_MANDATORY_LABEL_AUTHORITY, []uint32{SECURITY_MANDATORY_MEDIUM_PLUS_RID}},
	"HI": {SID_REVISION, 1, SECURITY_MANDATORY_LABEL_AUTHORITY, []uint32{SECURITY_MANDATORY_HIGH_RID}},
	"SI": {SID_REVISION, 1, SECURITY_MANDATORY_LABEL_AUTHORITY, []uint32{SECURITY_MANDATORY_SYSTEM_RID}},
	"AC": {SID_REVISION, 2, SECURITY_APP_PACKAGE_AUTHORITY, []uint32{SECURITY_APP_PACKAGE_BASE_RID, SECURITY_BUILTIN_PACKAGE_ANY_PACKAGE}},

	"AS": {SID_REVISION, 1, SECURITY_AUTHENTICATION_AUTHORITY, []uint32{SECURITY_AUTHENTICATION_AUTHORITY_ASSERTED_RID}},
	"SS": {SID_REVISION, 1, SECURITY_AUTHENTICATION_AUTHORITY, []uint32{SECURITY_AUTHENTICATION_SERVICE_ASSERTED_RID}},
}

// TODO: Validate completeness/correctness.
var domainRidShortcuts = map[string]uint32{
	"RO": DOMAIN_GROUP_RID_ENTERPRISE_READONLY_DOMAIN_CONTROLLERS,
	"LA": DOMAIN_USER_RID_ADMIN,
	"LG": DOMAIN_USER_RID_GUEST,
	"DA": DOMAIN_GROUP_RID_ADMINS,
	"DU": DOMAIN_GROUP_RID_USERS,
	"DG": DOMAIN_GROUP_RID_GUESTS,
	"DC": DOMAIN_GROUP_RID_COMPUTERS,
	"DD": DOMAIN_GROUP_RID_CONTROLLERS,
	"CA": DOMAIN_GROUP_RID_CERT_ADMINS,
	"SA": DOMAIN_GROUP_RID_SCHEMA_ADMINS,
	"EA": DOMAIN_GROUP_RID_ENTERPRISE_ADMINS,
	"PA": DOMAIN_GROUP_RID_POLICY_ADMINS,
	"CN": DOMAIN_GROUP_RID_CLONEABLE_CONTROLLERS,
	"AP": DOMAIN_GROUP_RID_PROTECTED_USERS,
	"KA": DOMAIN_GROUP_RID_KEY_ADMINS,
	"EK": DOMAIN_GROUP_RID_ENTERPRISE_KEY_ADMINS,
	"RS": DOMAIN_ALIAS_RID_RAS_SERVERS,
}

/****************************************************************************/

// Test whether sd refers to a valid Security Descriptor.
// We do some basic validations of the SECURITY_DESCRIPTOR_RELATIVE header.
// 'flags' is used to convey what all information does the caller want us to verify in the binary SD.
func sdRelativeIsValid(sd []byte, flags SECURITY_INFORMATION) error {
	if len(sd) < int(unsafe.Sizeof(SECURITY_DESCRIPTOR_RELATIVE{})) {
		return fmt.Errorf("sd too small (%d bytes)", len(sd))
	}

	// Fetch various fields of the Security Descriptor.
	revision := sd[0]
	sbz1 := sd[1]
	control := binary.LittleEndian.Uint16(sd[2:4])
	offsetOwner := binary.LittleEndian.Uint32(sd[4:8])
	offsetGroup := binary.LittleEndian.Uint32(sd[8:12])
	offsetSacl := binary.LittleEndian.Uint32(sd[12:16])
	offsetDacl := binary.LittleEndian.Uint32(sd[16:20])

	// Now validate sanity of these fields.
	if revision != SDDL_REVISION {
		return fmt.Errorf("Invalid SD revision (%d), expected %d", revision, SDDL_REVISION)
	}

	if sbz1 != 0 {
		return fmt.Errorf("sbz1 must be 0, is %d", sbz1)
	}

	// SE_SELF_RELATIVE must be set.
	if (control & SE_SELF_RELATIVE) == 0 {
		return fmt.Errorf("SE_SELF_RELATIVE control bit must be set (control=0x%x)", control)
	}

	// Caller wants us to validate DACL information?
	if (flags & DACL_SECURITY_INFORMATION) != 0 {
		// SE_DACL_PRESENT bit MUST be *always* set.
		if (control & SE_DACL_PRESENT) == 0 {
			return fmt.Errorf("SE_DACL_PRESENT control bit must always be set (control=0x%x)", control)
		}

		// offsetDacl may be 0 which would mean "No ACLs" aka "allow all users".
		// If non-zero, OffsetDacl must point inside the relative Security Descriptor.
		if offsetDacl != 0 && offsetDacl+uint32(unsafe.Sizeof(ACL{})) > uint32(len(sd)) {
			return fmt.Errorf("DACL (offsetDacl=%d) must lie within sd (length=%d)", offsetDacl, len(sd))
		}
	}

	// Caller wants us to validate SACL information?
	if (flags & SACL_SECURITY_INFORMATION) != 0 {
		// SE_SACL_PRESENT bit is optional. If not set it means there is no SACL present.
		if (control&SE_SACL_PRESENT) != 0 && offsetSacl != 0 {
			// OffsetSacl must point inside the relative Security Descriptor.
			if offsetSacl+uint32(unsafe.Sizeof(ACL{})) > uint32(len(sd)) {
				return fmt.Errorf("SACL (offsetSacl=%d) must lie within sd (length=%d)", offsetSacl, len(sd))
			}
		}
	}

	// Caller wants us to validate OwnerSID?
	if (flags & OWNER_SECURITY_INFORMATION) != 0 {
		if offsetOwner == 0 {
			return fmt.Errorf("offsetOwner must not be 0")
		}

		// OffsetOwner must point inside the relative Security Descriptor.
		if offsetOwner+uint32(unsafe.Sizeof(SID{})) > uint32(len(sd)) {
			return fmt.Errorf("OwnerSID (offsetOwner=%d) must lie within sd (length=%d)",
				offsetOwner, len(sd))
		}
	}

	// Caller wants us to validate GroupSID?
	if (flags & GROUP_SECURITY_INFORMATION) != 0 {
		if offsetGroup == 0 {
			return fmt.Errorf("offsetGroup must not be 0")
		}

		// OffsetGroup must point inside the relative Security Descriptor.
		if offsetGroup+uint32(unsafe.Sizeof(SID{})) > uint32(len(sd)) {
			return fmt.Errorf("GroupSID (offsetGroup=%d) must lie within sd (length=%d)",
				offsetGroup, len(sd))
		}
	}

	return nil
}

// sidToString returns a stringified version of a binary SID object contained in sidSlice.
// The layout of the binary SID object is as per "SID struct".
func sidToString(sidSlice []byte) (string, error) {
	// Ensure we have enough bytes till SID.IdentifierAuthority.
	if len(sidSlice) < 8 {
		return "", fmt.Errorf("Invalid binary SID [size (%d) < 8]", len(sidSlice))
	}

	// SID.Revision.
	revision := sidSlice[:1][0]

	// SID.SubAuthorityCount.
	subAuthorityCount := sidSlice[1:2][0]

	// Ensure we have enough bytes for subAuthorityCount authority values, where each is a 4-byte DWORD
	// in little endian format.
	if len(sidSlice) < int(8+(4*subAuthorityCount)) {
		return "", fmt.Errorf("Invalid binary SID [subAuthorityCount=%d, size (%d) < %d]",
			subAuthorityCount, len(sidSlice), (8 + (4 * subAuthorityCount)))
	}

	// SID.IdentifierAuthority.
	// The 48-bit authority is laid out in big endian format.
	authorityHigh := uint64(binary.BigEndian.Uint16(sidSlice[2:4]))
	authorityLow := uint64(binary.BigEndian.Uint32(sidSlice[4:8]))
	authority := (authorityHigh<<32 | authorityLow)

	sidString := fmt.Sprintf("S-%d-%d", revision, authority)

	// Offset to start of SID.SubAuthority array.
	offset := 8

	// Parse and include all SubAuthority values in the SID string.
	for i := 0; i < int(subAuthorityCount); i++ {
		sidString += fmt.Sprintf("-%d", binary.LittleEndian.Uint32(sidSlice[offset:offset+4]))
		offset += 4
	}

	return sidString, nil
}

// Return the next token (after '-' till the next '-' or end of string) from 'sidString' and the remaining
// sidString after the token.
func getNextToken(sidString string) (string /* token */, string /* remaining sidString */) {
	token := ""
	charsProcessed := 0

	for _, c := range sidString {
		charsProcessed++
		if c == '-' {
			break
		}
		token += string(c)
	}

	return token, sidString[charsProcessed:]
}

// stringToSid converts the string sid into a byte slice in the form of "struct SID".
// The returned byte slice can be copied to fill the sid in a binary Security Descriptor in the form of
// struct SECURITY_DESCRIPTOR_RELATIVE.
func stringToSid(sidString string) ([]byte, error) {
	// Allocate a byte slice large enough to hold the binary SID.
	maxSidBytes := int(unsafe.Sizeof(SID{}) + (unsafe.Sizeof(uint32(0)) * SID_MAX_SUB_AUTHORITIES))
	sid := make([]byte, maxSidBytes)

	sidStringOriginal := sidString
	offset := 0

	if (sidString[0] == 'S' || sidString[0] == 's') && sidString[1] == '-' { /* S-R-I-S-S */
		// R-I-S-S.
		sidString = sidString[2:]
		var subAuthorityCount byte = 0

		token := ""
		tokenIdx := 0
		for sidString != "" {
			token, sidString = getNextToken(sidString)

			if tokenIdx == 0 {
				// SID.Revision.
				revision, err := strconv.ParseUint(token, 10, 8)
				if err != nil {
					return nil, fmt.Errorf("stringToSid: Error parsing Revision: %v", err)
				}
				if revision != SID_REVISION {
					return nil, fmt.Errorf("stringToSid: Invalid SID Revision %d", revision)
				}
				sid[0] = byte(revision)
				// Increment offset by 2 as we will fill SubAuthorityCount later.
				offset += 2
			} else if tokenIdx == 1 {
				// SID.IdentifierAuthority.
				authority, err := strconv.ParseUint(token, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("stringToSid: Error parsing IdentifierAuthority: %v", err)
				}
				authorityHigh := uint16(authority >> 32)
				authorityLow := uint32(authority & 0xFFFFFFFF)
				binary.BigEndian.PutUint16(sid[2:4], authorityHigh)
				binary.BigEndian.PutUint32(sid[4:8], authorityLow)
				offset += 6
			} else {
				// SID.SubAuthority[].
				subAuth, err := strconv.ParseUint(token, 10, 32)
				if err != nil {
					// If not numeric, maybe domain RID, but domain RID must be the last component.
					if rid, ok := domainRidShortcuts[token]; ok {
						if sidString != "" {
							return nil, fmt.Errorf("Domain RID (%s) seen but is not the last SubAuthority. SID=%s", token, sidStringOriginal)
						}
						subAuth = uint64(rid)
					} else {
						return nil, err
					}
				}
				binary.LittleEndian.PutUint32(sid[offset:offset+4], uint32(subAuth))
				offset += 4
				subAuthorityCount++
			}

			tokenIdx++
		}

		// Now we know SubAuthorityCount, fill it.
		sid[1] = subAuthorityCount

	} else {
		// String SID like "BA"?
		if wks, ok := wellKnownSidShortcuts[sidString]; ok {
			// SID.Revision.
			sid[0] = wks.Revision
			// SID.SubAuthorityCount.
			sid[1] = wks.SubAuthorityCount
			// SID.IdentifierAuthority.
			copy(sid[2:8], wks.IdentifierAuthority[:])

			offset = 8
			for i := 0; i < int(wks.SubAuthorityCount); i++ {
				// SID.SubAuthority[].
				binary.LittleEndian.PutUint32(sid[offset:offset+4], wks.SubAuthority[i])
				offset += 4
			}
		} else if rid, ok := domainRidShortcuts[sidString]; ok {
			// Domain RID like "DU"?
			// TODO: Add domain RID support. We need to prefix the domain SID.
			fmt.Printf("Got well known RID %d\n", rid)

			panic("Domain RIDs not yet implemented!")
		} else {
			return nil, fmt.Errorf("Invalid SID: %s", sidStringOriginal)
		}

	}

	return sid[:offset], nil
}

// Return a string representation of the 4-byte ACE rights.
func aceRightsToString(aceRights uint32) string {
	/*
	 * Check if the aceRights exactly maps to a shorthand name.
	 */
	if v, ok := aceRightsToStringMap[aceRights]; ok {
		return v
	}

	/*
	 * Check if the rights can be expressed as a concatenation of shorthand names.
	 * Only if we can map all the OR'ed rights to shorthand names, we use it.
	 */
	aceRightsString := ""
	var allRights uint32 = 0

	for k, v := range aceRightsToStringMap {
		if (aceRights & k) == k {
			aceRightsString += v
			allRights |= k
		}
	}

	// Use stringified rights only if *all* available rights can be represented with a shorthand name.
	// The else part is commented as it's being hit too often. One such common aceRights value is 0x1200a9.
	if allRights == aceRights {
		return aceRightsString
	}
	/*
		else if allRights != 0 {
			fmt.Printf("aceRightsString: Only partial rights could be stringified (aceRights=0x%x, allRights=0x%x)",
				aceRights, allRights)
		}
	*/

	// Fallback to integral mask value.
	return fmt.Sprintf("0x%x", aceRights)
}

// Does the aceType correspond to an object ACE?
// We don't support object ACEs.
//
//nolint:deadcode,unused
func isObjectAce(aceType byte) bool {
	switch aceType {
	case ACCESS_ALLOWED_OBJECT_ACE_TYPE,
		ACCESS_DENIED_OBJECT_ACE_TYPE,
		SYSTEM_AUDIT_OBJECT_ACE_TYPE,
		SYSTEM_ALARM_OBJECT_ACE_TYPE,
		ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE,
		ACCESS_DENIED_CALLBACK_OBJECT_ACE_TYPE,
		SYSTEM_AUDIT_CALLBACK_OBJECT_ACE_TYPE,
		SYSTEM_ALARM_CALLBACK_OBJECT_ACE_TYPE:
		return true

	default:
		return false
	}
}

// Returns true for aceTypes that we support.
// TODO: Allow SACL ACE type, conditional ACE Types.
func isUnsupportedAceType(aceType byte) bool {
	switch aceType {
	case ACCESS_ALLOWED_ACE_TYPE,
		ACCESS_DENIED_ACE_TYPE:
		return false
	default:
		return true
	}
}

// Convert numeric ace type to string.
func aceTypeToString(aceType BYTE) (string, error) {
	for k, v := range aceTypeStringMap {
		if v == aceType {
			return k, nil
		}
	}

	return "", fmt.Errorf("Unknown aceType: %d", aceType)
}

// aceToString returns a stringified version of a binary ACE object contained in aceSlice.
// The layout of the binary ACE object is as per "struct ACCESS_ALLOWED_ACE".
func aceToString(aceSlice []byte) (string, error) {
	// We access 8 bytes in this function, ensure we have at least 8 bytes.
	if len(aceSlice) < 8 {
		return "", fmt.Errorf("Short aceSlice: %d bytes", len(aceSlice))
	}

	aceString := "("

	// ACCESS_ALLOWED_ACE.Header.AceType.
	aceType := aceSlice[:1][0]

	// This is our gatekeeper for blocking unsupported ace types.
	// We open up ACEs as we add support for them.
	if isUnsupportedAceType(aceType) {
		return "", fmt.Errorf("Unsupported ACE type: 0x%x", aceType)
	}

	// ACCESS_ALLOWED_ACE.Header.AceFlags.
	aceFlags := aceSlice[1:2][0]
	// ACCESS_ALLOWED_ACE.AccessMask.
	aceRights := binary.LittleEndian.Uint32(aceSlice[4:8])

	aceTypeString, err := aceTypeToString(BYTE(aceType))
	if err != nil {
		return "", fmt.Errorf("aceToString: %v", err)
	}
	aceString += aceTypeString
	aceString += ";"

	if (aceFlags & CONTAINER_INHERIT_ACE) != 0 {
		aceString += "CI"
	}
	if (aceFlags & OBJECT_INHERIT_ACE) != 0 {
		aceString += "OI"
	}
	if (aceFlags & NO_PROPAGATE_INHERIT_ACE) != 0 {
		aceString += "NP"
	}
	if (aceFlags & INHERIT_ONLY_ACE) != 0 {
		aceString += "IO"
	}
	if (aceFlags & INHERITED_ACE) != 0 {
		aceString += "ID"
	}
	if (aceFlags & SUCCESSFUL_ACCESS_ACE_FLAG) != 0 {
		aceString += "SA"
	}
	if (aceFlags & FAILED_ACCESS_ACE_FLAG) != 0 {
		aceString += "FA"
	}
	if (aceType == SYSTEM_ACCESS_FILTER_ACE_TYPE) && (aceFlags&TRUST_PROTECTED_FILTER_ACE_FLAG) != 0 {
		aceString += "TP"
	}
	if (aceFlags & CRITICAL_ACE_FLAG) != 0 {
		aceString += "CR"
	}

	aceString += ";"
	aceString += aceRightsToString(aceRights)
	aceString += ";"

	// TODO: Empty object_guid;inherit_object_guid.
	aceString += ";"
	aceString += ";"

	sidoffset := 8
	sidStr, err := sidToString(aceSlice[sidoffset:])
	if err != nil {
		return "", fmt.Errorf("aceToString: sidToString failed: %v", err)
	}
	aceString += sidStr
	aceString += ")"

	return aceString, nil
}

// Given the entrire xattr value buffer, return the SD revision.
func getRevision(sd []byte) BYTE {
	if len(sd) < 1 {
		return 0
	}

	// SECURITY_DESCRIPTOR_RELATIVE.Revision.
	return BYTE(sd[0])
}

// Given the entrire xattr value buffer, return the owner sid string.
func getOwnerSidString(sd []byte) (string, error) {
	// Make sure we have enough bytes to safely read the required fields.
	if len(sd) < int(unsafe.Sizeof(SECURITY_DESCRIPTOR_RELATIVE{})) {
		return "", fmt.Errorf("Short Security Descriptor: %d bytes!", len(sd))
	}

	// Only valid revision is 1, verify that.
	revision := getRevision(sd)
	if revision != SID_REVISION {
		return "", fmt.Errorf("Invalid SID revision (%d), expected %d!", revision, SID_REVISION)
	}

	// SECURITY_DESCRIPTOR_RELATIVE.OffsetOwner.
	offsetOwner := binary.LittleEndian.Uint32(sd[4:8])
	if offsetOwner >= uint32(len(sd)) {
		return "", fmt.Errorf("offsetOwner (%d) points outside Security Descriptor of size %d bytes!",
			offsetOwner, len(sd))
	}

	sidStr, err := sidToString(sd[offsetOwner:])
	if err != nil {
		return "", err
	}
	return "O:" + sidStr, nil
}

// Given the entrire xattr value buffer, return the primary group sid string.
func getGroupSidString(sd []byte) (string, error) {
	// Make sure we have enough bytes to safely read the required fields.
	if len(sd) < int(unsafe.Sizeof(SECURITY_DESCRIPTOR_RELATIVE{})) {
		return "", fmt.Errorf("Short Security Descriptor: %d bytes!", len(sd))
	}

	// Only valid revision is 1, verify that.
	revision := getRevision(sd)
	if revision != 1 {
		return "", fmt.Errorf("Invalid SD revision (%d), expected 1!", revision)
	}

	// SECURITY_DESCRIPTOR_RELATIVE.OffsetGroup.
	offsetGroup := binary.LittleEndian.Uint32(sd[8:12])
	if offsetGroup >= uint32(len(sd)) {
		return "", fmt.Errorf("offsetGroup (%d) points outside Security Descriptor of size %d bytes!",
			offsetGroup, len(sd))
	}

	sidStr, err := sidToString(sd[offsetGroup:])
	if err != nil {
		return "", err
	}
	return "G:" + sidStr, nil
}

// Given the entrire xattr value buffer, return the DACL string.
func getDaclString(sd []byte) (string, error) {
	// Make sure we have enough bytes to safely read the required fields.
	if len(sd) < int(unsafe.Sizeof(SECURITY_DESCRIPTOR_RELATIVE{})) {
		return "", fmt.Errorf("Short Security Descriptor: %d bytes!", len(sd))
	}

	// Only valid revision is 1, verify that.
	revision := getRevision(sd)
	if revision != SDDL_REVISION {
		return "", fmt.Errorf("Invalid SD revision (%d), expected %d!", revision, SDDL_REVISION)
	}

	// SECURITY_DESCRIPTOR_RELATIVE.Control.
	control := binary.LittleEndian.Uint16(sd[2:4])

	// DACL not present?
	//
	// Note: I have observed that Windows always sets SE_DACL_PRESENT even if we save a binary SD with
	//       SE_DACL_PRESENT cleared, so we don't expect the following but we still have it for resilience.
	//       Since user has not specified SE_DACL_PRESENT, it means he doesn't want to set any ACLs, which means
	//       he wants to "allow all users", hence "D:NO_ACCESS_CONTROL" is most appropriate.
	//       If we just return "D:" it would mean user wants access control but has not specified any ACEs, which
	//       would instead mean "allow nobody".
	//
	if (control & SE_DACL_PRESENT) == 0 {
		fmt.Printf("[UNEXPECTED] SE_DACL_PRESENT bit not set, control word is 0x%x", control)
		return "D:NO_ACCESS_CONTROL", nil
	}

	daclString := "D:"

	dacl_flags := ""
	if (control & SE_DACL_PROTECTED) != 0 {
		dacl_flags += "P"
	}
	if (control & SE_DACL_AUTO_INHERIT_REQ) != 0 {
		dacl_flags += "AR"
	}
	if (control & SE_DACL_AUTO_INHERITED) != 0 {
		dacl_flags += "AI"
	}
	daclString += dacl_flags

	// SE_DACL_AUTO_INHERITED.OffsetDacl.
	dacloffset := binary.LittleEndian.Uint32(sd[16:20])

	if dacloffset == 0 {
		// dacloffset==0 means that user doesn't want any explicit ACL to be set, which means "allow all users".
		// This can be represented as "D:<flags>NO_ACCESS_CONTROL".
		daclString += "NO_ACCESS_CONTROL"

		return daclString, nil
	}

	if (dacloffset + 8) > uint32(len(sd)) {
		return "", fmt.Errorf("dacloffset (%d) points outside Security Descriptor of size %d bytes!",
			dacloffset+8, len(sd))
	}

	// ACL.AclRevision.
	aclRevision := sd[dacloffset]

	//
	// Though we support only ACCESS_ALLOWED_ACE_TYPE and ACCESS_DENIED_ACE_TYPE which as per docs should be
	// present with ACL revision 2, but I've seen some objects with these ACE types but acl revision 4.
	// Instead of failing here, we let it proceed. Later isUnsupportedAceType() will catch unsupported ACE types.
	//
	// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/20233ed8-a6c6-4097-aafa-dd545ed24428
	//
	if aclRevision != ACL_REVISION && aclRevision != ACL_REVISION_DS {
		// More importantly we don't support Object ACEs (ACL_REVISION_DS).
		return "", fmt.Errorf("Invalid ACL Revision (%d), valid values are 2 and 4.", aclRevision)
	}

	// ACL.AceCount.
	numAces := binary.LittleEndian.Uint32(sd[dacloffset+4 : dacloffset+8])

	// Offset of the first ACE.
	offset := dacloffset + 8

	// Go over all the ACEs and stringify them.
	// If numAces is 0 it'll result in daclString to have only flags and no ACEs.
	// Such an ACL will mean "allow nobody".
	for i := 0; i < int(numAces); i++ {
		if (offset + 4) > uint32(len(sd)) {
			return "", fmt.Errorf("Short ACE (offset=%d), Security Descriptor size=%d bytes!",
				offset, len(sd))
		}

		// ACCESS_ALLOWED_ACE.Header.AceSize.
		ace_size := uint32(binary.LittleEndian.Uint16(sd[offset+2 : offset+4]))

		if (offset + ace_size) > uint32(len(sd)) {
			return "", fmt.Errorf("ACE (offset=%d, ace_size=%d) lies outside Security Descriptor of size %d bytes!", offset, ace_size, len(sd))
		}

		aceStr, err := aceToString(sd[offset : offset+ace_size])
		if err != nil {
			return "", err
		}
		daclString += aceStr
		offset += ace_size
	}

	return daclString, nil
}

// getBinarySdSizeFromSDDLString returns the estimated number of bytes enough for binary representation of the
// given SDDLString in the SECURITY_DESCRIPTOR_RELATIVE form.
func getBinarySdSizeFromSDDLString(parsedSDDL SDDLString) uint32 {
	// Maximum possible binary SID size.
	maxSidBytes := uint32(unsafe.Sizeof(SID{}) + (unsafe.Sizeof(uint32(0)) * SID_MAX_SUB_AUTHORITIES))

	sdSize := uint32(unsafe.Sizeof(SECURITY_DESCRIPTOR_RELATIVE{}))

	if parsedSDDL.OwnerSID != "" {
		sdSize += maxSidBytes
	}

	if parsedSDDL.GroupSID != "" {
		sdSize += maxSidBytes
	}

	if parsedSDDL.DACL.Flags != "" || len(parsedSDDL.DACL.ACLEntries) != 0 {
		sdSize += uint32(unsafe.Sizeof(ACL{}))
		sdSize += (uint32(unsafe.Sizeof(ACCESS_ALLOWED_ACE{})) + maxSidBytes) * uint32(len(parsedSDDL.DACL.ACLEntries))
	}

	if parsedSDDL.SACL.Flags != "" || len(parsedSDDL.SACL.ACLEntries) != 0 {
		sdSize += uint32(unsafe.Sizeof(ACL{}))
		sdSize += (uint32(unsafe.Sizeof(ACCESS_ALLOWED_ACE{})) + maxSidBytes) * uint32(len(parsedSDDL.SACL.ACLEntries))
	}

	return sdSize
}

/****************************************************************************
 **                               Exported APIs                            **
 ****************************************************************************/

// GetControl returns the security descriptor control bits.
func GetControl(sd []byte) (SECURITY_DESCRIPTOR_CONTROL, error) {
	if len(sd) < 4 {
		return 0, fmt.Errorf("SECURITY_DESCRIPTOR too small (%d bytes)", len(sd))
	}
	return SECURITY_DESCRIPTOR_CONTROL(binary.LittleEndian.Uint16(sd[2:4])), nil
}

// SetControl sets the requested control bits in the given security descriptor.
func SetControl(sd []byte, controlBitsOfInterest, controlBitsToSet SECURITY_DESCRIPTOR_CONTROL) error {
	// GetControl() also does min length check for sd.
	control, err := GetControl(sd)
	if err != nil {
		return err
	}

	control = (control & ^controlBitsOfInterest) | controlBitsToSet
	binary.LittleEndian.PutUint16(sd[2:4], uint16(control))

	return nil
}

// Convert a possibly non-numeric SID to numeric SID.
func CanonicalizeSid(sidString string) (string, error) {
	// Convert to binary SID and back to canonicalize it.
	sidSlice, err := stringToSid(sidString)
	if err != nil {
		return "", err
	}

	canonicalSid, err := sidToString(sidSlice)
	if err != nil {
		return "", err
	}

	return canonicalSid, nil
}

// SecurityDescriptorToString returns an SDDL format string corresponding to the passed in binary Security Descriptor
// in SECURITY_DESCRIPTOR_RELATIVE format.
func SecurityDescriptorToString(sd []byte) (string, error) {
	// We support only DACL/Owner/Group.
	// TODO: Add support for SACL.
	const flags SECURITY_INFORMATION = (DACL_SECURITY_INFORMATION | OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION)

	// Ensure Security Descriptor is valid so that rest of the code can safely access various fields.
	if err := sdRelativeIsValid(sd, flags); err != nil {
		return "", fmt.Errorf("SecurityDescriptorToString: %v", err)
	}

	ownerSidString, err := getOwnerSidString(sd)
	if err != nil {
		return "", fmt.Errorf("SecurityDescriptorToString: getOwnerSidString failed: %v", err)
	}

	groupSidString, err := getGroupSidString(sd)
	if err != nil {
		return "", fmt.Errorf("SecurityDescriptorToString: getGroupSidString failed: %v", err)
	}

	daclString, err := getDaclString(sd)
	if err != nil {
		return "", fmt.Errorf("SecurityDescriptorToString: getDaclString failed: %v", err)
	}

	sddlString := ownerSidString + groupSidString + daclString

	return sddlString, nil
}

// SecurityDescriptorFromString converts a SDDL formatted string into a binary Security Descriptor in
// SECURITY_DESCRIPTOR_RELATIVE format.
func SecurityDescriptorFromString(sddlString string) ([]byte, error) {

	// Since NO_ACCESS_CONTROL friendly flag does not have a corresponding binary flag, we return it separately
	// as a boolean. Caller can then act appropriately.
	aclFlagsToControlBitmap := func(aclFlags string, forSacl bool) (SECURITY_DESCRIPTOR_CONTROL, bool, error) {
		var control SECURITY_DESCRIPTOR_CONTROL = 0
		var no_access_control bool = false

		for i := 0; i < len(aclFlags); {
			if aclFlags[i] == 'P' {
				if forSacl {
					control |= SE_SACL_PROTECTED
				} else {
					control |= SE_DACL_PROTECTED
				}
				i++
			} else if aclFlags[i] == 'A' {
				if i == len(aclFlags) {
					return 0, false, fmt.Errorf("Incomplete ACL Flags, ends at 'A': %s", aclFlags)
				}
				i++
				if aclFlags[i] == 'R' { // AR.
					if forSacl {
						control |= SE_SACL_AUTO_INHERIT_REQ
					} else {
						control |= SE_DACL_AUTO_INHERIT_REQ
					}
					i++
				} else if aclFlags[i] == 'I' { // AI.
					if forSacl {
						control |= SE_SACL_AUTO_INHERITED
					} else {
						control |= SE_DACL_AUTO_INHERITED
					}
					i++
				} else {
					return 0, false, fmt.Errorf("Encountered unsupported ACL Flag '%s' after 'A'",
						string(aclFlags[i]))
				}
			} else if aclFlags[i] == 'N' {
				nacLen := len("NO_ACCESS_CONTROL")
				if i+nacLen > len(aclFlags) {
					return 0, false, fmt.Errorf("Incomplete NO_ACCESS_CONTROL Flag: %s", aclFlags)
				}
				if aclFlags[i:i+nacLen] == "NO_ACCESS_CONTROL" {
					// NO_ACCESS_CONTROL seen.
					no_access_control = true
				}
				i += nacLen
			} else {
				return 0, false, fmt.Errorf("Encountered unsupported ACL Flag '%s'", string(aclFlags[i]))
			}
		}

		return control, no_access_control, nil
	}

	aceFlagsToByte := func(aceFlags string) (byte, error) {
		var flags byte = 0

		for i := 0; i < len(aceFlags); {
			// Must have even number of characters.
			if i+1 == len(aceFlags) {
				return byte(0), fmt.Errorf("Invalid aceFlags: %s", aceFlags)
			}

			flag := aceFlags[i : i+2]

			if flag == "CI" {
				flags |= CONTAINER_INHERIT_ACE
			} else if flag == "OI" {
				flags |= OBJECT_INHERIT_ACE
			} else if flag == "NP" {
				flags |= NO_PROPAGATE_INHERIT_ACE
			} else if flag == "IO" {
				flags |= INHERIT_ONLY_ACE
			} else if flag == "ID" {
				flags |= INHERITED_ACE
			} else if flag == "SA" {
				flags |= SUCCESSFUL_ACCESS_ACE_FLAG
			} else if flag == "FA" {
				flags |= FAILED_ACCESS_ACE_FLAG
			} else if flag == "TP" {
				flags |= TRUST_PROTECTED_FILTER_ACE_FLAG
			} else if flag == "CR" {
				flags |= CRITICAL_ACE_FLAG
			} else {
				return byte(0), fmt.Errorf("Unsupported aceFlags: %s", aceFlags)
			}

			i += 2
		}

		return flags, nil
	}

	aceRightsToAccessMask := func(aceRights string) (uint32, error) {
		var accessMask uint32 = 0

		// Hex right string will start with 0x or 0X.
		if len(aceRights) > 2 && (aceRights[0:2] == "0x" || aceRights[0:2] == "0X") {
			accessMask, err := strconv.ParseUint(aceRights[2:], 16, 32)
			if err != nil {
				return 0, fmt.Errorf("Failed to parse integral aceRights %s: %v", aceRights, err)
			}
			return uint32(accessMask), nil
		}

		for i := 0; i < len(aceRights); {
			// Must have even number of characters.
			if i+1 == len(aceRights) {
				return 0, fmt.Errorf("Invalid aceRights: %s", aceRights)
			}

			right := aceRights[i : i+2]

			if mask, ok := aceStringToRightsMap[right]; ok {
				accessMask |= mask
			} else {
				return 0, fmt.Errorf("Unknown aceRight(%s): %s", right, aceRights)
			}

			i += 2
		}

		return accessMask, nil
	}

	aclEntryToSlice := func(aclEntry ACLEntry) ([]byte, error) {
		// ace_type;ace_flags;rights;object_guid;inherit_object_guid;account_sid;(resource_attribute)
		if len(aclEntry.Sections) != 6 {
			return nil, fmt.Errorf("aclEntry has %d sections (expected 6)", len(aclEntry.Sections))
		}
		// Maximum possible binary SID size.
		maxSidBytes := int(unsafe.Sizeof(SID{}) + (unsafe.Sizeof(uint32(0)) * SID_MAX_SUB_AUTHORITIES))

		sliceSize := int(unsafe.Sizeof(ACCESS_ALLOWED_ACE{})) + maxSidBytes
		ace := make([]byte, sliceSize)

		// Base aceSize. We will add SID size to it to get complete ACE size.
		var aceSize uint16 = 8

		// ACCESS_ALLOWED_ACE.Header.AceType.
		if aceType, ok := aceTypeStringMap[aclEntry.Sections[0]]; ok {
			ace[0] = byte(aceType)
		} else {
			return nil, fmt.Errorf("Unknown aceType: %s", aclEntry.Sections[0])
		}

		// ACCESS_ALLOWED_ACE.Header.AceFlags.
		flags, err := aceFlagsToByte(aclEntry.Sections[1])
		if err != nil {
			return nil, fmt.Errorf("Unknown aceFlag %s: %v", aclEntry.Sections[1], err)
		}
		ace[1] = flags

		// ACCESS_ALLOWED_ACE.AccessMask.
		accessMask, err := aceRightsToAccessMask(aclEntry.Sections[2])
		if err != nil {
			return nil, fmt.Errorf("Unknown aceRights %s: %v", aclEntry.Sections[2], err)
		}
		binary.LittleEndian.PutUint32(ace[4:8], accessMask)

		// TODO: Support object ACEs?
		if aclEntry.Sections[3] != "" {
			return nil, fmt.Errorf("object_guid not supported: %s", aclEntry.Sections[3])
		}

		if aclEntry.Sections[4] != "" {
			return nil, fmt.Errorf("inherit_object_guid not supported: %s", aclEntry.Sections[5])
		}

		if aclEntry.Sections[5] != "" {
			sidSlice, err := stringToSid(aclEntry.Sections[5])
			if err != nil {
				return nil, fmt.Errorf("Bad SID (%s): %v", aclEntry.Sections[5], err)
			}
			copy(ace[8:8+len(sidSlice)], sidSlice)
			aceSize += uint16(len(sidSlice))
		}

		// ACCESS_ALLOWED_ACE.Header.AceSize.
		binary.LittleEndian.PutUint16(ace[2:4], aceSize)

		return ace[:aceSize], nil
	}

	// Use sddl.ParseSDDL() instead of reinventing SDDL parsing.
	parsedSDDL, err := ParseSDDL(sddlString)
	if err != nil {
		return nil, fmt.Errorf("ParseSDDL(%s) failed: %v", sddlString, err)
	}

	// Allocate a byte slice large enough to contain the binary Security Descriptor in SECURITY_DESCRIPTOR_RELATIVE
	// format.
	sdSize := getBinarySdSizeFromSDDLString(parsedSDDL)
	sd := make([]byte, sdSize)

	// Returned Security Descriptor is in Self Relative format.
	//
	// Note: We always set SE_DACL_PRESENT as we have observed that Windows always sets that.
	//       It then uses offsetDacl to control whether ACLs are checked or not.
	//       offsetDacl==0 would mean that there are no ACLs and hence the file will have the "allow all users"
	//       permission.
	//       offsetDacl!=0 would cause the ACEs to be inspected from offsetDacl and if there are no ACEs present it
	//       would mean "allow nobody".
	control := SECURITY_DESCRIPTOR_CONTROL(SE_SELF_RELATIVE | SE_DACL_PRESENT)
	offsetOwner := 0
	offsetGroup := 0
	offsetDacl := 0
	offsetSacl := 0

	// sd.Revision.
	sd[0] = SDDL_REVISION
	// sd.Sbz1.
	sd[1] = 0

	// OwnerSID follows immediately after SECURITY_DESCRIPTOR_RELATIVE header.
	offset := 20
	if parsedSDDL.OwnerSID != "" {
		offsetOwner = offset
		sidSlice, err := stringToSid(parsedSDDL.OwnerSID)
		if err != nil {
			return nil, err
		}
		copy(sd[offset:offset+len(sidSlice)], sidSlice)
		offset += len(sidSlice)
	}

	if parsedSDDL.GroupSID != "" {
		offsetGroup = offset
		sidSlice, err := stringToSid(parsedSDDL.GroupSID)
		if err != nil {
			return nil, err
		}
		copy(sd[offset:offset+len(sidSlice)], sidSlice)
		offset += len(sidSlice)
	}

	// TODO: Add and audit SACL support.
	if parsedSDDL.SACL.Flags != "" || len(parsedSDDL.SACL.ACLEntries) != 0 {
		flags, no_access_control, err := aclFlagsToControlBitmap(parsedSDDL.SACL.Flags, true /* forSacl */)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse SACL Flags %s: %v", parsedSDDL.SACL.Flags, err)
		}
		control |= flags

		// If NO_ACCESS_CONTROL flag is set we will skip the following, which will result in offsetSacl to be set as 0
		// in the binary SD, which would mean "No ACLs" aka "allow all users".
		if !no_access_control {
			offsetSacl = offset

			// ACL.AclRevision.
			sd[offsetSacl] = ACL_REVISION
			// ACL.Sbz1.
			sd[offsetSacl+1] = 0

			// Base aclSize. We will add ACE sizes to it to get complete ACL size.
			var aclSize uint16 = 8

			// ACL.AceCount.
			binary.LittleEndian.PutUint16(sd[offsetSacl+4:offsetSacl+6], uint16(len(parsedSDDL.SACL.ACLEntries)))
			// ACL.Sbz2.
			binary.LittleEndian.PutUint16(sd[offsetSacl+6:offsetSacl+8], 0)

			offset += 8 // struct ACL.
			for i := 0; i < len(parsedSDDL.SACL.ACLEntries); i++ {
				aceSlice, err := aclEntryToSlice(parsedSDDL.SACL.ACLEntries[i])
				if err != nil {
					return nil, err
				}
				copy(sd[offset:offset+len(aceSlice)], aceSlice)
				offset += len(aceSlice)
				aclSize += uint16(len(aceSlice))
			}

			// ACL.AclSize.
			binary.LittleEndian.PutUint16(sd[offsetSacl+2:offsetSacl+4], aclSize)

			// Put in the end to prevent "unreachable code" complaints from vet.
			panic("SACLs not supported!")
		} else {
			// If NO_ACCESS_CONTROL flag is set, there shouldn't be any ACEs.
			// TODO: Is it safer to skip/ignore the ACEs?
			if len(parsedSDDL.SACL.ACLEntries) != 0 {
				return nil, fmt.Errorf("%d ACEs present along with NO_ACCESS_CONTROL SACL flag (%s): %v",
					len(parsedSDDL.SACL.ACLEntries), parsedSDDL.SACL.Flags, err)
			}
		}
	}

	if parsedSDDL.DACL.Flags != "" || len(parsedSDDL.DACL.ACLEntries) != 0 {
		flags, no_access_control, err := aclFlagsToControlBitmap(parsedSDDL.DACL.Flags, false /* forSacl */)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse DACL Flags %s: %v", parsedSDDL.DACL.Flags, err)
		}
		control |= flags

		// If NO_ACCESS_CONTROL flag is set we will skip the following, which will result in offsetDacl to be set as 0
		// in the binary SD, which would mean "No ACLs" aka "allow all users".
		if !no_access_control {
			offsetDacl = offset

			// ACL.AclRevision.
			sd[offsetDacl] = ACL_REVISION
			// ACL.Sbz1.
			sd[offsetDacl+1] = 0

			// Base aclSize. We will add ACE sizes to it to get complete ACL size.
			var aclSize uint16 = 8

			// ACL.AceCount.
			binary.LittleEndian.PutUint16(sd[offsetDacl+4:offsetDacl+6], uint16(len(parsedSDDL.DACL.ACLEntries)))
			// ACL.Sbz2.
			binary.LittleEndian.PutUint16(sd[offsetDacl+6:offsetDacl+8], 0)

			offset += 8 // struct ACL.
			for i := 0; i < len(parsedSDDL.DACL.ACLEntries); i++ {
				aceSlice, err := aclEntryToSlice(parsedSDDL.DACL.ACLEntries[i])
				if err != nil {
					return nil, err
				}
				copy(sd[offset:offset+len(aceSlice)], aceSlice)
				offset += len(aceSlice)
				aclSize += uint16(len(aceSlice))
			}

			// ACL.AclSize.
			binary.LittleEndian.PutUint16(sd[offsetDacl+2:offsetDacl+4], aclSize)
		} else {
			// If NO_ACCESS_CONTROL flag is set, there shouldn't be any ACEs.
			// TODO: Is it safer to skip/ignore the ACEs?
			if len(parsedSDDL.DACL.ACLEntries) != 0 {
				return nil, fmt.Errorf("%d ACEs present along with NO_ACCESS_CONTROL DACL flag (%s): %v",
					len(parsedSDDL.DACL.ACLEntries), parsedSDDL.DACL.Flags, err)
			}
		}
	}

	// sd.Control.
	binary.LittleEndian.PutUint16(sd[2:4], uint16(control))
	// sd.OffsetOwner.
	binary.LittleEndian.PutUint32(sd[4:8], uint32(offsetOwner))
	// sd.OffsetGroup.
	binary.LittleEndian.PutUint32(sd[8:12], uint32(offsetGroup))
	// sd.OffsetSacl.
	binary.LittleEndian.PutUint32(sd[12:16], uint32(offsetSacl))
	// sd.OffsetDacl.
	binary.LittleEndian.PutUint32(sd[16:20], uint32(offsetDacl))

	return sd[:offset], nil
}

// SetSecurityObject is the equivalent of ntdll.NtSetSecurityObject method.
// It sets the given SECURITY_DESCRIPTOR for the given file.
// flags instructs what all needs to be set.
// sd should be a valid binary SECURITY_DESCRIPTOR_RELATIVE structure as a byte slice.
func SetSecurityObject(path string, flags SECURITY_INFORMATION, sd []byte) error {
	var xattrKey string

	if len(sd) < int(unsafe.Sizeof(SECURITY_DESCRIPTOR_RELATIVE{})) {
		panic(fmt.Errorf("SetSecurityObject: sd too small (%d bytes)", len(sd)))
	}

	// Pick the right xattr key that allows us to pass the needed information to the cifs client.
	if flags == DACL_SECURITY_INFORMATION {
		// Only DACL.
		xattrKey = common.CIFS_XATTR_CIFS_ACL

		// sd.OffsetOwner = 0.
		binary.LittleEndian.PutUint32(sd[4:8], 0)
		// sd.OffsetGroup = 0.
		binary.LittleEndian.PutUint32(sd[8:12], 0)
		// sd.OffsetSacl = 0.
		binary.LittleEndian.PutUint32(sd[12:16], 0)
	} else if flags == (DACL_SECURITY_INFORMATION | OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION) {
		// DACL + Owner + Group.
		xattrKey = common.CIFS_XATTR_CIFS_NTSD

		// sd.OffsetSacl = 0.
		binary.LittleEndian.PutUint32(sd[12:16], 0)
	} else if flags == (DACL_SECURITY_INFORMATION | SACL_SECURITY_INFORMATION |
		OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION) {
		// DACL + SACL + Owner + Group.
		xattrKey = common.CIFS_XATTR_CIFS_NTSD_FULL

		// Put in the end to prevent "unreachable code" complaints from vet.
		// TODO: Add support for "DACL + SACL + Owner + Group".
		//       Remove this panic only after rest of the code correctly supports SACL.
		panic(fmt.Errorf("SetSecurityObject: Unsupported flags value 0x%x", flags))

	} else {
		panic(fmt.Errorf("SetSecurityObject: Unsupported flags value 0x%x", flags))
	}

	// Ensure Security Descriptor is valid before writing to the cifs client.
	if err := sdRelativeIsValid(sd, flags); err != nil {
		panic(fmt.Errorf("SetSecurityObject: %v", err))
	}

	err := xattr.Set(path, xattrKey, sd)
	if err != nil {
		return fmt.Errorf("SetSecurityObject: xattr.Set(%s) failed for file %s: %v", xattrKey, path, err)
	}

	return nil
}

// QuerySecurityObject is the equivalent of ntdll.NtQuerySecurityObject method.
// It fetches the binary SECURITY_DESCRIPTOR for the given file.
// 'flags' instructs what parts of the Security Descriptor needs to be queried.
// Returns a valid binary SECURITY_DESCRIPTOR_RELATIVE structure as a byte slice.
func QuerySecurityObject(path string, flags SECURITY_INFORMATION) ([]byte, error) {
	var xattrKey string

	// Pick the right xattr key that allows us to pass the needed information to the cifs client.
	if flags == DACL_SECURITY_INFORMATION {
		// Only DACL.
		xattrKey = common.CIFS_XATTR_CIFS_ACL
	} else if flags == (DACL_SECURITY_INFORMATION | OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION) {
		// DACL + Owner + Group.
		xattrKey = common.CIFS_XATTR_CIFS_NTSD
	} else if flags == (DACL_SECURITY_INFORMATION | SACL_SECURITY_INFORMATION |
		OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION) {
		// DACL + SACL + Owner + Group.
		xattrKey = common.CIFS_XATTR_CIFS_NTSD_FULL

		// Put in the end to prevent "unreachable code" complaints from vet.
		// TODO: Add support for "DACL + SACL + Owner + Group".
		//       Remove this panic only after rest of the code correctly supports SACL.
		panic(fmt.Errorf("QuerySecurityObject: Unsupported flags value 0x%x", flags))
	} else {
		panic(fmt.Errorf("QuerySecurityObject: Unsupported flags value 0x%x", flags))
	}

	sd, err := xattr.Get(path, xattrKey)
	if err != nil {
		return nil, fmt.Errorf("QuerySecurityObject: xattr.Get(%s, %s) failed: %v", path, xattrKey, err)
	}

	// Ensure Security Descriptor returned by the cifs client is fine.
	if err := sdRelativeIsValid(sd, flags); err != nil {
		// panic because we expect cifs client to return a valid Security Descriptor.
		panic(fmt.Errorf("QuerySecurityObject: %v", err))
	}

	return sd, nil
}
