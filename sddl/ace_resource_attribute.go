package sddl

import (
	"fmt"
)

type ACEResourceAttribute interface {
	StringifyResourceAttribute() string
}

// ParseResourceAttribute requires an underlying ACE Type (RA, XA, XD) to determine how the resource attribute will be parsed.
// SDDL_RESOURCE_ATTRIBUTE, SDDL_CALLBACK_ACCESS_ALLOWED, and SDDL_CALLBACK_ACCESS_DENIED all work here.
// Any other ACE type will result in an error.
func ParseResourceAttribute(input string, aceType ACEType) (ACEResourceAttribute, error) {
	switch aceType {
	case EACEType.SDDL_RESOURCE_ATTRIBUTE():
		return ParseACEResourceAttributeValues(input)
	case EACEType.SDDL_CALLBACK_ACCESS_ALLOWED(), EACEType.SDDL_CALLBACK_ACCESS_DENIED():
		return ParseConditionalACEResourceAttribute(input)
	default:
		return nil, fmt.Errorf("%s is an invalid ACE resource type to parse resource attributes for", aceType.String())
	}
}
